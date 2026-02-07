use crate::db_queue::DbSender;
use crate::health::SharedHealth;
use crate::models::Event;
use crate::pair_aggregator::PairMessage;
use crate::shard::{ShardConfig, ShardMessage, ShardMetrics, ShardWorker};
use crate::token_registry::RegistryState;
use crate::time_utils::now_ts_ms;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::warn;

pub const DISPATCH_QUEUE_CAPACITY: usize = 2048;
const SHARD_QUEUE_CAPACITY: usize = 1024;
const DISPATCH_EVENT_LAG_WARN_MS: i64 = 2_000;
static REST_DROP_COUNTER: AtomicUsize = AtomicUsize::new(0);
static REST_SEEN_COUNTER: AtomicUsize = AtomicUsize::new(0);
static DISPATCH_LAG_COUNTER: AtomicUsize = AtomicUsize::new(0);
static SHARD_SEND_FAIL_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub struct DispatcherHandle {
    pub sender: mpsc::Sender<DispatcherMessage>,
}

pub enum DispatcherMessage {
    Event(Event),
    Tick(i64),
}

pub async fn spawn_dispatcher(
    shard_count: usize,
    registry_state: std::sync::Arc<tokio::sync::RwLock<RegistryState>>,
    db_sender: DbSender,
    pair_senders: Arc<Vec<mpsc::Sender<PairMessage>>>,
    health: SharedHealth,
    shard_metrics: Arc<ShardMetrics>,
    shard_config: ShardConfig,
) -> DispatcherHandle {
    let mut shard_senders = Vec::with_capacity(shard_count);
    for shard_id in 0..shard_count {
        let (tx, rx) = mpsc::channel::<ShardMessage>(SHARD_QUEUE_CAPACITY);
        let worker = ShardWorker::new(
            shard_id,
            rx,
            db_sender.clone(),
            pair_senders.clone(),
            registry_state.clone(),
            health.clone(),
            shard_metrics.clone(),
            shard_config,
        );
        tokio::spawn(worker.run());
        shard_senders.push(tx);
    }

    let (dispatch_tx, mut dispatch_rx) =
        mpsc::channel::<DispatcherMessage>(DISPATCH_QUEUE_CAPACITY);

    let health_for_dispatch = health.clone();
    tokio::spawn(async move {
        while let Some(msg) = dispatch_rx.recv().await {
            match msg {
                DispatcherMessage::Event(event) => {
                    let token_key = event.token_key.clone();
                    let event_kind = event.kind;
                    let event_venue = event.venue;
                    let is_rest_snapshot = event.exchange_ts_source
                        == crate::models::ExchangeTsSource::VenueRest
                        && event.kind == crate::models::EventKind::BookSnapshot;
                    let shard = pick_shard(&token_key, shard_senders.len());
                    let lag_ms = now_ts_ms().saturating_sub(event.local_ts_ms);
                    if lag_ms > DISPATCH_EVENT_LAG_WARN_MS {
                        let count = DISPATCH_LAG_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
                        if count == 1 || count % 100 == 0 {
                            let shard_remaining = shard_senders
                                .get(shard)
                                .map(|sender| sender.capacity())
                                .unwrap_or(0);
                            let shard_used =
                                SHARD_QUEUE_CAPACITY.saturating_sub(shard_remaining);
                            warn!(
                                "dispatcher_event_lag count={} lag_ms={} venue={:?} kind={:?} token_key={} exchange_ts_source={:?} shard_queue_used={} shard_queue_capacity={}",
                                count,
                                lag_ms,
                                event_venue,
                                event_kind,
                                token_key,
                                event.exchange_ts_source,
                                shard_used,
                                SHARD_QUEUE_CAPACITY
                            );
                        }
                    }
                    if is_rest_snapshot {
                        let seen = REST_SEEN_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
                        if seen == 1 || seen % 50 == 0 {
                            warn!(
                                "rest_snapshot_received shard_id={} seen={} token_key={}",
                                shard, seen, token_key
                            );
                        }
                    }
                    if let Some(sender) = shard_senders.get(shard) {
                        let venue = event_venue;
                        if sender.try_send(ShardMessage::Event(event)).is_err() {
                            let mut state = health_for_dispatch.lock().await;
                            state.record_drop(venue);
                            let dropped = SHARD_SEND_FAIL_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
                            if dropped == 1 || dropped % 100 == 0 {
                                warn!(
                                    "dispatcher_shard_queue_full shard_id={} dropped={} venue={:?} kind={:?} token_key={}",
                                    shard,
                                    dropped,
                                    venue,
                                    event_kind,
                                    token_key
                                );
                            }
                            if is_rest_snapshot {
                                let dropped = REST_DROP_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
                                if dropped == 1 || dropped % 100 == 0 {
                                    warn!(
                                        "rest_snapshot_drop shard_id={} dropped={} token_key={}",
                                        shard, dropped, token_key
                                    );
                                }
                            }
                        }
                    }
                }
                DispatcherMessage::Tick(bar_second) => {
                    for sender in &shard_senders {
                        let _ = sender.send(ShardMessage::Tick(bar_second)).await;
                    }
                }
            }
        }
    });

    DispatcherHandle {
        sender: dispatch_tx,
    }
}

fn pick_shard(token_key: &str, shard_count: usize) -> usize {
    let hash = fnv1a_64(token_key.as_bytes());
    (hash as usize) % shard_count.max(1)
}

fn fnv1a_64(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
