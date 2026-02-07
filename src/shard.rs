use crate::db_queue::{try_send_db, DbMessage, DbSender};
use crate::health::{SharedHealth, WsState};
use crate::mem_stats;
use crate::models::{
    Bars1sToken, BookLevel, BookSide, DeltaType, Event, EventKind, ExchangeTsSource,
    ShockEventUpdate, ShockStatus, TokenSide, Venue,
};
use crate::pair_aggregator::{pick_pair_shard, PairInput, PairMessage};
use crate::shock::{ShockEngine, ShockSignal, WallPullSignal};
use crate::time_utils::now_ts_ms;
use crate::token_registry::RegistryState;
use hdrhistogram::Histogram;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::RwLock;
use tracing::info;

const PRICE_SCALE: f64 = 10_000.0;
const PRICE_MAX_IDX: usize = 10_000;
const PRICE_LEVELS: usize = PRICE_MAX_IDX + 1;
const SIZE_SCALE: f64 = 1_000_000.0;
const OFI_FAST_WINDOW_MS: i64 = 250;
const HEALTH_WS_CONNECTED: i64 = 1 << 0;
const HEALTH_HEARTBEAT_OK: i64 = 1 << 1;
const HEALTH_RESYNC_RECENT: i64 = 1 << 2;
const HEALTH_EVENT_DROP: i64 = 1 << 3;
const OPI_RESYNC_DEPTH_RATIO: f64 = 0.05;
const STATE_LOG_INTERVAL: usize = 100;
const REST_SNAPSHOT_LOG_INTERVAL: usize = 50;
const EVENT_LOG_INTERVAL: usize = 500;
const EVENT_METRICS_INTERVAL: usize = 100;
const REST_METRICS_INTERVAL: usize = 1;
const TICK_LOG_INTERVAL: usize = 60;
const METRICS_LOG_INTERVAL: usize = 6;
const PARTIAL_SNAPSHOT_SAMPLE_RATE: usize = 200;
const MID_BBO_SAMPLE_RATE: usize = 200;
const LATE_EVENT_BUFFER_SECS: i64 = 2;
const SHARD_EVENT_LAG_WARN_MS: i64 = 2_000;
const SHARD_HANDLE_WARN_MS: u128 = 200;
static SHOCK_COUNTER: AtomicI64 = AtomicI64::new(1);
static FIRST_WS_BOOK_APPLIED: AtomicBool = AtomicBool::new(false);
static PARTIAL_SNAPSHOT_SAMPLE: AtomicUsize = AtomicUsize::new(0);
static MID_BBO_SAMPLE: AtomicUsize = AtomicUsize::new(0);
static SHARD_EVENT_LAG_COUNTER: AtomicUsize = AtomicUsize::new(0);
static PAIR_SEND_DROP_COUNTER: AtomicUsize = AtomicUsize::new(0);
static PAIR_SEND_CLOSED_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn init_shock_counter(next_id: i64) {
    let start = next_id.max(1);
    SHOCK_COUNTER.store(start, Ordering::Relaxed);
}

fn try_send_pair_message(
    sender: &mpsc::Sender<PairMessage>,
    msg: PairMessage,
    shard_id: usize,
    pair_id: i64,
    token_key: &str,
    kind: &'static str,
) -> bool {
    match sender.try_send(msg) {
        Ok(()) => true,
        Err(TrySendError::Full(_)) => {
            let dropped = PAIR_SEND_DROP_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped == 1 || dropped % 100 == 0 {
                info!(
                    "pair_queue_full drop_count={} shard_id={} kind={} pair_id={} token_key={}",
                    dropped, shard_id, kind, pair_id, token_key
                );
            }
            false
        }
        Err(TrySendError::Closed(_)) => {
            let closed = PAIR_SEND_CLOSED_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
            if closed == 1 || closed % 100 == 0 {
                info!(
                    "pair_queue_closed drop_count={} shard_id={} kind={} pair_id={} token_key={}",
                    closed, shard_id, kind, pair_id, token_key
                );
            }
            false
        }
    }
}

pub struct ShardMetrics {
    slots: Vec<ShardMetricSlot>,
    bootstrap_applied: AtomicUsize,
}

struct ShardMetricSlot {
    token_states: AtomicUsize,
    orderbook_levels: AtomicUsize,
    pending_shocks: AtomicUsize,
    book_count_pm: AtomicUsize,
    book_count_opi: AtomicUsize,
    max_nonzero_levels: AtomicUsize,
    max_nonzero_levels_token_key: Mutex<String>,
}

pub struct MetricsSnapshot {
    pub token_states: usize,
    pub orderbook_levels_total: usize,
    pub pending_shocks: usize,
    pub book_count_pm: usize,
    pub book_count_opi: usize,
    pub max_nonzero_levels: usize,
    pub max_nonzero_levels_token_key: String,
    pub bootstrap_progress: usize,
}

#[derive(Clone, Copy, Debug)]
pub struct ShardConfig {
    pub pm_staleness_ms: i64,
    pub other_staleness_ms: i64,
    pub hard_clear_ms: i64,
    pub alpha_capture_enabled: bool,
}

impl ShardMetrics {
    pub fn new(shard_count: usize) -> Self {
        let mut slots = Vec::with_capacity(shard_count);
        for _ in 0..shard_count {
            slots.push(ShardMetricSlot::new());
        }
        ShardMetrics {
            slots,
            bootstrap_applied: AtomicUsize::new(0),
        }
    }

    fn slot(&self, shard_id: usize) -> &ShardMetricSlot {
        &self.slots[shard_id]
    }

    pub fn totals(&self) -> MetricsSnapshot {
        let mut token_states = 0usize;
        let mut orderbook_levels = 0usize;
        let mut pending_shocks = 0usize;
        let mut book_count_pm = 0usize;
        let mut book_count_opi = 0usize;
        let mut max_nonzero_levels = 0usize;
        let mut max_nonzero_levels_token_key = String::new();
        for slot in &self.slots {
            token_states += slot.token_states.load(Ordering::Relaxed);
            orderbook_levels += slot.orderbook_levels.load(Ordering::Relaxed);
            pending_shocks += slot.pending_shocks.load(Ordering::Relaxed);
            book_count_pm += slot.book_count_pm.load(Ordering::Relaxed);
            book_count_opi += slot.book_count_opi.load(Ordering::Relaxed);
            let slot_max = slot.max_nonzero_levels.load(Ordering::Relaxed);
            if slot_max > max_nonzero_levels {
                max_nonzero_levels = slot_max;
                if let Ok(key) = slot.max_nonzero_levels_token_key.lock() {
                    max_nonzero_levels_token_key = key.clone();
                }
            }
        }
        MetricsSnapshot {
            token_states,
            orderbook_levels_total: orderbook_levels,
            pending_shocks,
            book_count_pm,
            book_count_opi,
            max_nonzero_levels,
            max_nonzero_levels_token_key,
            bootstrap_progress: self.bootstrap_applied.load(Ordering::Relaxed),
        }
    }

    pub fn record_bootstrap_applied(&self) {
        self.bootstrap_applied.fetch_add(1, Ordering::Relaxed);
    }
}

impl ShardMetricSlot {
    fn new() -> Self {
        ShardMetricSlot {
            token_states: AtomicUsize::new(0),
            orderbook_levels: AtomicUsize::new(0),
            pending_shocks: AtomicUsize::new(0),
            book_count_pm: AtomicUsize::new(0),
            book_count_opi: AtomicUsize::new(0),
            max_nonzero_levels: AtomicUsize::new(0),
            max_nonzero_levels_token_key: Mutex::new(String::new()),
        }
    }
}

#[derive(Debug)]
pub enum ShardMessage {
    Event(Event),
    Tick(i64),
}

pub struct ShardWorker {
    shard_id: usize,
    receiver: mpsc::Receiver<ShardMessage>,
    db_sender: DbSender,
    pair_senders: Arc<Vec<mpsc::Sender<PairMessage>>>,
    registry: Arc<RwLock<RegistryState>>,
    health: SharedHealth,
    metrics: Arc<ShardMetrics>,
    state: HashMap<String, TokenState>,
    dirty_tokens: HashSet<String>,
    state_init_count: usize,
    rest_snapshot_applied: usize,
    rest_snapshot_received: usize,
    event_count: usize,
    tick_count: usize,
    metrics_update_count: usize,
    config: ShardConfig,
}

impl ShardWorker {
    pub fn new(
        shard_id: usize,
        receiver: mpsc::Receiver<ShardMessage>,
        db_sender: DbSender,
        pair_senders: Arc<Vec<mpsc::Sender<PairMessage>>>,
        registry: Arc<RwLock<RegistryState>>,
        health: SharedHealth,
        metrics: Arc<ShardMetrics>,
        config: ShardConfig,
    ) -> Self {
        ShardWorker {
            shard_id,
            receiver,
            db_sender,
            pair_senders,
            registry,
            health,
            metrics,
            state: HashMap::new(),
            dirty_tokens: HashSet::new(),
            state_init_count: 0,
            rest_snapshot_applied: 0,
            rest_snapshot_received: 0,
            event_count: 0,
            tick_count: 0,
            metrics_update_count: 0,
            config,
        }
    }

    pub async fn run(mut self) {
        info!(
            "shard_start shard_id={} alpha_capture_enabled={} pm_staleness_ms={} other_staleness_ms={} hard_clear_ms={}",
            self.shard_id,
            self.config.alpha_capture_enabled,
            self.config.pm_staleness_ms,
            self.config.other_staleness_ms,
            self.config.hard_clear_ms
        );
        while let Some(msg) = self.receiver.recv().await {
            let (msg_kind, slow_meta) = match &msg {
                ShardMessage::Event(event) => (
                    "event",
                    Some((event.venue, event.kind, event.token_key.clone())),
                ),
                ShardMessage::Tick(_) => ("tick", None),
            };
            let handle_start = Instant::now();
            match msg {
                ShardMessage::Event(event) => {
                    self.handle_event(event).await;
                }
                ShardMessage::Tick(bar_second) => {
                    self.handle_tick(bar_second).await;
                }
            }
            let handle_ms = handle_start.elapsed().as_millis();
            if handle_ms > SHARD_HANDLE_WARN_MS {
                if let Some((venue, kind, token_key)) = slow_meta {
                    info!(
                        "shard_handle_slow shard_id={} handle_ms={} msg={} venue={:?} kind={:?} token_key={}",
                        self.shard_id, handle_ms, msg_kind, venue, kind, token_key
                    );
                } else {
                    info!(
                        "shard_handle_slow shard_id={} handle_ms={} msg={}",
                        self.shard_id, handle_ms, msg_kind
                    );
                }
            }
        }
    }

    async fn handle_event(&mut self, mut event: Event) {
        let token_key = event.token_key.clone();
        let lag_ms = now_ts_ms().saturating_sub(event.local_ts_ms);
        if lag_ms > SHARD_EVENT_LAG_WARN_MS {
            let count = SHARD_EVENT_LAG_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
            if count == 1 || count % 100 == 0 {
                info!(
                    "shard_event_lag count={} shard_id={} lag_ms={} venue={:?} kind={:?} token_key={}",
                    count,
                    self.shard_id,
                    lag_ms,
                    event.venue,
                    event.kind,
                    token_key
                );
            }
        }
        self.event_count += 1;
        if self.event_count == 1 || self.event_count % EVENT_LOG_INTERVAL == 0 {
            info!(
                "shard_event_progress shard_id={} event_count={} state_len={} rest_snapshot_received={} rest_snapshot_applied={} token_key={} kind={:?} venue={:?}",
                self.shard_id,
                self.event_count,
                self.state.len(),
                self.rest_snapshot_received,
                self.rest_snapshot_applied,
                token_key,
                event.kind,
                event.venue
            );
        }
        if event.exchange_ts_source == ExchangeTsSource::VenueRest
            && event.kind == EventKind::BookSnapshot
        {
            self.rest_snapshot_received += 1;
            if self.rest_snapshot_received == 1
                || self.rest_snapshot_received % REST_SNAPSHOT_LOG_INTERVAL == 0
            {
                info!(
                    "rest_snapshot_received shard_id={} count={} token_key={}",
                    self.shard_id, self.rest_snapshot_received, token_key
                );
            }
        }
        let token_side = {
            let registry = self.registry.read().await;
            registry
                .token_sides
                .get(&token_key)
                .copied()
                .unwrap_or(TokenSide::Unknown)
        };
        let bar_second = event.exchange_ts_ms / 1000;

        let is_new = if !self.state.contains_key(&token_key) {
            self.state.insert(
                token_key.clone(),
                TokenState::new(event.venue, token_side, bar_second),
            );
            true
        } else {
            false
        };
        if is_new {
            self.state_init_count += 1;
            if self.state_init_count % STATE_LOG_INTERVAL == 0 {
                info!(
                    "shard_state_init shard_id={} total_states={} init_count={} recent_token_key={}",
                    self.shard_id,
                    self.state.len(),
                    self.state_init_count,
                    token_key
                );
                let stage = format!(
                    "shard_state_init shard={} count={}",
                    self.shard_id, self.state_init_count
                );
                mem_stats::snapshot(&stage);
            }
        }

        let mut resync_log = None;
        {
            let state = match self.state.get_mut(&token_key) {
                Some(state) => state,
                None => {
                    self.state.insert(
                        token_key.clone(),
                        TokenState::new(event.venue, token_side, bar_second),
                    );
                    self.state
                        .get_mut(&token_key)
                        .expect("token state missing after insert")
                }
            };

            if state.watermark_second <= 0 {
                state.watermark_second = bar_second;
                state.last_flushed_second = bar_second.saturating_sub(1);
            } else if bar_second > state.watermark_second {
                state.watermark_second = bar_second;
            }

            let metrics_second = if bar_second <= state.last_flushed_second {
                event.flags.out_of_order = true;
                None
            } else {
                Some(bar_second)
            };

            state.last_event_local_ts_ms = event.local_ts_ms;
            if let Some(metrics_second) = metrics_second {
                let acc = state.accumulator_mut(metrics_second);
                acc.record_latency(
                    event.exchange_ts_source,
                    event.local_ts_ms,
                    event.exchange_ts_ms,
                );
                if event.flags.ts_missing {
                    acc.ts_missing_count_1s += 1;
                }
                if event.flags.ts_anomaly {
                    acc.ts_anomaly_count_1s += 1;
                }
            }

            match event.kind {
                EventKind::BookSnapshot => {
                    if event.venue == Venue::Opinion
                        && event.exchange_ts_source == ExchangeTsSource::VenueRest
                    {
                        if let Some(log) = state.check_opinion_snapshot_drift(&event) {
                            resync_log = Some(log);
                            state.mark_resync(bar_second);
                        }
                    }
                    let bids = event.payload.bids.as_deref();
                    let asks = event.payload.asks.as_deref();
                    if bids.is_some() || asks.is_some() {
                        let invalid = state
                            .orderbook
                            .apply_snapshot(bids, asks, event.local_ts_ms);
                        if invalid > 0 {
                            if let Some(metrics_second) = metrics_second {
                                let acc = state.accumulator_mut(metrics_second);
                                acc.ts_anomaly_count_1s += invalid as i64;
                            }
                        }
                        if bids.is_none() || asks.is_none() {
                            log_partial_snapshot(&event, bids.is_none(), asks.is_none());
                        }
                        state.book_initialized = true;
                        let (book_bid, book_ask) = state.orderbook.best_bid_ask();
                        state.update_bbo(book_bid, book_ask);
                    }
                    if event.exchange_ts_source == ExchangeTsSource::VenueRest {
                        self.metrics.record_bootstrap_applied();
                        self.rest_snapshot_applied += 1;
                    }
                    state.resync_needed = false;
                    if let Some(metrics_second) = metrics_second {
                        let acc = state.accumulator_mut(metrics_second);
                        acc.book_updates_1s += 1;
                        acc.critical_event_count += 1;
                    }
                    if let Some(hash) = &event.payload.hash {
                        state.last_venue_hash = Some(hash.clone());
                    }
                    if let Some(metrics_second) = metrics_second {
                        state.update_ofi_and_levels(metrics_second, event.local_ts_ms);
                    }
                    log_first_ws_book_applied(&event, state.orderbook.levels_len());
                    if event.exchange_ts_source == ExchangeTsSource::VenueRest
                        && (self.rest_snapshot_applied == 1
                            || self.rest_snapshot_applied % REST_SNAPSHOT_LOG_INTERVAL == 0)
                    {
                        info!(
                            "rest_snapshot_applied shard_id={} count={} token_key={} levels_len={}",
                            self.shard_id,
                            self.rest_snapshot_applied,
                            token_key,
                            state.orderbook.levels_len()
                        );
                        let stage = format!(
                            "after_rest_snapshot_apply shard={} count={}",
                            self.shard_id, self.rest_snapshot_applied
                        );
                        mem_stats::snapshot(&stage);
                    }
                    if resync_log.is_none() {
                        resync_log = state.check_resync(&event, bar_second);
                    }
                    if let Some(log) = &resync_log {
                        state.mark_resync(bar_second);
                        if log.reason != "rest_mismatch" {
                            state.resync_needed = true;
                        }
                    }
                    let should_refresh_metrics =
                        if event.exchange_ts_source == ExchangeTsSource::VenueRest {
                            self.rest_snapshot_applied == 1
                                || self.rest_snapshot_applied % REST_METRICS_INTERVAL == 0
                        } else {
                            self.event_count == 1 || self.event_count % EVENT_METRICS_INTERVAL == 0
                        };
                    if should_refresh_metrics {
                        self.record_metrics(bar_second);
                    }
                }
                EventKind::BookDelta => {
                    let allow_apply = !(event.venue == Venue::Opinion && !state.book_initialized);
                    state.update_bbo(event.payload.best_bid, event.payload.best_ask);
                    if let (Some(side), Some(price), Some(size)) =
                        (event.payload.side, event.payload.price, event.payload.size)
                    {
                        if allow_apply && !state.resync_needed {
                            if let Some(signal) = state.orderbook.check_wall_pull(side, price, size)
                            {
                                if let Some(metrics_second) = metrics_second {
                                    let acc = state.accumulator_mut(metrics_second);
                                    acc.wall_pull_signal = Some(signal);
                                }
                            }
                            if state
                                .orderbook
                                .apply_delta(side, price, size, event.local_ts_ms)
                            {
                                if let Some(metrics_second) = metrics_second {
                                    let acc = state.accumulator_mut(metrics_second);
                                    acc.ts_anomaly_count_1s += 1;
                                }
                            }
                        }
                        if let Some(metrics_second) = metrics_second {
                            let acc = state.accumulator_mut(metrics_second);
                            match event.payload.delta_type.unwrap_or(DeltaType::Unknown) {
                                DeltaType::Add | DeltaType::AddOrUpdate => acc.add_updates_1s += 1,
                                DeltaType::Cancel => acc.cancel_updates_1s += 1,
                                DeltaType::Match => acc.match_updates_1s += 1,
                                DeltaType::Unknown => acc.remove_unknown_updates_1s += 1,
                            }
                        }
                        if let Some(metrics_second) = metrics_second {
                            let delta_type = event.payload.delta_type.unwrap_or(DeltaType::Unknown);
                            state.update_bid_ask_delta(metrics_second, event.local_ts_ms, side, delta_type, price, size);
                        }
                    }
                    if let Some(metrics_second) = metrics_second {
                        let acc = state.accumulator_mut(metrics_second);
                        acc.book_updates_1s += 1;
                        acc.critical_event_count += 1;
                    }
                    if allow_apply && !state.resync_needed {
                        if let Some(metrics_second) = metrics_second {
                            state.update_ofi_and_levels(metrics_second, event.local_ts_ms);
                        }
                    }
                    if let Some(hash) = &event.payload.hash {
                        state.last_venue_hash = Some(hash.clone());
                    }
                    if allow_apply {
                        resync_log = state.check_resync(&event, bar_second);
                        if resync_log.is_some() {
                            state.mark_resync(bar_second);
                            state.resync_needed = true;
                        }
                    }
                }
                EventKind::Trade => {
                    if let Some(price) = event.payload.price {
                        state.last_price = Some(price);
                    }
                    if let Some(metrics_second) = metrics_second {
                        let acc = state.accumulator_mut(metrics_second);
                        acc.trade_count_1s += 1;
                        if let (Some(price), Some(size)) = (event.payload.price, event.payload.size)
                        {
                            acc.volume_shares_1s += size;
                            acc.volume_notional_1s += size * price;
                            let notional = size * price;
                            if notional > acc.max_trade_notional_1s {
                                acc.max_trade_notional_1s = notional;
                            }
                        }
                        acc.match_updates_1s += 1;
                        acc.critical_event_count += 1;
                        if let Some(side) = event.payload.side {
                            let notional = event.payload.price.unwrap_or(0.0)
                                * event.payload.size.unwrap_or(0.0);
                            if side == BookSide::Bid {
                                acc.buy_notional_1s += notional;
                                acc.cvd_delta_1s += notional;
                            } else {
                                acc.sell_notional_1s += notional;
                                acc.cvd_delta_1s -= notional;
                            }
                            state.update_trade_window(
                                metrics_second,
                                event.local_ts_ms,
                                side,
                                notional,
                            );
                        } else if event.flags.trade_side_missing {
                            acc.trade_side_missing_count_1s += 1;
                        }
                    }
                }
                EventKind::LastPrice => {
                    if let Some(price) = event.payload.price {
                        state.last_price = Some(price);
                    }
                    if let Some(metrics_second) = metrics_second {
                        let acc = state.accumulator_mut(metrics_second);
                        acc.critical_event_count += 1;
                    }
                }
                EventKind::TickSizeChange => {
                    if let Some(tick) = event.payload.tick_size {
                        state.orderbook.tick_px = Some(tick);
                    }
                }
                EventKind::Health => {}
            }
        }

        self.dirty_tokens.insert(token_key.clone());
        self.flush_ready(token_key.clone()).await;
        event.payload.bids = None;
        event.payload.asks = None;
        if self.config.alpha_capture_enabled {
            try_send_db(&self.db_sender, DbMessage::RawEvent(event), "shard_raw_event");
            if let Some(log) = resync_log {
                try_send_db(&self.db_sender, DbMessage::ResyncLog(log), "shard_resync_log");
            }
        }
    }

    async fn handle_tick(&mut self, bar_second: i64) {
        self.tick_count += 1;
        if self.tick_count == 1 || self.tick_count % TICK_LOG_INTERVAL == 0 {
            info!(
                "shard_tick shard_id={} tick_count={} bar_second={} state_len={} dirty_count={} rest_snapshot_applied={} event_count={}",
                self.shard_id,
                self.tick_count,
                bar_second,
                self.state.len(),
                self.dirty_tokens.len(),
                self.rest_snapshot_applied,
                self.event_count
            );
        }
        let token_keys: Vec<String> = self.dirty_tokens.drain().collect();
        for token_key in token_keys {
            {
                if let Some(state) = self.state.get_mut(&token_key) {
                    if state.watermark_second > 0 && bar_second > state.watermark_second {
                        state.watermark_second = bar_second;
                    }
                }
            }
            self.flush_ready(token_key.clone()).await;
        }
        if bar_second % 5 == 0 {
            self.record_metrics(bar_second);
        }
    }

    fn record_metrics(&mut self, bar_second: i64) {
        self.metrics_update_count += 1;
        let mut orderbook_levels = 0usize;
        let mut pending_shocks = 0usize;
        let mut book_count_pm = 0usize;
        let mut book_count_opi = 0usize;
        let mut max_nonzero_levels = 0usize;
        let mut max_nonzero_levels_token_key = String::new();
        for (token_key, state) in &self.state {
            let levels = state.orderbook.levels_len();
            orderbook_levels += levels;
            pending_shocks += state.pending_shocks.len();
            match state.venue {
                Venue::Polymarket => book_count_pm += 1,
                Venue::Opinion => book_count_opi += 1,
            }
            if levels > max_nonzero_levels {
                max_nonzero_levels = levels;
                max_nonzero_levels_token_key = token_key.clone();
            }
        }
        let slot = self.metrics.slot(self.shard_id);
        slot.token_states.store(self.state.len(), Ordering::Relaxed);
        slot.orderbook_levels
            .store(orderbook_levels, Ordering::Relaxed);
        slot.pending_shocks.store(pending_shocks, Ordering::Relaxed);
        slot.book_count_pm.store(book_count_pm, Ordering::Relaxed);
        slot.book_count_opi.store(book_count_opi, Ordering::Relaxed);
        slot.max_nonzero_levels
            .store(max_nonzero_levels, Ordering::Relaxed);
        if let Ok(mut key) = slot.max_nonzero_levels_token_key.lock() {
            *key = max_nonzero_levels_token_key;
        }
        if self.metrics_update_count == 1 || self.metrics_update_count % METRICS_LOG_INTERVAL == 0 {
            info!(
                "shard_metrics_recorded shard_id={} metrics_update_count={} bar_second={} state_len={} orderbook_levels={} book_count_pm={} book_count_opi={} pending_shocks={} rest_snapshot_applied={} event_count={}",
                self.shard_id,
                self.metrics_update_count,
                bar_second,
                self.state.len(),
                orderbook_levels,
                book_count_pm,
                book_count_opi,
                pending_shocks,
                self.rest_snapshot_applied,
                self.event_count
            );
        }
    }

    async fn flush_ready(&mut self, token_key: String) {
        let mut results = Vec::new();
        if let Some(state) = self.state.get_mut(&token_key) {
            if state.watermark_second <= 0 {
                return;
            }
            let cutoff = state.watermark_second - LATE_EVENT_BUFFER_SECS;
            if cutoff <= state.last_flushed_second {
                return;
            }
            let mut current = state.last_flushed_second + 1;
            while current <= cutoff {
                let acc = state.accumulators.remove(&current).unwrap_or_default();
                results.push(state.flush(&token_key, current, acc, &self.config));
                current += 1;
            }
            state.last_flushed_second = cutoff;
        }
        for result in results {
            self.emit_result(&token_key, result).await;
        }
    }

    async fn emit_result(&mut self, token_key: &str, mut result: FlushResult) {
        {
            let state = self.health.lock().await;
            let snapshot = state.snapshot(result.bar.venue, result.bar.bar_second);
            let mut flags = result.bar.connection_health_flags;
            if snapshot.ws_state == WsState::Up as i64 {
                flags |= HEALTH_WS_CONNECTED;
            }
            if result.bar.venue == Venue::Opinion {
                if snapshot.heartbeat_fail.unwrap_or(0) == 0 {
                    flags |= HEALTH_HEARTBEAT_OK;
                }
            } else {
                flags |= HEALTH_HEARTBEAT_OK;
            }
            if let Some(state) = self.state.get_mut(token_key) {
                if snapshot.dropped_events > state.last_drop_count {
                    flags |= HEALTH_EVENT_DROP;
                    state.last_drop_count = snapshot.dropped_events;
                }
            }
            result.bar.connection_health_flags = flags;
        }
        result.bar.token_key = token_key.to_string();
        if self.config.alpha_capture_enabled {
            try_send_db(
                &self.db_sender,
                DbMessage::Bars1sToken(result.bar.clone()),
                "shard_bars1s_token",
            );
        }
        let pair_ids = {
            let registry = self.registry.read().await;
            registry.token_pairs.get(token_key).cloned()
        };
        if let Some(pair_ids) = pair_ids {
            for pair_id in pair_ids {
                let input = PairInput {
                    pair_id,
                    token_side: result.bar.token_side,
                    bar_second: result.bar.bar_second,
                    venue: result.bar.venue,
                    best_bid: result.bar.best_bid_px,
                    best_ask: result.bar.best_ask_px,
                    mid: result.bar.mid_px,
                    best_bid_state: result.bar.best_bid_px_state,
                    best_ask_state: result.bar.best_ask_px_state,
                    mid_state: result.bar.mid_px_state,
                    last_price: result.bar.last_price,
                    micro: result.bar.micro_px,
                    staleness_ms: result.bar.staleness_ms,
                    bar_gap_flag: result.bar.bar_gap_flag != 0,
                    bar_stale_flag: result.bar.bar_stale_flag != 0,
                    token_key: result.bar.token_key.clone(),
                    spread: result.bar.spread_px,
                    top10_depth_bid: result.bar.top10_depth_bid_notional,
                    top10_depth_ask: result.bar.top10_depth_ask_notional,
                    bid_l1_notional: result.bar.bid_l1_notional,
                    bid_l2_notional: result.bar.bid_l2_notional,
                    bid_l3_notional: result.bar.bid_l3_notional,
                    ask_l1_notional: result.bar.ask_l1_notional,
                    ask_l2_notional: result.bar.ask_l2_notional,
                    ask_l3_notional: result.bar.ask_l3_notional,
                    volume_notional_1s: result.bar.volume_notional_1s,
                    cvd_delta_1s: result.bar.cvd_delta_1s,
                    buy_notional_1s: result.bar.buy_notional_1s,
                    sell_notional_1s: result.bar.sell_notional_1s,
                    ofi_250ms: result.bar.ofi_250ms,
                    bid_delta_notional_250ms: result.bar.bid_delta_notional_250ms,
                    ask_delta_notional_250ms: result.bar.ask_delta_notional_250ms,
                    mid_slope_250ms: result.bar.mid_slope_250ms,
                    buy_notional_250ms: result.bar.buy_notional_250ms,
                    sell_notional_250ms: result.bar.sell_notional_250ms,
                    max_trade_notional_250ms: result.bar.max_trade_notional_250ms,
                    max_trade_notional_1s: result.bar.max_trade_notional_1s,
                    liquidity_pull_flag: result.bar.liquidity_pull_flag,
                    depth_withdrawal_ratio: result.bar.depth_withdrawal_ratio,
                    obs_latency_p99_ms: result.bar.obs_latency_venue_p99_ms,
                };
                let pair_shard = pick_pair_shard(pair_id, self.pair_senders.len());
                let pair_sender = self
                    .pair_senders
                    .get(pair_shard)
                    .cloned();
                if let Some(pair_sender) = pair_sender {
                    let _ = try_send_pair_message(
                        &pair_sender,
                        PairMessage::TokenBar(input),
                        self.shard_id,
                        pair_id,
                        token_key,
                        "tokenbar",
                    );
                }

                for signal in &result.shocks {
                    match signal {
                        ShockSignal::Pending(pending) => {
                            let shock_id = next_shock_id();
                            let key = (
                                pair_id,
                                pending.shock_type.to_string(),
                                pending.trigger_ts_ms,
                            );
                            if let Some(state) = self.state.get_mut(token_key) {
                                state.pending_shocks.insert(key, shock_id);
                            }
                            let shock_event = crate::models::ShockEvent {
                                shock_id,
                                token_key: result.bar.token_key.clone(),
                                pair_id: Some(pair_id),
                                token_side: result.bar.token_side,
                                shock_type: pending.shock_type.to_string(),
                                direction: pending.direction,
                                noise_flag: pending.noise_flag,
                                start_ts_ms: pending.start_ts_ms,
                                trigger_ts_ms: pending.trigger_ts_ms,
                                peak_ts_ms: pending.trigger_ts_ms,
                                magnitude: pending.magnitude,
                                context_json: pending.context_json.clone(),
                                obs_latency_ms: result.bar.obs_latency_venue_p99_ms,
                                connection_health_flags: result.bar.connection_health_flags,
                                status: ShockStatus::Pending,
                                peak_finalized: 0,
                            };
                            if self.config.alpha_capture_enabled {
                                try_send_db(
                                    &self.db_sender,
                                    DbMessage::ShockEvent(shock_event),
                                    "shard_shock_event",
                                );
                            }
                            let shock_input = crate::pair_aggregator::ShockInput {
                                shock_id,
                                pair_id,
                                token_side: result.bar.token_side,
                                trigger_bar_second: result.bar.bar_second,
                                shock_type: pending.shock_type.to_string(),
                                direction: pending.direction,
                                magnitude: pending.magnitude,
                                noise_flag: pending.noise_flag,
                                obs_latency_poly_p99_ms: result.bar.obs_latency_venue_p99_ms,
                            };
                            let pair_shard = pick_pair_shard(pair_id, self.pair_senders.len());
                            let pair_sender = self
                                .pair_senders
                                .get(pair_shard)
                                .cloned();
                            if let Some(pair_sender) = pair_sender {
                                let _ = try_send_pair_message(
                                    &pair_sender,
                                    PairMessage::Shock(shock_input),
                                    self.shard_id,
                                    pair_id,
                                    token_key,
                                    "shock",
                                );
                            }
                        }
                        ShockSignal::Final(finalized) => {
                            let key = (
                                pair_id,
                                finalized.shock_type.to_string(),
                                finalized.trigger_ts_ms,
                            );
                            let shock_id = if let Some(state) = self.state.get_mut(token_key) {
                                state.pending_shocks.remove(&key)
                            } else {
                                None
                            };
                            if let Some(shock_id) = shock_id {
                                let update = ShockEventUpdate {
                                    shock_id,
                                    start_ts_ms: finalized.start_ts_ms,
                                    peak_ts_ms: finalized.peak_ts_ms,
                                    magnitude: finalized.magnitude,
                                    context_json: finalized.context_json.clone(),
                                    direction: finalized.direction,
                                    noise_flag: finalized.noise_flag,
                                    status: ShockStatus::Final,
                                    peak_finalized: 1,
                                };
                                if self.config.alpha_capture_enabled {
                                    try_send_db(
                                        &self.db_sender,
                                        DbMessage::ShockEventUpdate(update),
                                        "shard_shock_update",
                                    );
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}

fn log_first_ws_book_applied(event: &Event, levels_len: usize) {
    if event.channel.as_deref() != Some("book") {
        return;
    }
    if FIRST_WS_BOOK_APPLIED
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        info!(
            "ws first book snapshot applied venue={:?} token_key={} local_ts_ms={} levels_len={}",
            event.venue, event.token_key, event.local_ts_ms, levels_len
        );
        mem_stats::snapshot("after_first_ws_book_snapshot_applied");
    }
}

fn log_partial_snapshot(event: &Event, missing_bids: bool, missing_asks: bool) {
    let count = PARTIAL_SNAPSHOT_SAMPLE.fetch_add(1, Ordering::Relaxed);
    if count % PARTIAL_SNAPSHOT_SAMPLE_RATE != 0 {
        return;
    }
    info!(
        "partial_snapshot venue={:?} token_key={} channel={:?} missing_bids={} missing_asks={}",
        event.venue, event.token_key, event.channel, missing_bids, missing_asks
    );
}

fn log_mid_source(token_key: &str, venue: Venue, source: &str, best_bid: f64, best_ask: f64) {
    let count = MID_BBO_SAMPLE.fetch_add(1, Ordering::Relaxed);
    if count % MID_BBO_SAMPLE_RATE != 0 {
        return;
    }
    info!(
        "mid_source venue={:?} token_key={} source={} best_bid={} best_ask={}",
        venue, token_key, source, best_bid, best_ask
    );
}

struct FlushResult {
    bar: Bars1sToken,
    shocks: Vec<ShockSignal>,
}

fn next_shock_id() -> i64 {
    SHOCK_COUNTER.fetch_add(1, Ordering::Relaxed)
}

struct TokenState {
    venue: Venue,
    token_side: TokenSide,
    orderbook: DenseBook,
    accumulators: HashMap<i64, SecondAccumulator>,
    watermark_second: i64,
    last_flushed_second: i64,
    last_event_local_ts_ms: i64,
    last_price: Option<f64>,
    prev_best_bid_px: Option<f64>,
    prev_best_ask_px: Option<f64>,
    prev_best_bid_sz: Option<f64>,
    prev_best_ask_sz: Option<f64>,
    prev_top10_bid: Option<f64>,
    prev_top10_ask: Option<f64>,
    last_bbo_bid_px: Option<f64>,
    last_bbo_ask_px: Option<f64>,
    bid_delta_window: VecDeque<(i64, f64)>,
    ask_delta_window: VecDeque<(i64, f64)>,
    mid_window: VecDeque<(i64, f64)>,
    trade_window: VecDeque<(i64, f64, BookSide)>,
    book_initialized: bool,
    last_resync_bar_second: Option<i64>,
    last_venue_hash: Option<String>,
    last_local_topn_hash: Option<String>,
    resync_needed: bool,
    last_drop_count: i64,
    pending_shocks: HashMap<(i64, String, i64), i64>,
    cancel_ratio_window: RollingWindow,
    ofi_history: std::collections::VecDeque<f64>,
    ofi_window: VecDeque<(i64, f64)>,
    shock_engine: Option<ShockEngine>,
}

impl TokenState {
    fn new(venue: Venue, token_side: TokenSide, initial_bar_second: i64) -> Self {
        let initial = if initial_bar_second > 0 {
            initial_bar_second
        } else {
            0
        };
        TokenState {
            venue,
            token_side,
            orderbook: DenseBook::default(),
            accumulators: HashMap::new(),
            watermark_second: initial,
            last_flushed_second: initial.saturating_sub(1),
            last_event_local_ts_ms: now_ts_ms(),
            last_price: None,
            prev_best_bid_px: None,
            prev_best_ask_px: None,
            prev_best_bid_sz: None,
            prev_best_ask_sz: None,
            prev_top10_bid: None,
            prev_top10_ask: None,
            last_bbo_bid_px: None,
            last_bbo_ask_px: None,
            bid_delta_window: VecDeque::with_capacity(128),
            ask_delta_window: VecDeque::with_capacity(128),
            mid_window: VecDeque::with_capacity(128),
            trade_window: VecDeque::with_capacity(128),
            book_initialized: false,
            last_resync_bar_second: None,
            last_venue_hash: None,
            last_local_topn_hash: None,
            resync_needed: false,
            last_drop_count: 0,
            pending_shocks: HashMap::new(),
            cancel_ratio_window: RollingWindow::new(21_600),
            ofi_history: std::collections::VecDeque::with_capacity(32),
            ofi_window: VecDeque::with_capacity(64),
            shock_engine: if venue == Venue::Polymarket {
                Some(ShockEngine::new())
            } else {
                None
            },
        }
    }

    fn accumulator_mut(&mut self, bar_second: i64) -> &mut SecondAccumulator {
        self.accumulators
            .entry(bar_second)
            .or_insert_with(SecondAccumulator::default)
    }

    fn update_bbo(&mut self, best_bid: Option<f64>, best_ask: Option<f64>) {
        if let Some(best_bid) = best_bid {
            if price_to_idx(best_bid).is_some() {
                self.last_bbo_bid_px = Some(best_bid);
            }
        }
        if let Some(best_ask) = best_ask {
            if price_to_idx(best_ask).is_some() {
                self.last_bbo_ask_px = Some(best_ask);
            }
        }
    }

    fn flush(
        &mut self,
        token_key: &str,
        bar_second: i64,
        mut acc: SecondAccumulator,
        config: &ShardConfig,
    ) -> FlushResult {
        let (book_bid, book_ask) = self.orderbook.best_bid_ask();
        let mut best_bid_out = book_bid;
        let mut best_ask_out = book_ask;
        let micro = self.orderbook.microprice();
        let mut mid_out = match (book_bid, book_ask) {
            (Some(bid), Some(ask)) => Some((bid + ask) / 2.0),
            _ => None,
        };
        if mid_out.is_none() {
            if best_bid_out.is_none() {
                best_bid_out = self.last_bbo_bid_px;
            }
            if best_ask_out.is_none() {
                best_ask_out = self.last_bbo_ask_px;
            }
            if let (Some(bid), Some(ask)) = (best_bid_out, best_ask_out) {
                mid_out = Some((bid + ask) / 2.0);
                log_mid_source(token_key, self.venue, "bbo", bid, ask);
            }
        }
        let spread = match (best_bid_out, best_ask_out) {
            (Some(bid), Some(ask)) => Some(ask - bid),
            _ => None,
        };
        let tick_px = self
            .orderbook
            .tick_px
            .or_else(|| spread.filter(|v| *v > 0.0));
        if self.orderbook.tick_px.is_none() {
            self.orderbook.tick_px = tick_px;
        }
        let spread_ticks = match (spread, tick_px) {
            (Some(spread), Some(tick)) if tick > 0.0 => Some((spread / tick).round() as i64),
            _ => None,
        };
        let top3_bid = self.orderbook.depth_top_n(BookSide::Bid, 3);
        let top3_ask = self.orderbook.depth_top_n(BookSide::Ask, 3);
        let bid_levels = self.orderbook.top_levels_notional(BookSide::Bid, 3);
        let ask_levels = self.orderbook.top_levels_notional(BookSide::Ask, 3);
        let bid_l1 = bid_levels.get(0).copied().filter(|v| *v > 0.0);
        let bid_l2 = bid_levels.get(1).copied().filter(|v| *v > 0.0);
        let bid_l3 = bid_levels.get(2).copied().filter(|v| *v > 0.0);
        let ask_l1 = ask_levels.get(0).copied().filter(|v| *v > 0.0);
        let ask_l2 = ask_levels.get(1).copied().filter(|v| *v > 0.0);
        let ask_l3 = ask_levels.get(2).copied().filter(|v| *v > 0.0);
        let top10_bid = self.orderbook.depth_top_n(BookSide::Bid, 10);
        let top10_ask = self.orderbook.depth_top_n(BookSide::Ask, 10);
        let depth_1pct_bid = self.orderbook.depth_1pct(BookSide::Bid, mid_out);
        let depth_1pct_ask = self.orderbook.depth_1pct(BookSide::Ask, mid_out);
        let imbalance_top10 = match (top10_bid, top10_ask) {
            (Some(bid), Some(ask)) if bid + ask > 0.0 => Some((bid - ask) / (bid + ask)),
            _ => None,
        };
        let (air_bid_ticks, air_ask_ticks) = self.orderbook.air_pocket_ticks(tick_px);
        let local_topn_hash = self.orderbook.topn_hash(20);
        let staleness_ms = Some(((bar_second * 1000) - self.last_event_local_ts_ms).max(0));
        let staleness_ms_val = staleness_ms.unwrap_or(0);
        let mut depth_withdrawal_ratio = None;
        let mut liquidity_pull_flag = 0;
        let book_resync_flag = if self.last_resync_bar_second == Some(bar_second) {
            1
        } else {
            0
        };
        if self.venue == Venue::Opinion {
            if let (Some(prev_bid), Some(prev_ask), Some(curr_bid), Some(curr_ask)) = (
                self.prev_top10_bid,
                self.prev_top10_ask,
                top10_bid,
                top10_ask,
            ) {
                let prev_sum = prev_bid + prev_ask;
                let curr_sum = curr_bid + curr_ask;
                if prev_sum > 0.0 {
                    let ratio = (curr_sum - prev_sum) / prev_sum;
                    depth_withdrawal_ratio = Some(ratio);
                    acc.depth_withdrawal_ratio = Some(ratio);
                    let total_updates =
                        acc.add_updates_1s + acc.cancel_updates_1s + acc.match_updates_1s;
                    let cancel_ratio = if total_updates > 0 {
                        acc.cancel_updates_1s as f64 / total_updates as f64
                    } else {
                        0.0
                    };
                    self.cancel_ratio_window.push(cancel_ratio);
                    let cancel_p90 = self.cancel_ratio_window.quantile(0.9).unwrap_or(1.0);
                    if ratio <= -0.5 && cancel_ratio >= cancel_p90 {
                        liquidity_pull_flag = 1;
                    }
                }
            }
        }

        let (venue_p50, venue_p90, venue_p99, venue_max) = acc.venue_latency.snapshot();
        let (est_p50, est_p90, est_p99, est_max) = acc.est_latency.snapshot();

        let stale_flag = if self.venue == Venue::Polymarket {
            staleness_ms_val > config.pm_staleness_ms
        } else {
            staleness_ms_val > config.other_staleness_ms
        };
        let gap_flag = staleness_ms_val > config.hard_clear_ms;
        let bar_gap = if gap_flag { 1 } else { 0 };
        let bar_stale = if stale_flag { 1 } else { 0 };
        let mut connection_flags = 0;
        if let Some(last_resync) = self.last_resync_bar_second {
            if bar_second.saturating_sub(last_resync) <= 10 {
                connection_flags |= HEALTH_RESYNC_RECENT;
            }
        }
        if bar_gap == 1 {
            connection_flags |= HEALTH_EVENT_DROP;
        }
        let mut micro_out = micro;
        let mut spread_out = spread;
        let mut tick_out = tick_px;
        let mut top3_bid_out = top3_bid;
        let mut top3_ask_out = top3_ask;
        let mut top10_bid_out = top10_bid;
        let mut top10_ask_out = top10_ask;
        let mut depth_1pct_bid_out = depth_1pct_bid;
        let mut depth_1pct_ask_out = depth_1pct_ask;
        let mut imbalance_out = imbalance_top10;
        let mut air_bid_out = air_bid_ticks;
        let mut air_ask_out = air_ask_ticks;
        let mut last_price_out = self.last_price;
        let mut local_topn_hash_out = local_topn_hash.clone();
        let mut venue_hash_out = self.last_venue_hash.clone();
        let mut depth_withdrawal_out = depth_withdrawal_ratio;
        let mut liquidity_pull_out = liquidity_pull_flag;
        if bar_gap == 1 {
            best_bid_out = None;
            best_ask_out = None;
            mid_out = None;
            micro_out = None;
            spread_out = None;
            tick_out = None;
            top3_bid_out = None;
            top3_ask_out = None;
            top10_bid_out = None;
            top10_ask_out = None;
            depth_1pct_bid_out = None;
            depth_1pct_ask_out = None;
            imbalance_out = None;
            air_bid_out = None;
            air_ask_out = None;
            last_price_out = None;
            local_topn_hash_out = None;
            venue_hash_out = None;
            depth_withdrawal_out = None;
            liquidity_pull_out = 0;
            self.last_bbo_bid_px = None;
            self.last_bbo_ask_px = None;
        }

        let has_bbo_update = acc.book_updates_1s > 0;
        let best_bid_state = best_bid_out;
        let best_ask_state = best_ask_out;
        let mid_state = mid_out;
        let (best_bid_update, best_ask_update, mid_update) = if has_bbo_update {
            (best_bid_out, best_ask_out, mid_out)
        } else {
            (None, None, None)
        };

        self.ofi_history.push_back(acc.ofi_1s);
        if self.ofi_history.len() > 32 {
            self.ofi_history.pop_front();
        }
        let ofi_10 = sum_last_f64(&self.ofi_history, 10);
        let ofi_z_30 = zscore_from_window(&self.ofi_history, acc.ofi_1s);

        let mut exchange_ts_source = acc.dominant_source();
        if self.venue == Venue::Opinion && exchange_ts_source == ExchangeTsSource::LocalFallback {
            exchange_ts_source = ExchangeTsSource::Estimated;
        }
        let bar = Bars1sToken {
            venue: self.venue,
            token_key: String::new(),
            token_side: self.token_side,
            bar_second,
            exchange_ts_source,
            obs_latency_venue_p50_ms: venue_p50,
            obs_latency_venue_p90_ms: venue_p90,
            obs_latency_venue_p99_ms: venue_p99,
            obs_latency_venue_max_ms: venue_max,
            obs_latency_est_p50_ms: est_p50,
            obs_latency_est_p90_ms: est_p90,
            obs_latency_est_p99_ms: est_p99,
            obs_latency_est_max_ms: est_max,
            connection_health_flags: connection_flags,
            bar_gap_flag: bar_gap,
            bar_stale_flag: bar_stale,
            staleness_ms,
            event_count_1s: acc.event_count,
            ts_missing_count_1s: acc.ts_missing_count_1s,
            ts_anomaly_count_1s: acc.ts_anomaly_count_1s,
            book_resync_flag,
            best_bid_px: best_bid_update,
            best_ask_px: best_ask_update,
            mid_px: mid_update,
            best_bid_px_state: best_bid_state,
            best_ask_px_state: best_ask_state,
            mid_px_state: mid_state,
            last_price: last_price_out,
            micro_px: micro_out,
            spread_px: spread_out,
            tick_px: tick_out,
            spread_ticks,
            top3_depth_bid_notional: top3_bid_out,
            top3_depth_ask_notional: top3_ask_out,
            bid_l1_notional: bid_l1,
            bid_l2_notional: bid_l2,
            bid_l3_notional: bid_l3,
            ask_l1_notional: ask_l1,
            ask_l2_notional: ask_l2,
            ask_l3_notional: ask_l3,
            top10_depth_bid_notional: top10_bid_out,
            top10_depth_ask_notional: top10_ask_out,
            depth_1pct_bid_notional: depth_1pct_bid_out,
            depth_1pct_ask_notional: depth_1pct_ask_out,
            imbalance_top10: imbalance_out,
            air_pocket_bid_ticks: air_bid_out,
            air_pocket_ask_ticks: air_ask_out,
            book_updates_1s: acc.book_updates_1s,
            add_updates_1s: acc.add_updates_1s,
            cancel_updates_1s: acc.cancel_updates_1s,
            match_updates_1s: acc.match_updates_1s,
            remove_unknown_updates_1s: acc.remove_unknown_updates_1s,
            trade_count_1s: acc.trade_count_1s,
            volume_shares_1s: acc.volume_shares_1s,
            volume_notional_1s: acc.volume_notional_1s,
            vwap_1s: acc.vwap(),
            buy_notional_1s: acc.buy_notional_1s,
            sell_notional_1s: acc.sell_notional_1s,
            cvd_delta_1s: acc.cvd_delta_1s,
            trade_side_missing_count_1s: acc.trade_side_missing_count_1s,
            ofi_1s: acc.ofi_1s,
            ofi_250ms: acc.ofi_250ms,
            bid_delta_notional_250ms: acc.bid_delta_notional_250ms,
            ask_delta_notional_250ms: acc.ask_delta_notional_250ms,
            mid_slope_250ms: acc.mid_slope_250ms,
            ofi_10s: ofi_10,
            ofi_z_30s: ofi_z_30,
            levels_crossed_1s: acc.levels_crossed_1s,
            max_trade_notional_1s: acc.max_trade_notional_1s,
            buy_notional_250ms: acc.buy_notional_250ms,
            sell_notional_250ms: acc.sell_notional_250ms,
            max_trade_notional_250ms: acc.max_trade_notional_250ms,
            depth_withdrawal_ratio: depth_withdrawal_out,
            liquidity_pull_flag: liquidity_pull_out,
            venue_book_hash: venue_hash_out,
            local_topn_hash: local_topn_hash_out,
        };

        let micro_mid_gap = match (micro_out, mid_out) {
            (Some(micro), Some(mid)) => Some(micro - mid),
            _ => None,
        };
        let mut shocks = Vec::new();
        if let Some(engine) = &mut self.shock_engine {
            shocks = engine.on_bar(
                bar_second,
                bar.micro_px.or(bar.mid_px_state),
                bar.tick_px,
                micro_mid_gap,
                acc.add_updates_1s,
                acc.cancel_updates_1s,
                acc.volume_notional_1s,
                acc.buy_notional_1s,
                acc.sell_notional_1s,
                acc.cvd_delta_1s,
                acc.ofi_1s,
                acc.levels_crossed_1s,
                acc.trade_count_1s,
                acc.trade_side_missing_count_1s,
                acc.max_trade_notional_1s,
                acc.wall_pull_signal.clone(),
            );
        }

        self.prev_top10_bid = top10_bid;
        self.prev_top10_ask = top10_ask;
        self.last_local_topn_hash = local_topn_hash;
        FlushResult { bar, shocks }
    }

    fn update_ofi_and_levels(&mut self, bar_second: i64, event_ts_ms: i64) {
        let (bid_px, bid_sz, ask_px, ask_sz) = self.orderbook.best_bid_ask_size();
        let (prev_bid_px, prev_ask_px, prev_bid_sz, prev_ask_sz) = (
            self.prev_best_bid_px,
            self.prev_best_ask_px,
            self.prev_best_bid_sz,
            self.prev_best_ask_sz,
        );
        let tick_px = self.orderbook.tick_px;
        if let (Some(bid_px), Some(ask_px), Some(bid_sz), Some(ask_sz)) =
            (bid_px, ask_px, bid_sz, ask_sz)
        {
            if let (Some(prev_bid_px), Some(prev_ask_px), Some(prev_bid_sz), Some(prev_ask_sz)) =
                (prev_bid_px, prev_ask_px, prev_bid_sz, prev_ask_sz)
            {
                let bid_component = if bid_px > prev_bid_px {
                    bid_sz
                } else if bid_px < prev_bid_px {
                    -prev_bid_sz
                } else {
                    bid_sz - prev_bid_sz
                };
                let ask_component = if ask_px < prev_ask_px {
                    ask_sz
                } else if ask_px > prev_ask_px {
                    -prev_ask_sz
                } else {
                    ask_sz - prev_ask_sz
                };
                let bid_notional = bid_component * bid_px;
                let ask_notional = ask_component * ask_px;
                let ofi_delta = bid_notional - ask_notional;
                if ofi_delta.abs() > 0.0 {
                    self.ofi_window.push_back((event_ts_ms, ofi_delta));
                }
                let cutoff = event_ts_ms - OFI_FAST_WINDOW_MS;
                while let Some((ts, _)) = self.ofi_window.front() {
                    if *ts < cutoff {
                        self.ofi_window.pop_front();
                    } else {
                        break;
                    }
                }
                let mut ofi_fast = 0.0;
                for (_, val) in &self.ofi_window {
                    ofi_fast += *val;
                }
                let levels_crossed = if let Some(tick) = tick_px {
                    if tick > 0.0 {
                        let bid_cross = ((bid_px - prev_bid_px) / tick).abs().floor() as i64;
                        let ask_cross = ((ask_px - prev_ask_px) / tick).abs().floor() as i64;
                        bid_cross.max(ask_cross)
                    } else {
                        0
                    }
                } else {
                    0
                };

                let acc = self.accumulator_mut(bar_second);
                acc.ofi_1s += ofi_delta;
                acc.ofi_250ms = ofi_fast;
                acc.levels_crossed_1s += levels_crossed;
            }

            let mid = (bid_px + ask_px) * 0.5;
            self.mid_window.push_back((event_ts_ms, mid));
            let cutoff = event_ts_ms - OFI_FAST_WINDOW_MS;
            while let Some((ts, _)) = self.mid_window.front() {
                if *ts < cutoff {
                    self.mid_window.pop_front();
                } else {
                    break;
                }
            }
            let mid_slope = if let (Some((ts, mid_then)), true) = (
                self.mid_window.front(),
                event_ts_ms > self.mid_window.front().unwrap().0,
            ) {
                let dt_ms = (event_ts_ms - ts).max(1) as f64;
                Some((mid - *mid_then) / (dt_ms / 1000.0))
            } else {
                None
            };
            let acc = self.accumulator_mut(bar_second);
            acc.mid_slope_250ms = mid_slope;

            self.prev_best_bid_px = Some(bid_px);
            self.prev_best_ask_px = Some(ask_px);
            self.prev_best_bid_sz = Some(bid_sz);
            self.prev_best_ask_sz = Some(ask_sz);
        }
    }

    fn update_bid_ask_delta(
        &mut self,
        bar_second: i64,
        event_ts_ms: i64,
        side: BookSide,
        delta_type: DeltaType,
        price: f64,
        size: f64,
    ) {
        if price <= 0.0 || size <= 0.0 {
            return;
        }
        let notional = price * size;
        let signed = match delta_type {
            DeltaType::Add | DeltaType::AddOrUpdate => notional,
            DeltaType::Cancel | DeltaType::Match => -notional,
            DeltaType::Unknown => 0.0,
        };
        if signed.abs() <= 0.0 {
            return;
        }
        let window = if side == BookSide::Bid {
            &mut self.bid_delta_window
        } else {
            &mut self.ask_delta_window
        };
        window.push_back((event_ts_ms, signed));
        let cutoff = event_ts_ms - OFI_FAST_WINDOW_MS;
        while let Some((ts, _)) = window.front() {
            if *ts < cutoff {
                window.pop_front();
            } else {
                break;
            }
        }
        let mut bid_sum = 0.0;
        for (_, val) in &self.bid_delta_window {
            bid_sum += *val;
        }
        let mut ask_sum = 0.0;
        for (_, val) in &self.ask_delta_window {
            ask_sum += *val;
        }
        let acc = self.accumulator_mut(bar_second);
        acc.bid_delta_notional_250ms = bid_sum;
        acc.ask_delta_notional_250ms = ask_sum;
    }

    fn update_trade_window(
        &mut self,
        bar_second: i64,
        event_ts_ms: i64,
        side: BookSide,
        notional: f64,
    ) {
        if notional <= 0.0 {
            return;
        }
        self.trade_window.push_back((event_ts_ms, notional, side));
        let cutoff = event_ts_ms - OFI_FAST_WINDOW_MS;
        while let Some((ts, _, _)) = self.trade_window.front() {
            if *ts < cutoff {
                self.trade_window.pop_front();
            } else {
                break;
            }
        }
        let mut buy_sum = 0.0;
        let mut sell_sum = 0.0;
        let mut max_trade = 0.0;
        for (_, value, trade_side) in &self.trade_window {
            if *value > max_trade {
                max_trade = *value;
            }
            if *trade_side == BookSide::Bid {
                buy_sum += *value;
            } else {
                sell_sum += *value;
            }
        }
        let acc = self.accumulator_mut(bar_second);
        acc.buy_notional_250ms = buy_sum;
        acc.sell_notional_250ms = sell_sum;
        acc.max_trade_notional_250ms = max_trade;
    }

    fn mark_resync(&mut self, bar_second: i64) {
        self.last_resync_bar_second = Some(bar_second);
    }

    fn check_opinion_snapshot_drift(&self, event: &Event) -> Option<crate::models::ResyncLog> {
        let bids = event.payload.bids.as_ref()?;
        let asks = event.payload.asks.as_ref()?;
        let local_hash = self.orderbook.topn_hash(20);
        let (local_bid_idx, local_ask_idx) = self.orderbook.best_bid_ask_idx();
        let local_top10_bid = self.orderbook.depth_top_n(BookSide::Bid, 10);
        let local_top10_ask = self.orderbook.depth_top_n(BookSide::Ask, 10);
        let local_depth = local_top10_bid.unwrap_or(0.0) + local_top10_ask.unwrap_or(0.0);

        let snap_metrics = snapshot_metrics(bids, asks);
        let snap_hash = snap_metrics.topn_hash;
        let snap_bid_idx = snap_metrics.best_bid_idx;
        let snap_ask_idx = snap_metrics.best_ask_idx;
        let snap_depth = snap_metrics.depth_top10;

        let mut mismatch = false;
        match (local_bid_idx, snap_bid_idx) {
            (Some(local), Some(snap)) => {
                if local != snap {
                    mismatch = true;
                }
            }
            (None, None) => {}
            _ => mismatch = true,
        }
        match (local_ask_idx, snap_ask_idx) {
            (Some(local), Some(snap)) => {
                if local != snap {
                    mismatch = true;
                }
            }
            (None, None) => {}
            _ => mismatch = true,
        }

        if local_depth > 0.0 && snap_depth > 0.0 {
            let ratio = (snap_depth - local_depth).abs() / local_depth.max(1e-9);
            if ratio >= OPI_RESYNC_DEPTH_RATIO {
                mismatch = true;
            }
        } else if (local_depth > 0.0) != (snap_depth > 0.0) {
            mismatch = true;
        }

        if let (Some(local), Some(snap)) = (&local_hash, &snap_hash) {
            if local != snap {
                mismatch = true;
            }
        }

        if !mismatch {
            return None;
        }

        Some(crate::models::ResyncLog {
            venue: event.venue,
            token_key: event.token_key.clone(),
            reason: "rest_mismatch".to_string(),
            trigger_local_ts_ms: event.local_ts_ms,
            trigger_exchange_ts_ms: Some(event.exchange_ts_ms),
            venue_hash_before: None,
            venue_hash_after: None,
            local_topn_hash_before: local_hash,
            local_topn_hash_after: snap_hash,
        })
    }

    fn check_resync(&mut self, event: &Event, bar_second: i64) -> Option<crate::models::ResyncLog> {
        if self.last_resync_bar_second == Some(bar_second) {
            return None;
        }
        if event.flags.out_of_order {
            self.last_resync_bar_second = Some(bar_second);
            return Some(crate::models::ResyncLog {
                venue: event.venue,
                token_key: event.token_key.clone(),
                reason: "out_of_order".to_string(),
                trigger_local_ts_ms: event.local_ts_ms,
                trigger_exchange_ts_ms: Some(event.exchange_ts_ms),
                venue_hash_before: self.last_venue_hash.clone(),
                venue_hash_after: event.payload.hash.clone(),
                local_topn_hash_before: self.last_local_topn_hash.clone(),
                local_topn_hash_after: self.orderbook.topn_hash(20),
            });
        }
        let (bid, ask) = self.orderbook.best_bid_ask();
        if let (Some(bid), Some(ask)) = (bid, ask) {
            if bid >= ask {
                self.last_resync_bar_second = Some(bar_second);
                return Some(crate::models::ResyncLog {
                    venue: event.venue,
                    token_key: event.token_key.clone(),
                    reason: "impossible_state".to_string(),
                    trigger_local_ts_ms: event.local_ts_ms,
                    trigger_exchange_ts_ms: Some(event.exchange_ts_ms),
                    venue_hash_before: self.last_venue_hash.clone(),
                    venue_hash_after: event.payload.hash.clone(),
                    local_topn_hash_before: self.last_local_topn_hash.clone(),
                    local_topn_hash_after: self.orderbook.topn_hash(20),
                });
            }
        }
        if event.kind == EventKind::BookSnapshot {
            if let Some(venue_hash) = &event.payload.hash {
                let local_hash = self.orderbook.topn_hash(20);
                if let Some(local_hash) = &local_hash {
                    if venue_hash != local_hash {
                        self.last_resync_bar_second = Some(bar_second);
                        return Some(crate::models::ResyncLog {
                            venue: event.venue,
                            token_key: event.token_key.clone(),
                            reason: "hash_mismatch".to_string(),
                            trigger_local_ts_ms: event.local_ts_ms,
                            trigger_exchange_ts_ms: Some(event.exchange_ts_ms),
                            venue_hash_before: self.last_venue_hash.clone(),
                            venue_hash_after: Some(venue_hash.clone()),
                            local_topn_hash_before: self.last_local_topn_hash.clone(),
                            local_topn_hash_after: Some(local_hash.clone()),
                        });
                    }
                }
            }
        }
        if event.venue == Venue::Polymarket
            && event.kind == EventKind::BookDelta
            && event.channel.as_deref() == Some("best_bid_ask")
        {
            // Top-of-book updates are partial; skip resync mismatch checks.
            return None;
        }
        if event.venue == Venue::Polymarket && event.kind == EventKind::BookDelta {
            if let (Some(best_bid), Some(best_ask)) =
                (event.payload.best_bid, event.payload.best_ask)
            {
                let (local_bid, local_ask) = self.orderbook.best_bid_ask();
                let mut mismatch = false;
                match (local_bid, local_ask) {
                    (Some(local_bid), Some(local_ask)) => {
                        let local_bid_idx = price_to_idx(local_bid);
                        let local_ask_idx = price_to_idx(local_ask);
                        let best_bid_idx = price_to_idx(best_bid);
                        let best_ask_idx = price_to_idx(best_ask);
                        if local_bid_idx.is_none()
                            || local_ask_idx.is_none()
                            || best_bid_idx.is_none()
                            || best_ask_idx.is_none()
                            || local_bid_idx != best_bid_idx
                            || local_ask_idx != best_ask_idx
                        {
                            mismatch = true;
                        }
                    }
                    _ => mismatch = true,
                }
                if mismatch {
                    self.last_resync_bar_second = Some(bar_second);
                    return Some(crate::models::ResyncLog {
                        venue: event.venue,
                        token_key: event.token_key.clone(),
                        reason: "best_bid_ask_mismatch".to_string(),
                        trigger_local_ts_ms: event.local_ts_ms,
                        trigger_exchange_ts_ms: Some(event.exchange_ts_ms),
                        venue_hash_before: self.last_venue_hash.clone(),
                        venue_hash_after: event.payload.hash.clone(),
                        local_topn_hash_before: self.last_local_topn_hash.clone(),
                        local_topn_hash_after: self.orderbook.topn_hash(20),
                    });
                }
            }
        }
        None
    }
}

struct SnapshotMetrics {
    best_bid_idx: Option<usize>,
    best_ask_idx: Option<usize>,
    topn_hash: Option<String>,
    depth_top10: f64,
}

fn snapshot_metrics(bids: &[BookLevel], asks: &[BookLevel]) -> SnapshotMetrics {
    let bid_levels = collect_top_levels(bids, BookSide::Bid, 20);
    let ask_levels = collect_top_levels(asks, BookSide::Ask, 20);
    let best_bid_idx = bid_levels.first().map(|(idx, _)| *idx);
    let best_ask_idx = ask_levels.first().map(|(idx, _)| *idx);
    let topn_hash = hash_top_levels(&bid_levels, &ask_levels);
    let depth_top10 = sum_top_levels(&bid_levels, 10) + sum_top_levels(&ask_levels, 10);
    SnapshotMetrics {
        best_bid_idx,
        best_ask_idx,
        topn_hash,
        depth_top10,
    }
}

fn collect_top_levels(levels: &[BookLevel], side: BookSide, n: usize) -> Vec<(usize, i64)> {
    let mut top: Vec<(usize, i64)> = Vec::with_capacity(n);
    for level in levels {
        let idx = match price_to_idx(level.price) {
            Some(idx) => idx,
            None => continue,
        };
        let size = match size_to_int(level.size) {
            Some(size) if size > 0 => size,
            _ => continue,
        };
        if let Some(entry) = top.iter_mut().find(|(existing, _)| *existing == idx) {
            entry.1 = size;
            continue;
        }
        let insert_pos = top.iter().position(|(existing, _)| match side {
            BookSide::Bid => idx > *existing,
            BookSide::Ask => idx < *existing,
        });
        match insert_pos {
            Some(pos) => {
                top.insert(pos, (idx, size));
                if top.len() > n {
                    top.pop();
                }
            }
            None => {
                if top.len() < n {
                    top.push((idx, size));
                }
            }
        }
    }
    top
}

fn sum_top_levels(levels: &[(usize, i64)], n: usize) -> f64 {
    let mut sum = 0.0;
    for (idx, size) in levels.iter().take(n) {
        sum += idx_to_price(*idx) * size_to_float(*size);
    }
    sum
}

fn hash_top_levels(bids: &[(usize, i64)], asks: &[(usize, i64)]) -> Option<String> {
    if bids.is_empty() && asks.is_empty() {
        return None;
    }
    let mut hasher: u64 = 0xcbf29ce484222325;
    for (idx, size) in bids {
        hasher = hash_pair(hasher, *idx as i64, *size);
    }
    for (idx, size) in asks {
        hasher = hash_pair(hasher, *idx as i64, *size);
    }
    Some(format!("{:x}", hasher))
}

struct DenseBook {
    bids: Vec<i64>,
    asks: Vec<i64>,
    bid_nonzero: usize,
    ask_nonzero: usize,
    best_bid_idx: Option<usize>,
    best_ask_idx: Option<usize>,
    tick_px: Option<f64>,
    last_update_ts_ms: i64,
}

impl Default for DenseBook {
    fn default() -> Self {
        DenseBook {
            bids: vec![0; PRICE_LEVELS],
            asks: vec![0; PRICE_LEVELS],
            bid_nonzero: 0,
            ask_nonzero: 0,
            best_bid_idx: None,
            best_ask_idx: None,
            tick_px: None,
            last_update_ts_ms: 0,
        }
    }
}

impl DenseBook {
    fn levels_len(&self) -> usize {
        self.bid_nonzero + self.ask_nonzero
    }

    fn best_bid_ask_idx(&self) -> (Option<usize>, Option<usize>) {
        (self.best_bid_idx, self.best_ask_idx)
    }

    fn apply_snapshot(
        &mut self,
        bids: Option<&[BookLevel]>,
        asks: Option<&[BookLevel]>,
        local_ts_ms: i64,
    ) -> usize {
        if bids.is_some() {
            self.bids.fill(0);
            self.bid_nonzero = 0;
            self.best_bid_idx = None;
        }
        if asks.is_some() {
            self.asks.fill(0);
            self.ask_nonzero = 0;
            self.best_ask_idx = None;
        }
        self.last_update_ts_ms = local_ts_ms;
        let mut invalid = 0usize;
        if let Some(bids) = bids {
            for level in bids {
                let idx = match price_to_idx(level.price) {
                    Some(idx) => idx,
                    None => {
                        invalid += 1;
                        continue;
                    }
                };
                let size = match size_to_int(level.size) {
                    Some(size) if size > 0 => size,
                    _ => continue,
                };
                if self.bids[idx] == 0 {
                    self.bid_nonzero += 1;
                }
                self.bids[idx] = size;
                if self.best_bid_idx.map_or(true, |best| idx > best) {
                    self.best_bid_idx = Some(idx);
                }
            }
        }
        if let Some(asks) = asks {
            for level in asks {
                let idx = match price_to_idx(level.price) {
                    Some(idx) => idx,
                    None => {
                        invalid += 1;
                        continue;
                    }
                };
                let size = match size_to_int(level.size) {
                    Some(size) if size > 0 => size,
                    _ => continue,
                };
                if self.asks[idx] == 0 {
                    self.ask_nonzero += 1;
                }
                self.asks[idx] = size;
                if self.best_ask_idx.map_or(true, |best| idx < best) {
                    self.best_ask_idx = Some(idx);
                }
            }
        }
        invalid
    }

    fn apply_delta(&mut self, side: BookSide, price: f64, size: f64, local_ts_ms: i64) -> bool {
        let idx = match price_to_idx(price) {
            Some(idx) => idx,
            None => return true,
        };
        let size = match size_to_int(size) {
            Some(size) => size,
            None => return true,
        };
        self.last_update_ts_ms = local_ts_ms;
        let new_size = if size <= 0 { 0 } else { size };
        self.set_level(side, idx, new_size);
        false
    }

    fn set_level(&mut self, side: BookSide, idx: usize, size: i64) {
        match side {
            BookSide::Bid => {
                let prev = self.bids[idx];
                if prev == 0 && size > 0 {
                    self.bid_nonzero += 1;
                } else if prev > 0 && size == 0 {
                    self.bid_nonzero = self.bid_nonzero.saturating_sub(1);
                }
                self.bids[idx] = size;
                match self.best_bid_idx {
                    None => {
                        if size > 0 {
                            self.best_bid_idx = Some(idx);
                        }
                    }
                    Some(best) => {
                        if size > 0 && idx > best {
                            self.best_bid_idx = Some(idx);
                        } else if idx == best && size == 0 {
                            self.best_bid_idx = self.find_next_bid(best);
                        }
                    }
                }
            }
            BookSide::Ask => {
                let prev = self.asks[idx];
                if prev == 0 && size > 0 {
                    self.ask_nonzero += 1;
                } else if prev > 0 && size == 0 {
                    self.ask_nonzero = self.ask_nonzero.saturating_sub(1);
                }
                self.asks[idx] = size;
                match self.best_ask_idx {
                    None => {
                        if size > 0 {
                            self.best_ask_idx = Some(idx);
                        }
                    }
                    Some(best) => {
                        if size > 0 && idx < best {
                            self.best_ask_idx = Some(idx);
                        } else if idx == best && size == 0 {
                            self.best_ask_idx = self.find_next_ask(best);
                        }
                    }
                }
            }
        }
    }

    fn find_next_bid(&self, start: usize) -> Option<usize> {
        if start == 0 {
            return None;
        }
        let mut idx = start - 1;
        loop {
            if self.bids[idx] > 0 {
                return Some(idx);
            }
            if idx == 0 {
                break;
            }
            idx -= 1;
        }
        None
    }

    fn find_next_ask(&self, start: usize) -> Option<usize> {
        let mut idx = start + 1;
        while idx <= PRICE_MAX_IDX {
            if self.asks[idx] > 0 {
                return Some(idx);
            }
            idx += 1;
        }
        None
    }

    fn best_bid_ask(&self) -> (Option<f64>, Option<f64>) {
        let bid = self.best_bid_idx.map(idx_to_price);
        let ask = self.best_ask_idx.map(idx_to_price);
        (bid, ask)
    }

    fn best_bid_ask_size(&self) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
        let bid = self.best_bid_idx.and_then(|idx| {
            let size = self.bids[idx];
            if size > 0 {
                Some((idx_to_price(idx), size_to_float(size)))
            } else {
                None
            }
        });
        let ask = self.best_ask_idx.and_then(|idx| {
            let size = self.asks[idx];
            if size > 0 {
                Some((idx_to_price(idx), size_to_float(size)))
            } else {
                None
            }
        });
        let (bid_px, bid_sz) = bid.map(|v| (Some(v.0), Some(v.1))).unwrap_or((None, None));
        let (ask_px, ask_sz) = ask.map(|v| (Some(v.0), Some(v.1))).unwrap_or((None, None));
        (bid_px, bid_sz, ask_px, ask_sz)
    }

    fn microprice(&self) -> Option<f64> {
        let (bid_px, bid_sz, ask_px, ask_sz) = self.best_bid_ask_size();
        let (bid_px, bid_sz, ask_px, ask_sz) = match (bid_px, bid_sz, ask_px, ask_sz) {
            (Some(bid_px), Some(bid_sz), Some(ask_px), Some(ask_sz)) => {
                (bid_px, bid_sz, ask_px, ask_sz)
            }
            _ => return None,
        };
        let denom = bid_sz + ask_sz;
        if denom <= 0.0 {
            return None;
        }
        Some((bid_px * ask_sz + ask_px * bid_sz) / denom)
    }

    fn depth_top_n(&self, side: BookSide, n: usize) -> Option<f64> {
        let mut sum = 0.0;
        let mut count = 0usize;
        match side {
            BookSide::Bid => {
                let mut idx = match self.best_bid_idx {
                    Some(idx) => idx,
                    None => return None,
                };
                loop {
                    let size = self.bids[idx];
                    if size > 0 {
                        sum += idx_to_price(idx) * size_to_float(size);
                        count += 1;
                        if count >= n {
                            break;
                        }
                    }
                    if idx == 0 {
                        break;
                    }
                    idx -= 1;
                }
            }
            BookSide::Ask => {
                let mut idx = match self.best_ask_idx {
                    Some(idx) => idx,
                    None => return None,
                };
                loop {
                    let size = self.asks[idx];
                    if size > 0 {
                        sum += idx_to_price(idx) * size_to_float(size);
                        count += 1;
                        if count >= n {
                            break;
                        }
                    }
                    if idx >= PRICE_MAX_IDX {
                        break;
                    }
                    idx += 1;
                }
            }
        }
        if count == 0 {
            None
        } else {
            Some(sum)
        }
    }

    fn top_levels_notional(&self, side: BookSide, levels: usize) -> Vec<f64> {
        let mut out = Vec::with_capacity(levels);
        match side {
            BookSide::Bid => {
                let mut idx = match self.best_bid_idx {
                    Some(idx) => idx,
                    None => {
                        out.resize(levels, 0.0);
                        return out;
                    }
                };
                loop {
                    let size = self.bids[idx];
                    if size > 0 {
                        out.push(idx_to_price(idx) * size_to_float(size));
                        if out.len() >= levels {
                            break;
                        }
                    }
                    if idx == 0 {
                        break;
                    }
                    idx -= 1;
                }
            }
            BookSide::Ask => {
                let mut idx = match self.best_ask_idx {
                    Some(idx) => idx,
                    None => {
                        out.resize(levels, 0.0);
                        return out;
                    }
                };
                loop {
                    let size = self.asks[idx];
                    if size > 0 {
                        out.push(idx_to_price(idx) * size_to_float(size));
                        if out.len() >= levels {
                            break;
                        }
                    }
                    if idx >= PRICE_MAX_IDX {
                        break;
                    }
                    idx += 1;
                }
            }
        }
        if out.len() < levels {
            out.resize(levels, 0.0);
        }
        out
    }

    fn depth_1pct(&self, side: BookSide, mid: Option<f64>) -> Option<f64> {
        let mid = mid?;
        let (low, high) = (mid * 0.99, mid * 1.01);
        let mut sum = 0.0;
        let mut count = 0usize;
        match side {
            BookSide::Bid => {
                let mut idx = self.best_bid_idx?;
                loop {
                    let price = idx_to_price(idx);
                    if price < low {
                        break;
                    }
                    let size = self.bids[idx];
                    if size > 0 {
                        sum += price * size_to_float(size);
                        count += 1;
                    }
                    if idx == 0 {
                        break;
                    }
                    idx -= 1;
                }
            }
            BookSide::Ask => {
                let mut idx = self.best_ask_idx?;
                loop {
                    let price = idx_to_price(idx);
                    if price > high {
                        break;
                    }
                    let size = self.asks[idx];
                    if size > 0 {
                        sum += price * size_to_float(size);
                        count += 1;
                    }
                    if idx >= PRICE_MAX_IDX {
                        break;
                    }
                    idx += 1;
                }
            }
        }
        if count == 0 {
            None
        } else {
            Some(sum)
        }
    }

    fn size_at(&self, side: BookSide, price: f64) -> f64 {
        let idx = match price_to_idx(price) {
            Some(idx) => idx,
            None => return 0.0,
        };
        let size = match side {
            BookSide::Bid => self.bids[idx],
            BookSide::Ask => self.asks[idx],
        };
        size_to_float(size)
    }

    fn median_size_top_n(&self, side: BookSide, n: usize) -> Option<f64> {
        let mut sizes: Vec<f64> = Vec::with_capacity(n);
        match side {
            BookSide::Bid => {
                let mut idx = self.best_bid_idx?;
                loop {
                    let size = self.bids[idx];
                    if size > 0 {
                        sizes.push(size_to_float(size));
                        if sizes.len() >= n {
                            break;
                        }
                    }
                    if idx == 0 {
                        break;
                    }
                    idx -= 1;
                }
            }
            BookSide::Ask => {
                let mut idx = self.best_ask_idx?;
                loop {
                    let size = self.asks[idx];
                    if size > 0 {
                        sizes.push(size_to_float(size));
                        if sizes.len() >= n {
                            break;
                        }
                    }
                    if idx >= PRICE_MAX_IDX {
                        break;
                    }
                    idx += 1;
                }
            }
        }
        if sizes.is_empty() {
            return None;
        }
        sizes.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = sizes.len() / 2;
        sizes.get(idx).copied()
    }

    fn check_wall_pull(&self, side: BookSide, price: f64, new_size: f64) -> Option<WallPullSignal> {
        let tick = self.tick_px?;
        if tick <= 0.0 {
            return None;
        }
        let (best_bid, best_ask) = self.best_bid_ask();
        let touch = match side {
            BookSide::Bid => best_bid?,
            BookSide::Ask => best_ask?,
        };
        if (price - touch).abs() > 2.0 * tick {
            return None;
        }
        let prev_size = self.size_at(side, price);
        if prev_size <= 0.0 {
            return None;
        }
        if new_size > prev_size * 0.1 {
            return None;
        }
        let median = self.median_size_top_n(side, 10)?;
        if median <= 0.0 {
            return None;
        }
        if prev_size < 10.0 * median {
            return None;
        }
        let pull_ratio = (prev_size - new_size) / prev_size;
        Some(WallPullSignal {
            side,
            price,
            size_before: prev_size,
            pull_ratio,
        })
    }

    fn air_pocket_ticks(&self, tick_px: Option<f64>) -> (Option<i64>, Option<i64>) {
        let tick = match tick_px {
            Some(tick) if tick > 0.0 => tick,
            _ => return (None, None),
        };
        let bid_ticks = match self.best_bid_idx {
            Some(best) => match self.find_next_bid(best) {
                Some(second) => {
                    Some(((idx_to_price(best) - idx_to_price(second)) / tick).round() as i64)
                }
                None => None,
            },
            None => None,
        };
        let ask_ticks = match self.best_ask_idx {
            Some(best) => match self.find_next_ask(best) {
                Some(second) => {
                    Some(((idx_to_price(second) - idx_to_price(best)) / tick).round() as i64)
                }
                None => None,
            },
            None => None,
        };
        (bid_ticks, ask_ticks)
    }

    fn topn_hash(&self, n: usize) -> Option<String> {
        if self.levels_len() == 0 {
            return None;
        }
        let mut hasher: u64 = 0xcbf29ce484222325;
        let mut count = 0usize;
        if let Some(mut idx) = self.best_bid_idx {
            loop {
                let size = self.bids[idx];
                if size > 0 {
                    hasher = hash_pair(hasher, idx as i64, size);
                    count += 1;
                    if count >= n {
                        break;
                    }
                }
                if idx == 0 {
                    break;
                }
                idx -= 1;
            }
        }
        count = 0;
        if let Some(mut idx) = self.best_ask_idx {
            loop {
                let size = self.asks[idx];
                if size > 0 {
                    hasher = hash_pair(hasher, idx as i64, size);
                    count += 1;
                    if count >= n {
                        break;
                    }
                }
                if idx >= PRICE_MAX_IDX {
                    break;
                }
                idx += 1;
            }
        }
        Some(format!("{:x}", hasher))
    }
}

fn hash_pair(mut hash: u64, price_idx: i64, size: i64) -> u64 {
    for byte in price_idx.to_le_bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    for byte in size.to_le_bytes() {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

fn price_to_idx(value: f64) -> Option<usize> {
    if !value.is_finite() {
        return None;
    }
    let idx = (value * PRICE_SCALE).round() as i64;
    if idx < 0 || idx > PRICE_MAX_IDX as i64 {
        return None;
    }
    Some(idx as usize)
}

fn idx_to_price(idx: usize) -> f64 {
    idx as f64 / PRICE_SCALE
}

fn size_to_int(value: f64) -> Option<i64> {
    if !value.is_finite() {
        return None;
    }
    Some((value * SIZE_SCALE).round() as i64)
}

fn size_to_float(value: i64) -> f64 {
    value as f64 / SIZE_SCALE
}

#[derive(Default)]
struct SecondAccumulator {
    event_count: i64,
    critical_event_count: i64,
    book_updates_1s: i64,
    add_updates_1s: i64,
    cancel_updates_1s: i64,
    match_updates_1s: i64,
    remove_unknown_updates_1s: i64,
    trade_count_1s: i64,
    volume_shares_1s: f64,
    volume_notional_1s: f64,
    buy_notional_1s: f64,
    sell_notional_1s: f64,
    cvd_delta_1s: f64,
    trade_side_missing_count_1s: i64,
    ofi_1s: f64,
    ofi_250ms: f64,
    bid_delta_notional_250ms: f64,
    ask_delta_notional_250ms: f64,
    mid_slope_250ms: Option<f64>,
    levels_crossed_1s: i64,
    depth_withdrawal_ratio: Option<f64>,
    max_trade_notional_1s: f64,
    buy_notional_250ms: f64,
    sell_notional_250ms: f64,
    max_trade_notional_250ms: f64,
    wall_pull_signal: Option<WallPullSignal>,
    ts_missing_count_1s: i64,
    ts_anomaly_count_1s: i64,
    venue_ws_count: i64,
    venue_rest_count: i64,
    est_count: i64,
    fallback_count: i64,
    venue_latency: LatencyHistogram,
    est_latency: LatencyHistogram,
}

impl SecondAccumulator {
    fn record_latency(&mut self, source: ExchangeTsSource, local_ts_ms: i64, exchange_ts_ms: i64) {
        self.event_count += 1;
        match source {
            ExchangeTsSource::VenueWs | ExchangeTsSource::VenueRest => {
                let latency = (local_ts_ms - exchange_ts_ms).max(0) as u64;
                self.venue_latency.record(latency);
                if source == ExchangeTsSource::VenueWs {
                    self.venue_ws_count += 1;
                } else {
                    self.venue_rest_count += 1;
                }
            }
            ExchangeTsSource::Estimated => {
                let latency = (local_ts_ms - exchange_ts_ms).max(0) as u64;
                self.est_latency.record(latency);
                self.est_count += 1;
            }
            ExchangeTsSource::LocalFallback => {
                self.fallback_count += 1;
            }
        }
    }

    fn vwap(&self) -> Option<f64> {
        if self.volume_shares_1s <= 0.0 {
            return None;
        }
        Some(self.volume_notional_1s / self.volume_shares_1s)
    }

    fn dominant_source(&self) -> ExchangeTsSource {
        let mut best = ExchangeTsSource::LocalFallback;
        let mut best_count = self.fallback_count;
        if self.est_count > best_count {
            best = ExchangeTsSource::Estimated;
            best_count = self.est_count;
        }
        if self.venue_rest_count > best_count {
            best = ExchangeTsSource::VenueRest;
            best_count = self.venue_rest_count;
        }
        if self.venue_ws_count > best_count {
            best = ExchangeTsSource::VenueWs;
        }
        best
    }
}

#[derive(Default)]
struct LatencyHistogram {
    hist: Option<Histogram<u64>>, // lazy init
}

impl LatencyHistogram {
    fn record(&mut self, value: u64) {
        if self.hist.is_none() {
            self.hist = Histogram::new(3).ok();
        }
        if let Some(hist) = &mut self.hist {
            let _ = hist.record(value);
        }
    }

    fn snapshot(&self) -> (Option<i64>, Option<i64>, Option<i64>, Option<i64>) {
        if let Some(hist) = &self.hist {
            if hist.len() == 0 {
                return (None, None, None, None);
            }
            let p50 = hist.value_at_quantile(0.50) as i64;
            let p90 = hist.value_at_quantile(0.90) as i64;
            let p99 = hist.value_at_quantile(0.99) as i64;
            let max = hist.max() as i64;
            return (Some(p50), Some(p90), Some(p99), Some(max));
        }
        (None, None, None, None)
    }
}

#[derive(Default)]
struct RollingWindow {
    values: std::collections::VecDeque<f64>,
    max_len: usize,
}

impl RollingWindow {
    fn new(max_len: usize) -> Self {
        RollingWindow {
            values: std::collections::VecDeque::new(),
            max_len,
        }
    }

    fn push(&mut self, value: f64) {
        if self.values.len() >= self.max_len {
            self.values.pop_front();
        }
        self.values.push_back(value);
    }

    fn quantile(&self, q: f64) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }
        let mut data: Vec<f64> = self.values.iter().copied().collect();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((data.len() as f64 - 1.0) * q).round() as usize;
        data.get(idx).copied()
    }
}

fn sum_last_f64(values: &std::collections::VecDeque<f64>, count: usize) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sum = 0.0;
    let mut seen = 0usize;
    for value in values.iter().rev() {
        sum += *value;
        seen += 1;
        if seen >= count {
            break;
        }
    }
    Some(sum)
}

fn zscore_from_window(values: &std::collections::VecDeque<f64>, latest: f64) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let mean = values.iter().copied().sum::<f64>() / values.len() as f64;
    let mut var = 0.0;
    for value in values {
        var += (value - mean) * (value - mean);
    }
    let std = (var / values.len() as f64).sqrt();
    if std <= 0.0 {
        None
    } else {
        Some((latest - mean) / std)
    }
}
