use crate::clock::ClockOffset;
use crate::db_queue::{try_send_db, DbMessage, DbSender};
use crate::dispatcher::DispatcherMessage;
use crate::mem_stats;
use crate::models::{
    BookLevel, ClockOffsetSample, Event, EventFlags, EventKind, EventPayload, ExchangeTsSource,
    TokenSide, Venue,
};
use crate::time_utils::now_ts_ms;
use futures_util::{
    future,
    stream::{self, StreamExt},
};
use rand::Rng;
use reqwest::Client;
use serde::Deserialize;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{Duration, Instant};
use tracing::{info, warn};

const BOOTSTRAP_LOG_EVERY: usize = 50;
const FETCH_TRACE_SAMPLES: usize = 3;
static FETCH_TRACE_COUNTER: AtomicUsize = AtomicUsize::new(0);
const PRICE_EPS: f64 = 1e-6;
const TS_SKEW_LOG_INTERVAL_MS: i64 = 60_000;
const COOLDOWN_LOG_INTERVAL_MS: i64 = 60_000;
const OPI_REST_SLOW_MS: u128 = 1_500;
const OPI_REST_RATE_WAIT_WARN_MS: u128 = 200;
const REST_BACKOFF_MIN_MS: i64 = 2_000;
const REST_BACKOFF_MAX_MS: i64 = 60_000;
const REST_BACKOFF_JITTER_MIN: f64 = 0.5;
const REST_BACKOFF_JITTER_MAX: f64 = 1.5;
const REST_BACKOFF_RESET_MS: i64 = 120_000;
const REST_GLOBAL_BACKOFF_MIN_MS: i64 = 5_000;
const REST_GLOBAL_BACKOFF_MAX_MS: i64 = 120_000;
const REST_GLOBAL_BACKOFF_JITTER_MIN: f64 = 0.5;
const REST_GLOBAL_BACKOFF_JITTER_MAX: f64 = 1.5;
const REST_GLOBAL_BACKOFF_RESET_MS: i64 = 300_000;
const REST_RECONNECT_SUPPRESS_MS: i64 = 15_000;
const GAPFILL_PRIORITY_WINDOW_MS: i64 = 15 * 60 * 1000;
const PRIORITY_TOKEN_WINDOW_MS: i64 = 60_000;
static TS_SKEW_LOG_CACHE: OnceLock<StdMutex<HashMap<String, i64>>> = OnceLock::new();
static COOLDOWN_LOG_CACHE: OnceLock<StdMutex<HashMap<String, i64>>> = OnceLock::new();

#[derive(Debug, Clone, Copy)]
enum RestBucket {
    Active,
    Inactive,
    All,
}

impl RestBucket {
    fn label(self) -> &'static str {
        match self {
            RestBucket::Active => "active",
            RestBucket::Inactive => "inactive",
            RestBucket::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RestErrorKind {
    Timeout,
    HttpStatus,
    Parse,
    Empty,
    Other,
}

#[derive(Debug)]
struct RestFetchError {
    kind: RestErrorKind,
    error: anyhow::Error,
}

impl RestFetchError {
    fn new(kind: RestErrorKind, error: anyhow::Error) -> Self {
        Self { kind, error }
    }
}

#[derive(Debug)]
struct RestFetchResult {
    levels_total: usize,
    rest_latency_ms: u128,
    market_staleness_ms: Option<i64>,
    payload_ts_missing: bool,
}

#[derive(Debug, Clone, Copy)]
struct RestBackoffState {
    backoff_ms: i64,
    until_ms: i64,
    last_fail_ms: i64,
    failures: i64,
}

#[derive(Debug)]
struct LatestPriceResult {
    is_changed: bool,
    rest_latency_ms: u128,
    market_staleness_ms: Option<i64>,
    payload_ts_missing: bool,
}

#[derive(Clone)]
pub struct OpinionSnapshotTarget {
    pub token_key: String,
    pub token_id: String,
}

#[derive(Debug)]
pub enum SnapshotCommand {
    TriggerAll { reason: String },
    TriggerInactive { reason: String, cutoff_ms: i64 },
    WsActivity {
        token_key: String,
        last_price: bool,
        book_update: bool,
        payload_ts_ms: Option<i64>,
    },
    TriggerToken { token_key: String, reason: String },
}

async fn filter_orderbook_targets(
    targets: Vec<OpinionSnapshotTarget>,
    now_ms: i64,
    ws_book_stale_ms: i64,
    payload_staleness_ms: i64,
    last_ws_book_ms: &Arc<Mutex<HashMap<String, i64>>>,
    last_payload_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
) -> (Vec<OpinionSnapshotTarget>, usize) {
    if ws_book_stale_ms <= 0 && payload_staleness_ms <= 0 {
        return (targets, 0);
    }
    let mut filtered = Vec::with_capacity(targets.len());
    let mut skipped = 0usize;
    let last_book_cache = last_ws_book_ms.lock().await;
    let payload_cache = last_payload_ts_ms.lock().await;
    for target in targets {
        let last_book_ms = last_book_cache
            .get(&target.token_key)
            .copied()
            .unwrap_or(0);
        let book_stale = if ws_book_stale_ms <= 0 {
            false
        } else {
            last_book_ms == 0 || now_ms.saturating_sub(last_book_ms) > ws_book_stale_ms
        };
        let last_payload_ms = payload_cache
            .get(&target.token_key)
            .copied()
            .unwrap_or(0);
        let payload_stale = if payload_staleness_ms <= 0 {
            false
        } else {
            last_payload_ms == 0 || now_ms.saturating_sub(last_payload_ms) > payload_staleness_ms
        };
        if book_stale || payload_stale {
            filtered.push(target);
        } else {
            skipped += 1;
        }
    }
    (filtered, skipped)
}

pub async fn run_opinion_snapshotter(
    base_url: String,
    api_key: String,
    targets: Vec<OpinionSnapshotTarget>,
    clock_offset: Arc<ClockOffset>,
    dispatcher: mpsc::Sender<DispatcherMessage>,
    db_sender: DbSender,
    bootstrap_done: Option<Arc<AtomicBool>>,
    interval_min: u64,
    bootstrap_concurrency: usize,
    max_rps: f64,
    bootstrap_batch_size: usize,
    bootstrap_batch_delay_ms: u64,
    bootstrap_max_rps: f64,
    trigger_batch_size: usize,
    trigger_batch_delay_ms: u64,
    trigger_max_rps: f64,
    ws_connect_grace_ms: u64,
    fast_interval_sec: u64,
    inactive_sec: u64,
    latest_price_interval_sec: u64,
    latest_price_all: bool,
    latest_price_ws_stale_sec: u64,
    latest_price_book_stale_sec: u64,
    orderbook_ws_stale_sec: u64,
    orderbook_staleness_ms: i64,
    rest_active_only: bool,
    rest_ws_primary: bool,
    stale_cooldown_threshold_ms: i64,
    stale_cooldown_duration_sec: u64,
    active_window_sec: u64,
    opinion_ts_max_skew_ms: i64,
    mut trigger_rx: mpsc::Receiver<SnapshotCommand>,
) {
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| Client::new());
    let mut interval = tokio::time::interval(Duration::from_secs(interval_min * 60));
    let mut fast_interval = if fast_interval_sec > 0 {
        Some(tokio::time::interval(Duration::from_secs(
            fast_interval_sec,
        )))
    } else {
        None
    };
    let mut latest_price_interval = if latest_price_interval_sec > 0 {
        Some(tokio::time::interval(Duration::from_secs(
            latest_price_interval_sec,
        )))
    } else {
        None
    };
    let targets_by_key: HashMap<String, OpinionSnapshotTarget> = targets
        .iter()
        .cloned()
        .map(|target| (target.token_key.clone(), target))
        .collect();
    let last_ws_seen_ms: Arc<Mutex<HashMap<String, i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let last_ws_last_price_ms: Arc<Mutex<HashMap<String, i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let last_ws_book_ms: Arc<Mutex<HashMap<String, i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let last_payload_ts_ms: Arc<Mutex<HashMap<String, i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let last_latest_price_fetch_ms: Arc<Mutex<HashMap<String, i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    // Use local arrival timestamps for activity classification (separate from payload freshness).
    let last_arrival_ts_ms: Arc<Mutex<HashMap<String, i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let cooldown_until_ms: Arc<Mutex<HashMap<String, i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let rest_backoff: Arc<Mutex<HashMap<String, RestBackoffState>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let rest_global_backoff: Arc<Mutex<RestBackoffState>> =
        Arc::new(Mutex::new(RestBackoffState {
            backoff_ms: 0,
            until_ms: 0,
            last_fail_ms: 0,
            failures: 0,
        }));
    let reconnect_suppress_until_ms: Arc<Mutex<i64>> = Arc::new(Mutex::new(0));
    let priority_until_ms: Arc<Mutex<HashMap<String, i64>>> =
        Arc::new(Mutex::new(HashMap::new()));
    let active_window_ms = (active_window_sec as i64).saturating_mul(1000);
    let cooldown_threshold_ms = stale_cooldown_threshold_ms.max(0);
    let cooldown_duration_ms = (stale_cooldown_duration_sec as i64).saturating_mul(1000);
    let ws_last_price_stale_ms = (latest_price_ws_stale_sec as i64).saturating_mul(1000);
    let ws_book_stale_ms = (latest_price_book_stale_sec as i64).saturating_mul(1000);
    let orderbook_ws_stale_ms = (orderbook_ws_stale_sec as i64).saturating_mul(1000);
    let orderbook_staleness_ms = orderbook_staleness_ms.max(0);
    let cooldown_enabled = cooldown_threshold_ms > 0 && cooldown_duration_ms > 0;
    let log_snapshots = std::env::var("MONITOR_LOG_REST_SNAPSHOTS").ok().as_deref() == Some("1");
    let concurrency = bootstrap_concurrency.max(1);
    let shared_max_rps = match (max_rps > 0.0, trigger_max_rps > 0.0) {
        (true, true) => max_rps.min(trigger_max_rps),
        (true, false) => max_rps,
        (false, true) => trigger_max_rps,
        (false, false) => 0.0,
    };
    let rate_limiter = RateLimiter::new(shared_max_rps).map(Arc::new);
    let bootstrap_rate_limiter = RateLimiter::new(bootstrap_max_rps).map(Arc::new);
    let trigger_rate_limiter = rate_limiter.clone();
    if let Some(flag) = bootstrap_done.as_ref() {
        flag.store(false, Ordering::Relaxed);
    }

    info!(
        "opinion_rest bootstrap start targets_len={} concurrency={} max_rps={} shared_max_rps={} bootstrap_max_rps={} batch_size={} batch_delay_ms={}",
        targets.len(),
        concurrency,
        max_rps,
        shared_max_rps,
        bootstrap_max_rps,
        bootstrap_batch_size,
        bootstrap_batch_delay_ms
    );
    let latest_price_budget = if latest_price_interval_sec > 0 && shared_max_rps > 0.0 {
        let budget = (shared_max_rps * latest_price_interval_sec as f64).floor() as usize;
        Some(budget.max(1))
    } else {
        None
    };
    let latest_price_budget_log = latest_price_budget.unwrap_or(0);
    let latest_price_max_inflight = if shared_max_rps > 0.0 {
        ((shared_max_rps * 2.0).ceil() as usize).clamp(8, 64)
    } else {
        128
    };
    info!(
        "opinion_rest latest_price_config interval_sec={} latest_price_all={} ws_last_price_stale_sec={} ws_book_stale_sec={} active_only={} ws_primary={} inactive_sec={} active_window_sec={} cooldown_enabled={} cooldown_threshold_ms={} cooldown_duration_ms={} orderbook_ws_stale_sec={} orderbook_staleness_ms={} latest_price_budget={} latest_price_max_inflight={}",
        latest_price_interval_sec,
        latest_price_all,
        latest_price_ws_stale_sec,
        latest_price_book_stale_sec,
        rest_active_only,
        rest_ws_primary,
        inactive_sec,
        active_window_sec,
        cooldown_enabled,
        cooldown_threshold_ms,
        cooldown_duration_ms,
        orderbook_ws_stale_sec,
        orderbook_staleness_ms,
        latest_price_budget_log,
        latest_price_max_inflight
    );
    info!(
        "opinion_rest trigger_config batch_size={} batch_delay_ms={} trigger_max_rps={} shared_max_rps={} ws_connect_grace_ms={}",
        trigger_batch_size,
        trigger_batch_delay_ms,
        trigger_max_rps,
        shared_max_rps,
        ws_connect_grace_ms
    );
    mem_stats::snapshot("before_opinion_rest_bootstrap");
    let now_ms = now_ts_ms();
    let (bootstrap_targets, bootstrap_stale_skipped) = filter_orderbook_targets(
        targets.clone(),
        now_ms,
        orderbook_ws_stale_ms,
        orderbook_staleness_ms,
        &last_ws_book_ms,
        &last_payload_ts_ms,
    )
    .await;
    if bootstrap_stale_skipped > 0 {
        info!(
            "opinion_rest bootstrap stale_filter_skipped={} targets_len={}",
            bootstrap_stale_skipped,
            bootstrap_targets.len()
        );
    }
    let bootstrap_batch_size = if bootstrap_batch_size == 0 {
        bootstrap_targets.len().max(1)
    } else {
        bootstrap_batch_size
    };
    let bootstrap_rate_limiter_ref = bootstrap_rate_limiter.as_ref().or(rate_limiter.as_ref());
    let bootstrap = run_snapshot_round_batched(
        "bootstrap",
        &bootstrap_targets,
        &client,
        &base_url,
        &api_key,
        &clock_offset,
        &dispatcher,
        &db_sender,
        log_snapshots,
        concurrency,
        bootstrap_rate_limiter_ref,
        &rest_backoff,
        &rest_global_backoff,
        &priority_until_ms,
        bootstrap_batch_size,
        bootstrap_batch_delay_ms,
        &last_payload_ts_ms,
        &last_arrival_ts_ms,
        &cooldown_until_ms,
        cooldown_threshold_ms,
        cooldown_duration_ms,
        opinion_ts_max_skew_ms,
        RestBucket::All,
    )
    .await;
    info!(
        "opinion_rest bootstrap done snapshots_ok={} total_levels={}",
        bootstrap.successes, bootstrap.total_levels
    );
    if let Some(flag) = bootstrap_done.as_ref() {
        flag.store(true, Ordering::Relaxed);
    }
    mem_stats::snapshot("after_opinion_rest_bootstrap");
    let last_latest_price: Arc<Mutex<HashMap<String, f64>>> =
        Arc::new(Mutex::new(HashMap::new()));

    if latest_price_interval_sec > 0 {
        let latest_targets = targets.clone();
        let client = client.clone();
        let base_url = base_url.clone();
        let api_key = api_key.clone();
        let clock_offset = clock_offset.clone();
        let dispatcher = dispatcher.clone();
        let db_sender = db_sender.clone();
        let rate_limiter = rate_limiter.clone();
        let last_ws_seen_ms = last_ws_seen_ms.clone();
        let last_ws_last_price_ms = last_ws_last_price_ms.clone();
        let last_ws_book_ms = last_ws_book_ms.clone();
        let last_payload_ts_ms = last_payload_ts_ms.clone();
        let last_arrival_ts_ms = last_arrival_ts_ms.clone();
        let last_latest_price = last_latest_price.clone();
        let last_latest_price_fetch_ms = last_latest_price_fetch_ms.clone();
        let cooldown_until_ms = cooldown_until_ms.clone();
        let rest_backoff = rest_backoff.clone();
        let rest_global_backoff = rest_global_backoff.clone();
        let priority_until_ms = priority_until_ms.clone();
        let rest_active_only = rest_active_only;
        let rest_ws_primary = rest_ws_primary;
        let cooldown_threshold_ms = cooldown_threshold_ms;
        let cooldown_duration_ms = cooldown_duration_ms;
        let ws_last_price_stale_ms = ws_last_price_stale_ms;
        let ws_book_stale_ms = ws_book_stale_ms;
        let latest_price_budget = latest_price_budget;
        let latest_price_max_inflight = latest_price_max_inflight;

        tokio::spawn(async move {
            let mut latest_price_interval =
                tokio::time::interval(Duration::from_secs(latest_price_interval_sec));
            let mut latest_price_tick_count: u64 = 0;
            let mut latest_price_refresh_count: u64 = 0;
            let mut latest_price_skip_count: u64 = 0;
            let mut latest_price_last_tick_ms: i64 = 0;
            let mut latest_price_last_refresh_ms: i64 = 0;
            let mut latest_price_stall_warned = false;

            loop {
                latest_price_interval.tick().await;
                let now_ms = now_ts_ms();
                if let Some(until_ms) =
                    rest_global_backoff_until(now_ms, &rest_global_backoff).await
                {
                    if latest_price_interval_sec > 0 && latest_price_last_tick_ms > 0 {
                        let since_ms = now_ms.saturating_sub(latest_price_last_tick_ms);
                        if since_ms > (latest_price_interval_sec as i64 * 2000)
                            && !latest_price_stall_warned
                        {
                            warn!(
                                "opinion_rest latest_price_tick_stalled last_tick_ms={} since_ms={} interval_sec={}",
                                latest_price_last_tick_ms,
                                since_ms,
                                latest_price_interval_sec
                            );
                            latest_price_stall_warned = true;
                        }
                    }
                    latest_price_tick_count += 1;
                    latest_price_last_tick_ms = now_ms;
                    latest_price_stall_warned = false;
                    latest_price_skip_count += 1;
                    info!(
                        "opinion_rest latest_price_refresh_skipped_global_backoff tick_count={} skip_count={} backoff_until_ms={}",
                        latest_price_tick_count,
                        latest_price_skip_count,
                        until_ms
                    );
                    continue;
                }
                let cutoff_ms = now_ms.saturating_sub((inactive_sec as i64) * 1000);
                let (latest_targets_batch, active_count, inactive_count, unseen_count) = {
                    let last_ws_seen_ms = last_ws_seen_ms.lock().await;
                    let last_arrival_ts_ms = last_arrival_ts_ms.lock().await;
                    let (active_targets, inactive_targets, unseen) = partition_targets_by_activity(
                        &latest_targets,
                        &last_ws_seen_ms,
                        &last_arrival_ts_ms,
                        cutoff_ms,
                        rest_ws_primary,
                    );
                    let active_count = active_targets.len();
                    let inactive_count = inactive_targets.len();
                    let batch = if rest_active_only {
                        active_targets
                    } else if latest_price_all {
                        latest_targets.clone()
                    } else {
                        inactive_targets
                    };
                    (batch, active_count, inactive_count, unseen)
                };

                let (latest_targets_batch, price_recent_skipped, book_recent_skipped) = {
                    let mut price_skipped = 0usize;
                    let mut book_skipped = 0usize;
                    let mut filtered = Vec::with_capacity(latest_targets_batch.len());
                    let last_price_cache = last_ws_last_price_ms.lock().await;
                    let last_book_cache = last_ws_book_ms.lock().await;
                    for target in latest_targets_batch {
                        let last_price_ms =
                            last_price_cache.get(&target.token_key).copied().unwrap_or(0);
                        let last_book_ms =
                            last_book_cache.get(&target.token_key).copied().unwrap_or(0);
                        let price_stale = if ws_last_price_stale_ms <= 0 {
                            true
                        } else {
                            last_price_ms == 0
                                || now_ms.saturating_sub(last_price_ms) > ws_last_price_stale_ms
                        };
                        let book_stale = if ws_book_stale_ms <= 0 {
                            true
                        } else {
                            last_book_ms == 0
                                || now_ms.saturating_sub(last_book_ms) > ws_book_stale_ms
                        };
                        if price_stale && book_stale {
                            filtered.push(target);
                        } else {
                            if !price_stale {
                                price_skipped += 1;
                            }
                            if !book_stale {
                                book_skipped += 1;
                            }
                        }
                    }
                    (filtered, price_skipped, book_skipped)
                };

                let (latest_targets_batch, cooldown_skipped) = {
                    let mut skipped = 0usize;
                    let mut filtered = Vec::with_capacity(latest_targets_batch.len());
                    let mut cooldown_cache = cooldown_until_ms.lock().await;
                    for target in latest_targets_batch {
                        match cooldown_cache.get(&target.token_key).copied() {
                            Some(until_ms) if until_ms > now_ms => {
                                skipped += 1;
                            }
                            Some(_) => {
                                cooldown_cache.remove(&target.token_key);
                                filtered.push(target);
                            }
                            None => filtered.push(target),
                        }
                    }
                    (filtered, skipped)
                };
                let (latest_targets_batch, backoff_skipped) = filter_targets_by_backoff(
                    latest_targets_batch,
                    now_ms,
                    &rest_backoff,
                    &priority_until_ms,
                    false,
                )
                .await;
                let (latest_targets_batch, budget_skipped) = match latest_price_budget {
                    None => (latest_targets_batch, 0usize),
                    Some(budget) => {
                        if latest_targets_batch.len() <= budget {
                            (latest_targets_batch, 0usize)
                        } else {
                            let last_fetch = last_latest_price_fetch_ms.lock().await;
                            let mut scored: Vec<(i64, OpinionSnapshotTarget)> =
                                latest_targets_batch
                                    .into_iter()
                                    .map(|target| {
                                        (
                                            last_fetch
                                                .get(&target.token_key)
                                                .copied()
                                                .unwrap_or(0),
                                            target,
                                        )
                                    })
                                    .collect();
                            drop(last_fetch);
                            scored.sort_by(|a, b| {
                                a.0.cmp(&b.0)
                                    .then_with(|| a.1.token_key.cmp(&b.1.token_key))
                            });
                            let total = scored.len();
                            let keep = budget.min(total);
                            let mut limited = Vec::with_capacity(keep);
                            for (_, target) in scored.into_iter().take(keep) {
                                limited.push(target);
                            }
                            let skipped = total.saturating_sub(keep);
                            (limited, skipped)
                        }
                    }
                };

                if latest_price_interval_sec > 0 && latest_price_last_tick_ms > 0 {
                    let since_ms = now_ms.saturating_sub(latest_price_last_tick_ms);
                    if since_ms > (latest_price_interval_sec as i64 * 2000)
                        && !latest_price_stall_warned
                    {
                        warn!(
                            "opinion_rest latest_price_tick_stalled last_tick_ms={} since_ms={} interval_sec={}",
                            latest_price_last_tick_ms,
                            since_ms,
                            latest_price_interval_sec
                        );
                        latest_price_stall_warned = true;
                    }
                }

                latest_price_tick_count += 1;
                latest_price_last_tick_ms = now_ms;
                latest_price_stall_warned = false;
                let mode = if rest_active_only {
                    "active_only"
                } else if latest_price_all {
                    "all"
                } else {
                    "inactive"
                };
                info!(
                    "opinion_rest latest_price_tick tick_count={} targets_total={} targets_len={} mode={} active={} inactive={} unseen={} price_recent_skipped={} book_recent_skipped={} cooldown_skipped={} backoff_skipped={} budget_skipped={}",
                    latest_price_tick_count,
                    latest_targets.len(),
                    latest_targets_batch.len(),
                    mode,
                    active_count,
                    inactive_count,
                    unseen_count,
                    price_recent_skipped,
                    book_recent_skipped,
                    cooldown_skipped,
                    backoff_skipped,
                    budget_skipped
                );

                if !latest_targets_batch.is_empty() {
                    {
                        let mut fetch_cache = last_latest_price_fetch_ms.lock().await;
                        for target in &latest_targets_batch {
                            fetch_cache.insert(target.token_key.clone(), now_ms);
                        }
                    }
                    let bucket = if rest_active_only {
                        RestBucket::Active
                    } else if latest_price_all {
                        RestBucket::All
                    } else {
                        RestBucket::Inactive
                    };
                    info!(
                        "opinion_rest latest_price_refresh targets_len={} mode={} inactive_sec={}",
                        latest_targets_batch.len(),
                        mode,
                        inactive_sec
                    );
                    let started_at = Instant::now();
                    let round = run_latest_price_round(
                        "latest_price",
                        &latest_targets_batch,
                        &client,
                        &base_url,
                        &api_key,
                        &clock_offset,
                        &dispatcher,
                        &db_sender,
                        &last_latest_price,
                        latest_price_max_inflight,
                        rate_limiter.as_ref(),
                        &rest_backoff,
                        &rest_global_backoff,
                        &priority_until_ms,
                        &last_payload_ts_ms,
                        &last_arrival_ts_ms,
                        &cooldown_until_ms,
                        cooldown_threshold_ms,
                        cooldown_duration_ms,
                        opinion_ts_max_skew_ms,
                        bucket,
                    )
                    .await;
                    latest_price_refresh_count += 1;
                    latest_price_last_refresh_ms = now_ms;
                    let unchanged = round.successes.saturating_sub(round.changed);
                    info!(
                        "opinion_rest latest_price_round_done targets_len={} success={} changed={} unchanged={} elapsed_ms={} refresh_count={} last_refresh_ms={}",
                        latest_targets_batch.len(),
                        round.successes,
                        round.changed,
                        unchanged,
                        started_at.elapsed().as_millis(),
                        latest_price_refresh_count,
                        latest_price_last_refresh_ms
                    );
                } else {
                    latest_price_skip_count += 1;
                    info!(
                        "opinion_rest latest_price_refresh_skipped tick_count={} skip_count={}",
                        latest_price_tick_count,
                        latest_price_skip_count
                    );
                }
            }
        });
    }

    loop {
        let fast_tick = async {
            if let Some(interval) = fast_interval.as_mut() {
                interval.tick().await;
            } else {
                future::pending::<()>().await;
            }
        };
        tokio::select! {
            _ = interval.tick() => {
                let now_ms = now_ts_ms();
                if let Some(until_ms) = rest_global_backoff_until(now_ms, &rest_global_backoff).await
                {
                    info!(
                        "opinion_rest refresh skipped global_backoff_until_ms={}",
                        until_ms
                    );
                    continue;
                }
                let (active_targets, inactive_targets, unseen_count) = {
                    let last_ws_seen_ms = last_ws_seen_ms.lock().await;
                    let last_arrival_ts_ms = last_arrival_ts_ms.lock().await;
                    partition_targets_by_activity(
                        &targets,
                        &last_ws_seen_ms,
                        &last_arrival_ts_ms,
                        now_ms.saturating_sub(active_window_ms),
                        rest_ws_primary,
                    )
                };
                let (active_targets, cooldown_skipped) = {
                    let mut skipped = 0usize;
                    let mut filtered = Vec::with_capacity(active_targets.len());
                    let mut cooldown_cache = cooldown_until_ms.lock().await;
                    for target in active_targets {
                        match cooldown_cache.get(&target.token_key).copied() {
                            Some(until_ms) if until_ms > now_ms => {
                                skipped += 1;
                            }
                            Some(_) => {
                                cooldown_cache.remove(&target.token_key);
                                filtered.push(target);
                            }
                            None => filtered.push(target),
                        }
                    }
                    (filtered, skipped)
                };
                let (active_targets, stale_skipped) = filter_orderbook_targets(
                    active_targets,
                    now_ms,
                    orderbook_ws_stale_ms,
                    orderbook_staleness_ms,
                    &last_ws_book_ms,
                    &last_payload_ts_ms,
                )
                .await;
                let (active_targets, backoff_skipped) = filter_targets_by_backoff(
                    active_targets,
                    now_ms,
                    &rest_backoff,
                    &priority_until_ms,
                    false,
                )
                .await;
                if !active_targets.is_empty() {
                    let _ = run_snapshot_round(
                        "refresh",
                        &active_targets,
                        &client,
                        &base_url,
                        &api_key,
                        &clock_offset,
                        &dispatcher,
                        &db_sender,
                        false,
                        concurrency,
                        rate_limiter.as_ref(),
                        &rest_backoff,
                        &rest_global_backoff,
                        &priority_until_ms,
                        &last_payload_ts_ms,
                        &last_arrival_ts_ms,
                        &cooldown_until_ms,
                        cooldown_threshold_ms,
                        cooldown_duration_ms,
                        opinion_ts_max_skew_ms,
                        RestBucket::Active,
                    )
                    .await;
                } else {
                    info!(
                        "opinion_rest refresh skipped (no active targets) inactive_len={} unseen={} cooldown_skipped={} stale_skipped={} backoff_skipped={}",
                        inactive_targets.len(),
                        unseen_count,
                        cooldown_skipped,
                        stale_skipped,
                        backoff_skipped
                    );
                }
            }
            _ = fast_tick => {
                let now_ms = now_ts_ms();
                if let Some(until_ms) = rest_global_backoff_until(now_ms, &rest_global_backoff).await
                {
                    info!(
                        "opinion_rest fast_refresh skipped global_backoff_until_ms={}",
                        until_ms
                    );
                    continue;
                }
                if let Some(until_ms) =
                    rest_reconnect_suppressed(now_ms, &reconnect_suppress_until_ms).await
                {
                    info!(
                        "opinion_rest fast_refresh skipped reconnect_suppress_until_ms={}",
                        until_ms
                    );
                    continue;
                }
                let cutoff_ms = now_ms.saturating_sub((inactive_sec as i64) * 1000);
                let (active_targets, inactive_targets, unseen_count) = {
                    let last_ws_seen_ms = last_ws_seen_ms.lock().await;
                    let last_arrival_ts_ms = last_arrival_ts_ms.lock().await;
                    partition_targets_by_activity(
                        &targets,
                        &last_ws_seen_ms,
                        &last_arrival_ts_ms,
                        cutoff_ms,
                        rest_ws_primary,
                    )
                };
                if rest_active_only {
                    info!(
                        "opinion_rest fast_refresh skipped (active_only) active_len={} inactive_len={} unseen={}",
                        active_targets.len(),
                        inactive_targets.len(),
                        unseen_count
                    );
                    continue;
                }
                let (inactive_targets, cooldown_skipped) = {
                    let mut skipped = 0usize;
                    let mut filtered = Vec::with_capacity(inactive_targets.len());
                    let mut cooldown_cache = cooldown_until_ms.lock().await;
                    for target in inactive_targets {
                        match cooldown_cache.get(&target.token_key).copied() {
                            Some(until_ms) if until_ms > now_ms => {
                                skipped += 1;
                            }
                            Some(_) => {
                                cooldown_cache.remove(&target.token_key);
                                filtered.push(target);
                            }
                            None => filtered.push(target),
                        }
                    }
                    (filtered, skipped)
                };
                let (inactive_targets, stale_skipped) = filter_orderbook_targets(
                    inactive_targets,
                    now_ms,
                    orderbook_ws_stale_ms,
                    orderbook_staleness_ms,
                    &last_ws_book_ms,
                    &last_payload_ts_ms,
                )
                .await;
                let (inactive_targets, backoff_skipped) = filter_targets_by_backoff(
                    inactive_targets,
                    now_ms,
                    &rest_backoff,
                    &priority_until_ms,
                    false,
                )
                .await;
                if !inactive_targets.is_empty() {
                    info!(
                        "opinion_rest fast_refresh targets_len={} inactive_sec={} cooldown_skipped={} stale_skipped={} backoff_skipped={}",
                        inactive_targets.len(),
                        inactive_sec,
                        cooldown_skipped,
                        stale_skipped,
                        backoff_skipped
                    );
                    let _ = run_snapshot_round(
                        "fast_refresh",
                        &inactive_targets,
                        &client,
                        &base_url,
                        &api_key,
                        &clock_offset,
                        &dispatcher,
                        &db_sender,
                        false,
                        concurrency,
                        rate_limiter.as_ref(),
                        &rest_backoff,
                        &rest_global_backoff,
                        &priority_until_ms,
                        &last_payload_ts_ms,
                        &last_arrival_ts_ms,
                        &cooldown_until_ms,
                        cooldown_threshold_ms,
                        cooldown_duration_ms,
                        opinion_ts_max_skew_ms,
                        RestBucket::Inactive,
                    )
                    .await;
                }
            }
            Some(cmd) = trigger_rx.recv() => {
                let mut trigger_all = false;
                let mut trigger_all_reason: Option<String> = None;
                let mut trigger_inactive: Option<(String, i64)> = None;
                let mut trigger_tokens: HashMap<String, String> = HashMap::new();
                let now_ms = now_ts_ms();
                match cmd {
                    SnapshotCommand::TriggerAll { reason } => {
                        trigger_all = true;
                        trigger_all_reason = Some(reason.clone());
                        info!("opinion_rest trigger_all received 来源={}", reason);
                        if reason == "ws_connect" || reason == "ws_reconnect" {
                            let until_ms = now_ms.saturating_add(REST_RECONNECT_SUPPRESS_MS);
                            let mut suppress = reconnect_suppress_until_ms.lock().await;
                            if until_ms > *suppress {
                                *suppress = until_ms;
                            }
                            info!(
                                "opinion_rest reconnect_suppress set reason={} until_ms={} suppress_ms={}",
                                reason,
                                until_ms,
                                REST_RECONNECT_SUPPRESS_MS
                            );
                        }
                    }
                    SnapshotCommand::TriggerInactive { reason, cutoff_ms } => {
                        trigger_inactive = Some((reason, cutoff_ms));
                    }
                    SnapshotCommand::WsActivity {
                        token_key,
                        last_price,
                        book_update,
                        payload_ts_ms,
                    } => {
                        if targets_by_key.contains_key(&token_key) {
                            let mut last_ws_seen_ms = last_ws_seen_ms.lock().await;
                            last_ws_seen_ms.insert(token_key.clone(), now_ms);
                            if last_price {
                                let mut last_ws_last_price_ms = last_ws_last_price_ms.lock().await;
                                last_ws_last_price_ms.insert(token_key.clone(), now_ms);
                            }
                            if book_update {
                                let mut last_ws_book_ms = last_ws_book_ms.lock().await;
                                last_ws_book_ms.insert(token_key.clone(), now_ms);
                            }
                            if let Some(payload_ts_ms) = payload_ts_ms {
                                let mut payload_cache = last_payload_ts_ms.lock().await;
                                let entry = payload_cache
                                    .entry(token_key)
                                    .or_insert(payload_ts_ms);
                                if payload_ts_ms > *entry {
                                    *entry = payload_ts_ms;
                                }
                            }
                        }
                    }
                    SnapshotCommand::TriggerToken { token_key, reason } => {
                        mark_priority_token(&token_key, now_ms, &priority_until_ms).await;
                        trigger_tokens.insert(token_key, reason);
                    }
                }
                while let Ok(cmd) = trigger_rx.try_recv() {
                    match cmd {
                        SnapshotCommand::TriggerAll { reason } => {
                            trigger_all = true;
                            trigger_all_reason = Some(reason.clone());
                            info!("opinion_rest trigger_all received 来源={}", reason);
                            if reason == "ws_connect" || reason == "ws_reconnect" {
                                let until_ms = now_ms.saturating_add(REST_RECONNECT_SUPPRESS_MS);
                                let mut suppress = reconnect_suppress_until_ms.lock().await;
                                if until_ms > *suppress {
                                    *suppress = until_ms;
                                }
                                info!(
                                    "opinion_rest reconnect_suppress set reason={} until_ms={} suppress_ms={}",
                                    reason,
                                    until_ms,
                                    REST_RECONNECT_SUPPRESS_MS
                                );
                            }
                        }
                        SnapshotCommand::TriggerInactive { reason, cutoff_ms } => {
                            trigger_inactive = Some((reason, cutoff_ms));
                        }
                        SnapshotCommand::WsActivity {
                            token_key,
                            last_price,
                            book_update,
                            payload_ts_ms,
                        } => {
                            if targets_by_key.contains_key(&token_key) {
                                let mut last_ws_seen_ms = last_ws_seen_ms.lock().await;
                                last_ws_seen_ms.insert(token_key.clone(), now_ms);
                                if last_price {
                                    let mut last_ws_last_price_ms =
                                        last_ws_last_price_ms.lock().await;
                                    last_ws_last_price_ms.insert(token_key.clone(), now_ms);
                                }
                                if book_update {
                                    let mut last_ws_book_ms = last_ws_book_ms.lock().await;
                                    last_ws_book_ms.insert(token_key.clone(), now_ms);
                                }
                                if let Some(payload_ts_ms) = payload_ts_ms {
                                    let mut payload_cache = last_payload_ts_ms.lock().await;
                                    let entry = payload_cache
                                        .entry(token_key)
                                        .or_insert(payload_ts_ms);
                                    if payload_ts_ms > *entry {
                                        *entry = payload_ts_ms;
                                    }
                                }
                            }
                        }
                        SnapshotCommand::TriggerToken { token_key, reason } => {
                            mark_priority_token(&token_key, now_ms, &priority_until_ms).await;
                            trigger_tokens.insert(token_key, reason);
                        }
                    }
                }
                if !trigger_tokens.is_empty() {
                    let mut on_demand_targets = Vec::new();
                    let mut reasons = Vec::new();
                    for (token_key, reason) in trigger_tokens {
                        if let Some(target) = targets_by_key.get(&token_key) {
                            on_demand_targets.push(target.clone());
                            reasons.push(reason);
                        } else {
                            warn!(
                                "opinion_rest on_demand unknown_token token_key={}",
                                token_key
                            );
                        }
                    }
                    if !on_demand_targets.is_empty() {
                        let reason = if reasons.len() == 1 {
                            reasons[0].clone()
                        } else {
                            "mixed".to_string()
                        };
                        let (on_demand_targets, cooldown_skipped) = {
                            let mut skipped = 0usize;
                            let mut filtered = Vec::with_capacity(on_demand_targets.len());
                            let mut cooldown_cache = cooldown_until_ms.lock().await;
                            for target in on_demand_targets {
                                match cooldown_cache.get(&target.token_key).copied() {
                                    Some(until_ms) if until_ms > now_ms => {
                                        skipped += 1;
                                    }
                                    Some(_) => {
                                        cooldown_cache.remove(&target.token_key);
                                        filtered.push(target);
                                    }
                                    None => filtered.push(target),
                                }
                            }
                            (filtered, skipped)
                        };
                        let (on_demand_targets, stale_skipped) = filter_orderbook_targets(
                            on_demand_targets,
                            now_ms,
                            orderbook_ws_stale_ms,
                            orderbook_staleness_ms,
                            &last_ws_book_ms,
                            &last_payload_ts_ms,
                        )
                        .await;
                        let (on_demand_targets, backoff_skipped) = filter_targets_by_backoff(
                            on_demand_targets,
                            now_ms,
                            &rest_backoff,
                            &priority_until_ms,
                            true,
                        )
                        .await;
                        if on_demand_targets.is_empty() {
                            info!(
                                "opinion_rest on_demand skipped reason={} cooldown_skipped={} stale_skipped={} backoff_skipped={}",
                                reason,
                                cooldown_skipped,
                                stale_skipped,
                                backoff_skipped
                            );
                        } else {
                            info!(
                                "opinion_rest on_demand triggered reason={} targets_len={} cooldown_skipped={} stale_skipped={} backoff_skipped={}",
                                reason,
                                on_demand_targets.len(),
                                cooldown_skipped,
                                stale_skipped,
                                backoff_skipped
                            );
                            let _ = run_snapshot_round(
                                "on_demand",
                                &on_demand_targets,
                                &client,
                                &base_url,
                                &api_key,
                                &clock_offset,
                                &dispatcher,
                                &db_sender,
                                false,
                                concurrency,
                                rate_limiter.as_ref(),
                                &rest_backoff,
                                &rest_global_backoff,
                                &priority_until_ms,
                                &last_payload_ts_ms,
                                &last_arrival_ts_ms,
                                &cooldown_until_ms,
                                cooldown_threshold_ms,
                                cooldown_duration_ms,
                                opinion_ts_max_skew_ms,
                                RestBucket::All,
                            )
                            .await;
                        }
                    }
                }
                if let Some((reason, cutoff_ms)) = trigger_inactive.take() {
                    let cutoff_ms = cutoff_ms.max(0);
                    let now_ms = now_ts_ms();
                    if let Some(until_ms) =
                        rest_global_backoff_until(now_ms, &rest_global_backoff).await
                    {
                        info!(
                            "opinion_rest gapfill skipped global_backoff reason={} cutoff_ms={} until_ms={}",
                            reason,
                            cutoff_ms,
                            until_ms
                        );
                    } else if let Some(until_ms) =
                        rest_reconnect_suppressed(now_ms, &reconnect_suppress_until_ms).await
                    {
                        info!(
                            "opinion_rest gapfill skipped reconnect_suppress reason={} cutoff_ms={} until_ms={}",
                            reason,
                            cutoff_ms,
                            until_ms
                        );
                    } else {
                        let cutoff_ts_ms = now_ms.saturating_sub(cutoff_ms);
                        let inactive_targets: Vec<OpinionSnapshotTarget> = {
                            let last_ws_seen_ms = last_ws_seen_ms.lock().await;
                            let last_arrival_ts_ms = last_arrival_ts_ms.lock().await;
                            let (_active, inactive, _unseen) = partition_targets_by_activity(
                                &targets,
                                &last_ws_seen_ms,
                                &last_arrival_ts_ms,
                                cutoff_ts_ms,
                                rest_ws_primary,
                            );
                            inactive
                        };
                        let (inactive_targets, cooldown_skipped) = {
                            let mut skipped = 0usize;
                            let mut filtered = Vec::with_capacity(inactive_targets.len());
                            let mut cooldown_cache = cooldown_until_ms.lock().await;
                            for target in inactive_targets {
                                match cooldown_cache.get(&target.token_key).copied() {
                                    Some(until_ms) if until_ms > now_ms => {
                                        skipped += 1;
                                    }
                                    Some(_) => {
                                        cooldown_cache.remove(&target.token_key);
                                        filtered.push(target);
                                    }
                                    None => filtered.push(target),
                                }
                            }
                            (filtered, skipped)
                        };
                        let (inactive_targets, stale_skipped) = filter_orderbook_targets(
                            inactive_targets,
                            now_ms,
                            orderbook_ws_stale_ms,
                            orderbook_staleness_ms,
                            &last_ws_book_ms,
                            &last_payload_ts_ms,
                        )
                        .await;
                        let (inactive_targets, priority_skipped) = filter_gapfill_priority(
                            inactive_targets,
                            now_ms,
                            &last_ws_seen_ms,
                            &priority_until_ms,
                            GAPFILL_PRIORITY_WINDOW_MS,
                        )
                        .await;
                        let (inactive_targets, backoff_skipped) = filter_targets_by_backoff(
                            inactive_targets,
                            now_ms,
                            &rest_backoff,
                            &priority_until_ms,
                            false,
                        )
                        .await;
                        if !inactive_targets.is_empty() {
                            info!(
                                "opinion_rest gapfill triggered reason={} targets_len={} cutoff_ms={} cooldown_skipped={} stale_skipped={} priority_skipped={} backoff_skipped={}",
                                reason,
                                inactive_targets.len(),
                                cutoff_ms,
                                cooldown_skipped,
                                stale_skipped,
                                priority_skipped,
                                backoff_skipped
                            );
                            let _ = run_snapshot_round(
                                "gapfill",
                                &inactive_targets,
                                &client,
                                &base_url,
                                &api_key,
                                &clock_offset,
                                &dispatcher,
                                &db_sender,
                                false,
                                concurrency,
                                rate_limiter.as_ref(),
                                &rest_backoff,
                                &rest_global_backoff,
                                &priority_until_ms,
                                &last_payload_ts_ms,
                                &last_arrival_ts_ms,
                                &cooldown_until_ms,
                                cooldown_threshold_ms,
                                cooldown_duration_ms,
                                opinion_ts_max_skew_ms,
                                RestBucket::Inactive,
                            )
                            .await;
                        } else {
                            info!(
                                "opinion_rest gapfill skipped reason={} cutoff_ms={} (no inactive targets) cooldown_skipped={} stale_skipped={} priority_skipped={} backoff_skipped={}",
                                reason,
                                cutoff_ms,
                                cooldown_skipped,
                                stale_skipped,
                                priority_skipped,
                                backoff_skipped
                            );
                        }
                    }
                }
                if trigger_all {
                    let source = trigger_all_reason.as_deref().unwrap_or("unknown");
                    info!("opinion_rest refresh triggered 来源={}", source);
                    if source == "ws_connect" && ws_connect_grace_ms > 0 {
                        info!(
                            "opinion_rest ws_connect grace_wait_ms={}",
                            ws_connect_grace_ms
                        );
                        tokio::time::sleep(Duration::from_millis(ws_connect_grace_ms)).await;
                    }
                    let now_ms = now_ts_ms();
                    let (reconnect_targets, cooldown_skipped) = {
                        let mut skipped = 0usize;
                        let mut filtered = Vec::with_capacity(targets.len());
                        let mut cooldown_cache = cooldown_until_ms.lock().await;
                        for target in targets.iter().cloned() {
                            match cooldown_cache.get(&target.token_key).copied() {
                                Some(until_ms) if until_ms > now_ms => {
                                    skipped += 1;
                                }
                                Some(_) => {
                                    cooldown_cache.remove(&target.token_key);
                                    filtered.push(target);
                                }
                                None => filtered.push(target),
                            }
                        }
                        (filtered, skipped)
                    };
                    let (reconnect_targets, stale_skipped) = filter_orderbook_targets(
                        reconnect_targets,
                        now_ms,
                        orderbook_ws_stale_ms,
                        orderbook_staleness_ms,
                        &last_ws_book_ms,
                        &last_payload_ts_ms,
                    )
                    .await;
                    let (reconnect_targets, backoff_skipped) = filter_targets_by_backoff(
                        reconnect_targets,
                        now_ms,
                        &rest_backoff,
                        &priority_until_ms,
                        false,
                    )
                    .await;
                    let trigger_batch_size = if trigger_batch_size == 0 {
                        reconnect_targets.len().max(1)
                    } else {
                        trigger_batch_size
                    };
                    let trigger_rate_limiter_ref =
                        trigger_rate_limiter.as_ref().or(rate_limiter.as_ref());
                    let _ = run_snapshot_round_batched(
                        source,
                        &reconnect_targets,
                        &client,
                        &base_url,
                        &api_key,
                        &clock_offset,
                        &dispatcher,
                        &db_sender,
                        false,
                        concurrency,
                        trigger_rate_limiter_ref,
                        &rest_backoff,
                        &rest_global_backoff,
                        &priority_until_ms,
                        trigger_batch_size,
                        trigger_batch_delay_ms,
                        &last_payload_ts_ms,
                        &last_arrival_ts_ms,
                        &cooldown_until_ms,
                        cooldown_threshold_ms,
                        cooldown_duration_ms,
                        opinion_ts_max_skew_ms,
                        RestBucket::All,
                    )
                    .await;
                    if cooldown_skipped > 0 || stale_skipped > 0 || backoff_skipped > 0 {
                        info!(
                            "opinion_rest {} cooldown_skipped={} stale_skipped={} backoff_skipped={}",
                            source, cooldown_skipped, stale_skipped, backoff_skipped
                        );
                    }
                }
            }
        }
    }
}

struct SnapshotRound {
    successes: usize,
    total_levels: usize,
}

struct LatestPriceRound {
    successes: usize,
    changed: usize,
}

async fn run_snapshot_round(
    label: &str,
    targets: &[OpinionSnapshotTarget],
    client: &Client,
    base_url: &str,
    api_key: &str,
    clock_offset: &Arc<ClockOffset>,
    dispatcher: &mpsc::Sender<DispatcherMessage>,
    db_sender: &DbSender,
    log_snapshots: bool,
    concurrency: usize,
    rate_limiter: Option<&Arc<RateLimiter>>,
    rest_backoff: &Arc<Mutex<HashMap<String, RestBackoffState>>>,
    rest_global_backoff: &Arc<Mutex<RestBackoffState>>,
    priority_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    last_payload_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    last_arrival_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_threshold_ms: i64,
    cooldown_duration_ms: i64,
    opinion_ts_max_skew_ms: i64,
    bucket: RestBucket,
) -> SnapshotRound {
    let _ = priority_until_ms;
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let successes = Arc::new(AtomicUsize::new(0));
    let total_levels = Arc::new(AtomicUsize::new(0));
    let rest_latencies = Arc::new(StdMutex::new(Vec::with_capacity(targets.len())));
    let staleness_samples = Arc::new(StdMutex::new(Vec::new()));
    let payload_ts_missing = Arc::new(AtomicUsize::new(0));
    let error_total = Arc::new(AtomicUsize::new(0));
    let error_timeout = Arc::new(AtomicUsize::new(0));
    let error_http = Arc::new(AtomicUsize::new(0));
    let error_parse = Arc::new(AtomicUsize::new(0));
    let error_empty = Arc::new(AtomicUsize::new(0));
    let error_other = Arc::new(AtomicUsize::new(0));
    let total_targets = targets.len();

    stream::iter(targets.iter().cloned())
        .map(|target| {
            let client = client.clone();
            let base_url = base_url.to_string();
            let api_key = api_key.to_string();
            let clock_offset = clock_offset.clone();
            let dispatcher = dispatcher.clone();
            let db_sender = db_sender.clone();
            let rate_limiter = rate_limiter.cloned();
            let last_payload_ts_ms = last_payload_ts_ms.clone();
            let last_arrival_ts_ms = last_arrival_ts_ms.clone();
            let cooldown_until_ms = cooldown_until_ms.clone();
            let rest_backoff = rest_backoff.clone();
            let rest_global_backoff = rest_global_backoff.clone();
            let started = started.clone();
            let completed = completed.clone();
            async move {
                let started_count = started.fetch_add(1, Ordering::Relaxed) + 1;
                if started_count == 1 || started_count % BOOTSTRAP_LOG_EVERY == 0 {
                    let inflight = started_count.saturating_sub(completed.load(Ordering::Relaxed));
                    info!(
                        "opinion_rest {} request_started started={} inflight={}",
                        label, started_count, inflight
                    );
                }
                fetch_and_emit(
                    label,
                    &client,
                    &base_url,
                    &api_key,
                    &target,
                    &clock_offset,
                    &dispatcher,
                    &db_sender,
                    log_snapshots,
                    rate_limiter.as_ref(),
                    &rest_backoff,
                    &rest_global_backoff,
                    &last_payload_ts_ms,
                    &last_arrival_ts_ms,
                    &cooldown_until_ms,
                    cooldown_threshold_ms,
                    cooldown_duration_ms,
                    opinion_ts_max_skew_ms,
                )
                .await
            }
        })
        .buffer_unordered(concurrency)
        .for_each(|result| {
            let completed = completed.clone();
            let successes = successes.clone();
            let total_levels = total_levels.clone();
            let rest_latencies = rest_latencies.clone();
            let staleness_samples = staleness_samples.clone();
            let payload_ts_missing = payload_ts_missing.clone();
            let error_total = error_total.clone();
            let error_timeout = error_timeout.clone();
            let error_http = error_http.clone();
            let error_parse = error_parse.clone();
            let error_empty = error_empty.clone();
            let error_other = error_other.clone();
            async move {
                let finished = completed.fetch_add(1, Ordering::Relaxed) + 1;
                match result {
                    Ok(sample) => {
                        successes.fetch_add(1, Ordering::Relaxed);
                        total_levels.fetch_add(sample.levels_total, Ordering::Relaxed);
                        rest_latencies
                            .lock()
                            .unwrap_or_else(|err| err.into_inner())
                            .push(sample.rest_latency_ms);
                        if let Some(staleness) = sample.market_staleness_ms {
                            staleness_samples
                                .lock()
                                .unwrap_or_else(|err| err.into_inner())
                                .push(staleness);
                        }
                        if sample.payload_ts_missing {
                            payload_ts_missing.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(err) => {
                        error_total.fetch_add(1, Ordering::Relaxed);
                        match err.kind {
                            RestErrorKind::Timeout => {
                                error_timeout.fetch_add(1, Ordering::Relaxed);
                            }
                            RestErrorKind::HttpStatus => {
                                error_http.fetch_add(1, Ordering::Relaxed);
                            }
                            RestErrorKind::Parse => {
                                error_parse.fetch_add(1, Ordering::Relaxed);
                            }
                            RestErrorKind::Empty => {
                                error_empty.fetch_add(1, Ordering::Relaxed);
                            }
                            RestErrorKind::Other => {
                                error_other.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        warn!(
                            "opinion_rest {} error kind={:?}: {:?}",
                            label, err.kind, err.error
                        );
                    }
                }
                if finished % BOOTSTRAP_LOG_EVERY == 0 || finished == total_targets {
                    info!(
                        "opinion_rest {} progress completed={} success={} total_levels={}",
                        label,
                        finished,
                        successes.load(Ordering::Relaxed),
                        total_levels.load(Ordering::Relaxed)
                    );
                }
            }
        })
        .await;

    let latency_samples = rest_latencies
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .clone();
    let staleness_samples = staleness_samples
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .clone();
    let latency_summary = summarize_u128(&latency_samples);
    let staleness_summary = summarize_i64(&staleness_samples);
    let success_count = successes.load(Ordering::Relaxed);
    let payload_missing = payload_ts_missing.load(Ordering::Relaxed);
    let payload_missing_rate = if success_count > 0 {
        payload_missing as f64 / success_count as f64
    } else {
        0.0
    };
    info!(
        "opinion_rest round_summary label={} bucket={} targets={} success={} error={} timeout={} http_error={} parse_error={} empty_error={} other_error={} latency_samples={} latency_p95_ms={:?} latency_p99_ms={:?} latency_max_ms={:?} staleness_samples={} staleness_p90_ms={:?} staleness_p99_ms={:?} staleness_max_ms={:?} payload_ts_missing_rate={:.4}",
        label,
        bucket.label(),
        total_targets,
        success_count,
        error_total.load(Ordering::Relaxed),
        error_timeout.load(Ordering::Relaxed),
        error_http.load(Ordering::Relaxed),
        error_parse.load(Ordering::Relaxed),
        error_empty.load(Ordering::Relaxed),
        error_other.load(Ordering::Relaxed),
        latency_samples.len(),
        latency_summary.as_ref().map(|s| s.p95),
        latency_summary.as_ref().map(|s| s.p99),
        latency_summary.as_ref().map(|s| s.max),
        staleness_samples.len(),
        staleness_summary.as_ref().map(|s| s.p90),
        staleness_summary.as_ref().map(|s| s.p99),
        staleness_summary.as_ref().map(|s| s.max),
        payload_missing_rate
    );

    SnapshotRound {
        successes: successes.load(Ordering::Relaxed),
        total_levels: total_levels.load(Ordering::Relaxed),
    }
}

async fn run_snapshot_round_batched(
    label: &str,
    targets: &[OpinionSnapshotTarget],
    client: &Client,
    base_url: &str,
    api_key: &str,
    clock_offset: &Arc<ClockOffset>,
    dispatcher: &mpsc::Sender<DispatcherMessage>,
    db_sender: &DbSender,
    log_snapshots: bool,
    concurrency: usize,
    rate_limiter: Option<&Arc<RateLimiter>>,
    rest_backoff: &Arc<Mutex<HashMap<String, RestBackoffState>>>,
    rest_global_backoff: &Arc<Mutex<RestBackoffState>>,
    priority_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    batch_size: usize,
    batch_delay_ms: u64,
    last_payload_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    last_arrival_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_threshold_ms: i64,
    cooldown_duration_ms: i64,
    opinion_ts_max_skew_ms: i64,
    bucket: RestBucket,
) -> SnapshotRound {
    if targets.is_empty() {
        return SnapshotRound {
            successes: 0,
            total_levels: 0,
        };
    }
    if batch_size == 0 || batch_size >= targets.len() {
        return run_snapshot_round(
            label,
            targets,
            client,
            base_url,
            api_key,
            clock_offset,
            dispatcher,
            db_sender,
            log_snapshots,
            concurrency,
            rate_limiter,
            rest_backoff,
            rest_global_backoff,
            priority_until_ms,
            last_payload_ts_ms,
            last_arrival_ts_ms,
            cooldown_until_ms,
            cooldown_threshold_ms,
            cooldown_duration_ms,
            opinion_ts_max_skew_ms,
            bucket,
        )
        .await;
    }

    let total_batches = (targets.len() + batch_size - 1) / batch_size;
    info!(
        "opinion_rest {} batching enabled batch_size={} batch_delay_ms={} batches={}",
        label, batch_size, batch_delay_ms, total_batches
    );
    let mut successes = 0usize;
    let mut total_levels = 0usize;
    let mut offset = 0usize;
    let mut batch_index = 0usize;
    while offset < targets.len() {
        let end = (offset + batch_size).min(targets.len());
        batch_index += 1;
        let batch = run_snapshot_round(
            label,
            &targets[offset..end],
            client,
            base_url,
            api_key,
            clock_offset,
            dispatcher,
            db_sender,
            log_snapshots,
            concurrency,
            rate_limiter,
            rest_backoff,
            rest_global_backoff,
            priority_until_ms,
            last_payload_ts_ms,
            last_arrival_ts_ms,
            cooldown_until_ms,
            cooldown_threshold_ms,
            cooldown_duration_ms,
            opinion_ts_max_skew_ms,
            bucket,
        )
        .await;
        successes += batch.successes;
        total_levels += batch.total_levels;
        offset = end;
        if offset < targets.len() && batch_delay_ms > 0 {
            if batch_index % 5 == 0 || batch_index == 1 {
                info!(
                    "opinion_rest {} batch_pause batch={}/{} wait_ms={}",
                    label, batch_index, total_batches, batch_delay_ms
                );
            }
            tokio::time::sleep(Duration::from_millis(batch_delay_ms)).await;
        }
    }

    SnapshotRound {
        successes,
        total_levels,
    }
}

async fn run_latest_price_round(
    label: &str,
    targets: &[OpinionSnapshotTarget],
    client: &Client,
    base_url: &str,
    api_key: &str,
    clock_offset: &Arc<ClockOffset>,
    dispatcher: &mpsc::Sender<DispatcherMessage>,
    db_sender: &DbSender,
    last_latest_price: &Arc<Mutex<HashMap<String, f64>>>,
    max_inflight: usize,
    rate_limiter: Option<&Arc<RateLimiter>>,
    rest_backoff: &Arc<Mutex<HashMap<String, RestBackoffState>>>,
    rest_global_backoff: &Arc<Mutex<RestBackoffState>>,
    priority_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    last_payload_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    last_arrival_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_threshold_ms: i64,
    cooldown_duration_ms: i64,
    opinion_ts_max_skew_ms: i64,
    bucket: RestBucket,
) -> LatestPriceRound {
    let _ = priority_until_ms;
    let started = Arc::new(AtomicUsize::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let successes = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(AtomicUsize::new(0));
    let rest_latencies = Arc::new(StdMutex::new(Vec::with_capacity(targets.len())));
    let staleness_samples = Arc::new(StdMutex::new(Vec::new()));
    let payload_ts_missing = Arc::new(AtomicUsize::new(0));
    let error_total = Arc::new(AtomicUsize::new(0));
    let error_timeout = Arc::new(AtomicUsize::new(0));
    let error_http = Arc::new(AtomicUsize::new(0));
    let error_parse = Arc::new(AtomicUsize::new(0));
    let error_empty = Arc::new(AtomicUsize::new(0));
    let error_other = Arc::new(AtomicUsize::new(0));
    let total_targets = targets.len();

    stream::iter(targets.iter().cloned())
        .map(|target| {
            let client = client.clone();
            let base_url = base_url.to_string();
            let api_key = api_key.to_string();
            let clock_offset = clock_offset.clone();
            let dispatcher = dispatcher.clone();
            let db_sender = db_sender.clone();
            let last_latest_price = last_latest_price.clone();
            let rate_limiter = rate_limiter.cloned();
            let rest_backoff = rest_backoff.clone();
            let rest_global_backoff = rest_global_backoff.clone();
            let last_payload_ts_ms = last_payload_ts_ms.clone();
            let last_arrival_ts_ms = last_arrival_ts_ms.clone();
            let cooldown_until_ms = cooldown_until_ms.clone();
            let started = started.clone();
            let completed = completed.clone();
            async move {
                let started_count = started.fetch_add(1, Ordering::Relaxed) + 1;
                if started_count == 1 || started_count % BOOTSTRAP_LOG_EVERY == 0 {
                    let inflight = started_count.saturating_sub(completed.load(Ordering::Relaxed));
                    info!(
                        "opinion_rest {} request_started started={} inflight={}",
                        label, started_count, inflight
                    );
                }
                fetch_latest_price(
                    &client,
                    &base_url,
                    &api_key,
                    &target,
                    &clock_offset,
                    &dispatcher,
                    &db_sender,
                    &last_latest_price,
                    rate_limiter.as_ref(),
                    &rest_backoff,
                    &rest_global_backoff,
                    &last_payload_ts_ms,
                    &last_arrival_ts_ms,
                    &cooldown_until_ms,
                    cooldown_threshold_ms,
                    cooldown_duration_ms,
                    opinion_ts_max_skew_ms,
                )
                .await
            }
        })
        .buffer_unordered(targets.len().max(1).min(max_inflight.max(1)))
        .for_each(|result| {
            let completed = completed.clone();
            let successes = successes.clone();
            let changed = changed.clone();
            let rest_latencies = rest_latencies.clone();
            let staleness_samples = staleness_samples.clone();
            let payload_ts_missing = payload_ts_missing.clone();
            let error_total = error_total.clone();
            let error_timeout = error_timeout.clone();
            let error_http = error_http.clone();
            let error_parse = error_parse.clone();
            let error_empty = error_empty.clone();
            let error_other = error_other.clone();
            async move {
                let finished = completed.fetch_add(1, Ordering::Relaxed) + 1;
                match result {
                    Ok(sample) => {
                        successes.fetch_add(1, Ordering::Relaxed);
                        if sample.is_changed {
                            changed.fetch_add(1, Ordering::Relaxed);
                        }
                        rest_latencies
                            .lock()
                            .unwrap_or_else(|err| err.into_inner())
                            .push(sample.rest_latency_ms);
                        if let Some(staleness) = sample.market_staleness_ms {
                            staleness_samples
                                .lock()
                                .unwrap_or_else(|err| err.into_inner())
                                .push(staleness);
                        }
                        if sample.payload_ts_missing {
                            payload_ts_missing.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    Err(err) => {
                        error_total.fetch_add(1, Ordering::Relaxed);
                        match err.kind {
                            RestErrorKind::Timeout => {
                                error_timeout.fetch_add(1, Ordering::Relaxed);
                            }
                            RestErrorKind::HttpStatus => {
                                error_http.fetch_add(1, Ordering::Relaxed);
                            }
                            RestErrorKind::Parse => {
                                error_parse.fetch_add(1, Ordering::Relaxed);
                            }
                            RestErrorKind::Empty => {
                                error_empty.fetch_add(1, Ordering::Relaxed);
                            }
                            RestErrorKind::Other => {
                                error_other.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        warn!(
                            "opinion_rest {} error kind={:?}: {:?}",
                            label, err.kind, err.error
                        );
                    }
                }
                if finished % BOOTSTRAP_LOG_EVERY == 0 || finished == total_targets {
                    info!(
                        "opinion_rest {} progress completed={} success={}",
                        label,
                        finished,
                        successes.load(Ordering::Relaxed),
                    );
                }
            }
        })
        .await;

    let latency_samples = rest_latencies
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .clone();
    let staleness_samples = staleness_samples
        .lock()
        .unwrap_or_else(|err| err.into_inner())
        .clone();
    let latency_summary = summarize_u128(&latency_samples);
    let staleness_summary = summarize_i64(&staleness_samples);
    let success_count = successes.load(Ordering::Relaxed);
    let payload_missing = payload_ts_missing.load(Ordering::Relaxed);
    let payload_missing_rate = if success_count > 0 {
        payload_missing as f64 / success_count as f64
    } else {
        0.0
    };
    info!(
        "opinion_rest latest_price_summary label={} bucket={} targets={} success={} error={} timeout={} http_error={} parse_error={} empty_error={} other_error={} latency_samples={} latency_p95_ms={:?} latency_p99_ms={:?} latency_max_ms={:?} staleness_samples={} staleness_p90_ms={:?} staleness_p99_ms={:?} staleness_max_ms={:?} payload_ts_missing_rate={:.4}",
        label,
        bucket.label(),
        total_targets,
        success_count,
        error_total.load(Ordering::Relaxed),
        error_timeout.load(Ordering::Relaxed),
        error_http.load(Ordering::Relaxed),
        error_parse.load(Ordering::Relaxed),
        error_empty.load(Ordering::Relaxed),
        error_other.load(Ordering::Relaxed),
        latency_samples.len(),
        latency_summary.as_ref().map(|s| s.p95),
        latency_summary.as_ref().map(|s| s.p99),
        latency_summary.as_ref().map(|s| s.max),
        staleness_samples.len(),
        staleness_summary.as_ref().map(|s| s.p90),
        staleness_summary.as_ref().map(|s| s.p99),
        staleness_summary.as_ref().map(|s| s.max),
        payload_missing_rate
    );

    LatestPriceRound {
        successes: successes.load(Ordering::Relaxed),
        changed: changed.load(Ordering::Relaxed),
    }
}

async fn fetch_and_emit(
    label: &str,
    client: &Client,
    base_url: &str,
    api_key: &str,
    target: &OpinionSnapshotTarget,
    clock_offset: &Arc<ClockOffset>,
    dispatcher: &mpsc::Sender<DispatcherMessage>,
    db_sender: &DbSender,
    log_levels: bool,
    rate_limiter: Option<&Arc<RateLimiter>>,
    rest_backoff: &Arc<Mutex<HashMap<String, RestBackoffState>>>,
    rest_global_backoff: &Arc<Mutex<RestBackoffState>>,
    last_payload_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    last_arrival_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_threshold_ms: i64,
    cooldown_duration_ms: i64,
    opinion_ts_max_skew_ms: i64,
) -> Result<RestFetchResult, RestFetchError> {
    let trace = FETCH_TRACE_COUNTER.fetch_add(1, Ordering::Relaxed) < FETCH_TRACE_SAMPLES;
    if let Some(limiter) = rate_limiter {
        let wait_start = Instant::now();
        limiter.wait().await;
        let wait_ms = wait_start.elapsed().as_millis();
        if wait_ms > OPI_REST_RATE_WAIT_WARN_MS {
            warn!(
                "opinion_rest rate_limit_wait label={} endpoint=/token/orderbook token_key={} wait_ms={}",
                label,
                target.token_key,
                wait_ms
            );
        }
    }
    let req_start = Instant::now();
    let url = format!("{}/token/orderbook", base_url.trim_end_matches('/'));
    if trace {
        info!(
            "opinion_rest fetch_start token_key={} token_id={}",
            target.token_key, target.token_id
        );
    }
    let mut req = client
        .get(&url)
        .query(&[("token_id", target.token_id.as_str())])
        .header("apikey", api_key)
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::USER_AGENT, "monitor/1.0");
    if !api_key.is_empty() {
        req = req.query(&[("apikey", api_key)]);
    }
    let resp = req
        .send()
        .await
        .map_err(|err| RestFetchError::new(classify_reqwest_error(&err), err.into()))?;
    let status = resp.status();
    if trace {
        info!(
            "opinion_rest fetch_headers token_key={} status={} elapsed_ms={}",
            target.token_key,
            status.as_u16(),
            req_start.elapsed().as_millis()
        );
    }
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());
    let body = resp
        .bytes()
        .await
        .map_err(|err| RestFetchError::new(classify_reqwest_error(&err), err.into()))?;
    let rest_latency_ms = req_start.elapsed().as_millis();
    if trace {
        info!(
            "opinion_rest fetch_body token_key={} status={} body_len={} elapsed_ms={}",
            target.token_key,
            status.as_u16(),
            body.len(),
            rest_latency_ms
        );
    }
    if !status.is_success() {
        if status.as_u16() == 429 {
            let now_ms = now_ts_ms();
            mark_rest_backoff(&target.token_key, now_ms, rest_backoff).await;
            mark_rest_global_backoff(now_ms, rest_global_backoff).await;
        }
        let preview = body_preview(&body);
        return Err(RestFetchError::new(
            RestErrorKind::HttpStatus,
            anyhow::anyhow!(
                "opinion_rest http error label={} status={} content_type={:?} body_len={} body_prefix=\"{}\" token_key={} token_id={}",
                label,
                status.as_u16(),
                content_type,
                body.len(),
                preview,
                target.token_key,
                target.token_id
            ),
        ));
    }
    if body.is_empty() {
        return Err(RestFetchError::new(
            RestErrorKind::Empty,
            anyhow::anyhow!(
                "opinion_rest empty body status={} content_type={:?} token_key={} token_id={}",
                status.as_u16(),
                content_type,
                target.token_key,
                target.token_id
            ),
        ));
    }

    let envelope: OrderbookEnvelope =
        serde_json::from_slice(&body).map_err(|err| {
            let preview = body_preview(&body);
            RestFetchError::new(
                RestErrorKind::Parse,
                anyhow::anyhow!(
                    "opinion_rest parse json failed status={} content_type={:?} body_len={} body_prefix=\"{}\" token_key={} token_id={} err={}",
                    status.as_u16(),
                    content_type,
                    body.len(),
                    preview,
                    target.token_key,
                    target.token_id,
                    err
                ),
            )
        })?;
    let payload = envelope.into_payload();
    let bids = parse_levels(payload.bids);
    let asks = parse_levels(payload.asks);
    let levels_total = bids.len() + asks.len();
    if rest_latency_ms > OPI_REST_SLOW_MS {
        warn!(
            "opinion_rest slow_fetch token_key={} token_id={} status={} elapsed_ms={} levels_total={}",
            target.token_key,
            target.token_id,
            status.as_u16(),
            rest_latency_ms,
            levels_total
        );
    }
    if log_levels {
        info!(
            "opinion_rest snapshot token_key={} token_id={} bids_len={} asks_len={} levels_total={}",
            target.token_key,
            target.token_id,
            bids.len(),
            asks.len(),
            levels_total
        );
    }
    let local_ts = now_ts_ms();
    {
        let mut arrival_cache = last_arrival_ts_ms.lock().await;
        arrival_cache.insert(target.token_key.clone(), local_ts);
    }
    let raw_ts = payload.timestamp.or(payload.ts);
    let mut flags = EventFlags::default();
    let exchange_ts_source = ExchangeTsSource::Estimated;
    let exchange_ts_ms = local_ts.saturating_sub(clock_offset.current());
    let payload_ts_ms = raw_ts.and_then(normalize_ts_ms);
    let mut payload_ts_missing = false;
    let mut market_staleness_ms = None;
    if let Some(payload_ts_ms) = payload_ts_ms {
        let staleness_ms = local_ts.saturating_sub(payload_ts_ms);
        market_staleness_ms = Some(staleness_ms);
        if staleness_ms > opinion_ts_max_skew_ms {
            flags.ts_anomaly = true;
            if should_log_ts_skew(&target.token_key, local_ts) {
                tracing::info!(
                    "opinion_rest market_staleness token_key={} token_id={} ts_raw={} ts_norm={} staleness_ms={} warn_ms={} unit_guess={}",
                    target.token_key,
                    target.token_id,
                    raw_ts.unwrap_or_default(),
                    payload_ts_ms,
                    staleness_ms,
                    opinion_ts_max_skew_ms,
                    ts_unit_hint(raw_ts.unwrap_or_default()),
                );
            }
        }
        {
            let mut payload_cache = last_payload_ts_ms.lock().await;
            let entry = payload_cache.entry(target.token_key.clone()).or_insert(payload_ts_ms);
            if payload_ts_ms > *entry {
                *entry = payload_ts_ms;
            }
        }
    } else {
        payload_ts_missing = true;
        flags.ts_missing = true;
    }

    if cooldown_enabled(cooldown_threshold_ms, cooldown_duration_ms) {
        maybe_mark_cooldown(
            &target.token_key,
            market_staleness_ms,
            local_ts,
            cooldown_until_ms,
            cooldown_threshold_ms,
            cooldown_duration_ms,
        )
        .await;
    }

    let event = Event {
        venue: Venue::Opinion,
        token_key: target.token_key.clone(),
        pair_ids: smallvec::SmallVec::new(),
        token_side: TokenSide::Unknown,
        kind: EventKind::BookSnapshot,
        channel: Some("rest.orderbook".to_string()),
        exchange_ts_ms,
        local_ts_ms: local_ts,
        exchange_ts_source,
        flags,
        payload: EventPayload {
            bids: Some(bids),
            asks: Some(asks),
            ..Default::default()
        },
    };
    let send_start = Instant::now();
    let send_result = dispatcher.send(DispatcherMessage::Event(event)).await;
    let send_ms = send_start.elapsed().as_millis();
    if trace || send_ms > 200 {
        info!(
            "opinion_rest dispatch_sent token_key={} send_ms={}",
            target.token_key, send_ms
        );
    }
    if let Err(err) = send_result {
        warn!(
            "opinion_rest dispatch_failed token_key={} error={:?}",
            target.token_key, err
        );
    }
    Ok(RestFetchResult {
        levels_total,
        rest_latency_ms,
        market_staleness_ms,
        payload_ts_missing,
    })
}

async fn fetch_latest_price(
    client: &Client,
    base_url: &str,
    api_key: &str,
    target: &OpinionSnapshotTarget,
    clock_offset: &Arc<ClockOffset>,
    dispatcher: &mpsc::Sender<DispatcherMessage>,
    db_sender: &DbSender,
    last_latest_price: &Arc<Mutex<HashMap<String, f64>>>,
    rate_limiter: Option<&Arc<RateLimiter>>,
    rest_backoff: &Arc<Mutex<HashMap<String, RestBackoffState>>>,
    rest_global_backoff: &Arc<Mutex<RestBackoffState>>,
    last_payload_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    last_arrival_ts_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    cooldown_threshold_ms: i64,
    cooldown_duration_ms: i64,
    opinion_ts_max_skew_ms: i64,
) -> Result<LatestPriceResult, RestFetchError> {
    let trace = FETCH_TRACE_COUNTER.fetch_add(1, Ordering::Relaxed) < FETCH_TRACE_SAMPLES;
    if let Some(limiter) = rate_limiter {
        let wait_start = Instant::now();
        limiter.wait().await;
        let wait_ms = wait_start.elapsed().as_millis();
        if wait_ms > OPI_REST_RATE_WAIT_WARN_MS {
            warn!(
                "opinion_rest rate_limit_wait endpoint=/token/latest-price token_key={} wait_ms={}",
                target.token_key,
                wait_ms
            );
        }
    }
    let req_start = Instant::now();
    let url = format!("{}/token/latest-price", base_url.trim_end_matches('/'));
    if trace {
        info!(
            "opinion_rest latest_price_fetch_start token_key={} token_id={}",
            target.token_key, target.token_id
        );
    }
    let mut req = client
        .get(&url)
        .query(&[("token_id", target.token_id.as_str())])
        .header("apikey", api_key)
        .header(reqwest::header::ACCEPT, "application/json")
        .header(reqwest::header::USER_AGENT, "monitor/1.0");
    if !api_key.is_empty() {
        req = req.query(&[("apikey", api_key)]);
    }
    let resp = req
        .send()
        .await
        .map_err(|err| RestFetchError::new(classify_reqwest_error(&err), err.into()))?;
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(|value| value.to_string());
    let body = resp
        .bytes()
        .await
        .map_err(|err| RestFetchError::new(classify_reqwest_error(&err), err.into()))?;
    let rest_latency_ms = req_start.elapsed().as_millis();
    if trace {
        info!(
            "opinion_rest latest_price_fetch_body token_key={} status={} body_len={} elapsed_ms={}",
            target.token_key,
            status.as_u16(),
            body.len(),
            rest_latency_ms
        );
    }
    if !status.is_success() {
        if status.as_u16() == 429 {
            let now_ms = now_ts_ms();
            mark_rest_backoff(&target.token_key, now_ms, rest_backoff).await;
            mark_rest_global_backoff(now_ms, rest_global_backoff).await;
        }
        let preview = body_preview(&body);
        return Err(RestFetchError::new(
            RestErrorKind::HttpStatus,
            anyhow::anyhow!(
                "opinion_rest latest_price http error status={} content_type={:?} body_len={} body_prefix=\"{}\" token_key={} token_id={}",
                status.as_u16(),
                content_type,
                body.len(),
                preview,
                target.token_key,
                target.token_id
            ),
        ));
    }
    if body.is_empty() {
        return Err(RestFetchError::new(
            RestErrorKind::Empty,
            anyhow::anyhow!(
                "opinion_rest latest_price empty body status={} content_type={:?} token_key={} token_id={}",
                status.as_u16(),
                content_type,
                target.token_key,
                target.token_id
            ),
        ));
    }

    let envelope: Value = serde_json::from_slice(&body).map_err(|err| {
        let preview = body_preview(&body);
        RestFetchError::new(
            RestErrorKind::Parse,
            anyhow::anyhow!(
                "opinion_rest latest_price parse json failed status={} content_type={:?} body_len={} body_prefix=\"{}\" token_key={} token_id={} err={}",
                status.as_u16(),
                content_type,
                body.len(),
                preview,
                target.token_key,
                target.token_id,
                err
            ),
        )
    })?;
    let payload = envelope
        .get("result")
        .or_else(|| envelope.get("data"))
        .unwrap_or(&envelope);

    let price = payload
        .get("price")
        .or_else(|| payload.get("lastPrice"))
        .or_else(|| payload.get("last_price"))
        .or_else(|| payload.get("currentPrice"))
        .and_then(extract_f64_value)
        .ok_or_else(|| {
            let preview = body_preview(payload.to_string().as_bytes());
            RestFetchError::new(
                RestErrorKind::Parse,
                anyhow::anyhow!(
                    "opinion_rest latest_price missing price token_key={} token_id={} payload={}",
                    target.token_key,
                    target.token_id,
                    preview
                ),
            )
        })?;
    let is_changed = {
        let mut cache = last_latest_price.lock().await;
        let prev = cache.insert(target.token_key.clone(), price);
        match prev {
            Some(prev) => (price - prev).abs() > PRICE_EPS,
            None => true,
        }
    };
    let ts_raw = payload
        .get("timestamp")
        .or_else(|| payload.get("ts"))
        .or_else(|| payload.get("time"))
        .and_then(extract_i64_value);

    let local_ts = now_ts_ms();
    {
        let mut arrival_cache = last_arrival_ts_ms.lock().await;
        arrival_cache.insert(target.token_key.clone(), local_ts);
    }
    let mut flags = EventFlags::default();
    let exchange_ts_source = ExchangeTsSource::Estimated;
    let exchange_ts_ms = local_ts.saturating_sub(clock_offset.current());
    let payload_ts_ms = ts_raw.and_then(normalize_ts_ms);
    let mut payload_ts_missing = false;
    let mut market_staleness_ms = None;
    if let Some(payload_ts_ms) = payload_ts_ms {
        let staleness_ms = local_ts.saturating_sub(payload_ts_ms);
        market_staleness_ms = Some(staleness_ms);
        if staleness_ms > opinion_ts_max_skew_ms {
            flags.ts_anomaly = true;
            if should_log_ts_skew(&target.token_key, local_ts) {
                tracing::info!(
                    "opinion_rest market_staleness token_key={} token_id={} ts_raw={} ts_norm={} staleness_ms={} warn_ms={} unit_guess={}",
                    target.token_key,
                    target.token_id,
                    ts_raw.unwrap_or_default(),
                    payload_ts_ms,
                    staleness_ms,
                    opinion_ts_max_skew_ms,
                    ts_unit_hint(ts_raw.unwrap_or_default()),
                );
            }
        }
        {
            let mut payload_cache = last_payload_ts_ms.lock().await;
            let entry = payload_cache.entry(target.token_key.clone()).or_insert(payload_ts_ms);
            if payload_ts_ms > *entry {
                *entry = payload_ts_ms;
            }
        }
    } else {
        payload_ts_missing = true;
        flags.ts_missing = true;
    }

    if cooldown_enabled(cooldown_threshold_ms, cooldown_duration_ms) {
        maybe_mark_cooldown(
            &target.token_key,
            market_staleness_ms,
            local_ts,
            cooldown_until_ms,
            cooldown_threshold_ms,
            cooldown_duration_ms,
        )
        .await;
    }

    let event = Event {
        venue: Venue::Opinion,
        token_key: target.token_key.clone(),
        pair_ids: smallvec::SmallVec::new(),
        token_side: TokenSide::Unknown,
        kind: EventKind::LastPrice,
        channel: Some("rest.latest_price".to_string()),
        exchange_ts_ms,
        local_ts_ms: local_ts,
        exchange_ts_source,
        flags,
        payload: EventPayload {
            price: Some(price),
            ..Default::default()
        },
    };
    let send_start = Instant::now();
    let send_result = dispatcher.send(DispatcherMessage::Event(event)).await;
    let send_ms = send_start.elapsed().as_millis();
    if trace || send_ms > 200 {
        info!(
            "opinion_rest latest_price_dispatch_sent token_key={} send_ms={}",
            target.token_key, send_ms
        );
    }
    if let Err(err) = send_result {
        warn!(
            "opinion_rest latest_price_dispatch_failed token_key={} error={:?}",
            target.token_key, err
        );
    }
    Ok(LatestPriceResult {
        is_changed,
        rest_latency_ms,
        market_staleness_ms,
        payload_ts_missing,
    })
}

fn parse_levels(levels: Option<Vec<RestLevel>>) -> Vec<BookLevel> {
    let mut out = Vec::new();
    let levels = match levels {
        Some(levels) => levels,
        None => return out,
    };
    out.reserve(levels.len());
    for level in levels {
        let price = match level.price {
            Some(price) => price,
            None => continue,
        };
        let size = match level.amount.or(level.size) {
            Some(size) => size,
            None => continue,
        };
        out.push(BookLevel { price, size });
    }
    out
}

fn extract_f64_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(num) => num.as_f64(),
        Value::String(text) => text.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn extract_i64_value(value: &Value) -> Option<i64> {
    match value {
        Value::Number(num) => num.as_i64().or_else(|| num.as_u64().map(|v| v as i64)),
        Value::String(text) => text.trim().parse::<i64>().ok(),
        _ => None,
    }
}

fn normalize_ts_ms(ts_raw: i64) -> Option<i64> {
    if ts_raw <= 0 {
        return None;
    }
    if ts_raw < 100_000_000_000 {
        return Some(ts_raw * 1000);
    }
    if ts_raw < 100_000_000_000_000 {
        return Some(ts_raw);
    }
    if ts_raw < 100_000_000_000_000_000 {
        return Some(ts_raw / 1000);
    }
    Some(ts_raw / 1_000_000)
}

fn ts_unit_hint(ts_raw: i64) -> &'static str {
    if ts_raw < 100_000_000_000 {
        return "s";
    }
    if ts_raw < 100_000_000_000_000 {
        return "ms";
    }
    if ts_raw < 100_000_000_000_000_000 {
        return "us";
    }
    "ns"
}

fn should_log_ts_skew(token_key: &str, now_ms: i64) -> bool {
    let cache = TS_SKEW_LOG_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = cache.lock().unwrap_or_else(|err| err.into_inner());
    let last = guard.get(token_key).copied().unwrap_or(0);
    if now_ms.saturating_sub(last) >= TS_SKEW_LOG_INTERVAL_MS {
        guard.insert(token_key.to_string(), now_ms);
        true
    } else {
        false
    }
}

fn should_log_cooldown(token_key: &str, now_ms: i64) -> bool {
    let cache = COOLDOWN_LOG_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = cache.lock().unwrap_or_else(|err| err.into_inner());
    let last = guard.get(token_key).copied().unwrap_or(0);
    if now_ms.saturating_sub(last) >= COOLDOWN_LOG_INTERVAL_MS {
        guard.insert(token_key.to_string(), now_ms);
        true
    } else {
        false
    }
}

fn cooldown_enabled(threshold_ms: i64, duration_ms: i64) -> bool {
    threshold_ms > 0 && duration_ms > 0
}

async fn maybe_mark_cooldown(
    token_key: &str,
    market_staleness_ms: Option<i64>,
    now_ms: i64,
    cooldown_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    threshold_ms: i64,
    duration_ms: i64,
) {
    let staleness_ms = match market_staleness_ms {
        Some(value) => value,
        None => return,
    };
    if staleness_ms <= threshold_ms {
        return;
    }
    let until_ms = now_ms.saturating_add(duration_ms);
    let mut cache = cooldown_until_ms.lock().await;
    let prev = cache.get(token_key).copied().unwrap_or(0);
    if until_ms > prev {
        cache.insert(token_key.to_string(), until_ms);
        if should_log_cooldown(token_key, now_ms) {
            info!(
                "opinion_rest cooldown token_key={} staleness_ms={} threshold_ms={} cooldown_ms={}",
                token_key, staleness_ms, threshold_ms, duration_ms
            );
        }
    }
}

async fn mark_priority_token(
    token_key: &str,
    now_ms: i64,
    priority_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
) {
    let until_ms = now_ms.saturating_add(PRIORITY_TOKEN_WINDOW_MS);
    let mut cache = priority_until_ms.lock().await;
    let prev = cache.get(token_key).copied().unwrap_or(0);
    if until_ms > prev {
        cache.insert(token_key.to_string(), until_ms);
    }
}

async fn snapshot_priority_until(
    priority_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    now_ms: i64,
) -> HashMap<String, i64> {
    let mut cache = priority_until_ms.lock().await;
    cache.retain(|_, until_ms| *until_ms > now_ms);
    cache.clone()
}

async fn filter_targets_by_backoff(
    targets: Vec<OpinionSnapshotTarget>,
    now_ms: i64,
    rest_backoff: &Arc<Mutex<HashMap<String, RestBackoffState>>>,
    priority_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    allow_priority: bool,
) -> (Vec<OpinionSnapshotTarget>, usize) {
    if targets.is_empty() {
        return (targets, 0);
    }
    let priority_snapshot = snapshot_priority_until(priority_until_ms, now_ms).await;
    let mut cache = rest_backoff.lock().await;
    let mut filtered = Vec::with_capacity(targets.len());
    let mut skipped = 0usize;
    for target in targets {
        if let Some(state) = cache.get(&target.token_key).copied() {
            if state.last_fail_ms > 0
                && now_ms.saturating_sub(state.last_fail_ms) >= REST_BACKOFF_RESET_MS
            {
                cache.remove(&target.token_key);
                filtered.push(target);
                continue;
            }
            if state.until_ms > now_ms {
                let is_priority = allow_priority
                    && priority_snapshot
                        .get(&target.token_key)
                        .copied()
                        .unwrap_or(0)
                        > now_ms;
                if is_priority {
                    filtered.push(target);
                } else {
                    skipped += 1;
                }
            } else {
                filtered.push(target);
            }
        } else {
            filtered.push(target);
        }
    }
    (filtered, skipped)
}

async fn filter_gapfill_priority(
    targets: Vec<OpinionSnapshotTarget>,
    now_ms: i64,
    last_ws_seen_ms: &Arc<Mutex<HashMap<String, i64>>>,
    priority_until_ms: &Arc<Mutex<HashMap<String, i64>>>,
    window_ms: i64,
) -> (Vec<OpinionSnapshotTarget>, usize) {
    if targets.is_empty() || window_ms <= 0 {
        return (targets, 0);
    }
    let cutoff_ms = now_ms.saturating_sub(window_ms);
    let last_ws_snapshot = last_ws_seen_ms.lock().await.clone();
    let priority_snapshot = snapshot_priority_until(priority_until_ms, now_ms).await;
    let mut filtered = Vec::with_capacity(targets.len());
    let mut skipped = 0usize;
    for target in targets {
        let priority_until = priority_snapshot
            .get(&target.token_key)
            .copied()
            .unwrap_or(0);
        if priority_until > now_ms {
            filtered.push(target);
            continue;
        }
        let last_ws = last_ws_snapshot.get(&target.token_key).copied().unwrap_or(0);
        if last_ws > cutoff_ms {
            skipped += 1;
        } else {
            filtered.push(target);
        }
    }
    (filtered, skipped)
}

async fn mark_rest_backoff(
    token_key: &str,
    now_ms: i64,
    rest_backoff: &Arc<Mutex<HashMap<String, RestBackoffState>>>,
) {
    let mut cache = rest_backoff.lock().await;
    let mut state = cache.get(token_key).copied().unwrap_or(RestBackoffState {
        backoff_ms: 0,
        until_ms: 0,
        last_fail_ms: 0,
        failures: 0,
    });
    let reset = state.last_fail_ms == 0
        || now_ms.saturating_sub(state.last_fail_ms) >= REST_BACKOFF_RESET_MS;
    let next_backoff = if reset {
        REST_BACKOFF_MIN_MS
    } else if state.backoff_ms > 0 {
        (state.backoff_ms.saturating_mul(2)).min(REST_BACKOFF_MAX_MS)
    } else {
        REST_BACKOFF_MIN_MS
    };
    let jittered_ms = jitter_backoff_ms(next_backoff);
    state.backoff_ms = next_backoff;
    state.until_ms = now_ms.saturating_add(jittered_ms);
    state.last_fail_ms = now_ms;
    state.failures = if reset {
        1
    } else {
        state.failures.saturating_add(1)
    };
    cache.insert(token_key.to_string(), state);
}

async fn mark_rest_global_backoff(
    now_ms: i64,
    rest_global_backoff: &Arc<Mutex<RestBackoffState>>,
) {
    let mut state = rest_global_backoff.lock().await;
    let reset = state.last_fail_ms == 0
        || now_ms.saturating_sub(state.last_fail_ms) >= REST_GLOBAL_BACKOFF_RESET_MS;
    let next_backoff = if reset {
        REST_GLOBAL_BACKOFF_MIN_MS
    } else if state.backoff_ms > 0 {
        (state.backoff_ms.saturating_mul(2)).min(REST_GLOBAL_BACKOFF_MAX_MS)
    } else {
        REST_GLOBAL_BACKOFF_MIN_MS
    };
    let jittered_ms = jitter_backoff_ms_with_limits(
        next_backoff,
        REST_GLOBAL_BACKOFF_MIN_MS,
        REST_GLOBAL_BACKOFF_MAX_MS,
        REST_GLOBAL_BACKOFF_JITTER_MIN,
        REST_GLOBAL_BACKOFF_JITTER_MAX,
    );
    state.backoff_ms = next_backoff;
    state.until_ms = now_ms.saturating_add(jittered_ms);
    state.last_fail_ms = now_ms;
    state.failures = if reset {
        1
    } else {
        state.failures.saturating_add(1)
    };
}

async fn rest_global_backoff_until(
    now_ms: i64,
    rest_global_backoff: &Arc<Mutex<RestBackoffState>>,
) -> Option<i64> {
    let mut state = rest_global_backoff.lock().await;
    if state.last_fail_ms > 0
        && now_ms.saturating_sub(state.last_fail_ms) >= REST_GLOBAL_BACKOFF_RESET_MS
    {
        *state = RestBackoffState {
            backoff_ms: 0,
            until_ms: 0,
            last_fail_ms: 0,
            failures: 0,
        };
        return None;
    }
    if state.until_ms > now_ms {
        Some(state.until_ms)
    } else {
        None
    }
}

async fn rest_reconnect_suppressed(
    now_ms: i64,
    reconnect_suppress_until_ms: &Arc<Mutex<i64>>,
) -> Option<i64> {
    let mut guard = reconnect_suppress_until_ms.lock().await;
    if *guard > now_ms {
        Some(*guard)
    } else {
        if *guard != 0 {
            *guard = 0;
        }
        None
    }
}

fn jitter_backoff_ms(base_ms: i64) -> i64 {
    jitter_backoff_ms_with_limits(
        base_ms,
        REST_BACKOFF_MIN_MS,
        REST_BACKOFF_MAX_MS,
        REST_BACKOFF_JITTER_MIN,
        REST_BACKOFF_JITTER_MAX,
    )
}

fn jitter_backoff_ms_with_limits(
    base_ms: i64,
    min_ms: i64,
    max_ms: i64,
    jitter_min: f64,
    jitter_max: f64,
) -> i64 {
    if base_ms <= 0 {
        return 0;
    }
    let mut rng = rand::thread_rng();
    let jitter = rng.gen_range(jitter_min..=jitter_max);
    let jittered = (base_ms as f64 * jitter).round() as i64;
    jittered.clamp(min_ms, max_ms)
}

fn classify_reqwest_error(err: &reqwest::Error) -> RestErrorKind {
    if err.is_timeout() {
        RestErrorKind::Timeout
    } else {
        RestErrorKind::Other
    }
}

fn is_recent_activity(
    last_ws_seen_ms: i64,
    last_arrival_ts_ms: i64,
    cutoff_ms: i64,
    ws_primary: bool,
) -> bool {
    if ws_primary {
        last_ws_seen_ms > 0 && last_ws_seen_ms > cutoff_ms
    } else {
        (last_ws_seen_ms > 0 && last_ws_seen_ms > cutoff_ms)
            || (last_arrival_ts_ms > 0 && last_arrival_ts_ms > cutoff_ms)
    }
}

fn partition_targets_by_activity(
    targets: &[OpinionSnapshotTarget],
    last_ws_seen_ms: &HashMap<String, i64>,
    last_arrival_ts_ms: &HashMap<String, i64>,
    cutoff_ms: i64,
    ws_primary: bool,
) -> (Vec<OpinionSnapshotTarget>, Vec<OpinionSnapshotTarget>, usize) {
    let mut active = Vec::new();
    let mut inactive = Vec::new();
    let mut unseen = 0usize;
    for target in targets {
        let last_ws = last_ws_seen_ms
            .get(&target.token_key)
            .copied()
            .unwrap_or(0);
        let last_arrival = last_arrival_ts_ms
            .get(&target.token_key)
            .copied()
            .unwrap_or(0);
        if last_ws == 0 && last_arrival == 0 {
            unseen += 1;
        }
        if is_recent_activity(last_ws, last_arrival, cutoff_ms, ws_primary) {
            active.push(target.clone());
        } else {
            inactive.push(target.clone());
        }
    }
    (active, inactive, unseen)
}

struct SummaryU128 {
    p50: u128,
    p90: u128,
    p95: u128,
    p99: u128,
    max: u128,
}

struct SummaryI64 {
    p50: i64,
    p90: i64,
    p95: i64,
    p99: i64,
    max: i64,
}

fn summarize_u128(values: &[u128]) -> Option<SummaryU128> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let len = sorted.len();
    let p50 = sorted[percentile_index(len, 50.0)];
    let p90 = sorted[percentile_index(len, 90.0)];
    let p95 = sorted[percentile_index(len, 95.0)];
    let p99 = sorted[percentile_index(len, 99.0)];
    let max = *sorted.last().unwrap_or(&p99);
    Some(SummaryU128 {
        p50,
        p90,
        p95,
        p99,
        max,
    })
}

fn summarize_i64(values: &[i64]) -> Option<SummaryI64> {
    if values.is_empty() {
        return None;
    }
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let len = sorted.len();
    let p50 = sorted[percentile_index(len, 50.0)];
    let p90 = sorted[percentile_index(len, 90.0)];
    let p95 = sorted[percentile_index(len, 95.0)];
    let p99 = sorted[percentile_index(len, 99.0)];
    let max = *sorted.last().unwrap_or(&p99);
    Some(SummaryI64 {
        p50,
        p90,
        p95,
        p99,
        max,
    })
}

fn percentile_index(len: usize, percentile: f64) -> usize {
    if len <= 1 {
        return 0;
    }
    let rank = (percentile / 100.0) * (len as f64 - 1.0);
    rank.round()
        .min(len as f64 - 1.0)
        .max(0.0) as usize
}

fn body_preview(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut out = String::new();
    for ch in text.chars().take(160) {
        if ch.is_ascii_graphic() || ch == ' ' {
            out.push(ch);
        } else {
            out.push(' ');
        }
    }
    out.trim().to_string()
}

async fn update_clock_offset(
    clock_offset: &Arc<ClockOffset>,
    db_sender: &DbSender,
    local_ts_ms: i64,
    exchange_ts_ms: i64,
) {
    let sample_offset = local_ts_ms - exchange_ts_ms;
    let (smoothed, clamped) = clock_offset.apply_sample(sample_offset, ExchangeTsSource::VenueRest);
    let sample = ClockOffsetSample {
        venue: Venue::Opinion,
        local_ts_ms,
        exchange_ts_ms,
        sample_offset_ms: sample_offset,
        smoothed_offset_ms: smoothed,
        delta_clamped_ms: clamped,
    };
    try_send_db(db_sender, DbMessage::ClockOffset(sample), "opinion_clock_offset");
}

#[derive(Debug, Deserialize)]
struct OrderbookEnvelope {
    #[serde(default)]
    result: Option<OrderbookPayload>,
    #[serde(default)]
    data: Option<OrderbookPayload>,
    #[serde(default)]
    bids: Option<Vec<RestLevel>>,
    #[serde(default)]
    asks: Option<Vec<RestLevel>>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    ts: Option<i64>,
}

impl OrderbookEnvelope {
    fn into_payload(self) -> OrderbookPayload {
        if let Some(payload) = self.result {
            return payload;
        }
        if let Some(payload) = self.data {
            return payload;
        }
        OrderbookPayload {
            bids: self.bids,
            asks: self.asks,
            timestamp: self.timestamp,
            ts: self.ts,
        }
    }
}

#[derive(Debug, Deserialize)]
struct OrderbookPayload {
    #[serde(default)]
    bids: Option<Vec<RestLevel>>,
    #[serde(default)]
    asks: Option<Vec<RestLevel>>,
    #[serde(default)]
    timestamp: Option<i64>,
    #[serde(default)]
    ts: Option<i64>,
}

#[derive(Debug, Deserialize)]
struct RestLevel {
    #[serde(default, deserialize_with = "deserialize_opt_f64")]
    price: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_opt_f64")]
    amount: Option<f64>,
    #[serde(default, deserialize_with = "deserialize_opt_f64")]
    size: Option<f64>,
}

fn deserialize_opt_f64<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Number(num)) => num
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("invalid number"))
            .map(Some),
        Some(Value::String(text)) => {
            let trimmed = text.trim();
            if trimmed.is_empty() {
                Ok(None)
            } else {
                trimmed
                    .parse::<f64>()
                    .map(Some)
                    .map_err(|_| serde::de::Error::custom("invalid f64 string"))
            }
        }
        Some(other) => Err(serde::de::Error::custom(format!(
            "unsupported value for f64: {other:?}"
        ))),
    }
}

struct RateLimiter {
    interval: Duration,
    next_allowed: Mutex<Instant>,
}

impl RateLimiter {
    fn new(max_rps: f64) -> Option<Self> {
        if max_rps <= 0.0 {
            return None;
        }
        let interval = Duration::from_secs_f64(1.0 / max_rps.max(1.0));
        Some(RateLimiter {
            interval,
            next_allowed: Mutex::new(Instant::now()),
        })
    }

    async fn wait(&self) {
        let mut next = self.next_allowed.lock().await;
        let now = Instant::now();
        let scheduled = if *next > now { *next } else { now };
        if scheduled > now {
            tokio::time::sleep_until(scheduled).await;
        }
        *next = scheduled + self.interval;
    }
}
