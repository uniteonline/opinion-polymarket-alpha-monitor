use crate::chain::SharedChain;
use crate::db_queue::{try_send_db, DbMessage, DbSender};
use crate::fee::SharedFeeSchedule;
use crate::models::{Bars1sPair, OpinionResponseRow, TokenSide, Venue};
use crate::time_utils::now_ts_ms;
use crate::trade::TradeMessage;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock, RwLock};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;
use tracing::{info, warn};

const OPI_LAG_LOG_INTERVAL_S: i64 = 60;
const OPI_LAG_WARN_THRESHOLD_S: i64 = 5;
static OPI_LAG_LOG_CACHE: OnceLock<StdMutex<HashMap<String, i64>>> = OnceLock::new();
const PAIR_HANDLE_WARN_MS: u128 = 50;
const PAIR_HANDLE_STATS_INTERVAL_MS: i64 = 60_000;
const PENDING_PROCESS_BUDGET_MS: u128 = 10;
const PENDING_PROCESS_MAX: usize = 500;
const PENDING_WORKER_QUEUE_CAPACITY: usize = 10_000;
const PENDING_STATS_LOG_INTERVAL_S: i64 = 10;
const FLUSH_STALE_BUDGET_MS: u128 = 5;
const FLUSH_STALE_MAX_KEYS: usize = 500;
const FLUSH_BUILD_BUDGET_MS: u128 = 10;
const FLUSH_BUILD_MAX_KEYS: usize = 500;

pub fn pick_pair_shard(pair_id: i64, shard_count: usize) -> usize {
    if shard_count == 0 {
        return 0;
    }
    let mut x = pair_id as u64;
    x ^= x >> 33;
    x = x.wrapping_mul(0xff51afd7ed558ccd);
    x ^= x >> 33;
    x = x.wrapping_mul(0xc4ceb9fe1a85ec53);
    x ^= x >> 33;
    (x as usize) % shard_count
}

#[derive(Default)]
struct PairHandleStats {
    window_start_ms: i64,
    total: u64,
    tokenbar: u64,
    shock: u64,
    tick: u64,
    handle_ms_sum: u128,
    handle_ms_max: u128,
    slow_count: u64,
}

impl PairHandleStats {
    fn reset(&mut self, now_ms: i64) {
        self.window_start_ms = now_ms;
        self.total = 0;
        self.tokenbar = 0;
        self.shock = 0;
        self.tick = 0;
        self.handle_ms_sum = 0;
        self.handle_ms_max = 0;
        self.slow_count = 0;
    }
}

#[derive(Debug, Clone)]
pub struct PairInput {
    pub pair_id: i64,
    pub token_side: TokenSide,
    pub bar_second: i64,
    pub venue: Venue,
    pub token_key: String,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub mid: Option<f64>,
    pub best_bid_state: Option<f64>,
    pub best_ask_state: Option<f64>,
    pub mid_state: Option<f64>,
    pub last_price: Option<f64>,
    pub micro: Option<f64>,
    pub staleness_ms: Option<i64>,
    pub bar_gap_flag: bool,
    pub bar_stale_flag: bool,
    pub spread: Option<f64>,
    pub top10_depth_bid: Option<f64>,
    pub top10_depth_ask: Option<f64>,
    pub bid_l1_notional: Option<f64>,
    pub bid_l2_notional: Option<f64>,
    pub bid_l3_notional: Option<f64>,
    pub ask_l1_notional: Option<f64>,
    pub ask_l2_notional: Option<f64>,
    pub ask_l3_notional: Option<f64>,
    pub volume_notional_1s: f64,
    pub cvd_delta_1s: f64,
    pub buy_notional_1s: f64,
    pub sell_notional_1s: f64,
    pub ofi_250ms: f64,
    pub bid_delta_notional_250ms: f64,
    pub ask_delta_notional_250ms: f64,
    pub mid_slope_250ms: Option<f64>,
    pub buy_notional_250ms: f64,
    pub sell_notional_250ms: f64,
    pub max_trade_notional_250ms: f64,
    pub max_trade_notional_1s: f64,
    pub liquidity_pull_flag: i64,
    pub depth_withdrawal_ratio: Option<f64>,
    pub obs_latency_p99_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct ShockInput {
    pub shock_id: i64,
    pub pair_id: i64,
    pub token_side: TokenSide,
    pub trigger_bar_second: i64,
    pub shock_type: String,
    pub direction: i64,
    pub magnitude: f64,
    pub noise_flag: i64,
    pub obs_latency_poly_p99_ms: Option<i64>,
}

#[derive(Debug)]
pub enum PairMessage {
    TokenBar(PairInput),
    Tick(i64),
    Shock(ShockInput),
}

#[derive(Debug, Clone)]
struct PendingResponse {
    shock_id: i64,
    pair_id: i64,
    token_side: TokenSide,
    trigger_bar_second: i64,
    horizon_s: i64,
    opi_token_key: Option<String>,
    baseline_mid: Option<f64>,
    baseline_spread: Option<f64>,
    baseline_staleness: Option<i64>,
    baseline_depth: Option<f64>,
    baseline_liquidity_pull_flag: Option<i64>,
    true_net_spread_at_t0: Option<f64>,
    arb_mode_flag_at_t0: Option<i64>,
    estimated_lag_s_at_t0: Option<i64>,
    lag_confidence_at_t0: Option<f64>,
    obs_latency_poly_p99_at_t0: Option<i64>,
    obs_latency_opi_p99_at_t0: Option<i64>,
}

#[derive(Clone, Default)]
struct PairBucket {
    pm: Option<PairInput>,
    opi: Option<PairInput>,
}

#[derive(Clone)]
struct ReturnPoint {
    bar_second: i64,
    pm_ret: Option<f64>,
    opi_ret: Option<f64>,
}

#[derive(Default)]
struct PairStats {
    last_pm_mid: Option<f64>,
    last_opi_price: Option<f64>,
    returns: VecDeque<ReturnPoint>,
    pm_buy_hist: VecDeque<f64>,
    pm_sell_hist: VecDeque<f64>,
    pm_cvd_hist: VecDeque<f64>,
    last_lag_s: Option<i64>,
    last_lag_confidence: Option<f64>,
}

struct BuildContext {
    fee_bps_polymarket: i64,
    fee_bps_opinion: i64,
    gas_cost: Option<f64>,
    congestion_flag: i64,
}

const PAIR_STATS_LOG_INTERVAL_S: i64 = 60;
const LIQ_BUCKET_LABELS: [&str; 6] = ["<10", "10-100", "100-1k", "1k-10k", ">=10k", "unknown"];
const OPI_UNINIT_SAMPLE_RATE: usize = 1;
const MISSING_SAMPLE_LIMIT: usize = 20;
const REORDER_EXTRA_CAP_S: i64 = 120;
static OPI_UNINIT_SAMPLE_COUNTER: AtomicUsize = AtomicUsize::new(0);
static TRADE_SEND_DROP_COUNTER: AtomicUsize = AtomicUsize::new(0);
static TRADE_SEND_CLOSED_COUNTER: AtomicUsize = AtomicUsize::new(0);
static OPI_UNINIT_LOG_CACHE: Mutex<Vec<String>> = Mutex::const_new(Vec::new());

#[derive(Default)]
struct FlushStats {
    ready_on_time: u64,
    ready_late: u64,
    timeout_full: u64,
    timeout_missing: u64,
    timeout_missing_pm: u64,
    timeout_missing_opi: u64,
    timeout_opi_not_ready: u64,
}

impl FlushStats {
    fn reset(&mut self) {
        *self = FlushStats::default();
    }
}

#[derive(Default)]
struct ArrivalStats {
    pm_total: u64,
    opi_total: u64,
    pm_late: u64,
    opi_late: u64,
}

impl ArrivalStats {
    fn reset(&mut self) {
        *self = ArrivalStats::default();
    }
}

#[derive(Default)]
struct MissingReasonStats {
    pm_uninitialized: u64,
    pm_lagging: u64,
    pm_ahead: u64,
    pm_no_seen: u64,
    pm_lag_max_s: i64,
    pm_ahead_min_s: i64,
    opi_uninitialized: u64,
    opi_lagging: u64,
    opi_ahead: u64,
    opi_no_seen: u64,
    opi_lag_max_s: i64,
    opi_ahead_min_s: i64,
}

impl MissingReasonStats {
    fn reset(&mut self) {
        *self = MissingReasonStats::default();
    }
}

#[derive(Default)]
struct GapStaleStats {
    total: u64,
    gap: u64,
    stale: u64,
}

impl GapStaleStats {
    fn reset(&mut self) {
        *self = GapStaleStats::default();
    }
}

#[derive(Default)]
struct LiquidityStats {
    total: [u64; 6],
    mid_nonnull: [u64; 6],
}

impl LiquidityStats {
    fn reset(&mut self) {
        *self = LiquidityStats::default();
    }
}

fn depth_bucket_idx(depth: Option<f64>) -> usize {
    match depth {
        Some(v) if v >= 0.0 && v < 10.0 => 0,
        Some(v) if v < 100.0 => 1,
        Some(v) if v < 1000.0 => 2,
        Some(v) if v < 10_000.0 => 3,
        Some(_) => 4,
        None => 5,
    }
}

struct PairAggState {
    buckets: HashMap<(i64, TokenSide, i64), PairBucket>,
    bucket_index: BTreeMap<i64, Vec<(i64, TokenSide, i64)>>,
    current_tick: i64,
    opi_token_map: HashMap<(i64, TokenSide), String>,
    pm_token_map: HashMap<(i64, TokenSide), String>,
    pending_by_target: BTreeMap<i64, Vec<PendingResponse>>,
    pending_count: usize,
    pair_stats: HashMap<(i64, TokenSide), PairStats>,
    late_arrival_buffer_s: i64,
    last_stats_log_second: i64,
    flush_stats: FlushStats,
    arrival_stats: ArrivalStats,
    missing_reason_stats: MissingReasonStats,
    gap_stale_stats: GapStaleStats,
    liquidity_stats: LiquidityStats,
    last_seen_pm: HashMap<String, i64>,
    last_seen_opi: HashMap<String, i64>,
    missing_samples_pm_lagging: Vec<String>,
    missing_samples_pm_ahead: Vec<String>,
    missing_samples_opi_lagging: Vec<String>,
    missing_samples_opi_ahead: Vec<String>,
    last_pending_log_second: i64,
    flush_queue: VecDeque<(i64, TokenSide, i64)>,
    flush_pending_keys: HashSet<(i64, TokenSide, i64)>,
}

pub struct PairAggregator {
    rx: mpsc::Receiver<PairMessage>,
    db_sender: DbSender,
    trade_sender: Option<mpsc::Sender<TradeMessage>>,
    alpha_capture_enabled: bool,
    state: Arc<Mutex<PairAggState>>,
    fee_schedule: SharedFeeSchedule,
    chain_state: SharedChain,
    opi_bootstrap_done: Arc<AtomicBool>,
    analysis_staleness_ms: i64,
    analysis_gap_allow: usize,
    analysis_move_eps: f64,
    opi_bar_history: Arc<RwLock<HashMap<String, Vec<PairInput>>>>,
    pm_bar_history: Arc<RwLock<HashMap<String, Vec<PairInput>>>>,
    pending_tx: mpsc::Sender<PendingResponse>,
    pending_rx: Option<mpsc::Receiver<PendingResponse>>,
    pending_inflight: Arc<AtomicUsize>,
    pending_try_send_fail: Arc<AtomicUsize>,
    pending_worker_processed: Arc<AtomicUsize>,
    flush_notify: Arc<Notify>,
    handle_stats: PairHandleStats,
    last_handle_log_ms: i64,
}

impl PairAggregator {
    pub fn new(
        rx: mpsc::Receiver<PairMessage>,
        db_sender: DbSender,
        trade_sender: Option<mpsc::Sender<TradeMessage>>,
        fee_schedule: SharedFeeSchedule,
        chain_state: SharedChain,
        opi_bootstrap_done: Arc<AtomicBool>,
        analysis_staleness_ms: i64,
        analysis_gap_allow: usize,
        analysis_move_eps: f64,
        late_arrival_buffer_s: i64,
        opi_token_map: HashMap<(i64, TokenSide), String>,
        pm_token_map: HashMap<(i64, TokenSide), String>,
        alpha_capture_enabled: bool,
    ) -> Self {
        let (pending_tx, pending_rx) =
            mpsc::channel::<PendingResponse>(PENDING_WORKER_QUEUE_CAPACITY);
        let pending_inflight = Arc::new(AtomicUsize::new(0));
        let pending_try_send_fail = Arc::new(AtomicUsize::new(0));
        let pending_worker_processed = Arc::new(AtomicUsize::new(0));
        let flush_notify = Arc::new(Notify::new());
        let state = PairAggState {
            buckets: HashMap::new(),
            bucket_index: BTreeMap::new(),
            current_tick: 0,
            opi_token_map,
            pm_token_map,
            pending_by_target: BTreeMap::new(),
            pending_count: 0,
            pair_stats: HashMap::new(),
            late_arrival_buffer_s: late_arrival_buffer_s.max(0),
            last_stats_log_second: 0,
            flush_stats: FlushStats::default(),
            arrival_stats: ArrivalStats::default(),
            missing_reason_stats: MissingReasonStats::default(),
            gap_stale_stats: GapStaleStats::default(),
            liquidity_stats: LiquidityStats::default(),
            last_seen_pm: HashMap::new(),
            last_seen_opi: HashMap::new(),
            missing_samples_pm_lagging: Vec::new(),
            missing_samples_pm_ahead: Vec::new(),
            missing_samples_opi_lagging: Vec::new(),
            missing_samples_opi_ahead: Vec::new(),
            last_pending_log_second: 0,
            flush_queue: VecDeque::new(),
            flush_pending_keys: HashSet::new(),
        };
        PairAggregator {
            rx,
            db_sender,
            trade_sender,
            alpha_capture_enabled,
            state: Arc::new(Mutex::new(state)),
            fee_schedule,
            chain_state,
            opi_bootstrap_done,
            analysis_staleness_ms,
            analysis_gap_allow,
            analysis_move_eps,
            opi_bar_history: Arc::new(RwLock::new(HashMap::new())),
            pm_bar_history: Arc::new(RwLock::new(HashMap::new())),
            pending_tx,
            pending_rx: Some(pending_rx),
            pending_inflight,
            pending_try_send_fail,
            pending_worker_processed,
            flush_notify,
            handle_stats: PairHandleStats::default(),
            last_handle_log_ms: 0,
        }
    }

    pub async fn run(mut self) {
        info!(
            "pair_aggregator_start alpha_capture_enabled={} pending_queue_capacity={} analysis_staleness_ms={} analysis_gap_allow={} analysis_move_eps={}",
            self.alpha_capture_enabled,
            PENDING_WORKER_QUEUE_CAPACITY,
            self.analysis_staleness_ms,
            self.analysis_gap_allow,
            self.analysis_move_eps
        );
        if !self.alpha_capture_enabled {
            warn!("alpha_capture_enabled=false pending_responses will be skipped");
        }
        if let Some(pending_rx) = self.pending_rx.take() {
            let opi_history = self.opi_bar_history.clone();
            let analysis_gap_allow = self.analysis_gap_allow;
            let analysis_staleness_ms = self.analysis_staleness_ms;
            let analysis_move_eps = self.analysis_move_eps;
            let alpha_capture_enabled = self.alpha_capture_enabled;
            let db_sender = self.db_sender.clone();
            let pending_inflight = self.pending_inflight.clone();
            let pending_worker_processed = self.pending_worker_processed.clone();
            tokio::spawn(async move {
                pending_worker_loop(
                    pending_rx,
                    opi_history,
                    analysis_gap_allow,
                    analysis_staleness_ms,
                    analysis_move_eps,
                    alpha_capture_enabled,
                    db_sender,
                    pending_inflight,
                    pending_worker_processed,
                )
                .await;
            });
        }
        {
            let state = self.state.clone();
            let flush_notify = self.flush_notify.clone();
            let fee_schedule = self.fee_schedule.clone();
            let chain_state = self.chain_state.clone();
            let db_sender = self.db_sender.clone();
            let trade_sender = self.trade_sender.clone();
            let alpha_capture_enabled = self.alpha_capture_enabled;
            let opi_bootstrap_done = self.opi_bootstrap_done.clone();
            tokio::spawn(async move {
                flush_worker_loop(
                    state,
                    flush_notify,
                    fee_schedule,
                    chain_state,
                    db_sender,
                    trade_sender,
                    alpha_capture_enabled,
                    opi_bootstrap_done,
                )
                .await;
            });
        }
        while let Some(msg) = self.rx.recv().await {
            let handle_start = Instant::now();
            let mut handle_kind: &'static str = "unknown";
            let mut store_ms: u128 = 0;
            let mut build_ms: u128 = 0;
            let mut send_ms: u128 = 0;
            let mut db_send_ms: u128 = 0;
            let mut trade_send_ms: u128 = 0;
            let mut trade_queue_remaining_before: Option<usize> = None;
            let mut trade_queue_remaining_after: Option<usize> = None;
            let mut flush_ms: u128 = 0;
            let mut shock_ms: u128 = 0;
            match msg {
                PairMessage::TokenBar(input) => {
                    handle_kind = "tokenbar";
                    self.handle_stats.tokenbar += 1;
                    let store_start = Instant::now();
                    let key = (input.pair_id, input.token_side, input.bar_second);
                    if input.venue == Venue::Opinion {
                        self.store_opi_bar(&input);
                    } else if input.venue == Venue::Polymarket {
                        self.store_pm_bar(&input);
                    }
                    let opi_ready = self.opi_bootstrap_done.load(Ordering::Relaxed);
                    let mut ready_bucket: Option<PairBucket> = None;
                    {
                        let mut state = self.state.lock().await;
                        state.record_arrival(&input);
                        if input.venue == Venue::Opinion {
                            state
                                .opi_token_map
                                .insert((input.pair_id, input.token_side), input.token_key.clone());
                        } else if input.venue == Venue::Polymarket {
                            state
                                .pm_token_map
                                .insert((input.pair_id, input.token_side), input.token_key.clone());
                        }
                        if !state.buckets.contains_key(&key) {
                            state.bucket_index.entry(key.2).or_default().push(key);
                        }
                        let bucket = state
                            .buckets
                            .entry(key)
                            .or_insert_with(PairBucket::default);
                        match input.venue {
                            Venue::Polymarket => bucket.pm = Some(input),
                            Venue::Opinion => bucket.opi = Some(input),
                        }
                        if bucket.pm.is_some() && bucket.opi.is_some() {
                            ready_bucket = Some(bucket.clone());
                            state.buckets.remove(&key);
                            state.flush_pending_keys.remove(&key);
                        }
                    }
                    store_ms = store_start.elapsed().as_millis();
                    if let Some(bucket) = ready_bucket {
                        let build_start = Instant::now();
                        let ctx = load_build_context(&self.fee_schedule, &self.chain_state, key.2)
                            .await;
                        let bar = {
                            let mut state = self.state.lock().await;
                            let bar = state.build_bar_with_context(key, &bucket, &ctx, opi_ready);
                            state.record_flush_ready(key.2);
                            bar
                        };
                        build_ms = build_start.elapsed().as_millis();
                        let db_send_start = Instant::now();
                        if self.alpha_capture_enabled {
                            try_send_db(
                                &self.db_sender,
                                DbMessage::Bars1sPair(bar.clone()),
                                "pair_bars1s_pair_ready",
                            );
                        }
                        db_send_ms = db_send_start.elapsed().as_millis();
                        let trade_send_start = Instant::now();
                        if let Some(sender) = &self.trade_sender {
                            trade_queue_remaining_before = Some(sender.capacity());
                            let _ = try_send_trade(sender, TradeMessage::PairBar(bar), "pair_bar");
                            trade_queue_remaining_after = Some(sender.capacity());
                        }
                        trade_send_ms = trade_send_start.elapsed().as_millis();
                        send_ms = db_send_ms + trade_send_ms;
                    }
                }
                PairMessage::Tick(bar_second) => {
                    handle_kind = "tick";
                    self.handle_stats.tick += 1;
                    let flush_start = Instant::now();
                    let opi_ready = self.opi_bootstrap_done.load(Ordering::Relaxed);
                    {
                        let mut state = self.state.lock().await;
                        state.current_tick = bar_second;
                        let enqueued = state.flush_stale(opi_ready);
                        if enqueued > 0 {
                            self.flush_notify.notify_one();
                        }
                        state.flush_pending(
                            &self.pending_tx,
                            &self.pending_inflight,
                            &self.pending_try_send_fail,
                            self.alpha_capture_enabled,
                        );
                        state.maybe_log_pending_stats(
                            &self.pending_inflight,
                            &self.pending_try_send_fail,
                            &self.pending_worker_processed,
                        );
                        state.maybe_log_stats(
                            &self.pending_inflight,
                            &self.pending_try_send_fail,
                            &self.pending_worker_processed,
                        );
                    }
                    flush_ms = flush_start.elapsed().as_millis();
                }
                PairMessage::Shock(shock) => {
                    handle_kind = "shock";
                    self.handle_stats.shock += 1;
                    let shock_start = Instant::now();
                    self.handle_shock(shock).await;
                    shock_ms = shock_start.elapsed().as_millis();
                }
            }
            let handle_ms = handle_start.elapsed().as_millis();
            self.handle_stats.total += 1;
            self.handle_stats.handle_ms_sum += handle_ms;
            if handle_ms > self.handle_stats.handle_ms_max {
                self.handle_stats.handle_ms_max = handle_ms;
            }
            let (buckets_len, pending_count, flush_queue_len, current_tick, last_stats_log_second) = {
                let state = self.state.lock().await;
                (
                    state.buckets.len(),
                    state.pending_count,
                    state.flush_queue.len(),
                    state.current_tick,
                    state.last_stats_log_second,
                )
            };
            if handle_ms > PAIR_HANDLE_WARN_MS {
                self.handle_stats.slow_count += 1;
                warn!(
                    "pair_handle_slow handle_ms={} kind={} store_ms={} build_ms={} send_ms={} db_send_ms={} trade_send_ms={} trade_queue_remaining_before={:?} trade_queue_remaining_after={:?} flush_ms={} shock_ms={} buckets={} pending_responses={} flush_queue_len={} current_tick={} last_stats_log_second={}",
                    handle_ms,
                    handle_kind,
                    store_ms,
                    build_ms,
                    send_ms,
                    db_send_ms,
                    trade_send_ms,
                    trade_queue_remaining_before,
                    trade_queue_remaining_after,
                    flush_ms,
                    shock_ms,
                    buckets_len,
                    pending_count,
                    flush_queue_len,
                    current_tick,
                    last_stats_log_second
                );
            }
            self.maybe_log_handle_stats(buckets_len, pending_count, flush_queue_len);
        }
    }

    fn maybe_log_handle_stats(
        &mut self,
        buckets_len: usize,
        pending_count: usize,
        flush_queue_len: usize,
    ) {
        let now_ms = now_ts_ms();
        if self.handle_stats.window_start_ms == 0 {
            self.handle_stats.reset(now_ms);
            self.last_handle_log_ms = now_ms;
            return;
        }
        if now_ms.saturating_sub(self.last_handle_log_ms) < PAIR_HANDLE_STATS_INTERVAL_MS {
            return;
        }
        let avg_ms = if self.handle_stats.total > 0 {
            (self.handle_stats.handle_ms_sum / self.handle_stats.total as u128) as u64
        } else {
            0
        };
        info!(
            "pair_handle_stats window_ms={} total={} tokenbar={} shock={} tick={} avg_ms={} max_ms={} slow_count={} buckets={} pending_responses={} flush_queue_len={}",
            now_ms.saturating_sub(self.handle_stats.window_start_ms),
            self.handle_stats.total,
            self.handle_stats.tokenbar,
            self.handle_stats.shock,
            self.handle_stats.tick,
            avg_ms,
            self.handle_stats.handle_ms_max,
            self.handle_stats.slow_count,
            buckets_len,
            pending_count,
            flush_queue_len
        );
        self.handle_stats.reset(now_ms);
        self.last_handle_log_ms = now_ms;
    }

    // flush_stale and other state helpers live on PairAggState

    fn store_opi_bar(&mut self, input: &PairInput) {
        let Ok(mut history) = self.opi_bar_history.write() else {
            return;
        };
        let entry = history
            .entry(input.token_key.clone())
            .or_insert_with(Vec::new);
        entry.push(input.clone());
        if entry.len() > 600 {
            entry.remove(0);
        }
    }

    fn store_pm_bar(&mut self, input: &PairInput) {
        let Ok(mut history) = self.pm_bar_history.write() else {
            return;
        };
        let entry = history
            .entry(input.token_key.clone())
            .or_insert_with(Vec::new);
        entry.push(input.clone());
        if entry.len() > 600 {
            entry.remove(0);
        }
    }

}

impl PairAggState {
    fn record_flush_ready(&mut self, bar_second: i64) {
        if self.current_tick > 0 && bar_second < self.current_tick {
            self.flush_stats.ready_late += 1;
        } else {
            self.flush_stats.ready_on_time += 1;
        }
    }

    fn record_flush_timeout(
        &mut self,
        key: (i64, TokenSide, i64),
        bucket: &PairBucket,
        opi_ready: bool,
    ) {
        let (pair_id, token_side, bar_second) = key;
        let has_pm = bucket.pm.is_some();
        let has_opi = bucket.opi.is_some();
        if has_pm && has_opi {
            self.flush_stats.timeout_full += 1;
        } else {
            self.flush_stats.timeout_missing += 1;
            if !has_pm {
                self.flush_stats.timeout_missing_pm += 1;
                self.record_missing_reason(Venue::Polymarket, pair_id, token_side, bar_second);
            }
            if !has_opi {
                if opi_ready {
                    self.flush_stats.timeout_missing_opi += 1;
                    self.record_missing_reason(Venue::Opinion, pair_id, token_side, bar_second);
                } else {
                    self.flush_stats.timeout_opi_not_ready += 1;
                }
            }
        }
    }

    fn should_flush_bucket(
        &self,
        key: (i64, TokenSide, i64),
        bucket: &PairBucket,
        opi_ready: bool,
    ) -> bool {
        if self.current_tick <= 0 {
            return false;
        }
        let (pair_id, token_side, bar_second) = key;
        let mut extra_buffer = 0i64;
        if bucket.pm.is_none() {
            if let Some(token_key) = self.pm_token_map.get(&(pair_id, token_side)) {
                if let Some(last_seen) = self.last_seen_pm.get(token_key) {
                    let ahead = last_seen.saturating_sub(bar_second);
                    if ahead > 0 {
                        extra_buffer = extra_buffer.max(ahead.min(REORDER_EXTRA_CAP_S));
                    }
                }
            }
        }
        if bucket.opi.is_none() && opi_ready {
            if let Some(token_key) = self.opi_token_map.get(&(pair_id, token_side)) {
                if let Some(last_seen) = self.last_seen_opi.get(token_key) {
                    let ahead = last_seen.saturating_sub(bar_second);
                    if ahead > 0 {
                        extra_buffer = extra_buffer.max(ahead.min(REORDER_EXTRA_CAP_S));
                    }
                }
            }
        }
        let cutoff = self.current_tick.saturating_sub(
            self.late_arrival_buffer_s
                .max(0)
                .saturating_add(extra_buffer),
        );
        bar_second < cutoff
    }

    fn record_arrival(&mut self, input: &PairInput) {
        let cutoff = self
            .current_tick
            .saturating_sub(self.late_arrival_buffer_s.max(0));
        let is_late = self.current_tick > 0 && input.bar_second < cutoff;
        match input.venue {
            Venue::Polymarket => {
                self.arrival_stats.pm_total += 1;
                if is_late {
                    self.arrival_stats.pm_late += 1;
                }
                self.last_seen_pm
                    .insert(input.token_key.clone(), input.bar_second);
            }
            Venue::Opinion => {
                self.arrival_stats.opi_total += 1;
                if is_late {
                    self.arrival_stats.opi_late += 1;
                }
                self.last_seen_opi
                    .insert(input.token_key.clone(), input.bar_second);
            }
        }
    }

    fn flush_stale(&mut self, opi_ready: bool) -> usize {
        if self.current_tick <= 0 {
            return 0;
        }
        let cutoff = self
            .current_tick
            .saturating_sub(self.late_arrival_buffer_s.max(0));
        let start = Instant::now();
        let mut processed = 0usize;
        while processed < FLUSH_STALE_MAX_KEYS
            && start.elapsed().as_millis() <= FLUSH_STALE_BUDGET_MS
        {
            let Some((&bar_second, _)) = self.bucket_index.first_key_value() else {
                break;
            };
            if bar_second >= cutoff {
                break;
            }
            let (bar_second, keys) = self
                .bucket_index
                .pop_first()
                .expect("bucket_index should have first entry");
            let mut remaining_keys: Vec<(i64, TokenSide, i64)> = Vec::new();
            let mut iter = keys.into_iter();
            while let Some(key) = iter.next() {
                if processed >= FLUSH_STALE_MAX_KEYS
                    || start.elapsed().as_millis() > FLUSH_STALE_BUDGET_MS
                {
                    remaining_keys.push(key);
                    remaining_keys.extend(iter);
                    break;
                }
                if self.flush_pending_keys.contains(&key) {
                    continue;
                }
                let bucket = match self.buckets.get(&key) {
                    Some(bucket) => bucket,
                    None => {
                        continue;
                    }
                };
                if self.should_flush_bucket(key, bucket, opi_ready) {
                    if self.flush_pending_keys.insert(key) {
                        self.flush_queue.push_back(key);
                        processed += 1;
                    }
                } else {
                    remaining_keys.push(key);
                }
            }
            if !remaining_keys.is_empty() {
                self.bucket_index.insert(bar_second, remaining_keys);
            }
        }
        processed
    }

    fn drain_flush_queue(&mut self) -> Vec<(i64, TokenSide, i64)> {
        if self.flush_queue.is_empty() {
            return Vec::new();
        }
        let start = Instant::now();
        let mut processed = 0usize;
        let mut keys: Vec<(i64, TokenSide, i64)> = Vec::new();
        while processed < FLUSH_BUILD_MAX_KEYS
            && start.elapsed().as_millis() <= FLUSH_BUILD_BUDGET_MS
        {
            let Some(key) = self.flush_queue.pop_front() else {
                break;
            };
            if !self.flush_pending_keys.remove(&key) {
                continue;
            }
            keys.push(key);
            processed += 1;
        }
        keys
    }

    fn record_missing_reason(
        &mut self,
        venue: Venue,
        pair_id: i64,
        token_side: TokenSide,
        bar_second: i64,
    ) {
        match venue {
            Venue::Polymarket => {
                let token_key = self
                    .pm_token_map
                    .get(&(pair_id, token_side))
                    .cloned();
                let Some(token_key) = token_key else {
                    self.missing_reason_stats.pm_uninitialized += 1;
                    return;
                };
                let last_seen = self.last_seen_pm.get(&token_key).copied();
                let Some(last_seen) = last_seen else {
                    self.missing_reason_stats.pm_no_seen += 1;
                    return;
                };
                let lag = bar_second - last_seen;
                if lag > 0 {
                    self.missing_reason_stats.pm_lagging += 1;
                    if lag > self.missing_reason_stats.pm_lag_max_s {
                        self.missing_reason_stats.pm_lag_max_s = lag;
                    }
                    self.push_missing_sample(
                        Venue::Polymarket,
                        "lagging",
                        pair_id,
                        token_side,
                        bar_second,
                        &token_key,
                        last_seen,
                        lag,
                    );
                } else if lag < 0 {
                    self.missing_reason_stats.pm_ahead += 1;
                    if self.missing_reason_stats.pm_ahead_min_s == 0
                        || lag < self.missing_reason_stats.pm_ahead_min_s
                    {
                        self.missing_reason_stats.pm_ahead_min_s = lag;
                    }
                    self.push_missing_sample(
                        Venue::Polymarket,
                        "ahead",
                        pair_id,
                        token_side,
                        bar_second,
                        &token_key,
                        last_seen,
                        lag,
                    );
                }
            }
            Venue::Opinion => {
                let token_key = self
                    .opi_token_map
                    .get(&(pair_id, token_side))
                    .cloned();
                let Some(token_key) = token_key else {
                    self.missing_reason_stats.opi_uninitialized += 1;
                    self.log_opi_uninit_sample(pair_id, token_side, bar_second);
                    return;
                };
                let last_seen = self.last_seen_opi.get(&token_key).copied();
                let Some(last_seen) = last_seen else {
                    self.missing_reason_stats.opi_no_seen += 1;
                    return;
                };
                let lag = bar_second - last_seen;
                if lag > 0 {
                    self.missing_reason_stats.opi_lagging += 1;
                    if lag > self.missing_reason_stats.opi_lag_max_s {
                        self.missing_reason_stats.opi_lag_max_s = lag;
                    }
                    if lag >= OPI_LAG_WARN_THRESHOLD_S
                        && Self::should_log_opi_lag(&token_key, bar_second)
                    {
                        tracing::warn!(
                            "opi_lagging token_key={} pair_id={} token_side={:?} bar_second={} last_seen={} lag_s={}",
                            token_key,
                            pair_id,
                            token_side,
                            bar_second,
                            last_seen,
                            lag
                        );
                    }
                    self.push_missing_sample(
                        Venue::Opinion,
                        "lagging",
                        pair_id,
                        token_side,
                        bar_second,
                        &token_key,
                        last_seen,
                        lag,
                    );
                } else if lag < 0 {
                    self.missing_reason_stats.opi_ahead += 1;
                    if self.missing_reason_stats.opi_ahead_min_s == 0
                        || lag < self.missing_reason_stats.opi_ahead_min_s
                    {
                        self.missing_reason_stats.opi_ahead_min_s = lag;
                    }
                    self.push_missing_sample(
                        Venue::Opinion,
                        "ahead",
                        pair_id,
                        token_side,
                        bar_second,
                        &token_key,
                        last_seen,
                        lag,
                    );
                }
            }
        }
    }

    fn should_log_opi_lag(token_key: &str, bar_second: i64) -> bool {
        let cache = OPI_LAG_LOG_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
        let mut guard = cache.lock().unwrap_or_else(|err| err.into_inner());
        let last = guard.get(token_key).copied().unwrap_or(0);
        if bar_second.saturating_sub(last) >= OPI_LAG_LOG_INTERVAL_S {
            guard.insert(token_key.to_string(), bar_second);
            true
        } else {
            false
        }
    }

    fn push_missing_sample(
        &mut self,
        venue: Venue,
        reason: &str,
        pair_id: i64,
        token_side: TokenSide,
        bar_second: i64,
        token_key: &str,
        last_seen: i64,
        lag_s: i64,
    ) {
        let cutoff = self
            .current_tick
            .saturating_sub(self.late_arrival_buffer_s.max(0));
        let entry = format!(
            "pair_id={} token_side={:?} bar_second={} last_seen={} lag_s={} current_tick={} cutoff={} token_key={}",
            pair_id, token_side, bar_second, last_seen, lag_s, self.current_tick, cutoff, token_key
        );
        let target = match (venue, reason) {
            (Venue::Polymarket, "lagging") => &mut self.missing_samples_pm_lagging,
            (Venue::Polymarket, "ahead") => &mut self.missing_samples_pm_ahead,
            (Venue::Opinion, "lagging") => &mut self.missing_samples_opi_lagging,
            (Venue::Opinion, "ahead") => &mut self.missing_samples_opi_ahead,
            _ => return,
        };
        if target.len() < MISSING_SAMPLE_LIMIT {
            target.push(entry);
        }
    }

    fn log_opi_uninit_sample(&self, pair_id: i64, token_side: TokenSide, bar_second: i64) {
        let count = OPI_UNINIT_SAMPLE_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        if count % OPI_UNINIT_SAMPLE_RATE != 0 {
            return;
        }
        let pm_token_key = self
            .pm_token_map
            .get(&(pair_id, token_side))
            .cloned()
            .unwrap_or_else(|| "pm_token_key_missing".to_string());
        let entry = format!(
            "pair_id={} token_side={:?} bar_second={} opi_token_key_map_missing pm_token_key={}",
            pair_id, token_side, bar_second, pm_token_key
        );
        let cache = &OPI_UNINIT_LOG_CACHE;
        tokio::spawn(async move {
            let mut guard = cache.lock().await;
            if guard.len() >= 50 {
                guard.remove(0);
            }
            guard.push(entry);
            tracing::warn!(
                "opi_uninit_sample count={} sample={:?}",
                count,
                guard.as_slice()
            );
        });
    }

    fn maybe_log_stats(
        &mut self,
        pending_inflight: &Arc<AtomicUsize>,
        pending_try_send_fail: &Arc<AtomicUsize>,
        pending_worker_processed: &Arc<AtomicUsize>,
    ) {
        if self.current_tick <= 0 {
            return;
        }
        if self.last_stats_log_second == 0 {
            self.last_stats_log_second = self.current_tick;
            return;
        }
        if self.current_tick - self.last_stats_log_second < PAIR_STATS_LOG_INTERVAL_S {
            return;
        }
        let gap_ratio = if self.gap_stale_stats.total > 0 {
            self.gap_stale_stats.gap as f64 / self.gap_stale_stats.total as f64
        } else {
            0.0
        };
        let stale_ratio = if self.gap_stale_stats.total > 0 {
            self.gap_stale_stats.stale as f64 / self.gap_stale_stats.total as f64
        } else {
            0.0
        };
        let mut liq_parts = Vec::new();
        let mut pending_targets_total = 0usize;
        let mut pending_targets_due = 0usize;
        let mut pending_items_total = 0usize;
        let mut pending_items_due = 0usize;
        for (target, items) in self.pending_by_target.iter() {
            pending_targets_total += 1;
            pending_items_total += items.len();
            if *target <= self.current_tick {
                pending_targets_due += 1;
                pending_items_due += items.len();
            } else {
                break;
            }
        }
        for (idx, label) in LIQ_BUCKET_LABELS.iter().enumerate() {
            let total = self.liquidity_stats.total[idx];
            let nonnull = self.liquidity_stats.mid_nonnull[idx];
            if total > 0 {
                let ratio = nonnull as f64 / total as f64;
                liq_parts.push(format!("{label}:{ratio:.3}({nonnull}/{total})"));
            }
        }
        tracing::info!(
            "pair_stats window_s={} ready_on_time={} ready_late={} timeout_full={} timeout_missing={} missing_pm={} missing_opi={} opi_not_ready={} gap_ratio={:.3} stale_ratio={:.3} bars_in_pm={} bars_in_opi={} late_pm={} late_opi={} pending_targets_total={} pending_targets_due={} pending_items_total={} pending_items_due={} pending_inflight={} pending_try_send_fail={} pending_worker_processed={} missing_reason_pm=[uninit={} lagging={} ahead={} no_seen={} lag_max_s={} ahead_min_s={}] missing_reason_opi=[uninit={} lagging={} ahead={} no_seen={} lag_max_s={} ahead_min_s={}] poly_mid_ratio_by_depth=[{}]",
            PAIR_STATS_LOG_INTERVAL_S,
            self.flush_stats.ready_on_time,
            self.flush_stats.ready_late,
            self.flush_stats.timeout_full,
            self.flush_stats.timeout_missing,
            self.flush_stats.timeout_missing_pm,
            self.flush_stats.timeout_missing_opi,
            self.flush_stats.timeout_opi_not_ready,
            gap_ratio,
            stale_ratio,
            self.arrival_stats.pm_total,
            self.arrival_stats.opi_total,
            self.arrival_stats.pm_late,
            self.arrival_stats.opi_late,
            pending_targets_total,
            pending_targets_due,
            pending_items_total,
            pending_items_due,
            pending_inflight.load(Ordering::Relaxed),
            pending_try_send_fail.load(Ordering::Relaxed),
            pending_worker_processed.load(Ordering::Relaxed),
            self.missing_reason_stats.pm_uninitialized,
            self.missing_reason_stats.pm_lagging,
            self.missing_reason_stats.pm_ahead,
            self.missing_reason_stats.pm_no_seen,
            self.missing_reason_stats.pm_lag_max_s,
            self.missing_reason_stats.pm_ahead_min_s,
            self.missing_reason_stats.opi_uninitialized,
            self.missing_reason_stats.opi_lagging,
            self.missing_reason_stats.opi_ahead,
            self.missing_reason_stats.opi_no_seen,
            self.missing_reason_stats.opi_lag_max_s,
            self.missing_reason_stats.opi_ahead_min_s,
            liq_parts.join(", ")
        );
        if !self.missing_samples_pm_lagging.is_empty()
            || !self.missing_samples_pm_ahead.is_empty()
            || !self.missing_samples_opi_lagging.is_empty()
            || !self.missing_samples_opi_ahead.is_empty()
        {
            tracing::info!(
                "missing_reason_samples pm_lagging=[{}] pm_ahead=[{}] opi_lagging=[{}] opi_ahead=[{}]",
                self.missing_samples_pm_lagging.join(", "),
                self.missing_samples_pm_ahead.join(", "),
                self.missing_samples_opi_lagging.join(", "),
                self.missing_samples_opi_ahead.join(", ")
            );
        }
        self.last_stats_log_second = self.current_tick;
        self.flush_stats.reset();
        self.arrival_stats.reset();
        self.missing_reason_stats.reset();
        self.missing_samples_pm_lagging.clear();
        self.missing_samples_pm_ahead.clear();
        self.missing_samples_opi_lagging.clear();
        self.missing_samples_opi_ahead.clear();
        self.gap_stale_stats.reset();
        self.liquidity_stats.reset();
    }

    fn maybe_log_pending_stats(
        &mut self,
        pending_inflight: &Arc<AtomicUsize>,
        pending_try_send_fail: &Arc<AtomicUsize>,
        pending_worker_processed: &Arc<AtomicUsize>,
    ) {
        if self.current_tick <= 0 {
            return;
        }
        if self.last_pending_log_second == 0 {
            self.last_pending_log_second = self.current_tick;
            return;
        }
        if self.current_tick - self.last_pending_log_second < PENDING_STATS_LOG_INTERVAL_S {
            return;
        }
        let mut pending_targets_total = 0usize;
        let mut pending_targets_due = 0usize;
        let mut pending_items_total = 0usize;
        let mut pending_items_due = 0usize;
        for (target, items) in self.pending_by_target.iter() {
            pending_targets_total += 1;
            pending_items_total += items.len();
            if *target <= self.current_tick {
                pending_targets_due += 1;
                pending_items_due += items.len();
            } else {
                break;
            }
        }
        tracing::info!(
            "pending_stats tick={} targets_total={} targets_due={} items_total={} items_due={} inflight={} try_send_fail={} worker_processed={}",
            self.current_tick,
            pending_targets_total,
            pending_targets_due,
            pending_items_total,
            pending_items_due,
            pending_inflight.load(Ordering::Relaxed),
            pending_try_send_fail.load(Ordering::Relaxed),
            pending_worker_processed.load(Ordering::Relaxed),
        );
        self.last_pending_log_second = self.current_tick;
    }

    fn build_bar_with_context(
        &mut self,
        key: (i64, TokenSide, i64),
        bucket: &PairBucket,
        ctx: &BuildContext,
        opi_ready: bool,
    ) -> Bars1sPair {
        let (pair_id, token_side, bar_second) = key;
        let pm = bucket.pm.as_ref();
        let opi = bucket.opi.as_ref();

        let poly_best_bid = pm.and_then(|v| v.best_bid);
        let poly_best_ask = pm.and_then(|v| v.best_ask);
        let opi_best_bid = opi.and_then(|v| v.best_bid);
        let opi_best_ask = opi.and_then(|v| v.best_ask);
        let poly_best_bid_state = pm.and_then(|v| v.best_bid_state.or(v.best_bid));
        let poly_best_ask_state = pm.and_then(|v| v.best_ask_state.or(v.best_ask));
        let opi_best_bid_state = opi.and_then(|v| v.best_bid_state.or(v.best_bid));
        let opi_best_ask_state = opi.and_then(|v| v.best_ask_state.or(v.best_ask));

        let gross_spread_buy = match (poly_best_bid_state, opi_best_ask_state) {
            (Some(bid), Some(ask)) => Some(bid - ask),
            _ => None,
        };
        let gross_spread_sell = match (opi_best_bid_state, poly_best_ask_state) {
            (Some(bid), Some(ask)) => Some(bid - ask),
            _ => None,
        };

        let fee_bps_polymarket = ctx.fee_bps_polymarket;
        let fee_bps_opinion = ctx.fee_bps_opinion;
        let poly_fee = fee_bps_polymarket as f64 / 10_000.0;
        let opi_fee = fee_bps_opinion as f64 / 10_000.0;

        let gas_cost = ctx.gas_cost;
        let congestion_flag = ctx.congestion_flag;
        let gas_cost_value = gas_cost.unwrap_or(0.0);

        let true_net_spread_buy_opinion = match (poly_best_bid_state, opi_best_ask_state) {
            (Some(bid), Some(ask)) => {
                Some(bid * (1.0 - poly_fee) - ask * (1.0 + opi_fee) - gas_cost_value)
            }
            _ => None,
        };
        let true_net_spread_sell_opinion = match (opi_best_bid_state, poly_best_ask_state) {
            (Some(bid), Some(ask)) => {
                Some(bid * (1.0 - opi_fee) - ask * (1.0 + poly_fee) - gas_cost_value)
            }
            _ => None,
        };

        let arb_mode_flag = if true_net_spread_buy_opinion.unwrap_or(0.0) > 0.0
            || true_net_spread_sell_opinion.unwrap_or(0.0) > 0.0
        {
            1
        } else {
            0
        };

        let opi_missing = opi.is_none();
        let poly_gap = pm.map(|v| v.bar_gap_flag).unwrap_or(true);
        let opi_gap = if opi_missing && !opi_ready {
            false
        } else {
            opi.map(|v| v.bar_gap_flag).unwrap_or(true)
        };
        let poly_stale = pm.map(|v| v.bar_stale_flag).unwrap_or(false);
        let opi_stale = if opi_missing && !opi_ready {
            true
        } else {
            opi.map(|v| v.bar_stale_flag).unwrap_or(false)
        };
        let bar_gap_flag = if poly_gap || opi_gap { 1 } else { 0 };
        let bar_stale_flag = if poly_stale || opi_stale { 1 } else { 0 };

        let stats = self
            .pair_stats
            .entry((pair_id, token_side))
            .or_insert_with(PairStats::default);

        let pm_mid = pm.and_then(|v| v.mid_state.or(v.mid));
        let opi_mid = opi.and_then(|v| v.mid_state.or(v.mid));
        let opi_last_price = opi.and_then(|v| v.last_price);
        let opi_price_signal = opi_last_price.or(opi_mid);
        let pm_ret = log_return(pm_mid, stats.last_pm_mid);
        let opi_ret = log_return(opi_price_signal, stats.last_opi_price);
        if pm_mid.is_some() {
            stats.last_pm_mid = pm_mid;
        }
        if opi_price_signal.is_some() {
            stats.last_opi_price = opi_price_signal;
        }
        stats.returns.push_back(ReturnPoint {
            bar_second,
            pm_ret,
            opi_ret,
        });
        trim_returns(&mut stats.returns, 600);

        if let Some(pm_bar) = pm {
            if !pm_bar.bar_gap_flag && !pm_bar.bar_stale_flag {
                push_window(&mut stats.pm_buy_hist, pm_bar.buy_notional_1s, 30);
                push_window(&mut stats.pm_sell_hist, pm_bar.sell_notional_1s, 30);
                push_window(&mut stats.pm_cvd_hist, pm_bar.cvd_delta_1s, 30);
            }
        }

        let poly_cvd_30s = sum_window(&stats.pm_cvd_hist);
        let buy_30 = sum_window(&stats.pm_buy_hist);
        let sell_30 = sum_window(&stats.pm_sell_hist);
        let poly_buy_sell_ratio_30s = match (buy_30, sell_30) {
            (Some(buy), Some(sell)) if sell > 0.0 => Some(buy / sell),
            _ => None,
        };
        let price_pressure_index = match (poly_cvd_30s, opi.and_then(|v| v.top10_depth_ask)) {
            (Some(cvd), Some(depth)) if depth > 0.0 => Some(cvd / depth),
            _ => None,
        };

        let (rolling_corr_5m, beta_1m, corr_samples_5m) = compute_corr_beta(&stats.returns);
        let (estimated_lag_s, lag_confidence) = estimate_lag(&stats.returns, 60);
        stats.last_lag_s = estimated_lag_s;
        stats.last_lag_confidence = lag_confidence;

        self.gap_stale_stats.total += 1;
        if bar_gap_flag != 0 {
            self.gap_stale_stats.gap += 1;
        }
        if bar_stale_flag != 0 {
            self.gap_stale_stats.stale += 1;
        }
        let pm_depth_bid = pm.and_then(|v| v.top10_depth_bid.or(v.bid_l1_notional));
        let pm_depth_ask = pm.and_then(|v| v.top10_depth_ask.or(v.ask_l1_notional));
        let pm_depth = match (pm_depth_bid, pm_depth_ask) {
            (Some(bid), Some(ask)) => Some(bid + ask),
            (Some(bid), None) => Some(bid),
            (None, Some(ask)) => Some(ask),
            _ => None,
        };
        let pm_depth_imbalance = match (pm_depth_bid, pm_depth_ask) {
            (Some(bid), Some(ask)) if bid + ask > 0.0 => Some((bid - ask) / (bid + ask)),
            _ => None,
        };
        let pm_pressure_250ms = pm.and_then(|v| match pm_depth {
            Some(depth) if depth > 0.0 => {
                Some((v.bid_delta_notional_250ms - v.ask_delta_notional_250ms) / depth)
            }
            _ => None,
        });
        let pm_spread_bps = match (poly_best_bid_state, poly_best_ask_state, pm_mid) {
            (Some(bid), Some(ask), Some(mid)) if mid > 0.0 => Some((ask - bid) / mid * 10_000.0),
            _ => None,
        };
        let pm_micro_bias_bps = match (pm.and_then(|v| v.micro), pm_mid) {
            (Some(micro), Some(mid)) if mid > 0.0 => Some((micro - mid) / mid * 10_000.0),
            _ => None,
        };
        let book_alpha_raw = match (pm_pressure_250ms, pm_depth_imbalance) {
            (Some(pressure), Some(imbalance)) => Some(0.6 * pressure + 0.4 * imbalance),
            (Some(pressure), None) => Some(pressure),
            (None, Some(imbalance)) => Some(imbalance),
            _ => None,
        };
        let depth_idx = depth_bucket_idx(pm_depth);
        self.liquidity_stats.total[depth_idx] += 1;
        if pm_mid.is_some() {
            self.liquidity_stats.mid_nonnull[depth_idx] += 1;
        }

        let arb_proximity = match (true_net_spread_buy_opinion, true_net_spread_sell_opinion) {
            (Some(buy), Some(sell)) => Some(-buy.abs().min(sell.abs())),
            (Some(buy), None) => Some(-buy.abs()),
            (None, Some(sell)) => Some(-sell.abs()),
            _ => None,
        };
        let staleness_factor = opi
            .and_then(|v| v.staleness_ms)
            .map(|v| (v as f64 / 1000.0).min(5.0) / 5.0)
            .unwrap_or(0.0);
        let pm_staleness_factor = pm
            .and_then(|v| v.staleness_ms)
            .map(|v| (v as f64 / 10_000.0).min(1.0))
            .unwrap_or(0.0);
        let lag_score = lag_confidence.unwrap_or(0.0).abs();
        let pressure_score = price_pressure_index.unwrap_or(0.0).abs();
        let follow_score_raw = arb_proximity
            .map(|arb| arb.abs() * (1.0 + staleness_factor) * (1.0 + pressure_score) * lag_score);

        let latency_penalty = {
            let pm_lat = pm.and_then(|v| v.obs_latency_p99_ms).unwrap_or(0) as f64;
            let opi_lat = opi.and_then(|v| v.obs_latency_p99_ms).unwrap_or(0) as f64;
            let worst = pm_lat.max(opi_lat);
            (worst / 5000.0).min(1.0)
        };
        let liquidity_penalty = opi
            .map(|v| if v.liquidity_pull_flag == 1 { 1.0 } else { 0.0 })
            .unwrap_or(0.0);
        let gas_penalty = if congestion_flag == 1 { 1.0 } else { 0.0 };
        let follow_score_adj = follow_score_raw.map(|raw| {
            raw * (1.0 - latency_penalty) * (1.0 - liquidity_penalty) * (1.0 - gas_penalty)
        });
        let book_alpha_adj = book_alpha_raw.map(|raw| raw * (1.0 - pm_staleness_factor) * (1.0 - latency_penalty));

        Bars1sPair {
            pair_id,
            token_side,
            bar_second,
            poly_bar_gap_flag: if poly_gap { 1 } else { 0 },
            opi_bar_gap_flag: if opi_gap { 1 } else { 0 },
            bar_gap_flag,
            poly_bar_stale_flag: if poly_stale { 1 } else { 0 },
            opi_bar_stale_flag: if opi_stale { 1 } else { 0 },
            bar_stale_flag,
            poly_staleness_ms: pm.and_then(|v| v.staleness_ms),
            opi_staleness_ms: opi.and_then(|v| v.staleness_ms),
            poly_best_bid,
            poly_best_ask,
            poly_mid: pm.and_then(|v| v.mid),
            poly_micro: pm.and_then(|v| v.micro),
            poly_best_bid_state,
            poly_best_ask_state,
            poly_mid_state: pm_mid,
            opi_best_bid,
            opi_best_ask,
            opi_mid: opi.and_then(|v| v.mid),
            opi_best_bid_state,
            opi_best_ask_state,
            opi_mid_state: opi_mid,
            opi_last_price,
            opi_taker_buy_notional_1s: opi.map(|v| v.buy_notional_1s),
            opi_taker_sell_notional_1s: opi.map(|v| v.sell_notional_1s),
            opi_micro: opi.and_then(|v| v.micro),
            opi_bid_l1_notional: opi.and_then(|v| v.bid_l1_notional),
            opi_bid_l2_notional: opi.and_then(|v| v.bid_l2_notional),
            opi_bid_l3_notional: opi.and_then(|v| v.bid_l3_notional),
            opi_ask_l1_notional: opi.and_then(|v| v.ask_l1_notional),
            opi_ask_l2_notional: opi.and_then(|v| v.ask_l2_notional),
            opi_ask_l3_notional: opi.and_then(|v| v.ask_l3_notional),
            poly_fee_bps: Some(fee_bps_polymarket),
            opi_fee_bps: Some(fee_bps_opinion),
            gas_cost,
            network_congestion_flag: congestion_flag,
            gross_spread_buy,
            gross_spread_sell,
            true_net_spread_buy_opinion,
            true_net_spread_sell_opinion,
            arb_mode_flag,
            directional_mode_flag: 1 - arb_mode_flag,
            poly_cvd_30s,
            poly_buy_sell_ratio_30s,
            price_pressure_index,
            poly_ofi_250ms: pm.map(|v| v.ofi_250ms),
            poly_buy_notional_1s: pm.map(|v| v.buy_notional_1s),
            poly_sell_notional_1s: pm.map(|v| v.sell_notional_1s),
            poly_max_trade_notional_1s: pm.map(|v| v.max_trade_notional_1s),
            poly_buy_notional_250ms: pm.map(|v| v.buy_notional_250ms),
            poly_sell_notional_250ms: pm.map(|v| v.sell_notional_250ms),
            poly_max_trade_notional_250ms: pm.map(|v| v.max_trade_notional_250ms),
            poly_bid_delta_notional_250ms: pm.map(|v| v.bid_delta_notional_250ms),
            poly_ask_delta_notional_250ms: pm.map(|v| v.ask_delta_notional_250ms),
            poly_mid_slope_250ms: pm.and_then(|v| v.mid_slope_250ms),
            poly_obs_latency_p99_ms: pm.and_then(|v| v.obs_latency_p99_ms),
            opi_obs_latency_p99_ms: opi.and_then(|v| v.obs_latency_p99_ms),
            rolling_corr_5m,
            rolling_corr_samples_5m: Some(corr_samples_5m as i64),
            estimated_lag_s,
            lag_confidence,
            beta_1m,
            follow_score_raw,
            follow_score_adj,
            poly_depth_imbalance_top10: pm_depth_imbalance,
            poly_book_pressure_250ms: pm_pressure_250ms,
            poly_spread_bps: pm_spread_bps,
            poly_micro_bias_bps: pm_micro_bias_bps,
            book_alpha_raw,
            book_alpha_adj,
        }
    }

}

async fn load_build_context(
    fee_schedule: &SharedFeeSchedule,
    chain_state: &SharedChain,
    bar_second: i64,
) -> BuildContext {
    let ts_ms = bar_second * 1000;
    let (fee_bps_polymarket, fee_bps_opinion) = {
        let schedule = fee_schedule.read().await;
        (
            schedule.fee_bps(Venue::Polymarket, ts_ms),
            schedule.fee_bps(Venue::Opinion, ts_ms),
        )
    };
    let (gas_cost, congestion_flag) = {
        let guard = chain_state.lock().await;
        let since_ms = ts_ms.saturating_sub(10_000);
        (guard.p90_recent(since_ms), guard.last_congestion_flag())
    };
    BuildContext {
        fee_bps_polymarket,
        fee_bps_opinion,
        gas_cost,
        congestion_flag,
    }
}

fn try_send_trade(
    sender: &mpsc::Sender<TradeMessage>,
    msg: TradeMessage,
    kind: &'static str,
) -> bool {
    match sender.try_send(msg) {
        Ok(()) => true,
        Err(TrySendError::Full(msg)) => {
            let dropped = TRADE_SEND_DROP_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped == 1 || dropped % 100 == 0 {
                let pair_id = match &msg {
                    TradeMessage::PairBar(bar) => bar.pair_id,
                    TradeMessage::Shock(shock) => shock.pair_id,
                    TradeMessage::OpinionOrderUpdate(update) => update.pair_id,
                    TradeMessage::OpinionTradeRecord(record) => record.pair_id,
                };
                warn!(
                    "trade_queue_full drop_count={} kind={} pair_id={}",
                    dropped, kind, pair_id
                );
            }
            false
        }
        Err(TrySendError::Closed(_)) => {
            let closed = TRADE_SEND_CLOSED_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
            if closed == 1 || closed % 100 == 0 {
                warn!("trade_queue_closed drop_count={} kind={}", closed, kind);
            }
            false
        }
    }
}

impl PairAggregator {
    async fn handle_shock(&mut self, shock: ShockInput) {
        if let Some(sender) = &self.trade_sender {
            let _ = try_send_trade(sender, TradeMessage::Shock(shock.clone()), "shock");
        }
        if !self.alpha_capture_enabled {
            return;
        }
        let (opi_token_key, pm_token_key, lag_pair) = {
            let state = self.state.lock().await;
            (
                state
                    .opi_token_map
                    .get(&(shock.pair_id, shock.token_side))
                    .cloned(),
                state
                    .pm_token_map
                    .get(&(shock.pair_id, shock.token_side))
                    .cloned(),
                state
                    .pair_stats
                    .get(&(shock.pair_id, shock.token_side))
                    .map(|stats| (stats.last_lag_s, stats.last_lag_confidence))
                    .unwrap_or((None, None)),
            )
        };
        let (estimated_lag_s_at_t0, lag_confidence_at_t0) = lag_pair;
        let baseline_opi = opi_token_key.as_ref().and_then(|key| {
            let Ok(history) = self.opi_bar_history.read() else {
                return None;
            };
            history
                .get(key)
                .and_then(|hist| {
                    find_bar_with_lookback(hist, shock.trigger_bar_second, self.analysis_gap_allow)
                })
        });
        let baseline_pm = pm_token_key.as_ref().and_then(|key| {
            let Ok(history) = self.pm_bar_history.read() else {
                return None;
            };
            history.get(key).and_then(|hist| find_bar(hist, shock.trigger_bar_second))
        });

        let baseline_mid = baseline_opi
            .as_ref()
            .and_then(|b| b.last_price.or(b.mid_state).or(b.mid));
        let baseline_spread = baseline_opi.as_ref().and_then(|b| b.spread);
        let baseline_staleness = baseline_opi.as_ref().and_then(|b| b.staleness_ms);
        let baseline_depth = baseline_opi
            .as_ref()
            .map(|b| b.top10_depth_bid.unwrap_or(0.0) + b.top10_depth_ask.unwrap_or(0.0));
        let baseline_liq_flag = baseline_opi.as_ref().map(|b| b.liquidity_pull_flag);
        let obs_latency_opi_p99 = baseline_opi.as_ref().and_then(|b| b.obs_latency_p99_ms);

        let (true_net_spread_at_t0, arb_mode_flag_at_t0) = self
            .compute_true_net_spread(
                baseline_pm.as_ref(),
                baseline_opi.as_ref(),
                shock.trigger_bar_second,
            )
            .await;

        let horizons = [10, 30, 60, 120, 300];
        {
            let mut state = self.state.lock().await;
            for horizon in horizons {
                let target = shock.trigger_bar_second + horizon;
                let entry = state.pending_by_target.entry(target).or_default();
                entry.push(PendingResponse {
                    shock_id: shock.shock_id,
                    pair_id: shock.pair_id,
                    token_side: shock.token_side,
                    trigger_bar_second: shock.trigger_bar_second,
                    horizon_s: horizon,
                    opi_token_key: opi_token_key.clone(),
                    baseline_mid,
                    baseline_spread,
                    baseline_staleness,
                    baseline_depth,
                    baseline_liquidity_pull_flag: baseline_liq_flag,
                    true_net_spread_at_t0,
                    arb_mode_flag_at_t0,
                    estimated_lag_s_at_t0,
                    lag_confidence_at_t0,
                    obs_latency_poly_p99_at_t0: shock.obs_latency_poly_p99_ms,
                    obs_latency_opi_p99_at_t0: obs_latency_opi_p99,
                });
                state.pending_count += 1;
            }
        }
    }
}

impl PairAggState {
    fn flush_pending(
        &mut self,
        pending_tx: &mpsc::Sender<PendingResponse>,
        pending_inflight: &Arc<AtomicUsize>,
        pending_try_send_fail: &Arc<AtomicUsize>,
        alpha_capture_enabled: bool,
    ) {
        let now = self.current_tick;
        let mut processed = 0usize;
        let start = Instant::now();
        let mut targets: Vec<i64> = Vec::new();
        let mut due_targets = 0usize;
        let mut due_items_total = 0usize;
        let mut skipped_alpha_disabled = 0usize;
        let mut enqueued = 0usize;
        let mut try_send_fail_local = 0usize;
        let mut break_reason: Option<&'static str> = None;
        for target in self.pending_by_target.keys() {
            if *target <= now {
                targets.push(*target);
                due_targets += 1;
                if let Some(items) = self.pending_by_target.get(target) {
                    due_items_total += items.len();
                }
            } else {
                break;
            }
        }
        for target in targets {
            let pending_items = match self.pending_by_target.remove(&target) {
                Some(items) => items,
                None => continue,
            };
            let mut remaining: Vec<PendingResponse> = Vec::new();
            let mut iter = pending_items.into_iter();
            while let Some(pending) = iter.next() {
                if processed >= PENDING_PROCESS_MAX
                    || start.elapsed().as_millis() > PENDING_PROCESS_BUDGET_MS
                {
                    remaining.push(pending);
                    remaining.extend(iter);
                    break_reason = Some(if processed >= PENDING_PROCESS_MAX {
                        "process_max"
                    } else {
                        "budget_ms"
                    });
                    break;
                }
                if !alpha_capture_enabled {
                    if self.pending_count > 0 {
                        self.pending_count -= 1;
                    }
                    processed += 1;
                    skipped_alpha_disabled += 1;
                    continue;
                }
                match pending_tx.try_send(pending) {
                    Ok(()) => {
                        if self.pending_count > 0 {
                            self.pending_count -= 1;
                        }
                        pending_inflight.fetch_add(1, Ordering::Relaxed);
                        enqueued += 1;
                    }
                    Err(err) => {
                        pending_try_send_fail.fetch_add(1, Ordering::Relaxed);
                        try_send_fail_local += 1;
                        remaining.push(err.into_inner());
                        remaining.extend(iter);
                        break_reason = Some("try_send_failed");
                        break;
                    }
                }
                processed += 1;
            }
            if !remaining.is_empty() {
                self.pending_by_target.insert(target, remaining);
                break;
            }
        }
        if due_targets > 0
            && (processed == 0 || try_send_fail_local > 0 || break_reason.is_some())
        {
            warn!(
                "flush_pending stalled: due_targets={} due_items={} processed={} enqueued={} skipped_alpha_disabled={} try_send_fail={} break_reason={:?} elapsed_ms={}",
                due_targets,
                due_items_total,
                processed,
                enqueued,
                skipped_alpha_disabled,
                try_send_fail_local,
                break_reason,
                start.elapsed().as_millis()
            );
        }
    }

}

impl PairAggregator {
    async fn compute_true_net_spread(
        &self,
        pm: Option<&PairInput>,
        opi: Option<&PairInput>,
        bar_second: i64,
    ) -> (Option<f64>, Option<i64>) {
        let ts_ms = bar_second * 1000;
        let (fee_bps_polymarket, fee_bps_opinion) = {
            let schedule = self.fee_schedule.read().await;
            (
                schedule.fee_bps(Venue::Polymarket, ts_ms),
                schedule.fee_bps(Venue::Opinion, ts_ms),
            )
        };
        let poly_fee = fee_bps_polymarket as f64 / 10_000.0;
        let opi_fee = fee_bps_opinion as f64 / 10_000.0;
        let (gas_cost, _) = {
            let guard = self.chain_state.lock().await;
            let since_ms = ts_ms.saturating_sub(10_000);
            (guard.p90_recent(since_ms), guard.last_congestion_flag())
        };
        let gas_cost = gas_cost.unwrap_or(0.0);

        let poly_best_bid = pm.and_then(|v| v.best_bid_state.or(v.best_bid));
        let poly_best_ask = pm.and_then(|v| v.best_ask_state.or(v.best_ask));
        let opi_best_bid = opi.and_then(|v| v.best_bid_state.or(v.best_bid));
        let opi_best_ask = opi.and_then(|v| v.best_ask_state.or(v.best_ask));
        let true_net_spread_buy = match (poly_best_bid, opi_best_ask) {
            (Some(bid), Some(ask)) => {
                Some(bid * (1.0 - poly_fee) - ask * (1.0 + opi_fee) - gas_cost)
            }
            _ => None,
        };
        let true_net_spread_sell = match (opi_best_bid, poly_best_ask) {
            (Some(bid), Some(ask)) => {
                Some(bid * (1.0 - opi_fee) - ask * (1.0 + poly_fee) - gas_cost)
            }
            _ => None,
        };
        let arb_mode_flag = if true_net_spread_buy.unwrap_or(0.0) > 0.0
            || true_net_spread_sell.unwrap_or(0.0) > 0.0
        {
            Some(1)
        } else {
            Some(0)
        };
        let spread = match (true_net_spread_buy, true_net_spread_sell) {
            (Some(buy), Some(sell)) => Some(buy.max(sell)),
            (Some(buy), None) => Some(buy),
            (None, Some(sell)) => Some(sell),
            _ => None,
        };
        (spread, arb_mode_flag)
    }

    fn build_response_from_history(
        opi_history: &HashMap<String, Vec<PairInput>>,
        pending: &PendingResponse,
        target_bar_second: i64,
        analysis_gap_allow: usize,
        analysis_staleness_ms: i64,
        analysis_move_eps: f64,
    ) -> Option<OpinionResponseRow> {
        let token_key = pending.opi_token_key.as_ref()?;
        let history = opi_history.get(token_key)?;
        let baseline_bar =
            find_bar_with_lookback(history, pending.trigger_bar_second, analysis_gap_allow);
        let target_bar_h =
            find_bar_with_lookback(history, target_bar_second, analysis_gap_allow);

        let mut sample_valid = 1;
        let price_signal = |bar: &PairInput| bar.last_price.or(bar.mid_state).or(bar.mid);
        let is_bar_valid = |bar: &PairInput| {
            price_signal(bar).is_some()
                && bar
                    .staleness_ms
                    .map(|value| value <= analysis_staleness_ms)
                    .unwrap_or(false)
        };

        let baseline_mid = baseline_bar.as_ref().and_then(|b| price_signal(b));
        let baseline_spread = baseline_bar.as_ref().and_then(|b| b.spread);
        let baseline_depth = baseline_bar
            .as_ref()
            .map(|b| b.top10_depth_bid.unwrap_or(0.0) + b.top10_depth_ask.unwrap_or(0.0));
        let baseline_staleness = baseline_bar.as_ref().and_then(|b| b.staleness_ms);
        let baseline_liq_flag = baseline_bar.as_ref().map(|b| b.liquidity_pull_flag);

        let target_mid = target_bar_h.as_ref().and_then(|b| price_signal(b));
        let target_spread = target_bar_h.as_ref().and_then(|b| b.spread);
        let target_depth = target_bar_h
            .as_ref()
            .map(|b| b.top10_depth_bid.unwrap_or(0.0) + b.top10_depth_ask.unwrap_or(0.0));

        let baseline_ok = baseline_bar
            .as_ref()
            .map(|b| is_bar_valid(b))
            .unwrap_or(false);
        let target_ok = target_bar_h
            .as_ref()
            .map(|b| is_bar_valid(b))
            .unwrap_or(false);
        if !baseline_ok || !target_ok {
            sample_valid = 0;
        }

        let move_flag = match (baseline_mid, target_mid) {
            (Some(start), Some(end)) if (end - start).abs() > analysis_move_eps => 1,
            _ => 0,
        };
        let range_start = first_index_after(history, pending.trigger_bar_second);
        let mut range_end = first_index_after(history, target_bar_second);
        if range_end < range_start {
            range_end = range_start;
        }

        let mut first_move_lag_s: Option<i64> = None;
        if let Some(baseline_mid) = baseline_mid {
            for bar in history[range_start..range_end].iter() {
                if !is_bar_valid(bar) {
                    continue;
                }
                if let Some(price) = price_signal(bar) {
                    if (price - baseline_mid).abs() > analysis_move_eps {
                        first_move_lag_s = Some(bar.bar_second - pending.trigger_bar_second);
                        break;
                    }
                }
            }
        }

        let mut volume_sum = 0.0;
        let mut cvd_sum = 0.0;
        for bar in history[range_start..range_end].iter() {
            volume_sum += bar.volume_notional_1s;
            cvd_sum += bar.cvd_delta_1s;
        }

        let opi_return = match (baseline_mid, target_mid) {
            (Some(start), Some(end)) if start > 0.0 && end > 0.0 => Some((end / start).ln()),
            _ => None,
        };
        let opi_price_delta = match (baseline_mid, target_mid) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        };
        let opi_spread_change = match (baseline_spread, target_spread) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        };
        let opi_depth_change = match (baseline_depth, target_depth) {
            (Some(start), Some(end)) => Some(end - start),
            _ => None,
        };

        Some(OpinionResponseRow {
            shock_id: pending.shock_id,
            horizon_s: pending.horizon_s,
            sample_valid_flag: sample_valid,
            move_flag,
            first_move_lag_s,
            opi_return,
            opi_price_delta,
            opi_volume_sum: Some(volume_sum),
            opi_cvd_sum: Some(cvd_sum),
            opi_spread_change,
            opi_depth_change,
            opi_staleness_at_t0_ms: baseline_staleness,
            opi_liquidity_pull_flag_at_t0: baseline_liq_flag,
            true_net_spread_at_t0: pending.true_net_spread_at_t0,
            arb_mode_flag_at_t0: pending.arb_mode_flag_at_t0,
            estimated_lag_s_at_t0: pending.estimated_lag_s_at_t0,
            lag_confidence_at_t0: pending.lag_confidence_at_t0,
            obs_latency_poly_p99_at_t0: pending.obs_latency_poly_p99_at_t0,
            obs_latency_opi_p99_at_t0: pending.obs_latency_opi_p99_at_t0,
        })
    }
}

async fn pending_worker_loop(
    mut rx: mpsc::Receiver<PendingResponse>,
    opi_history: Arc<RwLock<HashMap<String, Vec<PairInput>>>>,
    analysis_gap_allow: usize,
    analysis_staleness_ms: i64,
    analysis_move_eps: f64,
    alpha_capture_enabled: bool,
    db_sender: DbSender,
    pending_inflight: Arc<AtomicUsize>,
    pending_worker_processed: Arc<AtomicUsize>,
) {
    if !alpha_capture_enabled {
        warn!("pending_worker alpha_capture_enabled=false responses will be skipped");
    }
    while let Some(pending) = rx.recv().await {
        let row_opt = {
            let Ok(history) = opi_history.read() else {
                pending_inflight.fetch_sub(1, Ordering::Relaxed);
                pending_worker_processed.fetch_add(1, Ordering::Relaxed);
                continue;
            };
            PairAggregator::build_response_from_history(
                &history,
                &pending,
                pending.trigger_bar_second + pending.horizon_s,
                analysis_gap_allow,
                analysis_staleness_ms,
                analysis_move_eps,
            )
        };
        if let Some(row) = row_opt {
            if alpha_capture_enabled {
                try_send_db(
                    &db_sender,
                    DbMessage::OpinionResponse(row),
                    "pair_opinion_response",
                );
            }
        }
        pending_inflight.fetch_sub(1, Ordering::Relaxed);
        pending_worker_processed.fetch_add(1, Ordering::Relaxed);
    }
}

async fn flush_worker_loop(
    state: Arc<Mutex<PairAggState>>,
    flush_notify: Arc<Notify>,
    fee_schedule: SharedFeeSchedule,
    chain_state: SharedChain,
    db_sender: DbSender,
    trade_sender: Option<mpsc::Sender<TradeMessage>>,
    alpha_capture_enabled: bool,
    opi_bootstrap_done: Arc<AtomicBool>,
) {
    loop {
        flush_notify.notified().await;
        loop {
            let keys = {
                let mut guard = state.lock().await;
                guard.drain_flush_queue()
            };
            if keys.is_empty() {
                break;
            }
            for key in keys {
                let has_bucket = {
                    let guard = state.lock().await;
                    guard.buckets.contains_key(&key)
                };
                if !has_bucket {
                    continue;
                }
                let ctx = load_build_context(&fee_schedule, &chain_state, key.2).await;
                let bar_opt = {
                    let mut guard = state.lock().await;
                    let bucket = guard.buckets.get(&key).cloned();
                    if let Some(bucket) = bucket {
                        let opi_ready = opi_bootstrap_done.load(Ordering::Relaxed);
                        let bar = guard.build_bar_with_context(key, &bucket, &ctx, opi_ready);
                        guard.record_flush_timeout(key, &bucket, opi_ready);
                        guard.buckets.remove(&key);
                        Some(bar)
                    } else {
                        None
                    }
                };
                let Some(bar) = bar_opt else {
                    continue;
                };
                if alpha_capture_enabled {
                    try_send_db(
                        &db_sender,
                        DbMessage::Bars1sPair(bar.clone()),
                        "pair_bars1s_pair_flush",
                    );
                }
                if let Some(sender) = &trade_sender {
                    let _ = try_send_trade(sender, TradeMessage::PairBar(bar), "pair_bar_flush");
                }
            }
            let has_more = {
                let guard = state.lock().await;
                !guard.flush_queue.is_empty()
            };
            if !has_more {
                break;
            }
            tokio::task::yield_now().await;
        }
    }
}

fn log_return(current: Option<f64>, prev: Option<f64>) -> Option<f64> {
    match (current, prev) {
        (Some(cur), Some(prev)) if cur > 0.0 && prev > 0.0 => Some((cur / prev).ln()),
        _ => None,
    }
}

fn trim_returns(values: &mut VecDeque<ReturnPoint>, max_len: usize) {
    while values.len() > max_len {
        values.pop_front();
    }
}

fn push_window(values: &mut VecDeque<f64>, value: f64, max_len: usize) {
    if values.len() >= max_len {
        values.pop_front();
    }
    values.push_back(value);
}

fn sum_window(values: &VecDeque<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    Some(values.iter().copied().sum())
}

fn compute_corr_beta(values: &VecDeque<ReturnPoint>) -> (Option<f64>, Option<f64>, usize) {
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    for point in values.iter().rev().take(300) {
        if let (Some(x), Some(y)) = (point.pm_ret, point.opi_ret) {
            xs.push(x);
            ys.push(y);
        }
    }
    let corr = correlation(&xs, &ys);
    let beta = beta(&xs, &ys);
    (corr, beta, xs.len())
}

fn estimate_lag(values: &VecDeque<ReturnPoint>, max_shift: i64) -> (Option<i64>, Option<f64>) {
    let mut pm_map = HashMap::new();
    let mut opi_map = HashMap::new();
    for point in values.iter().rev().take(600) {
        if let Some(pm) = point.pm_ret {
            pm_map.insert(point.bar_second, pm);
        }
        if let Some(opi) = point.opi_ret {
            opi_map.insert(point.bar_second, opi);
        }
    }
    let mut best_shift = None;
    let mut best_corr: f64 = 0.0;
    for shift in -max_shift..=max_shift {
        let mut xs = Vec::new();
        let mut ys = Vec::new();
        for (t, pm) in pm_map.iter() {
            let target = t + shift;
            if let Some(opi) = opi_map.get(&target) {
                xs.push(*pm);
                ys.push(*opi);
            }
        }
        if xs.len() < 30 {
            continue;
        }
        if let Some(corr) = correlation(&xs, &ys) {
            if corr.abs() > best_corr.abs() {
                best_corr = corr;
                best_shift = Some(shift);
            }
        }
    }
    if best_shift.is_none() {
        return (None, None);
    }
    (best_shift, Some(best_corr.abs()))
}

fn correlation(xs: &[f64], ys: &[f64]) -> Option<f64> {
    if xs.len() < 2 || xs.len() != ys.len() {
        return None;
    }
    let mean_x = xs.iter().sum::<f64>() / xs.len() as f64;
    let mean_y = ys.iter().sum::<f64>() / ys.len() as f64;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    let mut var_y = 0.0;
    for (x, y) in xs.iter().zip(ys.iter()) {
        let dx = x - mean_x;
        let dy = y - mean_y;
        cov += dx * dy;
        var_x += dx * dx;
        var_y += dy * dy;
    }
    if var_x <= 0.0 || var_y <= 0.0 {
        return None;
    }
    Some(cov / (var_x.sqrt() * var_y.sqrt()))
}

fn beta(xs: &[f64], ys: &[f64]) -> Option<f64> {
    if xs.len() < 2 || xs.len() != ys.len() {
        return None;
    }
    let mean_x = xs.iter().sum::<f64>() / xs.len() as f64;
    let mean_y = ys.iter().sum::<f64>() / ys.len() as f64;
    let mut cov = 0.0;
    let mut var_x = 0.0;
    for (x, y) in xs.iter().zip(ys.iter()) {
        let dx = x - mean_x;
        cov += dx * (y - mean_y);
        var_x += dx * dx;
    }
    if var_x <= 0.0 {
        return None;
    }
    Some(cov / var_x)
}

fn find_bar(history: &Vec<PairInput>, bar_second: i64) -> Option<PairInput> {
    for bar in history.iter().rev() {
        if bar.bar_second == bar_second {
            return Some(bar.clone());
        }
        if bar.bar_second < bar_second.saturating_sub(1) && bar.bar_second + 5 < bar_second {
            break;
        }
    }
    None
}

fn find_bar_with_lookback(
    history: &Vec<PairInput>,
    bar_second: i64,
    gap_allow: usize,
) -> Option<PairInput> {
    let min_second = bar_second.saturating_sub(gap_allow as i64);
    for bar in history.iter().rev() {
        if bar.bar_second > bar_second {
            continue;
        }
        if bar.bar_second < min_second {
            break;
        }
        return Some(bar.clone());
    }
    None
}

fn first_index_after(history: &Vec<PairInput>, bar_second: i64) -> usize {
    let mut lo = 0usize;
    let mut hi = history.len();
    while lo < hi {
        let mid = (lo + hi) / 2;
        if history[mid].bar_second <= bar_second {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}
