mod opinion_private;

use crate::chain::SharedChain;
use crate::config::TradeConfig;
use crate::alpha_log::{build_alpha_entry_base, init_alpha_logger, AlphaLogger, AlphaLogEntry};
use crate::db_queue::{try_send_db, DbMessage, DbSender};
use crate::fee::SharedFeeSchedule;
use crate::models::{Bars1sPair, CancelTriggerSnapshotRow, TokenSide, TradeAuditRow, Venue};
use crate::pair_aggregator::ShockInput;
use crate::rest::opinion::SnapshotCommand;
use crate::time_utils::now_ts_ms;
use opinion_private::{
    round_price_for_side, OrderAmount, OrderSide, OpinionOrderTarget, OpinionPrivateClient,
    PlacedOrder, TradeFill,
};
use rand::Rng;
use serde_json::{json, Map, Value};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::Semaphore;
use tokio::time::Duration;

pub use opinion_private::{build_opinion_client, build_opinion_order_map};

const OPI_STALE_WINDOW_LEN: usize = 120;
const GAP_WINDOW_LEN: usize = 60;
const TRADE_HANDLE_WARN_MS: u128 = 200;
const TRADE_CANCEL_TICK_WARN_MS: u128 = 200;
const TRADE_POLL_FILLS_WARN_MS: u128 = 500;
const TRADE_CANCEL_ORDER_WARN_MS: u128 = 500;
const TRADE_FLUSH_PENDING_WARN_MS: u128 = 200;
const TRADE_CHECK_ENTRY_WARN_MS: u128 = 200;
const TRADE_CHECK_EXIT_WARN_MS: u128 = 200;
const TRADE_PLACE_ORDER_WARN_MS: u128 = 500;
const TRADE_EXIT_CHASE_WARN_MS: u128 = 500;
const FILL_POLL_BACKOFF_MIN_MS: i64 = 2_000;
const FILL_POLL_BACKOFF_MAX_MS: i64 = 60_000;
const CANCEL_BACKOFF_MIN_MS: i64 = 2_000;
const CANCEL_BACKOFF_MAX_MS: i64 = 60_000;
const CANCEL_BACKOFF_JITTER_MIN: f64 = 0.5;
const CANCEL_BACKOFF_JITTER_MAX: f64 = 1.5;
const CANCEL_DEDUP_WINDOW_MS: i64 = 1_000;
const CANCEL_RETRY_BASE_MS: i64 = 5_000;
const CANCEL_RETRY_MAX_MS: i64 = 120_000;
const CANCEL_RETRY_MAX_FAILURES: u32 = 8;
const CANCEL_CIRCUIT_FAILS: i64 = 5;
const CANCEL_CIRCUIT_OPEN_MS: i64 = 30_000;
const CANCEL_FAIL_RESET_MS: i64 = 60_000;
const CANCEL_RATE_LIMIT_RPS: f64 = 5.0;
const CANCEL_RATE_LIMIT_BURST: f64 = 10.0;
const CANCEL_RATE_EXIT_RESERVE: f64 = 1.0;
const CANCEL_VERIFY_CACHE_TTL_MS: i64 = 5_000;
const CANCEL_VERIFY_PAGE_LIMIT: i64 = 20;
const CANCEL_VERIFY_MAX_PAGES: i64 = 5;

#[derive(Debug)]
pub enum TradeMessage {
    PairBar(Bars1sPair),
    Shock(ShockInput),
    OpinionOrderUpdate(OpinionUserOrderUpdate),
    OpinionTradeRecord(OpinionUserTradeRecord),
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum PlaceKind {
    Entry,
    Exit,
}

#[derive(Debug, Clone)]
pub struct OpinionUserOrderUpdate {
    pub pair_id: i64,
    pub token_side: TokenSide,
    pub order_id: String,
    pub order_update_type: Option<String>,
    pub status: Option<i64>,
    pub side: Option<String>,
    pub outcome_side: Option<i32>,
    pub price: Option<f64>,
    pub shares: Option<f64>,
    pub amount: Option<f64>,
    pub filled_shares: Option<f64>,
    pub filled_amount: Option<f64>,
    pub ts_ms: Option<i64>,
    pub market_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct OpinionUserTradeRecord {
    pub pair_id: i64,
    pub token_side: TokenSide,
    pub order_id: String,
    pub side: Option<String>,
    pub outcome_side: Option<i32>,
    pub price: Option<f64>,
    pub shares: Option<f64>,
    pub amount: Option<f64>,
    pub ts_ms: Option<i64>,
    pub market_id: Option<String>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum FillKind {
    Entry,
    Exit,
}

#[derive(Debug)]
struct OrderRequest {
    id: u64,
    kind: OrderRequestKind,
    response_tx: mpsc::UnboundedSender<OrderResponse>,
}

#[derive(Debug)]
enum OrderRequestKind {
    PlaceChase {
        pair_key: (i64, TokenSide),
        kind: PlaceKind,
        market_id: i64,
        token_id: String,
        side: OrderSide,
        qty: f64,
        candidate_prices: Vec<f64>,
        client_prefix: String,
        client_tag: &'static str,
        post_only: bool,
    },
    CancelOrder {
        order_id: String,
    },
    FetchFills {
        pair_key: (i64, TokenSide),
        kind: FillKind,
        market_id: i64,
        token_id: String,
        since_ms: Option<i64>,
    },
}

#[derive(Debug)]
pub(crate) struct PlaceResult {
    order: Option<PlacedOrder>,
    client_id_used: Option<String>,
    price_used: Option<f64>,
    chase_count: i64,
    post_only_rejects: i64,
    last_err: Option<String>,
}

#[derive(Debug)]
pub(crate) struct CancelResult {
    ok: bool,
    rate_limited: bool,
    server_error: bool,
    error: Option<String>,
}

#[derive(Debug)]
pub(crate) struct FillPollResult {
    fills: Vec<TradeFill>,
    rate_limited: bool,
    error: Option<String>,
}

#[derive(Debug)]
pub(crate) enum OrderResponse {
    PlaceChase {
        id: u64,
        pair_key: (i64, TokenSide),
        kind: PlaceKind,
        result: PlaceResult,
    },
    CancelOrder {
        id: u64,
        order_id: String,
        result: CancelResult,
    },
    FetchFills {
        id: u64,
        pair_key: (i64, TokenSide),
        kind: FillKind,
        result: FillPollResult,
    },
}

#[derive(Clone)]
pub struct OrderExecutorHandle {
    tx: mpsc::UnboundedSender<OrderRequest>,
}

impl OrderExecutorHandle {
    fn send(&self, req: OrderRequest) -> Result<(), mpsc::error::SendError<OrderRequest>> {
        self.tx.send(req)
    }
}

pub fn spawn_order_executor(
    client: Option<Arc<OpinionPrivateClient>>,
    max_concurrency: usize,
) -> OrderExecutorHandle {
    let (tx, rx) = mpsc::unbounded_channel();
    let executor = OrderExecutor {
        rx,
        client,
        semaphore: Arc::new(Semaphore::new(max_concurrency.max(1))),
    };
    tokio::spawn(executor.run());
    OrderExecutorHandle { tx }
}

struct OrderExecutor {
    rx: mpsc::UnboundedReceiver<OrderRequest>,
    client: Option<Arc<OpinionPrivateClient>>,
    semaphore: Arc<Semaphore>,
}

struct CancelRateLimiter {
    tokens: f64,
    last_refill_ms: i64,
}

#[derive(Debug, Clone)]
struct CancelRetryState {
    failures: u32,
    next_allowed_ms: i64,
}

#[derive(Debug, Clone)]
struct OpenOrdersCache {
    fetched_ms: i64,
    order_ids: HashSet<String>,
    complete: bool,
}

impl CancelRateLimiter {
    fn new(now_ms: i64) -> Self {
        Self {
            tokens: CANCEL_RATE_LIMIT_BURST,
            last_refill_ms: now_ms,
        }
    }

    fn allow(&mut self, now_ms: i64, is_exit: bool) -> bool {
        self.refill(now_ms);
        if is_exit {
            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return true;
            }
            return false;
        }
        if self.tokens - 1.0 >= CANCEL_RATE_EXIT_RESERVE {
            self.tokens -= 1.0;
            return true;
        }
        false
    }

    fn refill(&mut self, now_ms: i64) {
        let elapsed_ms = now_ms.saturating_sub(self.last_refill_ms);
        if elapsed_ms <= 0 {
            return;
        }
        self.last_refill_ms = now_ms;
        let refill = (elapsed_ms as f64 / 1000.0) * CANCEL_RATE_LIMIT_RPS;
        self.tokens = (self.tokens + refill).min(CANCEL_RATE_LIMIT_BURST);
    }
}

impl OrderExecutor {
    async fn run(mut self) {
        while let Some(req) = self.rx.recv().await {
            let client = self.client.clone();
            let semaphore = self.semaphore.clone();
            tokio::spawn(async move {
                let _permit = match semaphore.acquire_owned().await {
                    Ok(permit) => permit,
                    Err(_) => return,
                };
                let response = match req.kind {
                    OrderRequestKind::PlaceChase {
                        pair_key,
                        kind,
                        market_id,
                        token_id,
                        side,
                        qty,
                        candidate_prices,
                        client_prefix,
                        client_tag,
                        post_only,
                    } => {
                        let result = place_with_chase(
                            client.as_ref(),
                            pair_key,
                            kind,
                            market_id,
                            &token_id,
                            side,
                            qty,
                            &candidate_prices,
                            &client_prefix,
                            client_tag,
                            post_only,
                        )
                        .await;
                        OrderResponse::PlaceChase {
                            id: req.id,
                            pair_key,
                            kind,
                            result,
                        }
                    }
                    OrderRequestKind::CancelOrder { order_id } => {
                        let result = cancel_order(client.as_ref(), &order_id).await;
                        OrderResponse::CancelOrder {
                            id: req.id,
                            order_id,
                            result,
                        }
                    }
                    OrderRequestKind::FetchFills {
                        pair_key,
                        kind,
                        market_id,
                        token_id,
                        since_ms,
                    } => {
                        let result =
                            fetch_fills(client.as_ref(), pair_key, market_id, &token_id, since_ms)
                                .await;
                        OrderResponse::FetchFills {
                            id: req.id,
                            pair_key,
                            kind,
                            result,
                        }
                    }
                };
                let _ = req.response_tx.send(response);
            });
        }
    }
}

fn build_client_order_id(prefix: &str, pair_id: i64, token_side: TokenSide, tag: &str) -> String {
    format!(
        "{}:{}:{}:{}",
        prefix,
        pair_id,
        token_side.as_str(),
        format!("{}-{}", tag, now_ts_ms())
    )
}

fn is_post_only_reject(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("post")
        || msg.contains("maker")
        || msg.contains("post-only")
        || msg.contains("post only")
        || msg.contains("would cross")
        || msg.contains("cross")
}

fn is_order_id_missing(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("order_id missing") || msg.contains("order id missing")
}

fn is_rate_limited_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    msg.contains("status=429")
        || msg.contains("429 too many requests")
        || msg.contains("rate limit")
        || msg.contains("rate_limit")
}

fn is_server_error(err: &anyhow::Error) -> bool {
    let msg = err.to_string().to_lowercase();
    if msg.contains("internal server error") {
        return true;
    }
    if let Some(code) = extract_status_code(&msg, "status=") {
        return (500..=599).contains(&code);
    }
    if let Some(code) = extract_status_code(&msg, "errno=") {
        return (500..=599).contains(&code);
    }
    false
}

fn extract_status_code(msg: &str, needle: &str) -> Option<i32> {
    let idx = msg.find(needle)?;
    let mut digits = String::new();
    for ch in msg[idx + needle.len()..].chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
            if digits.len() == 3 {
                break;
            }
        } else if !digits.is_empty() {
            break;
        }
    }
    if digits.len() == 3 {
        digits.parse::<i32>().ok()
    } else {
        None
    }
}

async fn place_with_chase(
    client: Option<&Arc<OpinionPrivateClient>>,
    pair_key: (i64, TokenSide),
    kind: PlaceKind,
    market_id: i64,
    token_id: &str,
    side: OrderSide,
    qty: f64,
    candidate_prices: &[f64],
    client_prefix: &str,
    client_tag: &'static str,
    post_only: bool,
) -> PlaceResult {
    let client = match client {
        Some(client) => client.as_ref(),
        None => {
            return PlaceResult {
                order: None,
                client_id_used: None,
                price_used: None,
                chase_count: 0,
                post_only_rejects: 0,
                last_err: Some("opinion client unavailable".to_string()),
            }
        }
    };
    let mut chase_count = 0i64;
    let mut post_only_rejects = 0i64;
    let mut last_err: Option<String> = None;
    let mut placed_order: Option<PlacedOrder> = None;
    let mut placed_client_id: Option<String> = None;
    let mut placed_price: Option<f64> = None;
    let chase_start = Instant::now();
    for price in candidate_prices.iter().copied() {
        chase_count += 1;
        let client_id = build_client_order_id(client_prefix, pair_key.0, pair_key.1, client_tag);
        let amount = match side {
            OrderSide::Buy => OrderAmount::Quote(price * qty),
            OrderSide::Sell => OrderAmount::Base(qty),
        };
        tracing::info!(
            "opinion_order_input market_id={} token_id={} side={} price={} amount={:?}",
            market_id,
            token_id,
            side.as_int(),
            price,
            amount
        );
        let place_start = Instant::now();
        let result = client
            .place_limit_order(
                market_id,
                token_id,
                side,
                price,
                amount,
                Some(&client_id),
                post_only,
            )
            .await;
        let place_ms = place_start.elapsed().as_millis();
        if place_ms > TRADE_PLACE_ORDER_WARN_MS {
            tracing::warn!(
                "trade_place_order_slow elapsed_ms={} market_id={} token_id={} side={} price={} qty={} post_only={} status={}",
                place_ms,
                market_id,
                token_id,
                side.as_int(),
                price,
                qty,
                post_only,
                if result.is_ok() { "ok" } else { "err" }
            );
        }
        match result {
            Ok(order) => {
                placed_order = Some(order);
                placed_client_id = Some(client_id);
                placed_price = Some(price);
                last_err = None;
                break;
            }
            Err(err) => {
                if is_order_id_missing(&err) {
                    placed_client_id = Some(client_id);
                    placed_price = Some(price);
                    last_err = Some("order_id_missing".to_string());
                    break;
                }
                if !is_post_only_reject(&err) {
                    last_err = Some(err.to_string());
                    break;
                }
                post_only_rejects += 1;
                last_err = Some(err.to_string());
                continue;
            }
        }
    }
    let chase_ms = chase_start.elapsed().as_millis();
    if matches!(kind, PlaceKind::Exit) && chase_ms > TRADE_EXIT_CHASE_WARN_MS {
        tracing::warn!(
            "trade_exit_chase_slow elapsed_ms={} pair_id={} token_side={} candidates_len={} chase_count={} post_only_rejects={} placed={} last_err={:?}",
            chase_ms,
            pair_key.0,
            pair_key.1.as_str(),
            candidate_prices.len(),
            chase_count,
            post_only_rejects,
            placed_order.is_some(),
            last_err
        );
    }
    PlaceResult {
        order: placed_order,
        client_id_used: placed_client_id,
        price_used: placed_price,
        chase_count,
        post_only_rejects,
        last_err,
    }
}

async fn cancel_order(client: Option<&Arc<OpinionPrivateClient>>, order_id: &str) -> CancelResult {
    let client = match client {
        Some(client) => client.as_ref(),
        None => {
            return CancelResult {
                ok: false,
                rate_limited: false,
                server_error: false,
                error: Some("opinion client unavailable".to_string()),
            }
        }
    };
    let cancel_start = Instant::now();
    let result = client.cancel_order(order_id).await;
    let cancel_ms = cancel_start.elapsed().as_millis();
    if cancel_ms > TRADE_CANCEL_ORDER_WARN_MS {
        tracing::warn!(
            "trade_cancel_slow order_id={} elapsed_ms={} status={}",
            order_id,
            cancel_ms,
            if result.is_ok() { "ok" } else { "err" }
        );
    }
    match result {
        Ok(_) => CancelResult {
            ok: true,
            rate_limited: false,
            server_error: false,
            error: None,
        },
        Err(err) => {
            let rate_limited = is_rate_limited_error(&err);
            let server_error = is_server_error(&err);
            tracing::warn!("trade cancel failed order_id={} err={}", order_id, err);
            CancelResult {
                ok: false,
                rate_limited,
                server_error,
                error: Some(err.to_string()),
            }
        }
    }
}

async fn fetch_fills(
    client: Option<&Arc<OpinionPrivateClient>>,
    pair_key: (i64, TokenSide),
    market_id: i64,
    token_id: &str,
    since_ms: Option<i64>,
) -> FillPollResult {
    let client = match client {
        Some(client) => client.as_ref(),
        None => {
            return FillPollResult {
                fills: Vec::new(),
                rate_limited: false,
                error: Some("opinion client unavailable".to_string()),
            }
        }
    };
    let poll_start = Instant::now();
    let result = client.fetch_trades(market_id, token_id, since_ms).await;
    let poll_ms = poll_start.elapsed().as_millis();
    if poll_ms > TRADE_POLL_FILLS_WARN_MS {
        tracing::warn!(
            "trade_fill_poll_slow elapsed_ms={} market_id={} token_id={} since_ms={:?} pair_id={} token_side={}",
            poll_ms,
            market_id,
            token_id,
            since_ms,
            pair_key.0,
            pair_key.1.as_str()
        );
    }
    match result {
        Ok(fills) => FillPollResult {
            fills,
            rate_limited: false,
            error: None,
        },
        Err(err) => {
            let rate_limited = is_rate_limited_error(&err);
            tracing::warn!(
                "trade fill poll failed err={} rate_limited={}",
                err,
                rate_limited
            );
            FillPollResult {
                fills: Vec::new(),
                rate_limited,
                error: Some(err.to_string()),
            }
        }
    }
}

#[derive(Debug, Clone)]
struct PendingEntry {
    key: (i64, TokenSide),
    bar: Bars1sPair,
    pm_move: f64,
    pm_mom: f64,
    direction: i64,
    alpha_type: AlphaType,
    shock: Option<ShockSignal>,
    ofi_value: f64,
    opi_mid: Option<f64>,
    base_pm: f64,
    pm_mid: f64,
    edge_raw: f64,
    edge_norm: f64,
    score: f64,
    conf_score: f64,
    cost_mult: f64,
    horizon_s: Option<i64>,
    alpha_source: String,
    alpha_source_cfg: Option<String>,
    alpha_value: Option<f64>,
    stale_scale: f64,
    entry_ttl_override_ms: Option<i64>,
    c_pred_dir: Option<i64>,
    c_ret: Option<f64>,
    c_modifier: Option<f64>,
}

#[derive(Debug, Clone)]
struct PendingEntryMeta {
    theo_price: f64,
    price_limit: f64,
    price_ref: f64,
    tick_px: f64,
}

#[derive(Debug, Clone)]
struct PendingExitPlaceContext {
    pm_move: f64,
    pm_mom: f64,
    ofi_value: f64,
    target: f64,
    exit_price: f64,
    gas_est: f64,
    retrace_ratio: Option<f64>,
    inv_ratio: Option<f64>,
    inv_factor: Option<f64>,
    decay_factor: Option<f64>,
    obs_latency_poly_p99_ms: Option<i64>,
    obs_latency_opi_p99_ms: Option<i64>,
    bar_gap_flag: Option<i64>,
    poly_bar_gap_flag: Option<i64>,
    opi_bar_gap_flag: Option<i64>,
    poly_staleness_ms: Option<i64>,
    opi_staleness_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct BudgetCaps {
    total: f64,
    free: f64,
    entry_pool_cap: f64,
    pool_available: f64,
    global_cap: f64,
    available_global: f64,
    max_per_pair: f64,
    available_pair: f64,
    strategy_cap: f64,
    available_strategy: f64,
    per_trade_cap: f64,
    notional_base: f64,
    notional_final: f64,
}

#[derive(Debug, Clone)]
struct BudgetDecision {
    notional: f64,
    caps: BudgetCaps,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TradeState {
    Idle,
    EntryReady,
    EntryPlacing,
    EntryWorking,
    PositionOpen,
    ExitWorking,
    Cooldown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum AlphaType {
    ShockA,
    GapB,
    LagC,
}

impl AlphaType {
    fn as_str(self) -> &'static str {
        match self {
            AlphaType::ShockA => "A_SHOCK",
            AlphaType::GapB => "B_GAP",
            AlphaType::LagC => "C_LAG_MOD",
        }
    }
}

#[derive(Debug, Clone)]
struct ShockSignal {
    shock_id: i64,
    shock_type: String,
    direction: i64,
    magnitude: f64,
    noise_flag: i64,
    trigger_bar_second: i64,
}

#[derive(Debug, Clone)]
struct PairTradeState {
    state: TradeState,
    base_pm: Option<f64>,
    last_pm_move: Option<f64>,
    last_pm_mom: Option<f64>,
    last_ofi_250ms: Option<f64>,
    last_base_pm: Option<f64>,
    pm_peak: Option<f64>,
    pm_move_entry: Option<f64>,
    pm_mom_entry: Option<f64>,
    entry_price: Option<f64>,
    entry_qty: Option<f64>,
    entry_notional: Option<f64>,
    entry_ts_ms: Option<i64>,
    entry_bar_second: Option<i64>,
    exit_price_target: Option<f64>,
    exit_price: Option<f64>,
    entry_order_id: Option<String>,
    entry_client_id: Option<String>,
    exit_order_id: Option<String>,
    exit_client_id: Option<String>,
    entry_cancel_pending: bool,
    entry_cancel_reason: Option<String>,
    entry_cancel_req_id: Option<u64>,
    exit_cancel_pending: bool,
    exit_cancel_reason: Option<String>,
    exit_cancel_req_id: Option<u64>,
    last_fill_poll_ms: Option<i64>,
    last_exit_place_ms: Option<i64>,
    direction: i64,
    cooldown_until: Option<i64>,
    mae: Option<f64>,
    mfe: Option<f64>,
    book_decay: Option<f64>,
    book_support: Option<f64>,
    book_scale: Option<f64>,
    edge_raw: Option<f64>,
    edge_norm: Option<f64>,
    ofi_250ms: Option<f64>,
    min_profit_usd: Option<f64>,
    last_opi_mid: Option<f64>,
    last_opi_spread_bps: Option<f64>,
    last_opi_bid_l1: Option<f64>,
    last_opi_ask_l1: Option<f64>,
    last_opi_bid_l3: Option<f64>,
    last_opi_ask_l3: Option<f64>,
    last_opi_bar_second: Option<i64>,
    alpha_type: Option<AlphaType>,
    alpha_source: Option<String>,
    shock: Option<ShockSignal>,
    horizon_s: Option<i64>,
    chase_count: i64,
    entry_ttl_override_ms: Option<i64>,
    pm_micro_history: std::collections::VecDeque<(i64, f64)>,
    opi_stale_hist: std::collections::VecDeque<i64>,
    opi_stale_p50_ms: Option<i64>,
    opi_stale_p90_ms: Option<i64>,
    gap_hist: std::collections::VecDeque<i64>,
    gap_ratio_1m: Option<f64>,
    last_entry_attempt_ms: Option<i64>,
    last_bar: Option<Bars1sPair>,
    entry_place_req_id: Option<u64>,
    exit_place_req_id: Option<u64>,
    fill_poll_req_id: Option<u64>,
    pending_entry: Option<PendingEntry>,
    pending_entry_meta: Option<PendingEntryMeta>,
    pending_exit_ctx: Option<PendingExitPlaceContext>,
}

type ExposureKey = (i64, TokenSide, i64);

impl Default for PairTradeState {
    fn default() -> Self {
        PairTradeState {
            state: TradeState::Idle,
            base_pm: None,
            last_pm_move: None,
            last_pm_mom: None,
            last_ofi_250ms: None,
            last_base_pm: None,
            pm_peak: None,
            pm_move_entry: None,
            pm_mom_entry: None,
            entry_price: None,
            entry_qty: None,
            entry_notional: None,
            entry_ts_ms: None,
            entry_bar_second: None,
            exit_price_target: None,
            exit_price: None,
            entry_order_id: None,
            entry_client_id: None,
            exit_order_id: None,
            exit_client_id: None,
            entry_cancel_pending: false,
            entry_cancel_reason: None,
            entry_cancel_req_id: None,
            exit_cancel_pending: false,
            exit_cancel_reason: None,
            exit_cancel_req_id: None,
            last_fill_poll_ms: None,
            last_exit_place_ms: None,
            direction: 0,
            cooldown_until: None,
            mae: None,
            mfe: None,
            book_decay: None,
            book_support: None,
            book_scale: None,
            edge_raw: None,
            edge_norm: None,
            ofi_250ms: None,
            min_profit_usd: None,
            last_opi_mid: None,
            last_opi_spread_bps: None,
            last_opi_bid_l1: None,
            last_opi_ask_l1: None,
            last_opi_bid_l3: None,
            last_opi_ask_l3: None,
            last_opi_bar_second: None,
            alpha_type: None,
            alpha_source: None,
            shock: None,
            horizon_s: None,
            chase_count: 0,
            entry_ttl_override_ms: None,
            pm_micro_history: VecDeque::new(),
            opi_stale_hist: VecDeque::new(),
            opi_stale_p50_ms: None,
            opi_stale_p90_ms: None,
            gap_hist: VecDeque::new(),
            gap_ratio_1m: None,
            last_entry_attempt_ms: None,
            last_bar: None,
            entry_place_req_id: None,
            exit_place_req_id: None,
            fill_poll_req_id: None,
            pending_entry: None,
            pending_entry_meta: None,
            pending_exit_ctx: None,
        }
    }
}

impl PairTradeState {
    fn update_peak(&mut self, pm_move: f64, direction: i64) {
        if direction == 0 {
            return;
        }
        self.pm_peak = Some(match self.pm_peak {
            Some(prev) => {
                if direction > 0 {
                    prev.max(pm_move)
                } else {
                    prev.min(pm_move)
                }
            }
            None => pm_move,
        });
    }

    fn reset_for_idle(&mut self) {
        self.state = TradeState::Idle;
        self.entry_price = None;
        self.entry_qty = None;
        self.entry_notional = None;
        self.entry_ts_ms = None;
        self.entry_bar_second = None;
        self.exit_price_target = None;
        self.exit_price = None;
        self.entry_order_id = None;
        self.entry_client_id = None;
        self.exit_order_id = None;
        self.exit_client_id = None;
        self.entry_cancel_pending = false;
        self.entry_cancel_reason = None;
        self.entry_cancel_req_id = None;
        self.exit_cancel_pending = false;
        self.exit_cancel_reason = None;
        self.exit_cancel_req_id = None;
        self.last_fill_poll_ms = None;
        self.last_exit_place_ms = None;
        self.direction = 0;
        self.pm_peak = None;
        self.pm_move_entry = None;
        self.pm_mom_entry = None;
        self.mae = None;
        self.mfe = None;
        self.book_decay = None;
        self.book_support = None;
        self.book_scale = None;
        self.edge_raw = None;
        self.edge_norm = None;
        self.ofi_250ms = None;
        self.min_profit_usd = None;
        self.last_opi_mid = None;
        self.last_opi_spread_bps = None;
        self.last_opi_bid_l1 = None;
        self.last_opi_ask_l1 = None;
        self.last_opi_bid_l3 = None;
        self.last_opi_ask_l3 = None;
        self.last_opi_bar_second = None;
        self.alpha_type = None;
        self.alpha_source = None;
        self.shock = None;
        self.horizon_s = None;
        self.chase_count = 0;
        self.entry_ttl_override_ms = None;
        self.entry_place_req_id = None;
        self.exit_place_req_id = None;
        self.fill_poll_req_id = None;
        self.pending_entry = None;
        self.pending_entry_meta = None;
        self.pending_exit_ctx = None;
    }

    fn update_opi_snapshot(&mut self, bar: &Bars1sPair) {
        let opi_mid = opi_mid_from_bar(bar);
        let spread_bps = opi_spread_bps_from_bar(bar, opi_mid);
        let bid_l1 = bar.opi_bid_l1_notional;
        let ask_l1 = bar.opi_ask_l1_notional;
        let bid_l3 = bid_l1.map(|v| v + bar.opi_bid_l2_notional.unwrap_or(0.0) + bar.opi_bid_l3_notional.unwrap_or(0.0));
        let ask_l3 = ask_l1.map(|v| v + bar.opi_ask_l2_notional.unwrap_or(0.0) + bar.opi_ask_l3_notional.unwrap_or(0.0));

        let mut updated = false;
        if let Some(value) = opi_mid {
            self.last_opi_mid = Some(value);
            updated = true;
        }
        if let Some(value) = spread_bps {
            self.last_opi_spread_bps = Some(value);
            updated = true;
        }
        if let Some(value) = bid_l1 {
            self.last_opi_bid_l1 = Some(value);
            updated = true;
        }
        if let Some(value) = ask_l1 {
            self.last_opi_ask_l1 = Some(value);
            updated = true;
        }
        if let Some(value) = bid_l3 {
            self.last_opi_bid_l3 = Some(value);
            updated = true;
        }
        if let Some(value) = ask_l3 {
            self.last_opi_ask_l3 = Some(value);
            updated = true;
        }
        if updated {
            self.last_opi_bar_second = Some(bar.bar_second);
        }
    }

    fn update_opi_staleness(&mut self, bar: &Bars1sPair, max_len: usize) {
        if let Some(stale) = bar.opi_staleness_ms {
            let value = stale.max(0);
            self.opi_stale_hist.push_back(value);
            while self.opi_stale_hist.len() > max_len {
                self.opi_stale_hist.pop_front();
            }
            if self.opi_stale_hist.len() >= 5 {
                let mut samples: Vec<i64> = self.opi_stale_hist.iter().copied().collect();
                samples.sort_unstable();
                let mid = samples.len() / 2;
                let p90_idx = (samples.len().saturating_sub(1) * 9) / 10;
                self.opi_stale_p50_ms = Some(samples[mid]);
                self.opi_stale_p90_ms = Some(samples[p90_idx]);
            }
        }
    }

    fn update_gap_ratio(&mut self, bar: &Bars1sPair, max_len: usize) {
        let flag = if bar.bar_gap_flag != 0 { 1 } else { 0 };
        self.gap_hist.push_back(flag);
        while self.gap_hist.len() > max_len {
            self.gap_hist.pop_front();
        }
        if !self.gap_hist.is_empty() {
            let gap_sum: i64 = self.gap_hist.iter().sum();
            self.gap_ratio_1m = Some(gap_sum as f64 / self.gap_hist.len() as f64);
        }
    }

    fn enter_cooldown(&mut self, bar_second: i64, cooldown_ms: i64) {
        self.state = TradeState::Cooldown;
        let cooldown_sec = (cooldown_ms.max(0) + 999) / 1000;
        self.cooldown_until = Some(bar_second + cooldown_sec);
        self.entry_order_id = None;
        self.entry_client_id = None;
        self.exit_order_id = None;
        self.exit_client_id = None;
        self.entry_cancel_pending = false;
        self.entry_cancel_reason = None;
        self.entry_cancel_req_id = None;
        self.exit_cancel_pending = false;
        self.exit_cancel_reason = None;
        self.exit_cancel_req_id = None;
        self.last_exit_place_ms = None;
        self.entry_place_req_id = None;
        self.exit_place_req_id = None;
        self.fill_poll_req_id = None;
        self.pending_entry = None;
        self.pending_entry_meta = None;
        self.pending_exit_ctx = None;
        self.pm_mom_entry = None;
    }

    fn check_cooldown(&mut self, bar_second: i64) {
        if self.state != TradeState::Cooldown {
            return;
        }
        if let Some(until) = self.cooldown_until {
            if bar_second >= until {
                self.reset_for_idle();
                self.cooldown_until = None;
            }
        }
    }

    fn update_pm_micro_history(&mut self, bar_second: i64, price: f64, max_len: usize) {
        if price <= 0.0 {
            return;
        }
        if let Some((last_second, last_price)) = self.pm_micro_history.back().copied() {
            if last_second == bar_second {
                if last_price != price {
                    if let Some(back) = self.pm_micro_history.back_mut() {
                        *back = (bar_second, price);
                    }
                }
                return;
            }
        }
        self.pm_micro_history.push_back((bar_second, price));
        while self.pm_micro_history.len() > max_len {
            self.pm_micro_history.pop_front();
        }
    }
}

#[derive(Debug, Clone)]
struct OpiDerived {
    mid: Option<f64>,
    spread_bps: Option<f64>,
    mid_ret_bps_1s: Option<f64>,
    mid_ret_abs_bps_1s: Option<f64>,
    bid_l1: Option<f64>,
    ask_l1: Option<f64>,
    bid_l3: Option<f64>,
    ask_l3: Option<f64>,
    bid_delta: Option<f64>,
    ask_delta: Option<f64>,
    imbalance_l1: Option<f64>,
    imbalance_l3: Option<f64>,
    taker_buy_notional_1s: Option<f64>,
    taker_sell_notional_1s: Option<f64>,
    trade_imbalance_1s: Option<f64>,
    order_edge_bps: Option<f64>,
}

fn opi_best_bid_from_bar(bar: &Bars1sPair) -> Option<f64> {
    bar.opi_best_bid_state.or(bar.opi_best_bid)
}

fn opi_best_ask_from_bar(bar: &Bars1sPair) -> Option<f64> {
    bar.opi_best_ask_state.or(bar.opi_best_ask)
}

fn pm_mid_from_bar(bar: &Bars1sPair) -> Option<f64> {
    bar.poly_mid_state.or(bar.poly_mid)
}

fn opi_mid_state_from_bar(bar: &Bars1sPair) -> Option<f64> {
    bar.opi_mid_state.or_else(|| match (bar.opi_best_bid_state, bar.opi_best_ask_state) {
        (Some(bid), Some(ask)) if bid > 0.0 && ask > 0.0 => Some((bid + ask) / 2.0),
        _ => None,
    })
}

fn opi_mid_from_bar(bar: &Bars1sPair) -> Option<f64> {
    bar.opi_mid_state
        .or(bar.opi_mid)
        .or_else(|| match (opi_best_bid_from_bar(bar), opi_best_ask_from_bar(bar)) {
            (Some(bid), Some(ask)) if bid > 0.0 && ask > 0.0 => Some((bid + ask) / 2.0),
            _ => None,
        })
        .or(bar.opi_last_price)
}

fn opi_spread_bps_from_bar(bar: &Bars1sPair, opi_mid: Option<f64>) -> Option<f64> {
    let mid = opi_mid?;
    if mid <= 0.0 {
        return None;
    }
    let (bid, ask) = (opi_best_bid_from_bar(bar)?, opi_best_ask_from_bar(bar)?);
    if bid <= 0.0 || ask <= 0.0 {
        return None;
    }
    Some((ask - bid) / mid * 10_000.0)
}

#[derive(Debug, Clone, Copy)]
struct BookShape {
    decay: f64,
    support: f64,
    scale: f64,
}

#[derive(Debug, Default)]
pub(crate) struct TradeRiskState {
    exposure_used: f64,
    strategy_exposure: HashMap<AlphaType, f64>,
    open_positions: usize,
    entry_pool_second: Option<i64>,
    entry_pool_remaining: f64,
}

pub fn new_shared_trade_risk() -> Arc<Mutex<TradeRiskState>> {
    Arc::new(Mutex::new(TradeRiskState::default()))
}

pub struct TradeEngine {
    rx: mpsc::Receiver<TradeMessage>,
    cfg: TradeConfig,
    fee_schedule: SharedFeeSchedule,
    chain_state: SharedChain,
    db_sender: DbSender,
    opinion_client: Option<Arc<OpinionPrivateClient>>,
    order_targets: Arc<HashMap<(i64, TokenSide), OpinionOrderTarget>>,
    order_poll_interval_ms: i64,
    opi_snapshot_trigger: Option<mpsc::Sender<SnapshotCommand>>,
    order_exec: OrderExecutorHandle,
    order_resp_tx: mpsc::UnboundedSender<OrderResponse>,
    order_resp_rx: mpsc::UnboundedReceiver<OrderResponse>,
    next_order_req_id: u64,
    risk: Arc<Mutex<TradeRiskState>>,
    opi_snapshot_last_ms: HashMap<String, i64>,
    last_ws_user_msg_ms: Option<i64>,
    alpha_logger: Option<AlphaLogger>,
    fill_poll_backoff_until_ms: Option<i64>,
    fill_poll_backoff_ms: i64,
    cancel_backoff_until_ms: Option<i64>,
    cancel_backoff_ms: i64,
    cancel_consecutive_failures: i64,
    cancel_last_failure_ms: Option<i64>,
    cancel_dedup_recent: HashMap<String, i64>,
    cancel_dedup_queue: VecDeque<(i64, String)>,
    cancel_rate_limiter: CancelRateLimiter,
    cancel_retry: HashMap<String, CancelRetryState>,
    open_orders_cache: HashMap<i64, OpenOrdersCache>,
    states: HashMap<(i64, TokenSide), PairTradeState>,
    pair_exposure: HashMap<ExposureKey, f64>,
    warned_no_balance: bool,
    pending_second: Option<i64>,
    pending_entries: Vec<PendingEntry>,
    pending_keys: HashSet<(i64, TokenSide)>,
    shock_signals: HashMap<(i64, TokenSide), Vec<ShockSignal>>,
    blocked_log_last: HashMap<(i64, TokenSide, AlphaType, &'static str), i64>,
}

impl TradeEngine {
    pub fn new(
        rx: mpsc::Receiver<TradeMessage>,
        cfg: TradeConfig,
        fee_schedule: SharedFeeSchedule,
        chain_state: SharedChain,
        db_sender: DbSender,
        opinion_client: Option<Arc<OpinionPrivateClient>>,
        order_targets: Arc<HashMap<(i64, TokenSide), OpinionOrderTarget>>,
        opi_snapshot_trigger: Option<mpsc::Sender<SnapshotCommand>>,
        order_exec: OrderExecutorHandle,
        order_resp_tx: mpsc::UnboundedSender<OrderResponse>,
        order_resp_rx: mpsc::UnboundedReceiver<OrderResponse>,
        risk: Arc<Mutex<TradeRiskState>>,
    ) -> Self {
        let order_poll_interval_ms = cfg.order_poll_interval_ms.max(250) as i64;
        let alpha_logger = init_alpha_logger(&cfg);
        TradeEngine {
            rx,
            cfg,
            fee_schedule,
            chain_state,
            db_sender,
            opinion_client,
            order_targets,
            order_poll_interval_ms,
            opi_snapshot_trigger,
            order_exec,
            order_resp_tx,
            order_resp_rx,
            next_order_req_id: 0,
            risk,
            opi_snapshot_last_ms: HashMap::new(),
            last_ws_user_msg_ms: None,
            alpha_logger,
            fill_poll_backoff_until_ms: None,
            fill_poll_backoff_ms: 0,
            cancel_backoff_until_ms: None,
            cancel_backoff_ms: 0,
            cancel_consecutive_failures: 0,
            cancel_last_failure_ms: None,
            cancel_dedup_recent: HashMap::new(),
            cancel_dedup_queue: VecDeque::new(),
            cancel_rate_limiter: CancelRateLimiter::new(now_ts_ms()),
            cancel_retry: HashMap::new(),
            open_orders_cache: HashMap::new(),
            states: HashMap::new(),
            pair_exposure: HashMap::new(),
            warned_no_balance: false,
            pending_second: None,
            pending_entries: Vec::new(),
            pending_keys: HashSet::new(),
            shock_signals: HashMap::new(),
            blocked_log_last: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        tracing::info!(
            "trade_engine started enabled={} dry_run={} alpha_a={} alpha_b={} alpha_c={} live_client={}",
            self.cfg.enabled,
            self.cfg.dry_run,
            self.cfg.alpha_a_enabled,
            self.cfg.alpha_b_enabled,
            self.cfg.alpha_c_enabled,
            self.opinion_client.is_some()
        );
        let mut cancel_interval = if self.cfg.cancel_check_interval_ms > 0 {
            Some(tokio::time::interval(Duration::from_millis(
                self.cfg.cancel_check_interval_ms as u64,
            )))
        } else {
            None
        };

        loop {
            tokio::select! {
                Some(msg) = self.rx.recv() => {
                    if !self.cfg.enabled {
                        continue;
                    }
                    match msg {
                        TradeMessage::PairBar(bar) => {
                            self.handle_bar(bar).await;
                        }
                        TradeMessage::Shock(shock) => {
                            self.handle_shock(shock);
                        }
                        TradeMessage::OpinionOrderUpdate(update) => {
                            self.handle_opinion_order_update(update).await;
                        }
                        TradeMessage::OpinionTradeRecord(record) => {
                            self.handle_opinion_trade_record(record).await;
                        }
                    }
                }
                Some(resp) = self.order_resp_rx.recv() => {
                    if self.cfg.enabled {
                        self.handle_order_response(resp).await;
                    }
                }
                _ = async {
                    if let Some(interval) = cancel_interval.as_mut() {
                        interval.tick().await;
                    }
                }, if cancel_interval.is_some() => {
                    if self.cfg.enabled {
                        self.cancel_check_tick().await;
                    }
                }
                else => break,
            }
        }
    }

    async fn emit_audit(&self, row: TradeAuditRow) {
        let mut alpha_type: Option<String> = None;
        let mut alpha_source: Option<String> = None;
        if let Some(raw) = row.detail_json.as_deref() {
            if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(raw) {
                alpha_type = map
                    .get("alpha_type")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string());
                alpha_source = map
                    .get("alpha_source")
                    .and_then(|v| v.as_str())
                    .map(|v| v.to_string());
            }
        }
        tracing::info!(
            target: "monitor::trade",
            phase = %row.phase,
            pair_id = row.pair_id,
            token_side = %row.token_side.as_str(),
            direction = row.direction,
            reason = ?row.reason,
            alpha_type = ?alpha_type,
            alpha_source = ?alpha_source,
            entry_price = ?row.entry_price,
            entry_qty = ?row.entry_qty,
            exit_price_target = ?row.exit_price_target,
            exit_price = ?row.exit_price,
            pm_move = ?row.pm_move,
            pm_mom = ?row.pm_mom,
            ofi_250ms = ?row.ofi_250ms,
            edge_raw = ?row.edge_raw,
            edge_norm = ?row.edge_norm,
            gas_est_usd = ?row.gas_est_usd,
            min_profit_usd = ?row.min_profit_usd,
            retrace_ratio = ?row.retrace_ratio,
            inv_ratio = ?row.inv_ratio,
            inv_factor = ?row.inv_factor,
            decay_factor = ?row.decay_factor,
            pnl_usd_net = ?row.pnl_usd_net,
            holding_ms = ?row.holding_ms,
            detail_json = ?row.detail_json
        );
        try_send_db(&self.db_sender, DbMessage::TradeAudit(row), "trade_audit");
    }

    fn audit_health(
        bar: Option<&Bars1sPair>,
    ) -> (
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
        Option<i64>,
    ) {
        match bar {
            Some(b) => (
                b.poly_obs_latency_p99_ms,
                b.opi_obs_latency_p99_ms,
                Some(b.bar_gap_flag),
                Some(b.poly_bar_gap_flag),
                Some(b.opi_bar_gap_flag),
                b.poly_staleness_ms,
                b.opi_staleness_ms,
            ),
            None => (None, None, None, None, None, None, None),
        }
    }

    fn queue_entry(&mut self, entry: PendingEntry) {
        let bar_second = entry.bar.bar_second;
        if let Some(pending_second) = self.pending_second {
            if pending_second != bar_second {
                tracing::warn!(
                    "trade pending second mismatch pending={} new={}",
                    pending_second,
                    bar_second
                );
                self.pending_entries.clear();
                self.pending_keys.clear();
                self.pending_second = Some(bar_second);
            }
        } else {
            self.pending_second = Some(bar_second);
        }
        if self.pending_keys.insert(entry.key) {
            self.pending_entries.push(entry);
        }
    }

    async fn flush_pending_entries(&mut self, current_bar_second: i64) {
        let pending_second = match self.pending_second {
            Some(second) if second < current_bar_second => second,
            _ => return,
        };
        let flush_start = Instant::now();
        let pending_len = self.pending_entries.len();
        let pending_keys_len = self.pending_keys.len();
        let mut entries = std::mem::take(&mut self.pending_entries);
        self.pending_keys.clear();
        self.pending_second = None;
        if entries.is_empty() {
            return;
        }

        let total = self.cfg.balance_total_usd;
        if total > 0.0 {
            self.reset_entry_pool(pending_second, total);
        }
        let pool_available = if total > 0.0 {
            self.entry_pool_available(pending_second, total)
        } else {
            0.0
        };
        let mut a_entries: Vec<PendingEntry> = Vec::new();
        let mut b_entries: Vec<PendingEntry> = Vec::new();
        for entry in entries.drain(..) {
            match entry.alpha_type {
                AlphaType::ShockA => a_entries.push(entry),
                AlphaType::GapB => b_entries.push(entry),
                AlphaType::LagC => {}
            }
        }

        let a_len = a_entries.len();
        let b_len = b_entries.len();
        a_entries.sort_by(|a, b| b.conf_score.partial_cmp(&a.conf_score).unwrap_or(std::cmp::Ordering::Equal));
        b_entries.sort_by(|a, b| b.conf_score.partial_cmp(&a.conf_score).unwrap_or(std::cmp::Ordering::Equal));

        if self.cfg.rank_top_k_a > 0 && a_entries.len() > self.cfg.rank_top_k_a {
            a_entries.truncate(self.cfg.rank_top_k_a);
        }
        if self.cfg.rank_top_k_b > 0 && b_entries.len() > self.cfg.rank_top_k_b {
            b_entries.truncate(self.cfg.rank_top_k_b);
        }

        let ranked_len = a_entries.len() + b_entries.len();
        let entries: Vec<PendingEntry> = a_entries.into_iter().chain(b_entries.into_iter()).collect();
        let min_notional = self.cfg.min_order_notional_usd;

        for entry in entries.into_iter() {
            let pool_available = if total > 0.0 {
                self.entry_pool_available(pending_second, total)
            } else {
                0.0
            };
            if min_notional > 0.0 && pool_available < min_notional {
                break;
            }
            let pool_override = if pool_available > 0.0 {
                Some(pool_available)
            } else {
                None
            };
            let mut state = self.states.remove(&entry.key).unwrap_or_default();
            if !matches!(state.state, TradeState::Idle) {
                self.states.insert(entry.key, state);
                continue;
            }
            self.maybe_enter(&mut state, &entry, pool_override).await;
            self.states.insert(entry.key, state);
        }
        let flush_ms = flush_start.elapsed().as_millis();
        if flush_ms > TRADE_FLUSH_PENDING_WARN_MS {
            let (entry_pool_remaining, open_positions) = self
                .risk
                .lock()
                .map(|risk| (risk.entry_pool_remaining, risk.open_positions))
                .unwrap_or((0.0, 0));
            tracing::warn!(
                "trade_flush_pending_slow elapsed_ms={} pending_second={} pending_len={} pending_keys={} a_entries={} b_entries={} ranked_len={} total_balance={} entry_pool_remaining={} min_notional={} open_positions={} states={}",
                flush_ms,
                pending_second,
                pending_len,
                pending_keys_len,
                a_len,
                b_len,
                ranked_len,
                total,
                entry_pool_remaining,
                min_notional,
                open_positions,
                self.states.len()
            );
        }
    }

    fn is_live(&self) -> bool {
        self.opinion_client.is_some() && !self.cfg.dry_run
    }

    fn order_target(&self, key: (i64, TokenSide)) -> Option<&OpinionOrderTarget> {
        self.order_targets.get(&key)
    }

    fn fee_bps(&self, venue: Venue, ts_ms: i64) -> f64 {
        if let Ok(guard) = self.fee_schedule.try_read() {
            guard.fee_bps(venue, ts_ms) as f64
        } else {
            0.0
        }
    }

    fn entry_ttl_ms_for(&self, alpha_type: Option<AlphaType>) -> i64 {
        if alpha_type == Some(AlphaType::ShockA) && self.cfg.alpha_a_entry_ttl_ms > 0 {
            self.cfg.alpha_a_entry_ttl_ms
        } else {
            self.cfg.entry_ttl_ms
        }
    }

    fn entry_aggressiveness(&self, entry: &PendingEntry, now_ms: i64) -> f64 {
        let default_g = self.cfg.entry_aggr_default;
        match entry.alpha_type {
            AlphaType::ShockA => {
                let g_high = self.cfg.entry_aggr_shock_max;
                let g_low = self.cfg.entry_aggr_shock_min;
                let early_ratio = self.cfg.entry_aggr_shock_early_ratio.clamp(0.0, 1.0);
                let horizon_s = entry.horizon_s.unwrap_or(0);
                if horizon_s <= 0 {
                    return g_high.clamp(0.0, 1.0);
                }
                let signal_ms = (entry.bar.bar_second as i64).saturating_mul(1000);
                let dt_s = now_ms
                    .saturating_sub(signal_ms)
                    .max(0) as f64
                    / 1000.0;
                let ratio = (dt_s / horizon_s as f64).clamp(0.0, 1.0);
                if early_ratio >= 1.0 || ratio <= early_ratio {
                    g_high.clamp(0.0, 1.0)
                } else {
                    let t = ((ratio - early_ratio) / (1.0 - early_ratio)).clamp(0.0, 1.0);
                    (g_high + (g_low - g_high) * t).clamp(0.0, 1.0)
                }
            }
            AlphaType::GapB => self.cfg.entry_aggr_gap.clamp(0.0, 1.0),
            _ => default_g.clamp(0.0, 1.0),
        }
    }

    fn maybe_trigger_opi_snapshot(
        &mut self,
        pair_id: i64,
        token_side: TokenSide,
        reason: &str,
    ) -> bool {
        let trigger = match self.opi_snapshot_trigger.as_ref() {
            Some(value) => value,
            None => return false,
        };
        let target = match self.order_targets.get(&(pair_id, token_side)) {
            Some(value) => value,
            None => return false,
        };
        let token_key = format!("opi:token:{}", target.token_id);
        let now_ms = now_ts_ms();
        let min_interval = self.cfg.opi_snapshot_on_demand_min_interval_ms.max(0);
        if min_interval > 0 {
            if let Some(last) = self.opi_snapshot_last_ms.get(&token_key) {
                if now_ms.saturating_sub(*last) < min_interval {
                    return false;
                }
            }
        }
        if trigger
            .try_send(SnapshotCommand::TriggerToken {
                token_key: token_key.clone(),
                reason: reason.to_string(),
            })
            .is_ok()
        {
            self.opi_snapshot_last_ms.insert(token_key, now_ms);
            return true;
        }
        false
    }

    fn log_blocked_signal(
        &mut self,
        bar: &Bars1sPair,
        alpha_type: AlphaType,
        reason: &'static str,
        direction: i64,
        pm_move: f64,
        pm_mom: f64,
        ofi_value: f64,
        value: Option<f64>,
        threshold: Option<f64>,
        edge_raw: Option<f64>,
        edge_norm: Option<f64>,
        opi_stale: Option<i64>,
        opi_stale_p90_ms: Option<i64>,
        gap_ratio_1m: Option<f64>,
        pm_range_ratio: Option<f64>,
        shock_type: Option<&str>,
    ) {
        if !self.cfg.log_blocked_signals {
            return;
        }
        let now_ms = now_ts_ms();
        let key = (bar.pair_id, bar.token_side, alpha_type, reason);
        if let Some(last) = self.blocked_log_last.get(&key) {
            if now_ms.saturating_sub(*last) < self.cfg.log_blocked_interval_ms {
                return;
            }
        }
        self.blocked_log_last.insert(key, now_ms);
        tracing::info!(
            target: "monitor::trade",
            event = "ENTRY_SKIPPED",
            reason,
            alpha_type = %alpha_type.as_str(),
            shock_type = ?shock_type,
            pair_id = bar.pair_id,
            token_side = %bar.token_side.as_str(),
            direction,
            pm_move,
            pm_mom,
            ofi_250ms = ofi_value,
            value = ?value,
            threshold = ?threshold,
            edge_raw = ?edge_raw,
            edge_norm = ?edge_norm,
            opi_stale_ms = ?opi_stale,
            opi_stale_p90_ms = ?opi_stale_p90_ms,
            gap_ratio_1m = ?gap_ratio_1m,
            pm_range_ratio = ?pm_range_ratio,
            poly_latency_p99 = ?bar.poly_obs_latency_p99_ms,
            opi_latency_p99 = ?bar.opi_obs_latency_p99_ms,
        );
    }

    fn log_gate_relaxed(
        &mut self,
        bar: &Bars1sPair,
        alpha_type: AlphaType,
        reason: &'static str,
        direction: i64,
        pm_move: f64,
        pm_mom: f64,
        ofi_value: f64,
        value: Option<f64>,
        threshold: Option<f64>,
        edge_raw: Option<f64>,
        edge_norm: Option<f64>,
        opi_stale: Option<i64>,
        opi_stale_p90_ms: Option<i64>,
        gap_ratio_1m: Option<f64>,
        pm_range_ratio: Option<f64>,
        shock_type: Option<&str>,
    ) {
        if !self.cfg.log_blocked_signals {
            return;
        }
        let now_ms = now_ts_ms();
        let key = (bar.pair_id, bar.token_side, alpha_type, reason);
        if let Some(last) = self.blocked_log_last.get(&key) {
            if now_ms.saturating_sub(*last) < self.cfg.log_blocked_interval_ms {
                return;
            }
        }
        self.blocked_log_last.insert(key, now_ms);
        tracing::info!(
            target: "monitor::trade",
            event = "ENTRY_GATE_RELAXED",
            reason,
            alpha_type = %alpha_type.as_str(),
            shock_type = ?shock_type,
            pair_id = bar.pair_id,
            token_side = %bar.token_side.as_str(),
            direction,
            pm_move,
            pm_mom,
            ofi_250ms = ofi_value,
            value = ?value,
            threshold = ?threshold,
            edge_raw = ?edge_raw,
            edge_norm = ?edge_norm,
            opi_stale_ms = ?opi_stale,
            opi_stale_p90_ms = ?opi_stale_p90_ms,
            gap_ratio_1m = ?gap_ratio_1m,
            pm_range_ratio = ?pm_range_ratio,
            poly_latency_p99 = ?bar.poly_obs_latency_p99_ms,
            opi_latency_p99 = ?bar.opi_obs_latency_p99_ms,
        );
    }

    fn alpha_source_value(&self, bar: &Bars1sPair) -> Option<f64> {
        match self.cfg.alpha_source.to_ascii_lowercase().as_str() {
            "follow_score_raw" => bar.follow_score_raw,
            "follow_score_adj" => bar.follow_score_adj,
            "book_alpha_raw" => bar.book_alpha_raw,
            "book_alpha_adj" => bar.book_alpha_adj,
            _ => None,
        }
    }

    fn alpha_source_is_book_alpha(&self) -> bool {
        matches!(
            self.cfg.alpha_source.to_ascii_lowercase().as_str(),
            "book_alpha_raw" | "book_alpha_adj"
        )
    }

    fn alpha_source_cfg(&self) -> Option<String> {
        let value = self.cfg.alpha_source.trim();
        if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        }
    }

    fn trade_state_str(state: TradeState) -> &'static str {
        match state {
            TradeState::Idle => "IDLE",
            TradeState::EntryReady => "ENTRY_READY",
            TradeState::EntryPlacing => "ENTRY_PLACING",
            TradeState::EntryWorking => "ENTRY_WORKING",
            TradeState::PositionOpen => "POSITION_OPEN",
            TradeState::ExitWorking => "EXIT_WORKING",
            TradeState::Cooldown => "COOLDOWN",
        }
    }

    fn build_alpha_log_entry(
        &self,
        phase: &str,
        bar: &Bars1sPair,
        state: &PairTradeState,
        entry: Option<&PendingEntry>,
    ) -> AlphaLogEntry {
        let mut log = build_alpha_entry_base(
            bar,
            bar.token_side,
            bar.pair_id,
            phase,
            Self::trade_state_str(state.state),
            state.direction,
        );
        log.alpha_type = state.alpha_type.map(|t| t.as_str().to_string());
        log.alpha_source = state.alpha_source.clone().or_else(|| entry.map(|e| e.alpha_source.clone()));
        log.alpha_source_cfg = self.alpha_source_cfg();
        log.alpha_value = entry.and_then(|e| e.alpha_value).or_else(|| self.alpha_source_value(bar));
        if self.cfg.alpha_min > 0.0 {
            log.alpha_min = Some(self.cfg.alpha_min);
        }
        log.pm_move = entry.map(|e| e.pm_move).or(state.last_pm_move);
        log.pm_mom = entry.map(|e| e.pm_mom).or(state.last_pm_mom);
        log.ofi_250ms = entry.map(|e| e.ofi_value).or(state.last_ofi_250ms);
        log.edge_raw = entry.map(|e| e.edge_raw).or(state.edge_raw);
        log.edge_norm = entry.map(|e| e.edge_norm).or(state.edge_norm);
        log.base_pm = entry.map(|e| e.base_pm).or(state.last_base_pm).or(state.base_pm);
        log.entry_price = state.entry_price;
        log.entry_qty = state.entry_qty;
        log.entry_notional = state.entry_notional.or_else(|| {
            match (log.entry_price, log.entry_qty) {
                (Some(px), Some(qty)) => Some(px * qty),
                _ => None,
            }
        });
        log.exit_price_target = state.exit_price_target;
        log.exit_price = state.exit_price;
        log.entry_order_id = state.entry_order_id.clone();
        log.exit_order_id = state.exit_order_id.clone();
        log.entry_ts_ms = state.entry_ts_ms;
        log.entry_bar_second = state.entry_bar_second;
        log.holding_ms = state
            .entry_ts_ms
            .map(|ts| now_ts_ms().saturating_sub(ts));
        log.opi_bid_l1 = state.last_opi_bid_l1;
        log.opi_ask_l1 = state.last_opi_ask_l1;
        log.opi_bid_l3 = state.last_opi_bid_l3;
        log.opi_ask_l3 = state.last_opi_ask_l3;
        log.opi_bar_second = state.last_opi_bar_second;
        log
    }

    fn log_alpha(
        &self,
        phase: &str,
        bar: &Bars1sPair,
        state: &PairTradeState,
        entry: Option<&PendingEntry>,
    ) {
        if let Some(logger) = &self.alpha_logger {
            let entry = self.build_alpha_log_entry(phase, bar, state, entry);
            logger.log(entry);
        }
    }

    fn cancel_priority_for(reason: &str) -> i64 {
        match reason {
            "reverse_shock" => 1,
            "aggressive_flow" => 2,
            "retrace_cancel" | "retrace_exit" => 3,
            "bid_withdraw" => 4,
            r if r.starts_with("opi_") => 5,
            "book_shape_deterioration" => 6,
            "ttl" => 7,
            _ => 9,
        }
    }

    async fn emit_cancel_snapshot(
        &self,
        state: &PairTradeState,
        bar: &Bars1sPair,
        reason: &str,
        order_role: &str,
        order_id: Option<String>,
        client_id: Option<String>,
        pm_move: f64,
        ofi_value: f64,
    ) {
        let direction = state.direction;
        let retrace_ratio = self.retrace_ratio(pm_move, state.pm_peak, direction);
        let shape = self.book_shape(bar, direction);
        let (book_decay, book_support) = match shape {
            Some(shape) => (Some(shape.decay), Some(shape.support)),
            None => (state.book_decay, state.book_support),
        };
        let tick = if self.cfg.tick_px > 0.0 { self.cfg.tick_px } else { 1e-4 };
        let opi_spread_ticks = match (opi_best_bid_from_bar(bar), opi_best_ask_from_bar(bar)) {
            (Some(bid), Some(ask)) if ask > bid && tick > 0.0 => {
                Some(((ask - bid) / tick).round() as i64)
            }
            _ => None,
        };
        let row = CancelTriggerSnapshotRow {
            event_ts_ms: now_ts_ms(),
            pair_id: bar.pair_id,
            token_side: bar.token_side,
            direction,
            order_role: order_role.to_string(),
            order_id,
            client_id,
            cancel_reason: reason.to_string(),
            cancel_priority: Self::cancel_priority_for(reason),
            ofi_250ms: Some(ofi_value),
            pm_sell_trade_notional_250ms: bar.poly_sell_notional_250ms,
            pm_retrace_ratio: retrace_ratio,
            delta_bid_notional_250ms: bar.poly_bid_delta_notional_250ms,
            delta_ask_notional_250ms: bar.poly_ask_delta_notional_250ms,
            opi_book_decay: book_decay,
            opi_book_support: book_support,
            opi_spread_ticks,
        };
        try_send_db(
            &self.db_sender,
            DbMessage::CancelTriggerSnapshot(row),
            "trade_cancel_snapshot",
        );
    }

    fn next_client_order_id(&self, pair_id: i64, token_side: TokenSide, tag: &str) -> String {
        format!(
            "{}:{}:{}:{}",
            self.cfg.order_client_prefix,
            pair_id,
            token_side.as_str(),
            format!("{}-{}", tag, now_ts_ms())
        )
    }

    fn exit_candidate_prices(
        &self,
        bar: &Bars1sPair,
        direction: i64,
        base_price: f64,
        tick: f64,
    ) -> Vec<f64> {
        let mut prices = Vec::new();
        if base_price <= 0.0 {
            return prices;
        }
        let step_ticks = self.cfg.chase_step_ticks.max(1);
        let max_steps = self.cfg.max_chase_ticks.max(0);
        for step_idx in 0..=max_steps {
            let n = step_ticks * step_idx;
            let price = if direction > 0 {
                base_price + tick * n as f64
            } else {
                base_price - tick * n as f64
            };
            if price <= 0.0 {
                break;
            }
            let maker_ok = if direction > 0 {
                match opi_best_bid_from_bar(bar) {
                    Some(bid) => price > bid,
                    None => true,
                }
            } else {
                match opi_best_ask_from_bar(bar) {
                    Some(ask) => price < ask,
                    None => true,
                }
            };
            if !maker_ok {
                continue;
            }
            prices.push(price);
        }
        prices
    }

    async fn place_exit_with_chase(
        &self,
        pair_id: i64,
        token_side: TokenSide,
        pair_key: (i64, TokenSide),
        side: OrderSide,
        base_price: f64,
        qty: f64,
        bar: &Bars1sPair,
        direction: i64,
        tick: f64,
    ) -> (
        Option<PlacedOrder>,
        Option<String>,
        Option<f64>,
        i64,
        i64,
        Option<anyhow::Error>,
    ) {
        let candidates = self.exit_candidate_prices(bar, direction, base_price, tick);
        let candidates_len = candidates.len();
        let chase_start = Instant::now();
        let mut chase_count = 0i64;
        let mut post_only_rejects = 0i64;
        let mut last_err: Option<anyhow::Error> = None;
        let mut placed_order: Option<PlacedOrder> = None;
        let mut placed_client_id: Option<String> = None;
        let mut placed_price: Option<f64> = None;
        for price in candidates {
            chase_count += 1;
            let client_id = self.next_client_order_id(pair_id, token_side, "X");
            match self
                .place_live_order(pair_key, side, price, qty, &client_id)
                .await
            {
                Ok(order) => {
                    placed_order = Some(order);
                    placed_client_id = Some(client_id);
                    placed_price = Some(price);
                    last_err = None;
                    break;
                }
                Err(err) => {
                    if is_post_only_reject(&err) {
                        post_only_rejects += 1;
                        last_err = Some(err);
                        continue;
                    }
                    last_err = Some(err);
                    break;
                }
            }
        }
        let chase_ms = chase_start.elapsed().as_millis();
        if chase_ms > TRADE_EXIT_CHASE_WARN_MS {
            tracing::warn!(
                "trade_exit_chase_slow elapsed_ms={} pair_id={} token_side={} candidates_len={} chase_count={} post_only_rejects={} placed={} last_err={:?}",
                chase_ms,
                pair_id,
                token_side.as_str(),
                candidates_len,
                chase_count,
                post_only_rejects,
                placed_order.is_some(),
                last_err.as_ref().map(|err| err.to_string())
            );
        }
        (
            placed_order,
            placed_client_id,
            placed_price,
            chase_count,
            post_only_rejects,
            last_err,
        )
    }

    async fn place_live_order(
        &self,
        key: (i64, TokenSide),
        side: OrderSide,
        price: f64,
        qty: f64,
        client_order_id: &str,
    ) -> anyhow::Result<PlacedOrder> {
        let target = self
            .order_target(key)
            .ok_or_else(|| anyhow::anyhow!("missing opinion token mapping"))?;
        let client = self
            .opinion_client
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("opinion client unavailable"))?;
        let amount = match side {
            OrderSide::Buy => OrderAmount::Quote(price * qty),
            OrderSide::Sell => OrderAmount::Base(qty),
        };
        tracing::info!(
            "opinion_order_input market_id={} token_id={} side={} price={} amount={:?}",
            target.market_id,
            target.token_id,
            side.as_int(),
            price,
            amount
        );
        let place_start = Instant::now();
        let result = client
            .place_limit_order(
                target.market_id,
                &target.token_id,
                side,
                price,
                amount,
                Some(client_order_id),
                self.cfg.post_only,
            )
            .await;
        let place_ms = place_start.elapsed().as_millis();
        if place_ms > TRADE_PLACE_ORDER_WARN_MS {
            tracing::warn!(
                "trade_place_order_slow elapsed_ms={} market_id={} token_id={} side={} price={} qty={} post_only={} status={}",
                place_ms,
                target.market_id,
                target.token_id,
                side.as_int(),
                price,
                qty,
                self.cfg.post_only,
                if result.is_ok() { "ok" } else { "err" }
            );
        }
        result
    }

    async fn cancel_live_order(&self, order_id: &str) {
        if let Some(client) = &self.opinion_client {
            let cancel_start = Instant::now();
            let result = client.cancel_order(order_id).await;
            let cancel_ms = cancel_start.elapsed().as_millis();
            if cancel_ms > TRADE_CANCEL_ORDER_WARN_MS {
                tracing::warn!(
                    "trade_cancel_slow order_id={} elapsed_ms={} status={}",
                    order_id,
                    cancel_ms,
                    if result.is_ok() { "ok" } else { "err" }
                );
            }
            if let Err(err) = result {
                tracing::warn!("trade cancel failed order_id={} err={}", order_id, err);
            }
        }
    }

    async fn poll_fills(
        &self,
        key: (i64, TokenSide),
        since_ms: Option<i64>,
    ) -> Vec<TradeFill> {
        let target = match self.order_target(key) {
            Some(target) => target,
            None => return Vec::new(),
        };
        let client = match &self.opinion_client {
            Some(client) => client,
            None => return Vec::new(),
        };
        let poll_start = Instant::now();
        let result = client
            .fetch_trades(target.market_id, &target.token_id, since_ms)
            .await;
        let poll_ms = poll_start.elapsed().as_millis();
        if poll_ms > TRADE_POLL_FILLS_WARN_MS {
            tracing::warn!(
                "trade_fill_poll_slow elapsed_ms={} market_id={} token_id={} since_ms={:?}",
                poll_ms,
                target.market_id,
                target.token_id,
                since_ms
            );
        }
        match result {
            Ok(fills) => fills,
            Err(err) => {
                tracing::warn!("trade fill poll failed err={}", err);
                Vec::new()
            }
        }
    }

    fn handle_shock(&mut self, shock: ShockInput) {
        let key = (shock.pair_id, shock.token_side);
        let entry = self.shock_signals.entry(key).or_insert_with(Vec::new);
        entry.push(ShockSignal {
            shock_id: shock.shock_id,
            shock_type: shock.shock_type,
            direction: shock.direction,
            magnitude: shock.magnitude,
            noise_flag: shock.noise_flag,
            trigger_bar_second: shock.trigger_bar_second,
        });
        let max_age = self.cfg.alpha_a_max_age_s.max(0);
        let now_bar = shock.trigger_bar_second;
        entry.retain(|s| now_bar.saturating_sub(s.trigger_bar_second) <= max_age);
        if entry.len() > 50 {
            entry.drain(0..entry.len().saturating_sub(50));
        }
    }

    fn next_order_req_id(&mut self) -> u64 {
        self.next_order_req_id = self.next_order_req_id.wrapping_add(1);
        self.next_order_req_id
    }

    fn enqueue_order_request(&self, id: u64, kind: OrderRequestKind) -> bool {
        let req = OrderRequest {
            id,
            kind,
            response_tx: self.order_resp_tx.clone(),
        };
        if let Err(err) = self.order_exec.send(req) {
            tracing::warn!("order_executor send failed err={}", err);
            return false;
        }
        true
    }

    fn enqueue_place_chase(
        &mut self,
        pair_key: (i64, TokenSide),
        kind: PlaceKind,
        side: OrderSide,
        qty: f64,
        candidate_prices: Vec<f64>,
        client_tag: &'static str,
    ) -> Option<u64> {
        let target = self.order_target(pair_key)?;
        let market_id = target.market_id;
        let token_id = target.token_id.clone();
        let id = self.next_order_req_id();
        if !self.enqueue_order_request(
            id,
            OrderRequestKind::PlaceChase {
                pair_key,
                kind,
                market_id,
                token_id,
                side,
                qty,
                candidate_prices,
                client_prefix: self.cfg.order_client_prefix.clone(),
                client_tag,
                post_only: self.cfg.post_only,
            },
        ) {
            return None;
        }
        Some(id)
    }

    fn enqueue_cancel_order(&mut self, order_id: String) -> Option<u64> {
        let now_ms = now_ts_ms();
        self.evict_cancel_dedup(now_ms);
        if let Some(last) = self.cancel_dedup_recent.get(&order_id) {
            if now_ms.saturating_sub(*last) < CANCEL_DEDUP_WINDOW_MS {
                return None;
            }
        }
        self.cancel_dedup_recent.insert(order_id.clone(), now_ms);
        self.cancel_dedup_queue.push_back((now_ms, order_id.clone()));
        let id = self.next_order_req_id();
        if self.enqueue_order_request(
            id,
            OrderRequestKind::CancelOrder {
                order_id: order_id.clone(),
            },
        ) {
            Some(id)
        } else {
            self.cancel_dedup_recent.remove(&order_id);
            if let Some((ts, last_id)) = self.cancel_dedup_queue.back() {
                if *ts == now_ms && last_id == &order_id {
                    self.cancel_dedup_queue.pop_back();
                }
            }
            None
        }
    }

    fn evict_cancel_dedup(&mut self, now_ms: i64) {
        loop {
            let expired = match self.cancel_dedup_queue.front() {
                Some((ts, _)) => now_ms.saturating_sub(*ts) >= CANCEL_DEDUP_WINDOW_MS,
                None => false,
            };
            if !expired {
                break;
            }
            if let Some((ts, order_id)) = self.cancel_dedup_queue.pop_front() {
                if let Some(current) = self.cancel_dedup_recent.get(&order_id) {
                    if *current == ts {
                        self.cancel_dedup_recent.remove(&order_id);
                    }
                }
            }
        }
    }

    fn maybe_enqueue_entry_cancel(&mut self, state: &mut PairTradeState) {
        if !state.entry_cancel_pending {
            return;
        }
        if state.entry_cancel_req_id.is_some() {
            return;
        }
        if self.cancel_backoff_active() {
            return;
        }
        let now_ms = now_ts_ms();
        if !self.cancel_rate_limiter.allow(now_ms, false) {
            return;
        }
        let order_id = match state.entry_order_id.clone() {
            Some(id) => id,
            None => return,
        };
        if !self.cancel_retry_allowed(&order_id, now_ms) {
            return;
        }
        if let Some(req_id) = self.enqueue_cancel_order(order_id) {
            state.entry_cancel_req_id = Some(req_id);
        }
    }

    fn maybe_enqueue_exit_cancel(&mut self, state: &mut PairTradeState) {
        if !state.exit_cancel_pending {
            return;
        }
        if state.exit_cancel_req_id.is_some() {
            return;
        }
        if self.cancel_backoff_active() {
            return;
        }
        let now_ms = now_ts_ms();
        if !self.cancel_rate_limiter.allow(now_ms, true) {
            return;
        }
        let order_id = match state.exit_order_id.clone() {
            Some(id) => id,
            None => return,
        };
        if !self.cancel_retry_allowed(&order_id, now_ms) {
            return;
        }
        if let Some(req_id) = self.enqueue_cancel_order(order_id) {
            state.exit_cancel_req_id = Some(req_id);
        }
    }

    fn maybe_enqueue_fill_poll(
        &mut self,
        state: &mut PairTradeState,
        pair_key: (i64, TokenSide),
        kind: FillKind,
    ) {
        match kind {
            FillKind::Entry => {
                if state.entry_order_id.is_none() && state.entry_client_id.is_none() {
                    return;
                }
            }
            FillKind::Exit => {
                if state.exit_order_id.is_none() && state.exit_client_id.is_none() {
                    return;
                }
            }
        }
        if state.fill_poll_req_id.is_some() {
            return;
        }
        let now_ms = now_ts_ms();
        if self.cfg.ws_user_channels_enabled && self.ws_user_channel_active(now_ms) {
            return;
        }
        if let Some(until_ms) = self.fill_poll_backoff_until_ms {
            if now_ms < until_ms {
                return;
            }
        }
        if let Some(last) = state.last_fill_poll_ms {
            if now_ms.saturating_sub(last) < self.order_poll_interval_ms {
                return;
            }
        }
        let target = match self.order_target(pair_key) {
            Some(target) => target,
            None => return,
        };
        let market_id = target.market_id;
        let token_id = target.token_id.clone();
        let id = self.next_order_req_id();
        state.last_fill_poll_ms = Some(now_ms);
        state.fill_poll_req_id = Some(id);
        if !self.enqueue_order_request(
            id,
            OrderRequestKind::FetchFills {
                pair_key,
                kind,
                market_id,
                token_id,
                since_ms: state.entry_ts_ms,
            },
        ) {
            state.fill_poll_req_id = None;
        }
    }

    fn update_fill_poll_backoff(&mut self, rate_limited: bool, success: bool) {
        if rate_limited {
            let base = (self.order_poll_interval_ms * 2).max(FILL_POLL_BACKOFF_MIN_MS);
            let next = if self.fill_poll_backoff_ms > 0 {
                (self.fill_poll_backoff_ms.saturating_mul(2)).min(FILL_POLL_BACKOFF_MAX_MS)
            } else {
                base
            };
            self.fill_poll_backoff_ms = next;
            let now_ms = now_ts_ms();
            let until_ms = now_ms.saturating_add(next);
            self.fill_poll_backoff_until_ms = Some(until_ms);
            tracing::warn!(
                "trade fill poll backoff rate_limited=true backoff_ms={} until_ms={} order_poll_interval_ms={}",
                next,
                until_ms,
                self.order_poll_interval_ms
            );
            return;
        }
        if success && self.fill_poll_backoff_ms > 0 {
            self.fill_poll_backoff_ms = 0;
            self.fill_poll_backoff_until_ms = None;
            tracing::info!("trade fill poll backoff cleared");
        }
    }

    fn ws_user_channel_active(&self, now_ms: i64) -> bool {
        if !self.cfg.ws_user_channels_enabled {
            return false;
        }
        if self.cfg.ws_user_channel_stale_ms <= 0 {
            return false;
        }
        match self.last_ws_user_msg_ms {
            Some(last) => now_ms.saturating_sub(last) <= self.cfg.ws_user_channel_stale_ms,
            None => false,
        }
    }

    fn cancel_backoff_active(&self) -> bool {
        if let Some(until_ms) = self.cancel_backoff_until_ms {
            return now_ts_ms() < until_ms;
        }
        false
    }

    fn update_cancel_backoff(&mut self, result: &CancelResult) {
        if result.ok {
            if self.cancel_backoff_ms > 0 || self.cancel_consecutive_failures > 0 {
                self.cancel_backoff_ms = 0;
                self.cancel_backoff_until_ms = None;
                self.cancel_consecutive_failures = 0;
                self.cancel_last_failure_ms = None;
                tracing::info!("trade cancel backoff cleared");
            }
            return;
        }

        let now_ms = now_ts_ms();
        if let Some(last_ms) = self.cancel_last_failure_ms {
            if now_ms.saturating_sub(last_ms) > CANCEL_FAIL_RESET_MS {
                self.cancel_consecutive_failures = 0;
            }
        }
        self.cancel_last_failure_ms = Some(now_ms);
        self.cancel_consecutive_failures = self.cancel_consecutive_failures.saturating_add(1);

        if result.rate_limited || result.server_error {
            let base = (self.order_poll_interval_ms * 2).max(CANCEL_BACKOFF_MIN_MS);
            let next = if self.cancel_backoff_ms > 0 {
                (self.cancel_backoff_ms.saturating_mul(2)).min(CANCEL_BACKOFF_MAX_MS)
            } else {
                base
            };
            let jitter = rand::thread_rng().gen_range(CANCEL_BACKOFF_JITTER_MIN..=CANCEL_BACKOFF_JITTER_MAX);
            let jittered = ((next as f64) * jitter).round() as i64;
            let jittered = jittered.clamp(CANCEL_BACKOFF_MIN_MS, CANCEL_BACKOFF_MAX_MS);
            self.cancel_backoff_ms = next;
            let until_ms = now_ms.saturating_add(jittered);
            self.cancel_backoff_until_ms = Some(until_ms);
            tracing::warn!(
                "trade cancel backoff rate_limited={} server_error={} backoff_ms={} jitter_ms={} until_ms={} order_poll_interval_ms={}",
                result.rate_limited,
                result.server_error,
                next,
                jittered,
                until_ms,
                self.order_poll_interval_ms
            );
        }

        if self.cancel_consecutive_failures >= CANCEL_CIRCUIT_FAILS {
            let until_ms = now_ms.saturating_add(CANCEL_CIRCUIT_OPEN_MS);
            let replace = match self.cancel_backoff_until_ms {
                Some(existing) => existing < until_ms,
                None => true,
            };
            if replace {
                self.cancel_backoff_until_ms = Some(until_ms);
            }
            tracing::warn!(
                "trade cancel circuit open failures={} open_ms={} until_ms={}",
                self.cancel_consecutive_failures,
                CANCEL_CIRCUIT_OPEN_MS,
                until_ms
            );
        }
    }

    fn cancel_retry_allowed(&self, order_id: &str, now_ms: i64) -> bool {
        match self.cancel_retry.get(order_id) {
            Some(state) => now_ms >= state.next_allowed_ms,
            None => true,
        }
    }

    fn record_cancel_failure(&mut self, order_id: &str, now_ms: i64) {
        let entry = self
            .cancel_retry
            .entry(order_id.to_string())
            .or_insert(CancelRetryState {
                failures: 0,
                next_allowed_ms: now_ms,
            });
        entry.failures = entry.failures.saturating_add(1);
        let mut backoff = CANCEL_RETRY_BASE_MS;
        if entry.failures > 1 {
            for _ in 1..entry.failures {
                backoff = (backoff * 2).min(CANCEL_RETRY_MAX_MS);
            }
        }
        let jitter = rand::thread_rng().gen_range(0.8..=1.2);
        backoff = ((backoff as f64) * jitter).round() as i64;
        if backoff < CANCEL_RETRY_BASE_MS {
            backoff = CANCEL_RETRY_BASE_MS;
        }
        if entry.failures >= CANCEL_RETRY_MAX_FAILURES {
            backoff = CANCEL_RETRY_MAX_MS;
        }
        entry.next_allowed_ms = now_ms.saturating_add(backoff);
        tracing::warn!(
            "trade cancel retry scheduled order_id={} failures={} backoff_ms={} next_allowed_ms={}",
            order_id,
            entry.failures,
            backoff,
            entry.next_allowed_ms
        );
    }

    fn clear_cancel_retry(&mut self, order_id: &str) {
        self.cancel_retry.remove(order_id);
    }

    fn evict_open_order_cache(&mut self, market_id: i64, order_id: &str) {
        if let Some(cache) = self.open_orders_cache.get_mut(&market_id) {
            cache.order_ids.remove(order_id);
        }
    }

    async fn is_order_open_for_cancel(
        &mut self,
        pair_key: (i64, TokenSide),
        order_id: &str,
    ) -> Option<bool> {
        let client = self.opinion_client.as_ref()?;
        let target = self.order_target(pair_key)?;
        let market_id = target.market_id;
        let now_ms = now_ts_ms();
        if let Some(cache) = self.open_orders_cache.get(&market_id) {
            if now_ms.saturating_sub(cache.fetched_ms) <= CANCEL_VERIFY_CACHE_TTL_MS {
                if cache.order_ids.contains(order_id) {
                    return Some(true);
                }
                if cache.complete {
                    return Some(false);
                }
                return None;
            }
        }
        match client
            .fetch_open_order_ids(market_id, CANCEL_VERIFY_MAX_PAGES, CANCEL_VERIFY_PAGE_LIMIT)
            .await
        {
            Ok((order_ids, complete)) => {
                let is_open = order_ids.contains(order_id);
                self.open_orders_cache.insert(
                    market_id,
                    OpenOrdersCache {
                        fetched_ms: now_ms,
                        order_ids,
                        complete,
                    },
                );
                if is_open {
                    Some(true)
                } else if complete {
                    Some(false)
                } else {
                    None
                }
            }
            Err(err) => {
                tracing::warn!(
                    "trade cancel verify failed market_id={} order_id={} err={}",
                    market_id,
                    order_id,
                    err
                );
                None
            }
        }
    }

    async fn handle_order_response(&mut self, resp: OrderResponse) {
        match resp {
            OrderResponse::PlaceChase {
                id,
                pair_key,
                kind,
                result,
            } => match kind {
                PlaceKind::Entry => {
                    self.handle_entry_place_result(pair_key, id, result).await;
                }
                PlaceKind::Exit => {
                    self.handle_exit_place_result(pair_key, id, result).await;
                }
            },
            OrderResponse::FetchFills {
                id,
                pair_key,
                kind,
                result,
            } => {
                self.handle_fill_response(pair_key, id, kind, result).await;
            }
            OrderResponse::CancelOrder { id, order_id, result } => {
                self.handle_cancel_response(id, order_id, result).await;
            }
        }
    }

    async fn handle_cancel_response(
        &mut self,
        req_id: u64,
        order_id: String,
        result: CancelResult,
    ) {
        let mut found_key: Option<(i64, TokenSide)> = None;
        let mut is_entry = false;
        for (key, state) in self.states.iter() {
            if state.entry_cancel_req_id == Some(req_id) {
                found_key = Some(*key);
                is_entry = true;
                break;
            }
            if state.exit_cancel_req_id == Some(req_id) {
                found_key = Some(*key);
                is_entry = false;
                break;
            }
        }
        let key = match found_key {
            Some(key) => key,
            None => return,
        };
        let mut state = match self.states.remove(&key) {
            Some(state) => state,
            None => return,
        };
        let now_ms = now_ts_ms();
        let mut verified_cancel = false;
        if !result.ok && (result.rate_limited || result.server_error) {
            let now_ms = now_ts_ms();
            if !self.cfg.ws_user_channels_enabled || !self.ws_user_channel_active(now_ms) {
                if let Some(open) = self.is_order_open_for_cancel(key, &order_id).await {
                if !open {
                    verified_cancel = true;
                    tracing::info!(
                        "trade cancel verified_not_open order_id={} pair_id={} token_side={}",
                        order_id,
                        key.0,
                        key.1.as_str()
                    );
                } else {
                    tracing::warn!(
                        "trade cancel verify still_open order_id={} pair_id={} token_side={}",
                        order_id,
                        key.0,
                        key.1.as_str()
                    );
                }
                }
            }
        }
        self.update_cancel_backoff(&result);
        let cancel_ok = result.ok || verified_cancel;
        if cancel_ok {
            self.clear_cancel_retry(&order_id);
            if let Some(target) = self.order_target(key) {
                self.evict_open_order_cache(target.market_id, &order_id);
            }
        } else {
            self.record_cancel_failure(&order_id, now_ms);
        }
        if is_entry {
            if state.entry_cancel_req_id != Some(req_id) {
                self.states.insert(key, state);
                return;
            }
            state.entry_cancel_req_id = None;
            if cancel_ok {
                let reason = state
                    .entry_cancel_reason
                    .clone()
                    .unwrap_or_else(|| "entry_cancel".to_string());
                let bar = match state.last_bar.clone() {
                    Some(bar) => bar,
                    None => {
                        state.enter_cooldown(
                            state.entry_bar_second.unwrap_or(0),
                            self.cfg.cooldown_after_cancel_ms,
                        );
                        self.states.insert(key, state);
                        return;
                    }
                };
                let pm_move = state.last_pm_move.unwrap_or(0.0);
                let pm_mom = state.last_pm_mom.unwrap_or(0.0);
                let ofi_value = state.last_ofi_250ms.unwrap_or(0.0);
                self.finalize_entry_cancel(
                    &mut state,
                    &bar,
                    pm_move,
                    pm_mom,
                    ofi_value,
                    &reason,
                    key,
                )
                .await;
            } else {
                if result.rate_limited {
                    tracing::warn!(
                        "entry cancel rate_limited order_id={} pair_id={} token_side={}",
                        order_id,
                        key.0,
                        key.1.as_str()
                    );
                } else if let Some(err) = result.error.as_deref() {
                    tracing::warn!(
                        "entry cancel failed order_id={} pair_id={} token_side={} err={}",
                        order_id,
                        key.0,
                        key.1.as_str(),
                        err
                    );
                }
            }
        } else {
            if state.exit_cancel_req_id != Some(req_id) {
                self.states.insert(key, state);
                return;
            }
            state.exit_cancel_req_id = None;
            if cancel_ok {
                state.exit_order_id = None;
                state.exit_client_id = None;
                state.exit_cancel_pending = false;
                state.exit_cancel_reason = None;
            } else {
                if result.rate_limited {
                    tracing::warn!(
                        "exit cancel rate_limited order_id={} pair_id={} token_side={}",
                        order_id,
                        key.0,
                        key.1.as_str()
                    );
                } else if let Some(err) = result.error.as_deref() {
                    tracing::warn!(
                        "exit cancel failed order_id={} pair_id={} token_side={} err={}",
                        order_id,
                        key.0,
                        key.1.as_str(),
                        err
                    );
                }
            }
        }
        self.states.insert(key, state);
    }

    async fn handle_entry_place_result(
        &mut self,
        pair_key: (i64, TokenSide),
        req_id: u64,
        result: PlaceResult,
    ) {
        let mut state = match self.states.remove(&pair_key) {
            Some(state) => state,
            None => return,
        };
        if state.entry_place_req_id != Some(req_id) {
            self.states.insert(pair_key, state);
            return;
        }
        state.entry_place_req_id = None;
        let entry = state.pending_entry.take();
        let entry_meta = state.pending_entry_meta.take();
        let entry = match entry {
            Some(entry) => entry,
            None => {
                self.states.insert(pair_key, state);
                return;
            }
        };
        let bar = entry.bar.clone();
        let (
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
        ) = Self::audit_health(Some(&bar));
        let pm_move = entry.pm_move;
        let pm_mom = entry.pm_mom;
        let ofi_value = entry.ofi_value;
        let direction = entry.direction;
        let qty = state.entry_qty.unwrap_or(0.0);
        let now_ms = now_ts_ms();
        let order_id = result.order.as_ref().map(|order| order.order_id.clone());
        let client_id = result
            .order
            .as_ref()
            .and_then(|order| order.client_order_id.clone())
            .or(result.client_id_used.clone());
        let unknown_order = order_id.is_none()
            && result
                .last_err
                .as_deref()
                .map(|v| v == "order_id_missing")
                .unwrap_or(false);

        if order_id.is_some() || unknown_order {
            let entry_price = result
                .price_used
                .or(state.entry_price)
                .unwrap_or(0.0);
            state.state = TradeState::EntryWorking;
            state.entry_price = Some(entry_price);
            state.entry_qty = Some(qty);
            state.entry_notional = Some(entry_price * qty);
            state.entry_ts_ms = Some(now_ms);
            state.entry_bar_second = Some(bar.bar_second);
            state.pm_mom_entry = Some(pm_mom);
            state.entry_order_id = order_id;
            state.entry_client_id = client_id;
            state.chase_count = result.chase_count;

            let exposure_key = (pair_key.0, pair_key.1, direction);
            let alpha_type = state.alpha_type.unwrap_or(entry.alpha_type);
            self.add_exposure(exposure_key, entry_price * qty, alpha_type);

            let mut extra_map: Map<String, Value> = Map::new();
            extra_map.insert(
                "entry_order_id".to_string(),
                json!(state.entry_order_id.clone().unwrap_or_default()),
            );
            extra_map.insert(
                "entry_client_id".to_string(),
                json!(state.entry_client_id.clone().unwrap_or_default()),
            );
            extra_map.insert("chase_count".to_string(), json!(result.chase_count));
            extra_map.insert("post_only_rejects".to_string(), json!(result.post_only_rejects));
            if unknown_order {
                extra_map.insert("order_id_missing".to_string(), json!(true));
            }
            if let Some(meta) = entry_meta {
                extra_map.insert("theo_price".to_string(), json!(meta.theo_price));
                extra_map.insert("price_limit".to_string(), json!(meta.price_limit));
                extra_map.insert("price_ref".to_string(), json!(meta.price_ref));
                extra_map.insert("tick_px".to_string(), json!(meta.tick_px));
            }
            let working_row = TradeAuditRow {
                event_ts_ms: now_ms,
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "ENTRY_WORKING".to_string(),
                direction,
                reason: if unknown_order {
                    Some("order_id_missing".to_string())
                } else {
                    None
                },
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: None,
                exit_price: None,
                gas_est_usd: Some(self.gas_est_usd().max(0.0)),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: None,
                mfe: None,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(&state, Some(Value::Object(extra_map))),
            };
            self.emit_audit(working_row).await;
        } else {
            let err_msg = result
                .last_err
                .unwrap_or_else(|| "entry_place_failed".to_string());
            let total_balance = self.cfg.balance_total_usd;
            let reserved = state.entry_notional.unwrap_or(0.0);
            self.release_entry_pool(bar.bar_second, total_balance, reserved);
            state.entry_notional = None;
            state.enter_cooldown(bar.bar_second, self.cfg.cooldown_after_postonly_reject_ms);
            let row = TradeAuditRow {
                event_ts_ms: now_ms,
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "ENTRY_PLACE_FAILED".to_string(),
                direction,
                reason: Some(err_msg),
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: None,
                exit_price: None,
                gas_est_usd: Some(self.gas_est_usd().max(0.0)),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: None,
                mfe: None,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_entry(
                    &entry,
                    Some(json!({
                        "chase_count": result.chase_count,
                        "post_only_rejects": result.post_only_rejects,
                    })),
                ),
            };
            self.emit_audit(row).await;
        }
        self.states.insert(pair_key, state);
    }

    async fn handle_exit_place_result(
        &mut self,
        pair_key: (i64, TokenSide),
        req_id: u64,
        result: PlaceResult,
    ) {
        let mut state = match self.states.remove(&pair_key) {
            Some(state) => state,
            None => return,
        };
        if state.exit_place_req_id != Some(req_id) {
            self.states.insert(pair_key, state);
            return;
        }
        state.exit_place_req_id = None;
        let now_ms = now_ts_ms();
        if let Some(order) = result.order {
            state.last_exit_place_ms = Some(now_ms);
            state.exit_order_id = Some(order.order_id);
            state.exit_client_id = order.client_order_id.or(result.client_id_used);
            if let Some(price_used) = result.price_used {
                state.exit_price = Some(price_used);
            }
            state.pending_exit_ctx = None;
        } else if result
            .last_err
            .as_deref()
            .map(|v| v == "order_id_missing")
            .unwrap_or(false)
        {
            state.last_exit_place_ms = Some(now_ms);
            state.exit_order_id = None;
            state.exit_client_id = result.client_id_used;
            if let Some(price_used) = result.price_used {
                state.exit_price = Some(price_used);
            }
            state.pending_exit_ctx = None;
            let row = TradeAuditRow {
                event_ts_ms: now_ms,
                pair_id: pair_key.0,
                token_side: pair_key.1,
                phase: "EXIT_WORKING".to_string(),
                direction: state.direction,
                reason: Some("order_id_missing".to_string()),
                pm_move: state.last_pm_move,
                pm_mom: state.last_pm_mom,
                ofi_250ms: state.last_ofi_250ms,
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: state.exit_price_target,
                exit_price: state.exit_price,
                gas_est_usd: Some(self.gas_est_usd().max(0.0)),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: state.mae,
                mfe: state.mfe,
                obs_latency_poly_p99_ms: None,
                obs_latency_opi_p99_ms: None,
                bar_gap_flag: None,
                poly_bar_gap_flag: None,
                opi_bar_gap_flag: None,
                poly_staleness_ms: None,
                opi_staleness_ms: None,
                detail_json: self.detail_json_for_state(
                    &state,
                    Some(json!({
                        "exit_chase_count": result.chase_count,
                        "exit_post_only_rejects": result.post_only_rejects,
                        "order_id_missing": true
                    })),
                ),
            };
            self.emit_audit(row).await;
        } else if let Some(ctx) = state.pending_exit_ctx.take() {
            let err_msg = result
                .last_err
                .unwrap_or_else(|| "exit_replace_failed".to_string());
            let row = TradeAuditRow {
                event_ts_ms: now_ms,
                pair_id: pair_key.0,
                token_side: pair_key.1,
                phase: "EXIT_REPLACE_FAILED".to_string(),
                direction: state.direction,
                reason: Some(err_msg),
                pm_move: Some(ctx.pm_move),
                pm_mom: Some(ctx.pm_mom),
                ofi_250ms: Some(ctx.ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: Some(ctx.target),
                exit_price: Some(ctx.exit_price),
                gas_est_usd: Some(ctx.gas_est),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio: ctx.retrace_ratio,
                inv_ratio: ctx.inv_ratio,
                inv_factor: ctx.inv_factor,
                decay_factor: ctx.decay_factor,
                pnl_usd_net: None,
                holding_ms: None,
                mae: state.mae,
                mfe: state.mfe,
                obs_latency_poly_p99_ms: ctx.obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms: ctx.obs_latency_opi_p99_ms,
                bar_gap_flag: ctx.bar_gap_flag,
                poly_bar_gap_flag: ctx.poly_bar_gap_flag,
                opi_bar_gap_flag: ctx.opi_bar_gap_flag,
                poly_staleness_ms: ctx.poly_staleness_ms,
                opi_staleness_ms: ctx.opi_staleness_ms,
                detail_json: self.detail_json_for_state(
                    &state,
                    Some(json!({
                        "exit_chase_count": result.chase_count,
                        "exit_post_only_rejects": result.post_only_rejects
                    })),
                ),
            };
            self.emit_audit(row).await;
        }
        self.states.insert(pair_key, state);
    }

    async fn handle_fill_response(
        &mut self,
        pair_key: (i64, TokenSide),
        req_id: u64,
        kind: FillKind,
        result: FillPollResult,
    ) {
        let mut state = match self.states.remove(&pair_key) {
            Some(state) => state,
            None => return,
        };
        if state.fill_poll_req_id != Some(req_id) {
            self.states.insert(pair_key, state);
            return;
        }
        state.fill_poll_req_id = None;
        self.update_fill_poll_backoff(result.rate_limited, result.error.is_none());
        if result.fills.is_empty() {
            self.states.insert(pair_key, state);
            return;
        }
        let bar = match state.last_bar.clone() {
            Some(bar) => bar,
            None => {
                self.states.insert(pair_key, state);
                return;
            }
        };
        let pm_move = state.last_pm_move.unwrap_or(0.0);
        let pm_mom = state.last_pm_mom.unwrap_or(0.0);
        let ofi_value = state.last_ofi_250ms.unwrap_or(0.0);
        match kind {
            FillKind::Entry => {
                let filled = self
                    .apply_entry_fills(&mut state, &bar, pm_move, pm_mom, ofi_value, result.fills)
                    .await;
                if filled && self.is_live() {
                    let base_pm = state.last_base_pm.or(state.base_pm).unwrap_or(0.0);
                    if base_pm > 0.0 {
                        self.check_exit(&mut state, &bar, pm_move, pm_mom, ofi_value, base_pm, pair_key)
                            .await;
                    }
                }
            }
            FillKind::Exit => {
                let gas_est = self.gas_est_usd();
                self.apply_exit_fills(&mut state, &bar, pm_move, pm_mom, ofi_value, gas_est, result.fills)
                    .await;
            }
        }
        self.states.insert(pair_key, state);
    }

    async fn handle_opinion_order_update(&mut self, update: OpinionUserOrderUpdate) {
        self.last_ws_user_msg_ms = Some(now_ts_ms());
        let pair_key = (update.pair_id, update.token_side);
        let mut state = match self.states.remove(&pair_key) {
            Some(state) => state,
            None => {
                tracing::debug!(
                    "trade ws order_update ignored missing_state pair_id={} token_side={} order_id={}",
                    update.pair_id,
                    update.token_side.as_str(),
                    update.order_id
                );
                return;
            }
        };
        let order_id = update.order_id.clone();
        let matches_entry = state.entry_order_id.as_deref() == Some(order_id.as_str());
        let matches_exit = state.exit_order_id.as_deref() == Some(order_id.as_str());
        if !matches_entry && !matches_exit {
            self.states.insert(pair_key, state);
            return;
        }
        let update_type = update
            .order_update_type
            .as_deref()
            .unwrap_or("")
            .to_ascii_lowercase();
        let status = update.status.unwrap_or(0);
        let is_cancel = update_type == "ordercancel" || matches!(status, 3 | 4 | 5);
        let is_fill_update = update_type == "orderfill" || update_type == "orderconfirm";
        let mut filled_shares = update.filled_shares;
        let filled_amount = update.filled_amount;
        if filled_shares.is_none() && (is_fill_update || status == 2) {
            filled_shares = update.shares;
        }
        if filled_shares.is_none() {
            let amount = filled_amount.or(update.amount);
            if let (Some(price), Some(amount)) = (update.price, amount) {
                if price > 0.0 {
                    filled_shares = Some(amount / price);
                }
            }
        }
        let has_fill = filled_shares.unwrap_or(0.0) > 0.0;

        if is_cancel {
            self.clear_cancel_retry(&order_id);
            if matches_entry {
                state.entry_cancel_pending = false;
                state.entry_cancel_reason = None;
                state.entry_cancel_req_id = None;
                if let Some(bar) = state.last_bar.clone() {
                    let pm_move = state.last_pm_move.unwrap_or(0.0);
                    let pm_mom = state.last_pm_mom.unwrap_or(0.0);
                    let ofi_value = state.last_ofi_250ms.unwrap_or(0.0);
                    self.finalize_entry_cancel(
                        &mut state,
                        &bar,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        "ws_order_cancel",
                        pair_key,
                    )
                    .await;
                } else {
                    let bar_second = state.entry_bar_second.unwrap_or(0);
                    state.enter_cooldown(bar_second, self.cfg.cooldown_after_cancel_ms);
                }
            } else {
                state.exit_order_id = None;
                state.exit_client_id = None;
                state.exit_cancel_pending = false;
                state.exit_cancel_reason = None;
                state.exit_cancel_req_id = None;
            }
            self.states.insert(pair_key, state);
            return;
        }

        if is_fill_update || has_fill {
            let side = update.side.map(|value| value.to_ascii_uppercase());
            let fill = TradeFill {
                order_id: Some(order_id.clone()),
                client_order_id: None,
                side,
                price: update.price,
                size: filled_shares,
                ts_ms: update.ts_ms.or(Some(now_ts_ms())),
            };
            self.apply_ws_fills(pair_key, &mut state, matches_entry, matches_exit, vec![fill], "order_update")
                .await;
        } else if status == 2 {
            if matches_entry {
                self.maybe_enqueue_fill_poll(&mut state, pair_key, FillKind::Entry);
            } else if matches_exit {
                self.maybe_enqueue_fill_poll(&mut state, pair_key, FillKind::Exit);
            }
        }
        self.states.insert(pair_key, state);
    }

    async fn handle_opinion_trade_record(&mut self, record: OpinionUserTradeRecord) {
        self.last_ws_user_msg_ms = Some(now_ts_ms());
        let pair_key = (record.pair_id, record.token_side);
        let mut state = match self.states.remove(&pair_key) {
            Some(state) => state,
            None => {
                tracing::debug!(
                    "trade ws trade_record ignored missing_state pair_id={} token_side={} order_id={}",
                    record.pair_id,
                    record.token_side.as_str(),
                    record.order_id
                );
                return;
            }
        };
        let order_id = record.order_id.clone();
        let matches_entry = state.entry_order_id.as_deref() == Some(order_id.as_str());
        let matches_exit = state.exit_order_id.as_deref() == Some(order_id.as_str());
        if !matches_entry && !matches_exit {
            self.states.insert(pair_key, state);
            return;
        }
        let side = record.side.map(|value| value.to_ascii_uppercase());
        let mut size = record.shares;
        if size.is_none() {
            if let (Some(price), Some(amount)) = (record.price, record.amount) {
                if price > 0.0 {
                    size = Some(amount / price);
                }
            }
        }
        let fill = TradeFill {
            order_id: Some(order_id.clone()),
            client_order_id: None,
            side,
            price: record.price,
            size,
            ts_ms: record.ts_ms.or(Some(now_ts_ms())),
        };
        self.apply_ws_fills(pair_key, &mut state, matches_entry, matches_exit, vec![fill], "trade_record")
            .await;
        self.states.insert(pair_key, state);
    }

    async fn apply_ws_fills(
        &mut self,
        pair_key: (i64, TokenSide),
        state: &mut PairTradeState,
        matches_entry: bool,
        matches_exit: bool,
        fills: Vec<TradeFill>,
        source: &str,
    ) {
        if fills.is_empty() {
            return;
        }
        let bar = match state.last_bar.clone() {
            Some(bar) => bar,
            None => {
                tracing::warn!(
                    "trade ws fill ignored missing_bar pair_id={} token_side={} source={}",
                    pair_key.0,
                    pair_key.1.as_str(),
                    source
                );
                return;
            }
        };
        let pm_move = state.last_pm_move.unwrap_or(0.0);
        let pm_mom = state.last_pm_mom.unwrap_or(0.0);
        let ofi_value = state.last_ofi_250ms.unwrap_or(0.0);
        state.fill_poll_req_id = None;
        state.last_fill_poll_ms = Some(now_ts_ms());
        self.fill_poll_backoff_ms = 0;
        self.fill_poll_backoff_until_ms = None;
        if matches_entry {
            let filled = self
                .apply_entry_fills(state, &bar, pm_move, pm_mom, ofi_value, fills.clone())
                .await;
            if filled && self.is_live() {
                let base_pm = state.last_base_pm.or(state.base_pm).unwrap_or(0.0);
                if base_pm > 0.0 {
                    self.check_exit(state, &bar, pm_move, pm_mom, ofi_value, base_pm, pair_key)
                        .await;
                }
            }
        }
        if matches_exit {
            let gas_est = self.gas_est_usd();
            self.apply_exit_fills(state, &bar, pm_move, pm_mom, ofi_value, gas_est, fills)
                .await;
        }
    }

    fn finish_handle_bar(
        &mut self,
        key: (i64, TokenSide),
        state: PairTradeState,
        bar: &Bars1sPair,
        handle_start: Instant,
        flush_pending_ms: u128,
        check_entry_ms: u128,
        check_exit_ms: u128,
    ) {
        let handle_ms = handle_start.elapsed().as_millis();
        if check_entry_ms > TRADE_CHECK_ENTRY_WARN_MS {
            tracing::warn!(
                "trade_check_entry_slow elapsed_ms={} pair_id={} token_side={} bar_second={} state={:?}",
                check_entry_ms,
                bar.pair_id,
                bar.token_side.as_str(),
                bar.bar_second,
                state.state
            );
        }
        if check_exit_ms > TRADE_CHECK_EXIT_WARN_MS {
            tracing::warn!(
                "trade_check_exit_slow elapsed_ms={} pair_id={} token_side={} bar_second={} state={:?}",
                check_exit_ms,
                bar.pair_id,
                bar.token_side.as_str(),
                bar.bar_second,
                state.state
            );
        }
        if handle_ms > TRADE_HANDLE_WARN_MS {
            tracing::warn!(
                "trade_handle_slow handle_ms={} flush_pending_ms={} check_entry_ms={} check_exit_ms={} pair_id={} token_side={} bar_second={} state={:?} pending_entries={} pending_second={:?} pending_keys={}",
                handle_ms,
                flush_pending_ms,
                check_entry_ms,
                check_exit_ms,
                bar.pair_id,
                bar.token_side.as_str(),
                bar.bar_second,
                state.state,
                self.pending_entries.len(),
                self.pending_second,
                self.pending_keys.len(),
            );
        }
        self.states.insert(key, state);
    }

    fn fill_side_matches(direction: i64, side: &Option<String>) -> bool {
        let want_buy = direction > 0;
        match side.as_deref() {
            Some(raw) if raw.eq_ignore_ascii_case("BUY") => want_buy,
            Some(raw) if raw.eq_ignore_ascii_case("SELL") => !want_buy,
            Some(raw) if raw == "0" => want_buy,
            Some(raw) if raw == "1" => !want_buy,
            _ => false,
        }
    }

    async fn handle_bar(&mut self, bar: Bars1sPair) {
        let handle_start = Instant::now();
        let flush_start = Instant::now();
        self.flush_pending_entries(bar.bar_second).await;
        let flush_pending_ms = flush_start.elapsed().as_millis();
        let key = (bar.pair_id, bar.token_side);
        let bar_second = bar.bar_second;
        let ofi_value = bar.poly_ofi_250ms.unwrap_or(0.0);
        let mut state = self.states.remove(&key).unwrap_or_default();
        state.update_opi_staleness(&bar, OPI_STALE_WINDOW_LEN);
        state.update_gap_ratio(&bar, GAP_WINDOW_LEN);

        if let Some(pm_micro) = bar.poly_micro.or(bar.poly_mid_state).or(bar.poly_mid) {
            let max_len = (self.cfg.alpha_c_lookback_max_s
                .max(self.cfg.alpha_c_lookback_fixed_s)
                .max(1) as usize)
                + 5;
            state.update_pm_micro_history(bar.bar_second, pm_micro, max_len);
        }

        if let Some(pm_mid) = pm_mid_from_bar(&bar) {
            state.base_pm = Some(match state.base_pm {
                Some(base) => base * (1.0 - self.cfg.base_alpha) + pm_mid * self.cfg.base_alpha,
                None => pm_mid,
            });
        }

        if !self.is_bar_fresh(&bar) {
            state.check_cooldown(bar_second);
            self.finish_handle_bar(key, state, &bar, handle_start, flush_pending_ms, 0, 0);
            return;
        }

        let pm_mid = match pm_mid_from_bar(&bar) {
            Some(v) if v > 0.0 => v,
            _ => {
                state.check_cooldown(bar_second);
                self.finish_handle_bar(key, state, &bar, handle_start, flush_pending_ms, 0, 0);
                return;
            }
        };
        let opi_mid_state = opi_mid_state_from_bar(&bar);
        let base_pm = match state.base_pm {
            Some(v) if v > 0.0 => v,
            _ => {
                state.check_cooldown(bar_second);
                self.finish_handle_bar(key, state, &bar, handle_start, flush_pending_ms, 0, 0);
                return;
            }
        };

        let pm_move = (pm_mid - base_pm) / base_pm;
        let pm_mom = match state.last_pm_move {
            Some(prev) => pm_move - prev,
            None => 0.0,
        };
        state.last_bar = Some(bar.clone());
        state.last_pm_move = Some(pm_move);
        state.last_pm_mom = Some(pm_mom);
        state.last_ofi_250ms = Some(ofi_value);
        state.last_base_pm = Some(base_pm);
        if matches!(state.state, TradeState::Idle) {
            let sign = if pm_move >= 0.0 { 1 } else { -1 };
            state.update_peak(pm_move, sign);
        }

        let mut check_entry_ms: u128 = 0;
        let mut check_exit_ms: u128 = 0;
        match state.state {
            TradeState::Idle => {
                let c_signal = self.alpha_c_signal(&state, &bar);
                let pm_range_ratio = self.pm_range_ratio(&state, base_pm);
                let gap_ratio_1m = state.gap_ratio_1m;
                let opi_stale_p90_ms = state.opi_stale_p90_ms;
                let alpha_value = self.alpha_source_value(&bar);
                let mut candidate: Option<PendingEntry> = None;

                if self.cfg.alpha_a_enabled {
                    if let Some(shock) = self.pick_shock_candidate(key, bar.bar_second) {
                        if shock.direction != 0 {
                            if let Some(opi_mid) = opi_mid_state {
                                candidate = self.build_candidate(
                                    &bar,
                                    AlphaType::ShockA,
                                    shock.direction,
                                    pm_move,
                                    pm_mom,
                                    ofi_value,
                                    base_pm,
                                    pm_mid,
                                    opi_mid,
                                    alpha_value,
                                    Some(shock),
                                    c_signal,
                                    state.opi_stale_p50_ms,
                                    state.pm_peak,
                                    pm_range_ratio,
                                    gap_ratio_1m,
                                    opi_stale_p90_ms,
                                );
                            } else {
                                let snapshot_sent = self.maybe_trigger_opi_snapshot(
                                    bar.pair_id,
                                    bar.token_side,
                                    "opi_mid_missing",
                                );
                                let reason = if snapshot_sent {
                                    "opi_mid_missing_snapshot"
                                } else {
                                    "opi_mid_missing"
                                };
                                self.log_blocked_signal(
                                    &bar,
                                    AlphaType::ShockA,
                                    reason,
                                    shock.direction,
                                    pm_move,
                                    pm_mom,
                                    ofi_value,
                                    Some(if snapshot_sent { 1.0 } else { 0.0 }),
                                    None,
                                    None,
                                    None,
                                    bar.opi_staleness_ms,
                                    opi_stale_p90_ms,
                                    gap_ratio_1m,
                                    pm_range_ratio,
                                    Some(shock.shock_type.as_str()),
                                );
                            }
                        }
                    }
                }

                if candidate.is_none() && self.cfg.alpha_b_enabled {
                    let move_ok = pm_move.abs() >= self.cfg.alpha_b_move_min
                        && pm_mom.abs() >= self.cfg.alpha_b_mom_min;
                    let ofi_ok = ofi_value.abs() >= self.cfg.alpha_b_ofi_entry_min;
                    let mut direction = if move_ok {
                        if pm_move >= 0.0 { 1 } else { -1 }
                    } else if ofi_ok {
                        if ofi_value >= 0.0 { 1 } else { -1 }
                    } else {
                        0
                    };
                    if direction == 0 && self.alpha_source_is_book_alpha() {
                        if let Some(value) = alpha_value {
                            if self.cfg.alpha_min <= 0.0 || value.abs() >= self.cfg.alpha_min {
                                direction = if value >= 0.0 { 1 } else { -1 };
                            }
                        }
                    }
                    if direction != 0 {
                        if let Some(opi_mid) = opi_mid_state {
                            candidate = self.build_candidate(
                                &bar,
                                AlphaType::GapB,
                                direction,
                                pm_move,
                                pm_mom,
                                ofi_value,
                                base_pm,
                                pm_mid,
                                opi_mid,
                                alpha_value,
                                None,
                                c_signal,
                                state.opi_stale_p50_ms,
                                state.pm_peak,
                                pm_range_ratio,
                                gap_ratio_1m,
                                opi_stale_p90_ms,
                            );
                        } else {
                            self.log_blocked_signal(
                                &bar,
                                AlphaType::GapB,
                                "opi_mid_missing",
                                direction,
                                pm_move,
                                pm_mom,
                                ofi_value,
                                None,
                                None,
                                None,
                                None,
                                bar.opi_staleness_ms,
                                opi_stale_p90_ms,
                                gap_ratio_1m,
                                pm_range_ratio,
                                None,
                            );
                        }
                    }
                }

                if let Some(entry) = candidate {
                    self.log_alpha("ENTRY_SIGNAL", &bar, &state, Some(&entry));
                    self.queue_entry(entry);
                }
            }
            TradeState::EntryReady | TradeState::EntryPlacing | TradeState::EntryWorking => {
                state.update_peak(pm_move, state.direction);
                let check_start = Instant::now();
                self.check_entry(&mut state, &bar, pm_move, pm_mom, ofi_value, key)
                    .await;
                check_entry_ms = check_start.elapsed().as_millis();
            }
            TradeState::PositionOpen | TradeState::ExitWorking => {
                state.update_peak(pm_move, state.direction);
                let check_start = Instant::now();
                self.check_exit(&mut state, &bar, pm_move, pm_mom, ofi_value, base_pm, key)
                    .await;
                check_exit_ms = check_start.elapsed().as_millis();
            }
            TradeState::Cooldown => {
                state.check_cooldown(bar_second);
            }
        }

        state.update_opi_snapshot(&bar);
        if self.cfg.alpha_log_enabled
            && self.cfg.alpha_log_interval_sec > 0
            && bar.bar_second % self.cfg.alpha_log_interval_sec == 0
            && matches!(
                state.state,
                TradeState::EntryWorking | TradeState::ExitWorking | TradeState::PositionOpen
            )
        {
            self.log_alpha("MINUTE", &bar, &state, state.pending_entry.as_ref());
        }
        self.finish_handle_bar(
            key,
            state,
            &bar,
            handle_start,
            flush_pending_ms,
            check_entry_ms,
            check_exit_ms,
        );
    }

    async fn cancel_check_tick(&mut self) {
        let tick_start = Instant::now();
        let states_len = self.states.len();
        let mut working_states = 0usize;
        let keys: Vec<(i64, TokenSide)> = self.states.keys().copied().collect();
        for key in keys {
            let mut state = match self.states.remove(&key) {
                Some(state) => state,
                None => continue,
            };
            if !matches!(state.state, TradeState::EntryWorking | TradeState::ExitWorking) {
                self.states.insert(key, state);
                continue;
            }
            working_states += 1;
            let mut bar = match state.last_bar.take() {
                Some(bar) => bar,
                None => {
                    self.states.insert(key, state);
                    continue;
                }
            };
            let pm_move = state.last_pm_move.unwrap_or(0.0);
            let pm_mom = state.last_pm_mom.unwrap_or(0.0);
            let ofi_value = state.last_ofi_250ms.unwrap_or(0.0);
            if matches!(state.state, TradeState::EntryWorking) {
                self.check_entry(&mut state, &bar, pm_move, pm_mom, ofi_value, key)
                    .await;
            } else if matches!(state.state, TradeState::ExitWorking) {
                let base_pm = state
                    .last_base_pm
                    .or(state.base_pm)
                    .unwrap_or(0.0);
                if base_pm > 0.0 {
                    self.check_exit(&mut state, &bar, pm_move, pm_mom, ofi_value, base_pm, key)
                        .await;
                }
            }
            state.last_bar = Some(bar);
            self.states.insert(key, state);
        }
        let tick_ms = tick_start.elapsed().as_millis();
        if tick_ms > TRADE_CANCEL_TICK_WARN_MS {
            tracing::warn!(
                "trade_cancel_check_slow handle_ms={} states={} working_states={} pending_entries={} pending_second={:?}",
                tick_ms,
                states_len,
                working_states,
                self.pending_entries.len(),
                self.pending_second
            );
        }
    }

    fn is_bar_fresh(&self, bar: &Bars1sPair) -> bool {
        let poly_ok = bar
            .poly_staleness_ms
            .map(|s| s <= self.cfg.signal_staleness_ms)
            .unwrap_or(false);
        let max_opi_stale = self
            .cfg
            .alpha_a_opi_stale_max_ms
            .max(self.cfg.alpha_b_opi_stale_max_ms)
            .max(self.cfg.signal_staleness_ms);
        let opi_ok = bar
            .opi_staleness_ms
            .map(|s| s <= max_opi_stale)
            .unwrap_or(false);
        poly_ok && opi_ok
    }

    fn derive_opi_signals(&self, state: &PairTradeState, bar: &Bars1sPair) -> OpiDerived {
        let mid = opi_mid_from_bar(bar);
        let spread_bps = opi_spread_bps_from_bar(bar, mid);
        let bid_l1 = bar.opi_bid_l1_notional;
        let ask_l1 = bar.opi_ask_l1_notional;
        let bid_l3 = bid_l1.map(|v| v + bar.opi_bid_l2_notional.unwrap_or(0.0) + bar.opi_bid_l3_notional.unwrap_or(0.0));
        let ask_l3 = ask_l1.map(|v| v + bar.opi_ask_l2_notional.unwrap_or(0.0) + bar.opi_ask_l3_notional.unwrap_or(0.0));

        let delta_ok = bar.bar_gap_flag == 0
            && bar.opi_bar_gap_flag == 0
            && state.last_opi_bar_second == Some(bar.bar_second.saturating_sub(1));

        let mid_ret_bps_1s = if delta_ok {
            match (state.last_opi_mid, mid) {
                (Some(prev), Some(cur)) if prev > 0.0 => Some((cur - prev) / prev * 10_000.0),
                _ => None,
            }
        } else {
            None
        };

        let mid_ret_abs_bps_1s = mid_ret_bps_1s.map(|v| v.abs());

        let bid_delta = if delta_ok {
            match (state.last_opi_bid_l3, bid_l3) {
                (Some(prev), Some(cur)) => Some(cur - prev),
                _ => None,
            }
        } else {
            None
        };

        let ask_delta = if delta_ok {
            match (state.last_opi_ask_l3, ask_l3) {
                (Some(prev), Some(cur)) => Some(cur - prev),
                _ => None,
            }
        } else {
            None
        };

        let imbalance_l1 = match (bid_l1, ask_l1) {
            (Some(bid), Some(ask)) if bid + ask > 0.0 => Some((bid - ask) / (bid + ask)),
            _ => None,
        };
        let imbalance_l3 = match (bid_l3, ask_l3) {
            (Some(bid), Some(ask)) if bid + ask > 0.0 => Some((bid - ask) / (bid + ask)),
            _ => None,
        };

        let taker_buy_notional_1s = bar.opi_taker_buy_notional_1s;
        let taker_sell_notional_1s = bar.opi_taker_sell_notional_1s;
        let trade_imbalance_1s = match (taker_buy_notional_1s, taker_sell_notional_1s) {
            (Some(buy), Some(sell)) if buy + sell > 0.0 => Some((buy - sell) / (buy + sell)),
            _ => None,
        };

        OpiDerived {
            mid,
            spread_bps,
            mid_ret_bps_1s,
            mid_ret_abs_bps_1s,
            bid_l1,
            ask_l1,
            bid_l3,
            ask_l3,
            bid_delta,
            ask_delta,
            imbalance_l1,
            imbalance_l3,
            taker_buy_notional_1s,
            taker_sell_notional_1s,
            trade_imbalance_1s,
            order_edge_bps: None,
        }
    }

    fn compute_edge(&self, pm_mid: f64, opi_mid: f64, direction: i64, base_pm: f64) -> (f64, f64) {
        let raw = direction as f64 * (pm_mid - opi_mid);
        let norm = if base_pm > 0.0 { raw / base_pm } else { 0.0 };
        (raw, norm)
    }

    fn opi_edge_band_px(&self, bar: &Bars1sPair) -> f64 {
        let tick = if self.cfg.tick_px > 0.0 { self.cfg.tick_px } else { 1e-4 };
        let spread = match (opi_best_bid_from_bar(bar), opi_best_ask_from_bar(bar)) {
            (Some(bid), Some(ask)) if ask > bid && bid > 0.0 => ask - bid,
            _ => 0.0,
        };
        spread + 2.0 * tick.max(0.0)
    }

    fn pm_range_ratio(&self, state: &PairTradeState, base_pm: f64) -> Option<f64> {
        if base_pm <= 0.0 {
            return None;
        }
        if state.pm_micro_history.len() < 10 {
            return None;
        }
        let mut min_val = f64::INFINITY;
        let mut max_val = f64::NEG_INFINITY;
        for (_, price) in state.pm_micro_history.iter() {
            if *price > 0.0 {
                if *price < min_val {
                    min_val = *price;
                }
                if *price > max_val {
                    max_val = *price;
                }
            }
        }
        if !min_val.is_finite() || !max_val.is_finite() || max_val <= min_val {
            return None;
        }
        Some((max_val - min_val) / base_pm)
    }

    fn alpha_stale_max_ms(&self, alpha_type: AlphaType, opi_p50_ms: Option<i64>) -> i64 {
        let base = match alpha_type {
            AlphaType::ShockA => self.cfg.alpha_a_opi_stale_max_ms,
            AlphaType::GapB => self.cfg.alpha_b_opi_stale_max_ms,
            AlphaType::LagC => self.cfg.alpha_b_opi_stale_max_ms,
        };
        if alpha_type == AlphaType::ShockA {
            return base;
        }
        let (use_pair, plus_ms) = match alpha_type {
            AlphaType::ShockA => (
                self.cfg.alpha_a_use_pair_quantile,
                self.cfg.alpha_a_pair_p50_plus_ms,
            ),
            AlphaType::GapB | AlphaType::LagC => (
                self.cfg.alpha_b_use_pair_quantile,
                self.cfg.alpha_b_pair_p50_plus_ms,
            ),
        };
        if use_pair {
            if let Some(p50) = opi_p50_ms {
                let gate = p50.saturating_add(plus_ms.max(0));
                return base.min(gate);
            }
        }
        base
    }

    fn alpha_edge_norm_min(&self, alpha_type: AlphaType) -> f64 {
        match alpha_type {
            AlphaType::ShockA => self.cfg.alpha_a_edge_norm_min,
            AlphaType::GapB => self.cfg.alpha_b_edge_norm_min,
            AlphaType::LagC => self.cfg.alpha_b_edge_norm_min,
        }
    }

    fn alpha_edge_raw_min_abs(&self, alpha_type: AlphaType) -> f64 {
        match alpha_type {
            AlphaType::ShockA => self.cfg.alpha_a_edge_raw_min_abs,
            _ => 0.0,
        }
    }

    fn alpha_cost_mult(&self, alpha_type: AlphaType) -> f64 {
        match alpha_type {
            AlphaType::ShockA => self.cfg.alpha_a_cost_mult.max(1.0),
            AlphaType::GapB => self.cfg.alpha_b_cost_mult.max(1.0),
            AlphaType::LagC => 1.0,
        }
    }

    fn alpha_decay_per_sec(&self, alpha_type: AlphaType) -> f64 {
        match alpha_type {
            AlphaType::ShockA => self.cfg.alpha_a_decay_per_sec,
            AlphaType::GapB => self.cfg.alpha_b_decay_per_sec,
            AlphaType::LagC => self.cfg.target_decay_per_sec,
        }
    }

    fn entry_decay_factor(&self, state: &PairTradeState) -> Option<f64> {
        let alpha_type = state.alpha_type?;
        let entry_ts_ms = state.entry_ts_ms?;
        let holding_sec = now_ts_ms().saturating_sub(entry_ts_ms) as f64 / 1000.0;
        let decay_per_sec = self.alpha_decay_per_sec(alpha_type);
        Some((1.0 - decay_per_sec * holding_sec).max(0.1))
    }

    fn strategy_cap(&self, alpha_type: AlphaType) -> f64 {
        match alpha_type {
            AlphaType::ShockA => self.cfg.strategy_cap_a,
            AlphaType::GapB => self.cfg.strategy_cap_b,
            AlphaType::LagC => self.cfg.strategy_cap_c,
        }
    }

    fn per_trade_cap(&self, alpha_type: AlphaType) -> f64 {
        match alpha_type {
            AlphaType::ShockA => self.cfg.per_trade_cap_a,
            AlphaType::GapB => self.cfg.per_trade_cap_b,
            AlphaType::LagC => self.cfg.per_trade_cap_c,
        }
    }

    fn compute_score(
        &self,
        edge_norm: f64,
        driver_strength: f64,
        opi_staleness_ms: i64,
        cost_mult: f64,
        book_scale: f64,
    ) -> f64 {
        let stale_s = (opi_staleness_ms.max(0) as f64) / 1000.0;
        let edge_adj = edge_norm.abs() / cost_mult.max(1.0);
        let book_penalty = (1.0 - book_scale).max(0.0);
        self.cfg.rank_w_edge * edge_adj
            + self.cfg.rank_w_driver * driver_strength
            - self.cfg.rank_w_stale * stale_s
            - self.cfg.rank_w_cost * cost_mult
            - self.cfg.rank_w_bookshape * book_penalty
    }

    fn shock_allowed(&self, shock: &ShockSignal) -> bool {
        if self.cfg.alpha_a_require_noise_zero && shock.noise_flag != 0 {
            return false;
        }
        if !self.cfg.alpha_a_allowed_shock_types.is_empty()
            && !self
                .cfg
                .alpha_a_allowed_shock_types
                .iter()
                .any(|t| t == &shock.shock_type)
        {
            return false;
        }
        true
    }

    fn pick_shock_candidate(
        &self,
        key: (i64, TokenSide),
        bar_second: i64,
    ) -> Option<ShockSignal> {
        let list = self.shock_signals.get(&key)?;
        let max_age = self.cfg.alpha_a_max_age_s.max(0);
        list.iter()
            .rev()
            .find(|s| bar_second.saturating_sub(s.trigger_bar_second) <= max_age && self.shock_allowed(s))
            .cloned()
    }

    fn reverse_shock_active(
        &self,
        key: (i64, TokenSide),
        bar_second: i64,
        direction: i64,
    ) -> bool {
        let list = match self.shock_signals.get(&key) {
            Some(list) => list,
            None => return false,
        };
        let max_age = self.cfg.alpha_a_max_age_s.max(0);
        list.iter().rev().any(|s| {
            bar_second.saturating_sub(s.trigger_bar_second) <= max_age
                && self.shock_allowed(s)
                && s.direction == -direction
        })
    }

    fn alpha_c_signal(&self, state: &PairTradeState, bar: &Bars1sPair) -> Option<(i64, f64, f64)> {
        if !self.cfg.alpha_c_enabled {
            return None;
        }
        let lookback_s = if self.cfg.alpha_c_use_lag_clamp {
            if let Some(lag) = bar.estimated_lag_s {
                lag.clamp(self.cfg.alpha_c_lookback_min_s, self.cfg.alpha_c_lookback_max_s)
            } else {
                self.cfg.alpha_c_lookback_fixed_s
            }
        } else {
            self.cfg.alpha_c_lookback_fixed_s
        };
        let target_second = bar.bar_second.saturating_sub(lookback_s.max(1));
        let latest = state.pm_micro_history.back().copied();
        let prior = state
            .pm_micro_history
            .iter()
            .rev()
            .find(|(sec, _)| *sec <= target_second)
            .copied();
        let (cur_sec, cur_price) = latest?;
        if cur_sec < target_second {
            return None;
        }
        let (_, prev_price) = prior?;
        if prev_price <= 0.0 || cur_price <= 0.0 {
            return None;
        }
        let ret = (cur_price / prev_price).ln();
        if ret.abs() < self.cfg.alpha_c_min_abs_return {
            return None;
        }
        let pred_dir = if ret >= 0.0 { 1 } else { -1 };
        Some((pred_dir, ret, lookback_s as f64))
    }

    fn horizon_for(&self, bar: &Bars1sPair, alpha_type: AlphaType, shock: Option<&ShockSignal>) -> i64 {
        let default = match alpha_type {
            AlphaType::ShockA => self.cfg.alpha_a_horizon_default_s,
            AlphaType::GapB => self.cfg.alpha_b_horizon_default_s,
            AlphaType::LagC => self.cfg.alpha_b_horizon_default_s,
        };
        if alpha_type == AlphaType::ShockA {
            if let Some(shock) = shock {
                if let Some(value) = self.cfg.alpha_a_horizon_by_shock_type.get(&shock.shock_type) {
                    return (*value).clamp(30, 120);
                }
            }
        }
        let horizon = if let Some(lag) = bar.estimated_lag_s {
            lag.clamp(self.cfg.alpha_c_lookback_min_s, self.cfg.alpha_c_lookback_max_s)
        } else {
            default
        };
        if alpha_type == AlphaType::ShockA {
            horizon.clamp(30, 120)
        } else {
            horizon
        }
    }

    fn build_candidate(
        &mut self,
        bar: &Bars1sPair,
        alpha_type: AlphaType,
        direction: i64,
        pm_move: f64,
        pm_mom: f64,
        ofi_value: f64,
        base_pm: f64,
        pm_mid: f64,
        opi_mid: f64,
        alpha_value: Option<f64>,
        shock: Option<ShockSignal>,
        c_signal: Option<(i64, f64, f64)>,
        opi_stale_p50_ms: Option<i64>,
        pm_peak: Option<f64>,
        pm_range_ratio: Option<f64>,
        gap_ratio_1m: Option<f64>,
        opi_stale_p90_ms: Option<i64>,
    ) -> Option<PendingEntry> {
        if direction == 0 {
            return None;
        }
        if let Some(value) = alpha_value {
            if self.cfg.alpha_min > 0.0 && value.abs() < self.cfg.alpha_min {
                self.log_blocked_signal(
                    bar,
                    alpha_type,
                    "alpha_min",
                    direction,
                    pm_move,
                    pm_mom,
                    ofi_value,
                    Some(value),
                    Some(self.cfg.alpha_min),
                    None,
                    None,
                    bar.opi_staleness_ms,
                    opi_stale_p90_ms,
                    gap_ratio_1m,
                    pm_range_ratio,
                    shock.as_ref().map(|s| s.shock_type.as_str()),
                );
                return None;
            }
        }
        let corr_samples = bar.rolling_corr_samples_5m.unwrap_or(0).max(0) as usize;
        let corr_samples_ok = self.cfg.follow_corr_min_samples == 0
            || corr_samples >= self.cfg.follow_corr_min_samples;
        let corr_range_ok = self.cfg.follow_corr_min_range <= 0.0
            || pm_range_ratio.unwrap_or(0.0) >= self.cfg.follow_corr_min_range;
        let corr_ready = corr_samples_ok && corr_range_ok;
        if self.cfg.follow_min_corr > 0.0 {
            if !corr_ready {
                if alpha_type == AlphaType::GapB {
                    self.log_blocked_signal(
                        bar,
                        alpha_type,
                        "corr_insufficient",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        Some(corr_samples as f64),
                        Some(self.cfg.follow_corr_min_samples as f64),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                    return None;
                }
                if alpha_type == AlphaType::ShockA {
                    self.log_gate_relaxed(
                        bar,
                        alpha_type,
                        "corr_insufficient",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        Some(corr_samples as f64),
                        Some(self.cfg.follow_corr_min_samples as f64),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                }
            } else if bar.rolling_corr_5m.unwrap_or(-1.0) < self.cfg.follow_min_corr {
                self.log_blocked_signal(
                    bar,
                    alpha_type,
                    "follow_min_corr",
                    direction,
                    pm_move,
                    pm_mom,
                    ofi_value,
                    bar.rolling_corr_5m,
                    Some(self.cfg.follow_min_corr),
                    None,
                    None,
                    bar.opi_staleness_ms,
                    opi_stale_p90_ms,
                    gap_ratio_1m,
                    pm_range_ratio,
                    shock.as_ref().map(|s| s.shock_type.as_str()),
                );
                return None;
            }
        }
        if self.cfg.follow_min_lag_conf > 0.0 {
            if bar.lag_confidence.unwrap_or(0.0) < self.cfg.follow_min_lag_conf {
                if alpha_type == AlphaType::GapB {
                    self.log_blocked_signal(
                        bar,
                        alpha_type,
                        "follow_min_lag_conf",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        bar.lag_confidence,
                        Some(self.cfg.follow_min_lag_conf),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                    return None;
                } else {
                    self.log_gate_relaxed(
                        bar,
                        alpha_type,
                        "lag_conf_insufficient",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        bar.lag_confidence,
                        Some(self.cfg.follow_min_lag_conf),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                }
            }
        }
        if self.cfg.follow_min_range > 0.0 {
            if !corr_ready {
                let (value, threshold) = if !corr_samples_ok {
                    (
                        Some(corr_samples as f64),
                        Some(self.cfg.follow_corr_min_samples as f64),
                    )
                } else {
                    (pm_range_ratio, Some(self.cfg.follow_corr_min_range))
                };
                if alpha_type == AlphaType::GapB {
                    self.log_blocked_signal(
                        bar,
                        alpha_type,
                        "range_insufficient",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        value,
                        threshold,
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                    return None;
                } else {
                    self.log_gate_relaxed(
                        bar,
                        alpha_type,
                        "range_insufficient",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        value,
                        threshold,
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                }
            } else if pm_range_ratio.unwrap_or(0.0) < self.cfg.follow_min_range {
                if alpha_type == AlphaType::GapB {
                    self.log_blocked_signal(
                        bar,
                        alpha_type,
                        "follow_min_range",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        pm_range_ratio,
                        Some(self.cfg.follow_min_range),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                    return None;
                } else {
                    self.log_gate_relaxed(
                        bar,
                        alpha_type,
                        "range_insufficient",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        pm_range_ratio,
                        Some(self.cfg.follow_min_range),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                }
            }
        }
        if self.cfg.max_bar_gap_ratio_1m > 0.0 {
            if let Some(ratio) = gap_ratio_1m {
                if ratio > self.cfg.max_bar_gap_ratio_1m {
                    self.log_blocked_signal(
                        bar,
                        alpha_type,
                        "bar_gap_ratio_1m",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        Some(ratio),
                        Some(self.cfg.max_bar_gap_ratio_1m),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                    return None;
                }
            }
        }
        if self.cfg.max_opi_staleness_p90_ms > 0 {
            if let Some(p90) = opi_stale_p90_ms {
                if p90 > self.cfg.max_opi_staleness_p90_ms {
                    self.log_blocked_signal(
                        bar,
                        alpha_type,
                        "opi_stale_p90",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        Some(p90 as f64),
                        Some(self.cfg.max_opi_staleness_p90_ms as f64),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                    return None;
                }
            }
        }
        if self.cfg.max_poly_latency_p99_ms > 0 {
            if let Some(lat) = bar.poly_obs_latency_p99_ms {
                if lat > self.cfg.max_poly_latency_p99_ms {
                    self.log_blocked_signal(
                        bar,
                        alpha_type,
                        "poly_latency_p99",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        Some(lat as f64),
                        Some(self.cfg.max_poly_latency_p99_ms as f64),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                    return None;
                }
            }
        }
        if self.cfg.max_opi_latency_p99_ms > 0 {
            if let Some(lat) = bar.opi_obs_latency_p99_ms {
                if lat > self.cfg.max_opi_latency_p99_ms {
                    self.log_blocked_signal(
                        bar,
                        alpha_type,
                        "opi_latency_p99",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        Some(lat as f64),
                        Some(self.cfg.max_opi_latency_p99_ms as f64),
                        None,
                        None,
                        bar.opi_staleness_ms,
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                    return None;
                }
            }
        }
        let opi_stale = match bar.opi_staleness_ms {
            Some(value) => value,
            None => {
                self.log_blocked_signal(
                    bar,
                    alpha_type,
                    "opi_stale_missing",
                    direction,
                    pm_move,
                    pm_mom,
                    ofi_value,
                    None,
                    None,
                    None,
                    None,
                    None,
                    opi_stale_p90_ms,
                    gap_ratio_1m,
                    pm_range_ratio,
                    shock.as_ref().map(|s| s.shock_type.as_str()),
                );
                return None;
            }
        };
        let stale_limit = self.alpha_stale_max_ms(alpha_type, opi_stale_p50_ms);
        if opi_stale > stale_limit {
            self.log_blocked_signal(
                bar,
                alpha_type,
                "opi_stale_gate",
                direction,
                pm_move,
                pm_mom,
                ofi_value,
                Some(opi_stale as f64),
                Some(stale_limit as f64),
                None,
                None,
                Some(opi_stale),
                opi_stale_p90_ms,
                gap_ratio_1m,
                pm_range_ratio,
                shock.as_ref().map(|s| s.shock_type.as_str()),
            );
            return None;
        }
        let mut stale_scale = 1.0;
        let mut entry_ttl_override_ms = None;
        if alpha_type == AlphaType::ShockA {
            let mut soft_start_ms = self.cfg.alpha_a_opi_stale_soft_start_ms.max(0);
            if self.cfg.alpha_a_use_pair_quantile {
                if let Some(p50) = opi_stale_p50_ms {
                    if p50 > 0 {
                        soft_start_ms = p50;
                    }
                }
            }
            soft_start_ms = soft_start_ms.min(stale_limit.max(0));
            let tau_ms = self.cfg.alpha_a_opi_stale_soft_tau_ms.max(0);
            if soft_start_ms > 0 && tau_ms > 0 && opi_stale > soft_start_ms {
                let delta = (opi_stale - soft_start_ms) as f64;
                stale_scale = (-delta / (tau_ms as f64)).exp();
                stale_scale = stale_scale.clamp(self.cfg.alpha_a_opi_stale_soft_min_scale, 1.0);
                self.log_gate_relaxed(
                    bar,
                    alpha_type,
                    "opi_stale_soft",
                    direction,
                    pm_move,
                    pm_mom,
                    ofi_value,
                    Some(opi_stale as f64),
                    Some(soft_start_ms as f64),
                    None,
                    None,
                    Some(opi_stale),
                    opi_stale_p90_ms,
                    gap_ratio_1m,
                    pm_range_ratio,
                    shock.as_ref().map(|s| s.shock_type.as_str()),
                );
            }
        }
        let (edge_raw, edge_norm) = self.compute_edge(pm_mid, opi_mid, direction, base_pm);
        if alpha_type == AlphaType::ShockA {
            let edge_band = self.opi_edge_band_px(bar);
            if edge_raw < -edge_band {
                self.log_blocked_signal(
                    bar,
                    alpha_type,
                    "edge_raw_band",
                    direction,
                    pm_move,
                    pm_mom,
                    ofi_value,
                    Some(edge_raw),
                    Some(-edge_band),
                    Some(edge_raw),
                    Some(edge_norm),
                    Some(opi_stale),
                    opi_stale_p90_ms,
                    gap_ratio_1m,
                    pm_range_ratio,
                    shock.as_ref().map(|s| s.shock_type.as_str()),
                );
                return None;
            }
        } else if edge_raw <= 0.0 {
            self.log_blocked_signal(
                bar,
                alpha_type,
                "edge_raw_nonpositive",
                direction,
                pm_move,
                pm_mom,
                ofi_value,
                Some(edge_raw),
                None,
                Some(edge_raw),
                Some(edge_norm),
                Some(opi_stale),
                opi_stale_p90_ms,
                gap_ratio_1m,
                pm_range_ratio,
                shock.as_ref().map(|s| s.shock_type.as_str()),
            );
            return None;
        }
        if alpha_type != AlphaType::ShockA && edge_norm < self.alpha_edge_norm_min(alpha_type) {
            self.log_blocked_signal(
                bar,
                alpha_type,
                "edge_norm_min",
                direction,
                pm_move,
                pm_mom,
                ofi_value,
                Some(edge_norm),
                Some(self.alpha_edge_norm_min(alpha_type)),
                Some(edge_raw),
                Some(edge_norm),
                Some(opi_stale),
                opi_stale_p90_ms,
                gap_ratio_1m,
                pm_range_ratio,
                shock.as_ref().map(|s| s.shock_type.as_str()),
            );
            return None;
        }
        if alpha_type != AlphaType::ShockA {
            let raw_abs_min = self.alpha_edge_raw_min_abs(alpha_type);
            if raw_abs_min > 0.0 && edge_raw.abs() < raw_abs_min {
                self.log_blocked_signal(
                    bar,
                    alpha_type,
                    "edge_raw_abs_min",
                    direction,
                    pm_move,
                    pm_mom,
                    ofi_value,
                    Some(edge_raw.abs()),
                    Some(raw_abs_min),
                    Some(edge_raw),
                    Some(edge_norm),
                    Some(opi_stale),
                    opi_stale_p90_ms,
                    gap_ratio_1m,
                    pm_range_ratio,
                    shock.as_ref().map(|s| s.shock_type.as_str()),
                );
                return None;
            }
        }
        if alpha_type == AlphaType::GapB {
            if self.cfg.alpha_b_retrace_cancel_ratio > 0.0 {
                if let Some(ratio) = self.retrace_ratio(pm_move, pm_peak, direction) {
                    if ratio >= self.cfg.alpha_b_retrace_cancel_ratio {
                        self.log_blocked_signal(
                            bar,
                            alpha_type,
                            "gapb_retrace_cancel",
                            direction,
                            pm_move,
                            pm_mom,
                            ofi_value,
                            Some(ratio),
                            Some(self.cfg.alpha_b_retrace_cancel_ratio),
                            Some(edge_raw),
                            Some(edge_norm),
                            Some(opi_stale),
                            opi_stale_p90_ms,
                            gap_ratio_1m,
                            pm_range_ratio,
                            shock.as_ref().map(|s| s.shock_type.as_str()),
                        );
                        return None;
                    }
                }
            }
            let ofi_flip = self.cfg.alpha_b_ofi_flip_cancel_usd;
            if ofi_flip > 0.0 {
                if (direction > 0 && ofi_value <= -ofi_flip)
                    || (direction < 0 && ofi_value >= ofi_flip)
                {
                    self.log_blocked_signal(
                        bar,
                        alpha_type,
                        "gapb_ofi_flip",
                        direction,
                        pm_move,
                        pm_mom,
                        ofi_value,
                        Some(ofi_value),
                        Some(ofi_flip),
                        Some(edge_raw),
                        Some(edge_norm),
                        Some(opi_stale),
                        opi_stale_p90_ms,
                        gap_ratio_1m,
                        pm_range_ratio,
                        shock.as_ref().map(|s| s.shock_type.as_str()),
                    );
                    return None;
                }
            }
        }

        let driver_strength = match alpha_type {
            AlphaType::ShockA => shock.as_ref().map(|s| s.magnitude.abs()).unwrap_or(0.0),
            AlphaType::GapB => {
                pm_move.abs()
                    + pm_mom.abs()
                    + (ofi_value.abs() / self.cfg.rank_ofi_scale.max(1.0))
            }
            AlphaType::LagC => pm_move.abs(),
        };
        let cost_mult = self.alpha_cost_mult(alpha_type);
        let book_scale_score = self
            .book_shape(bar, direction)
            .map(|shape| shape.scale)
            .unwrap_or(1.0);
        let mut score =
            self.compute_score(edge_norm, driver_strength, opi_stale, cost_mult, book_scale_score);
        let mut conf_score = score;
        let mut c_pred_dir = None;
        let mut c_ret = None;
        let mut c_modifier = None;

        if let Some((pred_dir, ret, _lookback)) = c_signal {
            let modifier = if pred_dir == direction {
                self.cfg.alpha_c_align_bonus
            } else {
                -self.cfg.alpha_c_conflict_penalty
            };
            conf_score += modifier;
            score += modifier;
            c_pred_dir = Some(pred_dir);
            c_ret = Some(ret);
            c_modifier = Some(modifier);
            if alpha_type == AlphaType::GapB
                && pred_dir != direction
                && self.cfg.alpha_c_block_b_on_conflict
            {
                self.log_blocked_signal(
                    bar,
                    alpha_type,
                    "gapb_c_conflict",
                    direction,
                    pm_move,
                    pm_mom,
                    ofi_value,
                    Some(pred_dir as f64),
                    None,
                    Some(edge_raw),
                    Some(edge_norm),
                    Some(opi_stale),
                    opi_stale_p90_ms,
                    gap_ratio_1m,
                    pm_range_ratio,
                    shock.as_ref().map(|s| s.shock_type.as_str()),
                );
                return None;
            }
        }

        let horizon_s = Some(self.horizon_for(bar, alpha_type, shock.as_ref()));
        if let Some(horizon) = horizon_s {
            let horizon_ms = (horizon as i64).saturating_mul(1000);
            let (ttl_frac, ttl_cap_ms) = match alpha_type {
                AlphaType::ShockA => (
                    self.cfg.alpha_a_entry_ttl_frac,
                    self.cfg.alpha_a_entry_ttl_cap_ms,
                ),
                AlphaType::GapB => (
                    self.cfg.alpha_b_entry_ttl_frac,
                    self.cfg.alpha_b_entry_ttl_cap_ms,
                ),
                AlphaType::LagC => (0.0, 0),
            };
            if ttl_frac > 0.0 && horizon_ms > 0 {
                let mut ttl_ms = (horizon_ms as f64 * ttl_frac).round() as i64;
                if ttl_cap_ms > 0 {
                    ttl_ms = ttl_ms.min(ttl_cap_ms);
                }
                if alpha_type == AlphaType::ShockA && stale_scale < 1.0 {
                    ttl_ms = ((ttl_ms as f64) * stale_scale).round() as i64;
                }
                if ttl_ms > 0 {
                    entry_ttl_override_ms = Some(ttl_ms);
                }
            }
        }
        let alpha_source = match alpha_type {
            AlphaType::ShockA => shock
                .as_ref()
                .map(|s| format!("A_SHOCK:{}", s.shock_type))
                .unwrap_or_else(|| "A_SHOCK".to_string()),
            AlphaType::GapB => {
                let move_ok = pm_move.abs() >= self.cfg.alpha_b_move_min
                    && pm_mom.abs() >= self.cfg.alpha_b_mom_min;
                let ofi_ok = ofi_value.abs() >= self.cfg.alpha_b_ofi_entry_min;
                if move_ok {
                    "B_GAP:MOVE_MOM".to_string()
                } else if ofi_ok {
                    "B_GAP:OFI_ONLY".to_string()
                } else {
                    "B_GAP".to_string()
                }
            }
            AlphaType::LagC => "C_LAG_MOD".to_string(),
        };
        Some(PendingEntry {
            key: (bar.pair_id, bar.token_side),
            bar: bar.clone(),
            pm_move,
            pm_mom,
            direction,
            alpha_type,
            shock,
            ofi_value,
            opi_mid: Some(opi_mid),
            base_pm,
            pm_mid,
            edge_raw,
            edge_norm,
            score,
            conf_score,
            cost_mult,
            horizon_s,
            alpha_source,
            alpha_source_cfg: self.alpha_source_cfg(),
            alpha_value,
            stale_scale,
            entry_ttl_override_ms,
            c_pred_dir,
            c_ret,
            c_modifier,
        })
    }

    fn detail_json_for_entry(&self, entry: &PendingEntry, extra: Option<Value>) -> Option<String> {
        let mut map: Map<String, Value> = Map::new();
        map.insert("alpha_type".to_string(), json!(entry.alpha_type.as_str()));
        map.insert("alpha_source".to_string(), json!(entry.alpha_source));
        if let Some(cfg) = &entry.alpha_source_cfg {
            map.insert("alpha_source_cfg".to_string(), json!(cfg));
        }
        if let Some(value) = entry.alpha_value {
            map.insert("alpha_value".to_string(), json!(value));
        }
        map.insert("score".to_string(), json!(entry.score));
        map.insert("conf_score".to_string(), json!(entry.conf_score));
        map.insert("stale_scale".to_string(), json!(entry.stale_scale));
        if let Some(ttl_ms) = entry.entry_ttl_override_ms {
            map.insert("entry_ttl_override_ms".to_string(), json!(ttl_ms));
        }
        if let Some(horizon) = entry.horizon_s {
            map.insert("horizon_s".to_string(), json!(horizon));
        }
        if let Some(shock) = &entry.shock {
            map.insert("shock_id".to_string(), json!(shock.shock_id));
            map.insert("shock_type".to_string(), json!(shock.shock_type));
            map.insert("shock_magnitude".to_string(), json!(shock.magnitude));
            map.insert("shock_direction".to_string(), json!(shock.direction));
            map.insert("shock_trigger_bar_second".to_string(), json!(shock.trigger_bar_second));
        }
        if let Some(pred_dir) = entry.c_pred_dir {
            map.insert("c_pred_dir".to_string(), json!(pred_dir));
        }
        if let Some(ret) = entry.c_ret {
            map.insert("c_ret".to_string(), json!(ret));
        }
        if let Some(modifier) = entry.c_modifier {
            map.insert("c_modifier".to_string(), json!(modifier));
        }
        if let Some(extra) = extra {
            match extra {
                Value::Object(obj) => {
                    for (k, v) in obj {
                        map.insert(k, v);
                    }
                }
                other => {
                    map.insert("extra".to_string(), other);
                }
            }
        }
        serde_json::to_string(&map).ok()
    }

    fn detail_json_for_state(&self, state: &PairTradeState, extra: Option<Value>) -> Option<String> {
        let mut map: Map<String, Value> = Map::new();
        if let Some(alpha_type) = state.alpha_type {
            map.insert("alpha_type".to_string(), json!(alpha_type.as_str()));
        }
        if let Some(source) = &state.alpha_source {
            map.insert("alpha_source".to_string(), json!(source));
        }
        if let Some(horizon) = state.horizon_s {
            map.insert("horizon_s".to_string(), json!(horizon));
        }
        if let Some(shock) = &state.shock {
            map.insert("shock_id".to_string(), json!(shock.shock_id));
            map.insert("shock_type".to_string(), json!(shock.shock_type));
            map.insert("shock_magnitude".to_string(), json!(shock.magnitude));
            map.insert("shock_direction".to_string(), json!(shock.direction));
            map.insert("shock_trigger_bar_second".to_string(), json!(shock.trigger_bar_second));
        }
        if state.chase_count > 0 {
            map.insert("chase_count".to_string(), json!(state.chase_count));
        }
        if let Some(extra) = extra {
            match extra {
                Value::Object(obj) => {
                    for (k, v) in obj {
                        map.insert(k, v);
                    }
                }
                other => {
                    map.insert("extra".to_string(), other);
                }
            }
        }
        serde_json::to_string(&map).ok()
    }

    fn book_shape(&self, bar: &Bars1sPair, direction: i64) -> Option<BookShape> {
        let (n1, n2, n3) = if direction > 0 {
            (
                bar.opi_bid_l1_notional?,
                bar.opi_bid_l2_notional.unwrap_or(0.0),
                bar.opi_bid_l3_notional.unwrap_or(0.0),
            )
        } else {
            (
                bar.opi_ask_l1_notional?,
                bar.opi_ask_l2_notional.unwrap_or(0.0),
                bar.opi_ask_l3_notional.unwrap_or(0.0),
            )
        };
        if n1 <= 0.0 {
            return None;
        }
        let decay = ((n1 - n2) / n1).max(0.0);
        let support = ((n2 + n3) / n1).max(0.0);

        if decay > self.cfg.book_decay_block || support < self.cfg.book_support_block {
            return Some(BookShape {
                decay,
                support,
                scale: 0.0,
            });
        }

        let mut scale: f64 = 1.0;
        if decay > self.cfg.book_decay_scale {
            let adj = 1.0 - 2.0 * (decay - self.cfg.book_decay_scale);
            scale = scale.min(adj);
        }
        if support < self.cfg.book_support_scale {
            let adj = support / self.cfg.book_support_scale;
            scale = scale.min(adj);
        }
        if scale < self.cfg.book_scale_min {
            scale = self.cfg.book_scale_min;
        }
        if scale > 1.0 {
            scale = 1.0;
        }

        Some(BookShape {
            decay,
            support,
            scale,
        })
    }

    fn gas_est_usd(&self) -> f64 {
        let lookback = self.cfg.gas_lookback_ms;
        let since_ms = now_ts_ms() - lookback;
        let mut estimate = self.cfg.gas_est_usd;
        if let Ok(guard) = self.chain_state.try_lock() {
            if let Some(p90) = guard.p90_recent(since_ms) {
                if p90 > estimate {
                    estimate = p90;
                }
            }
        }
        if self.cfg.gas_include_cancel_allowance {
            let factor = self.cfg.gas_cancel_allowance_factor;
            if factor > 1.0 {
                estimate *= factor;
            }
        }
        estimate
    }

    fn reset_entry_pool(&self, bar_second: i64, total_balance: f64) {
        if let Ok(mut risk) = self.risk.lock() {
            if risk.entry_pool_second != Some(bar_second) {
                risk.entry_pool_second = Some(bar_second);
                risk.entry_pool_remaining = total_balance * self.cfg.entry_pool_frac;
            }
        }
    }

    fn entry_pool_available(&self, bar_second: i64, total_balance: f64) -> f64 {
        self.reset_entry_pool(bar_second, total_balance);
        self.risk
            .lock()
            .map(|risk| risk.entry_pool_remaining.max(0.0))
            .unwrap_or(0.0)
    }

    fn reserve_entry_pool(&self, bar_second: i64, total_balance: f64, notional: f64) {
        self.reset_entry_pool(bar_second, total_balance);
        if let Ok(mut risk) = self.risk.lock() {
            risk.entry_pool_remaining = (risk.entry_pool_remaining - notional).max(0.0);
        }
    }

    fn release_entry_pool(&self, bar_second: i64, total_balance: f64, notional: f64) {
        self.reset_entry_pool(bar_second, total_balance);
        if let Ok(mut risk) = self.risk.lock() {
            let max_pool = total_balance * self.cfg.entry_pool_frac;
            risk.entry_pool_remaining = (risk.entry_pool_remaining + notional).min(max_pool);
        }
    }

    fn budget_for_entry(
        &mut self,
        key: ExposureKey,
        bar_second: i64,
        pool_override: Option<f64>,
        alpha_type: AlphaType,
    ) -> Option<BudgetDecision> {
        let total = self.cfg.balance_total_usd;
        let free = self.cfg.balance_free_usd;
        if total <= 0.0 || free <= 0.0 {
            if !self.warned_no_balance {
                tracing::warn!("trade balance not configured; set trade.balance_total_usd + balance_free_usd");
                self.warned_no_balance = true;
            }
            return None;
        }
        self.reset_entry_pool(bar_second, total);
        let (open_positions, exposure_used, entry_pool_remaining, strategy_exposure) = if let Ok(risk) = self.risk.lock() {
            (
                risk.open_positions,
                risk.exposure_used,
                risk.entry_pool_remaining,
                risk.strategy_exposure.clone(),
            )
        } else {
            (0, 0.0, 0.0, HashMap::new())
        };
        if open_positions >= self.cfg.max_open_positions {
            return None;
        }
        let global_cap = total * self.cfg.max_global_exposure_frac;
        let available_global = (global_cap - exposure_used).max(0.0);
        if available_global <= 0.0 {
            return None;
        }

        let entry_pool_cap = total * self.cfg.entry_pool_frac;
        let mut notional_base = free.min(entry_pool_cap);
        if self.cfg.entry_fraction > 0.0 {
            notional_base = notional_base.min(total * self.cfg.entry_fraction);
        }
        if self.cfg.entry_fraction_free > 0.0 {
            notional_base = notional_base.min(free * self.cfg.entry_fraction_free);
        }
        let pool_available = if let Some(pool) = pool_override {
            pool.min(entry_pool_remaining)
        } else {
            entry_pool_remaining
        };
        let mut notional = notional_base.min(pool_available);
        notional = notional.min(available_global);
        let max_per_pair = total * self.cfg.max_per_pair_exposure_frac;
        let used_pair = self.pair_exposure.get(&key).copied().unwrap_or(0.0);
        let available_pair = (max_per_pair - used_pair).max(0.0);
        notional = notional.min(available_pair);
        let strategy_cap = total * self.strategy_cap(alpha_type);
        if strategy_cap > 0.0 {
            let used_strategy = strategy_exposure.get(&alpha_type).copied().unwrap_or(0.0);
            let available_strategy = (strategy_cap - used_strategy).max(0.0);
            notional = notional.min(available_strategy);
        }
        let per_trade_cap = total * self.per_trade_cap(alpha_type);
        if per_trade_cap > 0.0 {
            notional = notional.min(per_trade_cap);
        }
        if notional <= 0.0 {
            return None;
        }
        Some(BudgetDecision {
            notional,
            caps: BudgetCaps {
                total,
                free,
                entry_pool_cap,
                pool_available,
                global_cap,
                available_global,
                max_per_pair,
                available_pair,
                strategy_cap,
                available_strategy: (strategy_cap - strategy_exposure.get(&alpha_type).copied().unwrap_or(0.0))
                    .max(0.0),
                per_trade_cap,
                notional_base,
                notional_final: notional,
            },
        })
    }

    fn add_exposure(&mut self, key: ExposureKey, notional: f64, alpha_type: AlphaType) {
        if notional <= 0.0 {
            return;
        }
        if let Ok(mut risk) = self.risk.lock() {
            risk.exposure_used += notional;
            risk.open_positions = risk.open_positions.saturating_add(1);
            let strat_entry = risk.strategy_exposure.entry(alpha_type).or_insert(0.0);
            *strat_entry += notional;
        }
        let entry = self.pair_exposure.entry(key).or_insert(0.0);
        *entry += notional;
    }

    fn release_exposure(&mut self, key: ExposureKey, notional: f64, alpha_type: AlphaType) {
        if notional <= 0.0 {
            return;
        }
        if let Ok(mut risk) = self.risk.lock() {
            risk.exposure_used = (risk.exposure_used - notional).max(0.0);
            if risk.open_positions > 0 {
                risk.open_positions -= 1;
            }
            if let Some(entry) = risk.strategy_exposure.get_mut(&alpha_type) {
                *entry = (*entry - notional).max(0.0);
            }
        }
        if let Some(entry) = self.pair_exposure.get_mut(&key) {
            *entry = (*entry - notional).max(0.0);
        }
    }

    fn retrace_ratio(&self, pm_move: f64, peak: Option<f64>, direction: i64) -> Option<f64> {
        let peak = peak?;
        if peak.abs() <= 1e-9 {
            return None;
        }
        let ratio = if direction > 0 {
            (peak - pm_move) / peak.abs()
        } else {
            (pm_move - peak) / peak.abs()
        };
        Some(ratio)
    }

    fn compute_exit_target(
        &self,
        state: &PairTradeState,
        pm_move: f64,
        pm_mom: f64,
        base_pm: f64,
        gas_est: f64,
    ) -> (Option<f64>, Option<f64>, Option<f64>, Option<f64>) {
        let entry_price = match state.entry_price {
            Some(value) => value,
            None => return (None, None, None, None),
        };
        let entry_qty = match state.entry_qty {
            Some(value) => value,
            None => return (None, None, None, None),
        };
        let pm_peak = state.pm_peak.unwrap_or(pm_move);
        let pm_move_entry = state.pm_move_entry.unwrap_or(pm_move);

        let mut base_edge = self.cfg.tp_a1 * pm_move
            + self.cfg.tp_a2 * (pm_peak - pm_move_entry)
            + self.cfg.tp_a3 * pm_mom;
        if state.alpha_type == Some(AlphaType::ShockA) {
            if let Some(shock) = &state.shock {
                base_edge += self.cfg.tp_a4_shock * shock.magnitude;
            }
        }
        let edge_price = base_edge * base_pm;

        let fee_bps = self.fee_bps(Venue::Opinion, now_ts_ms());
        let fee_est = fee_bps / 10_000.0 * entry_price * entry_qty * 2.0;
        let min_profit_usd = gas_est * self.cfg.gas_mult + self.cfg.gas_buffer_usd + fee_est;
        let min_profit_per_unit = if entry_qty > 0.0 {
            min_profit_usd / entry_qty
        } else {
            0.0
        };

        let mut edge = edge_price * state.direction as f64;
        if edge.abs() < min_profit_per_unit {
            edge = min_profit_per_unit * state.direction as f64;
        }

        let holding_ms = state
            .entry_ts_ms
            .map(|ts| (now_ts_ms() - ts).max(0));
        let holding_sec = holding_ms.unwrap_or(0) as f64 / 1000.0;
        let decay_per_sec = state
            .alpha_type
            .map(|t| self.alpha_decay_per_sec(t))
            .unwrap_or(self.cfg.target_decay_per_sec);
        let decay_factor = (1.0 - decay_per_sec * holding_sec).max(0.1);

        let total = self.cfg.balance_total_usd.max(1.0);
        let global_cap = total * self.cfg.max_global_exposure_frac;
        let exposure_used = self
            .risk
            .lock()
            .map(|risk| risk.exposure_used)
            .unwrap_or(0.0);
        let inv_ratio = if global_cap > 0.0 {
            (exposure_used / global_cap).clamp(0.0, 1.0)
        } else {
            0.0
        };
        let inv_factor = (1.0 - self.cfg.inv_k * inv_ratio).clamp(0.5, 1.0);

        let mut target_edge = edge * decay_factor * inv_factor;
        let floor_edge = min_profit_per_unit * self.cfg.target_floor_gas_mult * state.direction as f64;
        if target_edge.abs() < floor_edge.abs() {
            target_edge = floor_edge;
        }
        let target_price = entry_price + target_edge;
        (Some(target_price), Some(inv_ratio), Some(inv_factor), Some(decay_factor))
    }

    async fn maybe_enter(
        &mut self,
        state: &mut PairTradeState,
        entry: &PendingEntry,
        pool_override: Option<f64>,
    ) {
        let bar = &entry.bar;
        let now_ms = now_ts_ms();
        if self.cfg.per_pair_min_interval_ms > 0 {
            if let Some(last) = state.last_entry_attempt_ms {
                if now_ms.saturating_sub(last) < self.cfg.per_pair_min_interval_ms {
                    return;
                }
            }
        }
        state.last_entry_attempt_ms = Some(now_ms);
        let pm_move = entry.pm_move;
        let pm_mom = entry.pm_mom;
        let direction = entry.direction;
        let ofi_value = entry.ofi_value;
        let base_pm = entry.base_pm;
        let pair_key = entry.key;
        if direction == 0 {
            return;
        }
        let (
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
        ) = Self::audit_health(Some(bar));
        let exposure_key = (bar.pair_id, bar.token_side, direction);
        if entry.opi_mid.map(|v| v > 0.0).unwrap_or(false) == false {
            return;
        }
        let edge_raw = entry.edge_raw;
        let edge_norm = entry.edge_norm;

        let shape = match self.book_shape(bar, direction) {
            Some(shape) if shape.scale > 0.0 => shape,
            _ => {
                let row = TradeAuditRow {
                    event_ts_ms: now_ts_ms(),
                    pair_id: bar.pair_id,
                    token_side: bar.token_side,
                    phase: "ENTRY_BLOCKED_BOOK".to_string(),
                    direction,
                    reason: Some("book_shape_block".to_string()),
                    pm_move: Some(pm_move),
                    pm_mom: Some(pm_mom),
                    ofi_250ms: Some(ofi_value),
                    edge_raw: Some(edge_raw),
                    edge_norm: Some(edge_norm),
                    entry_price: None,
                    entry_qty: None,
                    exit_price_target: None,
                    exit_price: None,
                    gas_est_usd: None,
                    min_profit_usd: None,
                    book_decay: None,
                    book_support: None,
                    book_scale: None,
                    retrace_ratio: None,
                    inv_ratio: None,
                    inv_factor: None,
                    decay_factor: None,
                    pnl_usd_net: None,
                    holding_ms: None,
                    mae: None,
                    mfe: None,
                    obs_latency_poly_p99_ms,
                    obs_latency_opi_p99_ms,
                    bar_gap_flag,
                    poly_bar_gap_flag,
                    opi_bar_gap_flag,
                    poly_staleness_ms,
                    opi_staleness_ms,
                    detail_json: self.detail_json_for_entry(entry, None),
                };
                self.emit_audit(row).await;
                return;
            }
        };

        let budget = match self.budget_for_entry(
            exposure_key,
            bar.bar_second,
            pool_override,
            entry.alpha_type,
        ) {
            Some(v) => v,
            None => return,
        };
        let notional = budget.notional;

        let gas_est = self.gas_est_usd();
        let fee_bps = self.fee_bps(Venue::Opinion, now_ts_ms());

        let tick = if self.cfg.tick_px > 0.0 {
            self.cfg.tick_px
        } else {
            1e-4
        };
        let (best_bid, best_ask) =
            match (opi_best_bid_from_bar(&bar), opi_best_ask_from_bar(&bar)) {
                (Some(bid), Some(ask)) if bid > 0.0 && ask > 0.0 => (bid, ask),
                _ => return,
            };
        let spread = best_ask - best_bid;
        if spread <= 0.0 {
            return;
        }
        let spread_ticks_f = if tick > 0.0 { spread / tick } else { 0.0 };
        let spread_ticks_gate = spread_ticks_f.round().max(0.0) as i64;
        let spread_ticks_price = spread_ticks_f.floor().max(1.0) as i64;
        let mid = (best_bid + best_ask) / 2.0;
        let spread_pct = if mid > 0.0 { spread / mid } else { 0.0 };
        let spread_too_wide = (self.cfg.entry_spread_max_ticks > 0
            && spread_ticks_gate > self.cfg.entry_spread_max_ticks)
            || (self.cfg.entry_spread_max_pct > 0.0
                && spread_pct > self.cfg.entry_spread_max_pct);
        let mut spread_scale = 1.0;
        if spread_too_wide {
            let scale = self.cfg.entry_spread_size_scale;
            if scale > 0.0 {
                spread_scale = scale.clamp(0.0, 1.0);
            } else {
                let row = TradeAuditRow {
                    event_ts_ms: now_ts_ms(),
                    pair_id: bar.pair_id,
                    token_side: bar.token_side,
                    phase: "ENTRY_BLOCKED_SPREAD".to_string(),
                    direction,
                    reason: Some("spread_wide".to_string()),
                    pm_move: Some(pm_move),
                    pm_mom: Some(pm_mom),
                    ofi_250ms: Some(ofi_value),
                    edge_raw: Some(edge_raw),
                    edge_norm: Some(edge_norm),
                    entry_price: None,
                    entry_qty: None,
                    exit_price_target: None,
                    exit_price: None,
                    gas_est_usd: None,
                    min_profit_usd: None,
                    book_decay: Some(shape.decay),
                    book_support: Some(shape.support),
                    book_scale: Some(shape.scale),
                    retrace_ratio: None,
                    inv_ratio: None,
                    inv_factor: None,
                    decay_factor: None,
                    pnl_usd_net: None,
                    holding_ms: None,
                    mae: None,
                    mfe: None,
                    obs_latency_poly_p99_ms,
                    obs_latency_opi_p99_ms,
                    bar_gap_flag,
                    poly_bar_gap_flag,
                    opi_bar_gap_flag,
                    poly_staleness_ms,
                    opi_staleness_ms,
                    detail_json: self.detail_json_for_entry(
                        entry,
                        Some(json!({
                            "opi_best_bid": best_bid,
                            "opi_best_ask": best_ask,
                            "opi_spread_ticks_gate": spread_ticks_gate,
                            "opi_spread_ticks_price": spread_ticks_price,
                            "opi_spread_pct": spread_pct,
                            "entry_spread_max_ticks": self.cfg.entry_spread_max_ticks,
                            "entry_spread_max_pct": self.cfg.entry_spread_max_pct
                        })),
                    ),
                };
                self.emit_audit(row).await;
                return;
            }
        }

        let price_ref = if direction > 0 { best_bid } else { best_ask };

        let qty_base = notional / price_ref;
        let mut qty = qty_base * shape.scale;
        if let Some(modifier) = entry.c_modifier {
            let scale = (1.0 + modifier).clamp(0.2, 1.5);
            qty *= scale;
        }
        if entry.stale_scale > 0.0 && entry.stale_scale.is_finite() {
            qty *= entry.stale_scale.clamp(0.0, 1.0);
        }
        if spread_scale < 1.0 {
            qty *= spread_scale;
        }
        if qty <= 0.0 {
            return;
        }

        if self.cfg.gas_emergency_pause_enabled
            && self.cfg.gas_emergency_max_usd > 0.0
            && gas_est >= self.cfg.gas_emergency_max_usd
        {
            let row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "ENTRY_BLOCKED_GAS_EMERGENCY".to_string(),
                direction,
                reason: Some("gas_emergency_pause".to_string()),
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: Some(edge_raw),
                edge_norm: Some(edge_norm),
                entry_price: None,
                entry_qty: Some(qty),
                exit_price_target: None,
                exit_price: None,
                gas_est_usd: Some(gas_est),
                min_profit_usd: None,
                book_decay: Some(shape.decay),
                book_support: Some(shape.support),
                book_scale: Some(shape.scale),
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: None,
                mfe: None,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_entry(entry, None),
            };
            self.emit_audit(row).await;
            return;
        }

        let fee_est = fee_bps / 10_000.0 * price_ref * qty * 2.0;
        let min_profit_usd = gas_est * self.cfg.gas_mult + self.cfg.gas_buffer_usd + fee_est;
        let min_profit_mult = entry.cost_mult.max(1.0);

        let pm_mom_price = pm_mom * base_pm;
        let shock_component = entry
            .shock
            .as_ref()
            .map(|s| s.magnitude)
            .unwrap_or(0.0);
        let theo_price = if direction > 0 {
            price_ref
                + self.cfg.k_edge * edge_raw
                + self.cfg.k_mom * pm_mom_price
                + self.cfg.k_shock * shock_component
        } else {
            price_ref
                - self.cfg.k_edge * edge_raw
                - self.cfg.k_mom * pm_mom_price
                - self.cfg.k_shock * shock_component
        };
        let mut expected_edge_per_unit = if entry.alpha_type == AlphaType::ShockA {
            direction as f64 * (theo_price - price_ref)
        } else {
            edge_raw
        };
        let edge_band = if entry.alpha_type == AlphaType::ShockA {
            let band = self.opi_edge_band_px(bar);
            expected_edge_per_unit -= band;
            Some(band)
        } else {
            None
        };
        let min_profit_per_unit = (min_profit_usd * min_profit_mult) / qty;
        let (price_limit, compare_ok): (f64, fn(f64, f64) -> bool) = if direction > 0 {
            (theo_price - min_profit_per_unit, |p, limit| p <= limit)
        } else {
            (theo_price + min_profit_per_unit, |p, limit| p >= limit)
        };

        let mut candidate_prices = Vec::new();
        let side = if direction > 0 { OrderSide::Buy } else { OrderSide::Sell };
        let entry_aggr = if self.cfg.entry_aggr_enabled {
            self.entry_aggressiveness(entry, now_ms)
        } else {
            0.0
        };
        let mut start_ticks = self.cfg.entry_tick_offset.max(0);
        let mut entry_k = start_ticks;
        if self.cfg.entry_aggr_enabled {
            entry_k = if spread_ticks_price <= 1 {
                0
            } else if spread_ticks_price == 2 {
                1
            } else {
                let span = (spread_ticks_price - 1).max(1) as f64;
                let k_f = (entry_aggr * span).round();
                k_f.clamp(0.0, span) as i64
            };
            start_ticks = entry_k;
        }
        if spread_ticks_price > 0 {
            let max_k = (spread_ticks_price - 1).max(0);
            if start_ticks > max_k {
                start_ticks = max_k;
            }
        }
        entry_k = start_ticks;
        let step_ticks = self.cfg.chase_step_ticks.max(1);
        let max_steps = self.cfg.max_chase_ticks.max(0);
        let base_price = if direction > 0 {
            price_ref + tick * start_ticks as f64
        } else {
            price_ref - tick * start_ticks as f64
        };
        let price0 = if direction > 0 {
            base_price.min(price_limit)
        } else {
            base_price.max(price_limit)
        };
        for step_idx in 0..=max_steps {
            let offset = tick * (step_ticks * step_idx) as f64;
            let price = if direction > 0 {
                price0 - offset
            } else {
                price0 + offset
            };
            let price = round_price_for_side(price, side);
            if price <= 0.0 {
                break;
            }
            if !compare_ok(price, price_limit) {
                continue;
            }
            let taker = if direction > 0 {
                price >= best_ask
            } else {
                price <= best_bid
            };
            if taker {
                continue;
            }
            if candidate_prices.last().copied() == Some(price) {
                continue;
            }
            candidate_prices.push(price);
        }
        let mut entry_price = match candidate_prices.first().copied() {
            Some(price) if price > 0.0 => price,
            _ => {
                state.enter_cooldown(
                    bar.bar_second,
                    self.cfg.cooldown_after_postonly_reject_ms,
                );
                let row = TradeAuditRow {
                    event_ts_ms: now_ts_ms(),
                    pair_id: bar.pair_id,
                    token_side: bar.token_side,
                    phase: "ENTRY_REJECTED".to_string(),
                    direction,
                    reason: Some("post_only_reject".to_string()),
                    pm_move: Some(pm_move),
                    pm_mom: Some(pm_mom),
                    ofi_250ms: Some(ofi_value),
                    edge_raw: Some(edge_raw),
                    edge_norm: Some(edge_norm),
                    entry_price: None,
                    entry_qty: Some(qty),
                    exit_price_target: None,
                    exit_price: None,
                    gas_est_usd: Some(gas_est),
                    min_profit_usd: Some(min_profit_usd),
                    book_decay: Some(shape.decay),
                    book_support: Some(shape.support),
                    book_scale: Some(shape.scale),
                    retrace_ratio: None,
                    inv_ratio: None,
                    inv_factor: None,
                    decay_factor: None,
                    pnl_usd_net: None,
                    holding_ms: None,
                    mae: None,
                    mfe: None,
                    obs_latency_poly_p99_ms,
                    obs_latency_opi_p99_ms,
                    bar_gap_flag,
                    poly_bar_gap_flag,
                    opi_bar_gap_flag,
                    poly_staleness_ms,
                    opi_staleness_ms,
                    detail_json: self.detail_json_for_entry(
                        entry,
                        Some(json!({
                            "post_only_reject": true,
                            "theo_price": theo_price,
                            "price_limit": price_limit,
                            "price_ref": price_ref,
                            "tick_px": tick
                        })),
                    ),
                };
                self.emit_audit(row).await;
                return;
            }
        };

        let mut min_notional_extra: Option<Value> = None;
        let min_notional = self.cfg.min_order_notional_usd;
        if min_notional > 0.0 {
            let notional_now = entry_price * qty;
            if notional_now < min_notional {
                let min_qty = min_notional / entry_price;
                let max_qty = notional / entry_price;
                if min_qty > 0.0 && min_qty <= max_qty {
                    let qty_before = qty;
                    qty = min_qty;
                    min_notional_extra = Some(json!({
                        "min_notional_usd": min_notional,
                        "notional_before": notional_now,
                        "qty_before": qty_before,
                        "qty_after": qty,
                        "max_qty": max_qty,
                        "price_entry": entry_price,
                    }));
                } else {
                    let row = TradeAuditRow {
                        event_ts_ms: now_ts_ms(),
                        pair_id: bar.pair_id,
                        token_side: bar.token_side,
                        phase: "ENTRY_BLOCKED_NOTIONAL".to_string(),
                        direction,
                        reason: Some("min_notional".to_string()),
                        pm_move: Some(pm_move),
                        pm_mom: Some(pm_mom),
                        ofi_250ms: Some(ofi_value),
                        edge_raw: Some(edge_raw),
                        edge_norm: Some(edge_norm),
                        entry_price: Some(entry_price),
                        entry_qty: Some(qty),
                        exit_price_target: None,
                        exit_price: None,
                        gas_est_usd: Some(gas_est),
                        min_profit_usd: None,
                        book_decay: Some(shape.decay),
                        book_support: Some(shape.support),
                        book_scale: Some(shape.scale),
                        retrace_ratio: None,
                        inv_ratio: None,
                        inv_factor: None,
                        decay_factor: None,
                        pnl_usd_net: None,
                        holding_ms: None,
                        mae: None,
                        mfe: None,
                        obs_latency_poly_p99_ms,
                        obs_latency_opi_p99_ms,
                        bar_gap_flag,
                        poly_bar_gap_flag,
                        opi_bar_gap_flag,
                        poly_staleness_ms,
                        opi_staleness_ms,
                        detail_json: self.detail_json_for_entry(
                            entry,
                            Some(json!({
                                "min_notional_usd": min_notional,
                                "notional_usd": notional_now,
                                "min_qty": min_qty,
                                "max_qty": max_qty,
                                "price_entry": entry_price,
                                "qty_base": qty_base,
                                "qty_scaled": qty,
                                "price_ref": price_ref,
                                "budget": {
                                    "total": budget.caps.total,
                                    "free": budget.caps.free,
                                    "entry_pool_cap": budget.caps.entry_pool_cap,
                                    "pool_available": budget.caps.pool_available,
                                    "global_cap": budget.caps.global_cap,
                                    "available_global": budget.caps.available_global,
                                    "max_per_pair": budget.caps.max_per_pair,
                                    "available_pair": budget.caps.available_pair,
                                    "strategy_cap": budget.caps.strategy_cap,
                                    "available_strategy": budget.caps.available_strategy,
                                    "per_trade_cap": budget.caps.per_trade_cap,
                                    "notional_base": budget.caps.notional_base,
                                    "notional_final": budget.caps.notional_final
                                }
                            })),
                        ),
                    };
                    self.emit_audit(row).await;
                    return;
                }
            }
        }

        if expected_edge_per_unit * qty < min_profit_usd * min_profit_mult {
            let row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "ENTRY_BLOCKED_GAS".to_string(),
                direction,
                reason: Some("edge_lt_min_profit".to_string()),
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: Some(edge_raw),
                edge_norm: Some(edge_norm),
                entry_price: Some(entry_price),
                entry_qty: Some(qty),
                exit_price_target: None,
                exit_price: None,
                gas_est_usd: Some(gas_est),
                min_profit_usd: Some(min_profit_usd),
                book_decay: Some(shape.decay),
                book_support: Some(shape.support),
                book_scale: Some(shape.scale),
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: None,
                mfe: None,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_entry(
                    entry,
                    Some(json!({
                        "min_profit_mult": min_profit_mult,
                        "expected_edge_per_unit": expected_edge_per_unit,
                        "edge_band_px": edge_band,
                        "theo_price": theo_price,
                        "price_ref": price_ref,
                        "price_entry": entry_price,
                    })),
                ),
            };
            self.emit_audit(row).await;
            return;
        }

        let candidate_first = candidate_prices.first().copied();
        let candidate_last = candidate_prices.last().copied();
        let mut detail_extra_map: Map<String, Value> = Map::new();
        detail_extra_map.insert("theo_price".to_string(), json!(theo_price));
        detail_extra_map.insert("price_limit".to_string(), json!(price_limit));
        detail_extra_map.insert("price_ref".to_string(), json!(price_ref));
        detail_extra_map.insert("price0".to_string(), json!(price0));
        detail_extra_map.insert("tick_px".to_string(), json!(tick));
        detail_extra_map.insert("opi_best_bid".to_string(), json!(best_bid));
        detail_extra_map.insert("opi_best_ask".to_string(), json!(best_ask));
        detail_extra_map.insert(
            "opi_spread_ticks_gate".to_string(),
            json!(spread_ticks_gate),
        );
        detail_extra_map.insert(
            "opi_spread_ticks_price".to_string(),
            json!(spread_ticks_price),
        );
        detail_extra_map.insert("opi_spread_pct".to_string(), json!(spread_pct));
        detail_extra_map.insert("entry_aggr".to_string(), json!(entry_aggr));
        detail_extra_map.insert("entry_aggr_k".to_string(), json!(entry_k));
        detail_extra_map.insert(
            "min_profit_per_unit".to_string(),
            json!(min_profit_per_unit),
        );
        detail_extra_map.insert("min_profit_mult".to_string(), json!(min_profit_mult));
        detail_extra_map.insert("candidate_prices".to_string(), json!(candidate_prices.clone()));
        detail_extra_map.insert(
            "candidate_prices_len".to_string(),
            json!(candidate_prices.len()),
        );
        detail_extra_map.insert("candidate_price_first".to_string(), json!(candidate_first));
        detail_extra_map.insert("candidate_price_last".to_string(), json!(candidate_last));
        detail_extra_map.insert("entry_tick_offset".to_string(), json!(start_ticks));
        detail_extra_map.insert(
            "entry_tick_offset_cfg".to_string(),
            json!(self.cfg.entry_tick_offset),
        );
        detail_extra_map.insert("chase_step_ticks".to_string(), json!(step_ticks));
        detail_extra_map.insert("max_chase_steps".to_string(), json!(max_steps));
        detail_extra_map.insert("post_only".to_string(), json!(self.cfg.post_only));
        if spread_scale < 1.0 {
            detail_extra_map.insert("entry_spread_scale".to_string(), json!(spread_scale));
        }
        if let Some(extra) = min_notional_extra {
            match extra {
                Value::Object(obj) => {
                    for (k, v) in obj {
                        detail_extra_map.insert(k, v);
                    }
                }
                other => {
                    detail_extra_map.insert("min_notional_extra".to_string(), other);
                }
            }
        }
        let detail_extra = Value::Object(detail_extra_map);

        let total_balance = self.cfg.balance_total_usd;
        let reserved_notional = entry_price * qty;
        self.reserve_entry_pool(bar.bar_second, total_balance, reserved_notional);
        state.state = TradeState::EntryReady;
        let ready_row = TradeAuditRow {
            event_ts_ms: now_ts_ms(),
            pair_id: bar.pair_id,
            token_side: bar.token_side,
            phase: "ENTRY_READY".to_string(),
            direction,
            reason: None,
            pm_move: Some(pm_move),
            pm_mom: Some(pm_mom),
            ofi_250ms: Some(ofi_value),
            edge_raw: Some(edge_raw),
            edge_norm: Some(edge_norm),
            entry_price: Some(entry_price),
            entry_qty: Some(qty),
            exit_price_target: None,
            exit_price: None,
            gas_est_usd: Some(gas_est),
            min_profit_usd: Some(min_profit_usd),
            book_decay: Some(shape.decay),
            book_support: Some(shape.support),
            book_scale: Some(shape.scale),
            retrace_ratio: None,
            inv_ratio: None,
            inv_factor: None,
            decay_factor: None,
            pnl_usd_net: None,
            holding_ms: None,
            mae: None,
            mfe: None,
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
            detail_json: self.detail_json_for_entry(entry, Some(detail_extra.clone())),
        };
        self.emit_audit(ready_row).await;
        self.log_alpha("ENTRY_READY", bar, state, Some(entry));

        state.state = TradeState::EntryPlacing;
        let placing_row = TradeAuditRow {
            event_ts_ms: now_ts_ms(),
            pair_id: bar.pair_id,
            token_side: bar.token_side,
            phase: "ENTRY_PLACING".to_string(),
            direction,
            reason: None,
            pm_move: Some(pm_move),
            pm_mom: Some(pm_mom),
            ofi_250ms: Some(ofi_value),
            edge_raw: Some(edge_raw),
            edge_norm: Some(edge_norm),
            entry_price: Some(entry_price),
            entry_qty: Some(qty),
            exit_price_target: None,
            exit_price: None,
            gas_est_usd: Some(gas_est),
            min_profit_usd: Some(min_profit_usd),
            book_decay: Some(shape.decay),
            book_support: Some(shape.support),
            book_scale: Some(shape.scale),
            retrace_ratio: None,
            inv_ratio: None,
            inv_factor: None,
            decay_factor: None,
            pnl_usd_net: None,
            holding_ms: None,
            mae: None,
            mfe: None,
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
            detail_json: self.detail_json_for_entry(entry, Some(detail_extra.clone())),
        };
        self.emit_audit(placing_row).await;
        self.log_alpha("ENTRY_PLACING", bar, state, Some(entry));

        if self.is_live() {
            let side = if direction > 0 {
                OrderSide::Buy
            } else {
                OrderSide::Sell
            };
            state.state = TradeState::EntryPlacing;
            state.direction = direction;
            state.entry_price = Some(entry_price);
            state.entry_qty = Some(qty);
            state.entry_notional = Some(entry_price * qty);
            state.entry_bar_second = Some(bar.bar_second);
            state.pm_move_entry = Some(pm_move);
            state.pm_peak = Some(pm_move);
            state.book_decay = Some(shape.decay);
            state.book_support = Some(shape.support);
            state.book_scale = Some(shape.scale);
            state.edge_raw = Some(edge_raw);
            state.edge_norm = Some(edge_norm);
            state.ofi_250ms = Some(ofi_value);
            state.min_profit_usd = Some(min_profit_usd);
            state.alpha_type = Some(entry.alpha_type);
            state.alpha_source = Some(entry.alpha_source.clone());
            state.shock = entry.shock.clone();
            state.horizon_s = entry.horizon_s;
            state.entry_ttl_override_ms = entry.entry_ttl_override_ms;
            state.pending_entry = Some(entry.clone());
            state.pending_entry_meta = Some(PendingEntryMeta {
                theo_price,
                price_limit,
                price_ref,
                tick_px: tick,
            });
            if let Some(req_id) = self.enqueue_place_chase(
                pair_key,
                PlaceKind::Entry,
                side,
                qty,
                candidate_prices.clone(),
                "E",
            ) {
                state.entry_place_req_id = Some(req_id);
                return;
            }
            self.release_entry_pool(bar.bar_second, total_balance, reserved_notional);
            state.enter_cooldown(bar.bar_second, self.cfg.cooldown_after_postonly_reject_ms);
            let row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "ENTRY_PLACE_FAILED".to_string(),
                direction,
                reason: Some("entry_enqueue_failed".to_string()),
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: Some(edge_raw),
                edge_norm: Some(edge_norm),
                entry_price: Some(entry_price),
                entry_qty: Some(qty),
                exit_price_target: None,
                exit_price: None,
                gas_est_usd: Some(gas_est),
                min_profit_usd: Some(min_profit_usd),
                book_decay: Some(shape.decay),
                book_support: Some(shape.support),
                book_scale: Some(shape.scale),
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: None,
                mfe: None,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_entry(
                    entry,
                    Some(json!({
                        "chase_count": 0,
                        "post_only_rejects": 0,
                    })),
                ),
            };
            self.emit_audit(row).await;
            return;
        }

        state.state = TradeState::EntryWorking;
        state.direction = direction;
        state.entry_price = Some(entry_price);
        state.entry_qty = Some(qty);
        state.entry_notional = Some(entry_price * qty);
        state.entry_ts_ms = Some(now_ts_ms());
        state.entry_bar_second = Some(bar.bar_second);
        state.pm_move_entry = Some(pm_move);
        state.pm_peak = Some(pm_move);
        state.book_decay = Some(shape.decay);
        state.book_support = Some(shape.support);
        state.book_scale = Some(shape.scale);
        state.edge_raw = Some(edge_raw);
        state.edge_norm = Some(edge_norm);
        state.ofi_250ms = Some(ofi_value);
        state.min_profit_usd = Some(min_profit_usd);
        state.alpha_type = Some(entry.alpha_type);
        state.alpha_source = Some(entry.alpha_source.clone());
        state.shock = entry.shock.clone();
        state.horizon_s = entry.horizon_s;
        state.chase_count = 0;
        state.entry_ttl_override_ms = entry.entry_ttl_override_ms;

        self.add_exposure(exposure_key, entry_price * qty, entry.alpha_type);

        let working_row = TradeAuditRow {
            event_ts_ms: now_ts_ms(),
            pair_id: bar.pair_id,
            token_side: bar.token_side,
            phase: "ENTRY_WORKING".to_string(),
            direction,
            reason: None,
            pm_move: Some(pm_move),
            pm_mom: Some(pm_mom),
            ofi_250ms: Some(ofi_value),
            edge_raw: Some(edge_raw),
            edge_norm: Some(edge_norm),
            entry_price: Some(entry_price),
            entry_qty: Some(qty),
            exit_price_target: None,
            exit_price: None,
            gas_est_usd: Some(gas_est),
            min_profit_usd: Some(min_profit_usd),
            book_decay: Some(shape.decay),
            book_support: Some(shape.support),
            book_scale: Some(shape.scale),
            retrace_ratio: None,
            inv_ratio: None,
            inv_factor: None,
            decay_factor: None,
            pnl_usd_net: None,
            holding_ms: None,
            mae: None,
            mfe: None,
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
            detail_json: self.detail_json_for_state(
                state,
                Some(json!({
                    "entry_order_id": state.entry_order_id.clone().unwrap_or_default(),
                    "entry_client_id": state.entry_client_id.clone().unwrap_or_default(),
                    "chase_count": state.chase_count,
                    "post_only_rejects": 0,
                    "theo_price": theo_price,
                    "price_limit": price_limit,
                    "price_ref": price_ref,
                    "tick_px": tick,
                })),
            ),
        };
        self.emit_audit(working_row).await;
        self.log_alpha("ENTRY_WORKING", bar, state, state.pending_entry.as_ref());
    }

    async fn check_entry(
        &mut self,
        state: &mut PairTradeState,
        bar: &Bars1sPair,
        pm_move: f64,
        pm_mom: f64,
        ofi_value: f64,
        pair_key: (i64, TokenSide),
    ) {
        let direction = state.direction;
        if direction == 0 {
            state.reset_for_idle();
            return;
        }
        let (
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
        ) = Self::audit_health(Some(bar));
        let opi_signals = self.derive_opi_signals(state, bar);
        let opi_fresh = bar.opi_bar_gap_flag == 0
            && bar
                .opi_staleness_ms
                .map(|s| s <= self.cfg.opi_cancel_staleness_ms)
                .unwrap_or(false);

        if self.is_live() {
            if state.entry_place_req_id.is_some() {
                return;
            }
            self.maybe_enqueue_fill_poll(state, pair_key, FillKind::Entry);
            if state.entry_cancel_pending {
                self.maybe_enqueue_entry_cancel(state);
                return;
            }
        } else if state.entry_cancel_pending {
            let reason = state
                .entry_cancel_reason
                .clone()
                .unwrap_or_else(|| "entry_cancel".to_string());
            self.finalize_entry_cancel(state, bar, pm_move, pm_mom, ofi_value, &reason, pair_key)
                .await;
            return;
        }

        let entry_ttl_ms = state
            .entry_ttl_override_ms
            .unwrap_or_else(|| self.entry_ttl_ms_for(state.alpha_type));
        if entry_ttl_ms > 0 {
            if let Some(ts) = state.entry_ts_ms {
                if now_ts_ms().saturating_sub(ts) >= entry_ttl_ms {
                    self.request_entry_cancel(state, bar, pm_move, pm_mom, ofi_value, "ttl", pair_key)
                        .await;
                    return;
                }
            }
        }

        let retrace_ratio = self.retrace_ratio(pm_move, state.pm_peak, direction);
        let buy_notional = bar.poly_buy_notional_250ms.unwrap_or(0.0);
        let sell_notional = bar.poly_sell_notional_250ms.unwrap_or(0.0);
        let max_trade = bar.poly_max_trade_notional_250ms.unwrap_or(0.0);
        let bid_delta = bar.poly_bid_delta_notional_250ms.unwrap_or(0.0);
        let ask_delta = bar.poly_ask_delta_notional_250ms.unwrap_or(0.0);
        let mid_slope = bar.poly_mid_slope_250ms.unwrap_or(0.0);
        let ofi_low_trigger = if direction > 0 {
            ofi_value <= self.cfg.ofi_cancel_low
        } else {
            ofi_value >= -self.cfg.ofi_cancel_low
        };
        let ofi_med_trigger = if direction > 0 {
            ofi_value <= self.cfg.ofi_cancel_med
        } else {
            ofi_value >= -self.cfg.ofi_cancel_med
        };
        let mom_bad = if direction > 0 { pm_mom < 0.0 } else { pm_mom > 0.0 };
        let ask_add_trigger = if direction > 0 {
            ask_delta >= self.cfg.ask_add_usd && ofi_low_trigger
        } else {
            bid_delta >= self.cfg.ask_add_usd && ofi_low_trigger
        };
        let slope_against = if direction > 0 {
            mid_slope < 0.0
        } else {
            mid_slope > 0.0
        };
        let aggressive_flow = max_trade >= self.cfg.trade_cancel_usd
            || (direction > 0 && sell_notional >= self.cfg.trade_cancel_usd)
            || (direction < 0 && buy_notional >= self.cfg.trade_cancel_usd)
            || (ofi_low_trigger && (mom_bad || slope_against))
            || ask_add_trigger;
        let bid_withdraw = if direction > 0 {
            bid_delta <= -self.cfg.bid_withdraw_usd && ofi_med_trigger
        } else {
            ask_delta <= -self.cfg.bid_withdraw_usd && ofi_med_trigger
        };
        let book_bad = self
            .book_shape(bar, direction)
            .map(|shape| shape.scale <= 0.0)
            .unwrap_or(true);

        let mut opi_cancel_reason: Option<&'static str> = None;
        if opi_fresh {
            if let Some(mid_ret_bps) = opi_signals.mid_ret_bps_1s {
                if (direction > 0 && mid_ret_bps <= -self.cfg.opi_mid_ret_cancel_bps)
                    || (direction < 0 && mid_ret_bps >= self.cfg.opi_mid_ret_cancel_bps)
                {
                    opi_cancel_reason = Some("opi_mid_ret");
                }
            }

            if opi_cancel_reason.is_none() {
                if let Some(spread_bps) = opi_signals.spread_bps {
                    if spread_bps <= self.cfg.opi_spread_bps_min {
                        opi_cancel_reason = Some("opi_spread");
                    }
                }
            }

            if opi_cancel_reason.is_none() {
                let edge_bps = match (state.entry_price, opi_signals.mid) {
                    (Some(entry), Some(mid)) if mid > 0.0 => {
                        let raw = if direction > 0 {
                            (mid - entry) / mid * 10_000.0
                        } else {
                            (entry - mid) / mid * 10_000.0
                        };
                        Some(raw)
                    }
                    _ => None,
                };
                if let Some(edge_bps) = edge_bps {
                    if edge_bps <= self.cfg.opi_edge_min_bps {
                        opi_cancel_reason = Some("opi_edge");
                    }
                }
            }

            if opi_cancel_reason.is_none() {
                let imbalance = opi_signals
                    .imbalance_l1
                    .or(opi_signals.imbalance_l3);
                if let Some(imb) = imbalance {
                    if (direction > 0 && imb <= -self.cfg.opi_imbalance_cancel)
                        || (direction < 0 && imb >= self.cfg.opi_imbalance_cancel)
                    {
                        opi_cancel_reason = Some("opi_imbalance");
                    }
                }
            }

            if opi_cancel_reason.is_none() {
                let bid_delta = opi_signals.bid_delta;
                let ask_delta = opi_signals.ask_delta;
                if direction > 0 {
                    if bid_delta.map(|v| v <= -self.cfg.opi_bid_withdraw_usd).unwrap_or(false)
                        || ask_delta.map(|v| v >= self.cfg.opi_ask_add_usd).unwrap_or(false)
                    {
                        opi_cancel_reason = Some("opi_depth_flow");
                    }
                } else if bid_delta.map(|v| v >= self.cfg.opi_ask_add_usd).unwrap_or(false)
                    || ask_delta.map(|v| v <= -self.cfg.opi_bid_withdraw_usd).unwrap_or(false)
                {
                    opi_cancel_reason = Some("opi_depth_flow");
                }
            }

            if opi_cancel_reason.is_none() {
                let taker_buy = opi_signals.taker_buy_notional_1s.unwrap_or(0.0);
                let taker_sell = opi_signals.taker_sell_notional_1s.unwrap_or(0.0);
                let imbalance = opi_signals.trade_imbalance_1s;
                if direction > 0 {
                    if taker_sell >= self.cfg.opi_taker_sell_cancel_usd
                        || imbalance
                            .map(|v| v <= -self.cfg.opi_trade_imbalance_cancel)
                            .unwrap_or(false)
                    {
                        opi_cancel_reason = Some("opi_taker_flow");
                    }
                } else if taker_buy >= self.cfg.opi_taker_buy_cancel_usd
                    || imbalance
                        .map(|v| v >= self.cfg.opi_trade_imbalance_cancel)
                        .unwrap_or(false)
                {
                    opi_cancel_reason = Some("opi_taker_flow");
                }
            }
        }

        let reverse_shock = state.alpha_type == Some(AlphaType::ShockA)
            && self.reverse_shock_active(pair_key, bar.bar_second, direction);
        let mut alpha_decay_reason: Option<&'static str> = None;
        if state.alpha_type == Some(AlphaType::ShockA) {
            if let Some(shock) = &state.shock {
                let mom_over_shock_min = self.cfg.entry_cancel_shock_mom_ratio_min;
                if mom_over_shock_min > 0.0 {
                    let mom_floor = self.cfg.entry_cancel_mom_floor.max(1e-12);
                    let shock_floor = self.cfg.entry_cancel_shock_floor.max(1e-12);
                    let mom_abs = pm_mom.abs().max(mom_floor);
                    let shock_abs = shock.magnitude.abs().max(shock_floor);
                    let ratio = mom_abs / shock_abs;
                    if ratio < mom_over_shock_min {
                        alpha_decay_reason = Some("alpha_decay_mom_over_shock");
                    }
                }
            }
        }
        if alpha_decay_reason.is_none() {
            let mom_decay_min = self.cfg.entry_cancel_mom_decay_min;
            if mom_decay_min > 0.0 {
                let mom_floor = self.cfg.entry_cancel_mom_floor.max(1e-12);
                if let Some(entry_mom) = state.pm_mom_entry {
                    let denom = entry_mom.abs().max(mom_floor);
                    let ratio = pm_mom.abs().max(mom_floor) / denom;
                    if ratio < mom_decay_min {
                        alpha_decay_reason = Some("alpha_decay_mom_entry");
                    }
                }
            }
        }

        let cancel_reason = if reverse_shock {
            Some("reverse_shock")
        } else if let Some(reason) = alpha_decay_reason {
            Some(reason)
        } else if aggressive_flow {
            Some("aggressive_flow")
        } else if retrace_ratio
            .map(|ratio| ratio >= self.cfg.retrace_cancel)
            .unwrap_or(false)
        {
            Some("retrace_cancel")
        } else if bid_withdraw {
            Some("bid_withdraw")
        } else if let Some(reason) = opi_cancel_reason {
            Some(reason)
        } else if book_bad {
            Some("book_shape_deterioration")
        } else {
            None
        };

        if let Some(reason) = cancel_reason {
            self.request_entry_cancel(state, bar, pm_move, pm_mom, ofi_value, reason, pair_key)
                .await;
            return;
        }

        if self.is_live() {
            return;
        }

        let fill_price = if direction > 0 {
            bar.opi_last_price
                .or(opi_best_ask_from_bar(bar))
                .or(opi_mid_from_bar(bar))
        } else {
            bar.opi_last_price
                .or(opi_best_bid_from_bar(bar))
                .or(opi_mid_from_bar(bar))
        };
        let entry_price = match state.entry_price {
            Some(v) => v,
            None => return,
        };
        let filled = match (fill_price, direction) {
            (Some(price), 1) => price <= entry_price,
            (Some(price), -1) => price >= entry_price,
            _ => false,
        };

        if filled {
            state.state = TradeState::PositionOpen;
            state.entry_price = fill_price.or(state.entry_price);
            state.entry_ts_ms = Some(now_ts_ms());
            state.pm_move_entry = Some(pm_move);
            let row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "ENTRY_FILLED".to_string(),
                direction,
                reason: None,
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: None,
                exit_price: None,
                gas_est_usd: None,
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: None,
                mfe: None,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(state, None),
            };
            self.emit_audit(row).await;
            self.log_alpha("ENTRY_FILLED", bar, state, state.pending_entry.as_ref());
            let position_row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "POSITION_OPEN".to_string(),
                direction,
                reason: None,
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: None,
                exit_price: None,
                gas_est_usd: None,
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: None,
                mfe: None,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(state, None),
            };
            self.emit_audit(position_row).await;
            self.log_alpha("POSITION_OPEN", bar, state, state.pending_entry.as_ref());
        }
    }

    async fn apply_entry_fills(
        &mut self,
        state: &mut PairTradeState,
        bar: &Bars1sPair,
        pm_move: f64,
        pm_mom: f64,
        ofi_value: f64,
        fills: Vec<TradeFill>,
    ) -> bool {
        if state.entry_order_id.is_none() && state.entry_client_id.is_none() {
            return false;
        }
        let (
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
        ) = Self::audit_health(Some(bar));
        let now_ms = now_ts_ms();
        for fill in fills {
            let order_match = state
                .entry_order_id
                .as_ref()
                .map(|id| fill.order_id.as_deref() == Some(id.as_str()))
                .unwrap_or(false);
            let client_match = state
                .entry_client_id
                .as_ref()
                .map(|id| fill.client_order_id.as_deref() == Some(id.as_str()))
                .unwrap_or(false);
            if !order_match && !client_match {
                continue;
            }
            if !Self::fill_side_matches(state.direction, &fill.side) {
                continue;
            }
            state.entry_cancel_pending = false;
            state.entry_cancel_reason = None;
            state.entry_cancel_req_id = None;
            state.state = TradeState::PositionOpen;
            if let Some(price) = fill.price {
                state.entry_price = Some(price);
            }
            if let Some(size) = fill.size {
                state.entry_qty = Some(size);
                state.entry_notional = state.entry_price.map(|p| p * size);
            }
            state.entry_ts_ms = fill.ts_ms.or(Some(now_ms));
            state.pm_move_entry = Some(pm_move);
            let row = TradeAuditRow {
                event_ts_ms: now_ms,
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "ENTRY_FILLED".to_string(),
                direction: state.direction,
                reason: None,
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: None,
                exit_price: None,
                gas_est_usd: None,
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: None,
                mfe: None,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(
                    state,
                    Some(json!({
                        "entry_order_id": state.entry_order_id.clone().unwrap_or_default(),
                        "entry_client_id": state.entry_client_id.clone().unwrap_or_default(),
                    })),
                ),
            };
            self.emit_audit(row).await;
            self.log_alpha("ENTRY_FILLED", bar, state, state.pending_entry.as_ref());
            let position_row = TradeAuditRow {
                event_ts_ms: now_ms,
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "POSITION_OPEN".to_string(),
                direction: state.direction,
                reason: None,
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: None,
                exit_price: None,
                gas_est_usd: None,
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: None,
                holding_ms: None,
                mae: None,
                mfe: None,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(state, None),
            };
            self.emit_audit(position_row).await;
            self.log_alpha("POSITION_OPEN", bar, state, state.pending_entry.as_ref());
            return true;
        }
        false
    }

    async fn request_entry_cancel(
        &mut self,
        state: &mut PairTradeState,
        bar: &Bars1sPair,
        pm_move: f64,
        pm_mom: f64,
        ofi_value: f64,
        reason: &str,
        pair_key: (i64, TokenSide),
    ) {
        if !state.entry_cancel_pending {
            state.entry_cancel_pending = true;
            state.entry_cancel_reason = Some(reason.to_string());
            self.emit_cancel_snapshot(
                state,
                bar,
                reason,
                "ENTRY",
                state.entry_order_id.clone(),
                state.entry_client_id.clone(),
                pm_move,
                ofi_value,
            )
            .await;
        } else if state.entry_cancel_reason.is_none() {
            state.entry_cancel_reason = Some(reason.to_string());
        }
        if !self.is_live() {
            self.finalize_entry_cancel(state, bar, pm_move, pm_mom, ofi_value, reason, pair_key)
                .await;
            return;
        }
        self.maybe_enqueue_entry_cancel(state);
    }

    async fn finalize_entry_cancel(
        &mut self,
        state: &mut PairTradeState,
        bar: &Bars1sPair,
        pm_move: f64,
        pm_mom: f64,
        ofi_value: f64,
        reason: &str,
        pair_key: (i64, TokenSide),
    ) {
        let (
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
        ) = Self::audit_health(Some(bar));
        if let Some(notional) = state.entry_notional.take() {
            let exposure_key = (pair_key.0, pair_key.1, state.direction);
            if let Some(alpha_type) = state.alpha_type {
                self.release_exposure(exposure_key, notional, alpha_type);
            } else {
                self.release_exposure(exposure_key, notional, AlphaType::GapB);
            }
        }
        let row = TradeAuditRow {
            event_ts_ms: now_ts_ms(),
            pair_id: bar.pair_id,
            token_side: bar.token_side,
            phase: "ENTRY_CANCEL".to_string(),
            direction: state.direction,
            reason: Some(reason.to_string()),
            pm_move: Some(pm_move),
            pm_mom: Some(pm_mom),
            ofi_250ms: Some(ofi_value),
            edge_raw: state.edge_raw,
            edge_norm: state.edge_norm,
            entry_price: state.entry_price,
            entry_qty: state.entry_qty,
            exit_price_target: None,
            exit_price: None,
            gas_est_usd: None,
            min_profit_usd: state.min_profit_usd,
            book_decay: state.book_decay,
            book_support: state.book_support,
            book_scale: state.book_scale,
            retrace_ratio: None,
            inv_ratio: None,
            inv_factor: None,
            decay_factor: self.entry_decay_factor(state),
            pnl_usd_net: None,
            holding_ms: None,
            mae: state.mae,
            mfe: state.mfe,
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
            detail_json: self.detail_json_for_state(state, None),
        };
        self.emit_audit(row).await;
        state.enter_cooldown(bar.bar_second, self.cfg.cooldown_after_cancel_ms);
        let cooldown_row = TradeAuditRow {
            event_ts_ms: now_ts_ms(),
            pair_id: bar.pair_id,
            token_side: bar.token_side,
            phase: "COOLDOWN".to_string(),
            direction: state.direction,
            reason: Some(format!("after_cancel:{reason}")),
            pm_move: Some(pm_move),
            pm_mom: Some(pm_mom),
            ofi_250ms: Some(ofi_value),
            edge_raw: state.edge_raw,
            edge_norm: state.edge_norm,
            entry_price: state.entry_price,
            entry_qty: state.entry_qty,
            exit_price_target: None,
            exit_price: None,
            gas_est_usd: None,
            min_profit_usd: state.min_profit_usd,
            book_decay: state.book_decay,
            book_support: state.book_support,
            book_scale: state.book_scale,
            retrace_ratio: None,
            inv_ratio: None,
            inv_factor: None,
            decay_factor: self.entry_decay_factor(state),
            pnl_usd_net: None,
            holding_ms: None,
            mae: state.mae,
            mfe: state.mfe,
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
            detail_json: self.detail_json_for_state(state, None),
        };
        self.emit_audit(cooldown_row).await;
    }

    async fn apply_exit_fills(
        &mut self,
        state: &mut PairTradeState,
        bar: &Bars1sPair,
        pm_move: f64,
        pm_mom: f64,
        ofi_value: f64,
        gas_est: f64,
        fills: Vec<TradeFill>,
    ) {
        if state.exit_order_id.is_none() && state.exit_client_id.is_none() {
            return;
        }
        let (
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
        ) = Self::audit_health(Some(bar));
        let now_ms = now_ts_ms();
        for fill in fills {
            let order_match = state
                .exit_order_id
                .as_ref()
                .map(|id| fill.order_id.as_deref() == Some(id.as_str()))
                .unwrap_or(false);
            let client_match = state
                .exit_client_id
                .as_ref()
                .map(|id| fill.client_order_id.as_deref() == Some(id.as_str()))
                .unwrap_or(false);
            if !order_match && !client_match {
                continue;
            }
            if !Self::fill_side_matches(-state.direction, &fill.side) {
                continue;
            }
            state.exit_cancel_pending = false;
            state.exit_cancel_reason = None;
            state.exit_cancel_req_id = None;
            let entry_price = match state.entry_price {
                Some(price) => price,
                None => return,
            };
            let qty = match state.entry_qty {
                Some(qty) => qty,
                None => return,
            };
            let exit_price_final = fill.price.or(state.exit_price).unwrap_or(entry_price);
            let pnl = state.direction as f64 * (exit_price_final - entry_price) * qty;
            let fee_bps = self.fee_bps(Venue::Opinion, now_ts_ms());
            let fee = fee_bps / 10_000.0 * (entry_price * qty + exit_price_final * qty);
            let pnl_net = pnl - fee - gas_est;
            let holding_ms = state
                .entry_ts_ms
                .map(|ts| (now_ts_ms() - ts).max(0));
            let row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "EXIT_FILLED".to_string(),
                direction: state.direction,
                reason: None,
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: state.exit_price_target,
                exit_price: Some(exit_price_final),
                gas_est_usd: Some(gas_est),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: Some(pnl_net),
                holding_ms,
                mae: state.mae,
                mfe: state.mfe,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(
                    state,
                    Some(json!({
                        "exit_order_id": state.exit_order_id.clone().unwrap_or_default(),
                        "exit_client_id": state.exit_client_id.clone().unwrap_or_default(),
                    })),
                ),
            };
            self.emit_audit(row).await;

            if let Some(notional) = state.entry_notional.take() {
                let exposure_key = (bar.pair_id, bar.token_side, state.direction);
                if let Some(alpha_type) = state.alpha_type {
                    self.release_exposure(exposure_key, notional, alpha_type);
                } else {
                    self.release_exposure(exposure_key, notional, AlphaType::GapB);
                }
            }
            state.enter_cooldown(bar.bar_second, self.cfg.cooldown_after_exit_ms);
            let cooldown_row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "COOLDOWN".to_string(),
                direction: state.direction,
                reason: Some("after_exit".to_string()),
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: state.exit_price_target,
                exit_price: Some(exit_price_final),
                gas_est_usd: Some(gas_est),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio: None,
                inv_ratio: None,
                inv_factor: None,
                decay_factor: None,
                pnl_usd_net: Some(pnl_net),
                holding_ms,
                mae: state.mae,
                mfe: state.mfe,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(state, None),
            };
            self.emit_audit(cooldown_row).await;
            return;
        }
    }

    async fn check_exit(
        &mut self,
        state: &mut PairTradeState,
        bar: &Bars1sPair,
        pm_move: f64,
        pm_mom: f64,
        ofi_value: f64,
        base_pm: f64,
        pair_key: (i64, TokenSide),
    ) {
        let direction = state.direction;
        let entry_price = match state.entry_price {
            Some(price) => price,
            None => return,
        };
        let qty = match state.entry_qty {
            Some(qty) => qty,
            None => return,
        };
        let (
            obs_latency_poly_p99_ms,
            obs_latency_opi_p99_ms,
            bar_gap_flag,
            poly_bar_gap_flag,
            opi_bar_gap_flag,
            poly_staleness_ms,
            opi_staleness_ms,
        ) = Self::audit_health(Some(bar));

        let current_price = opi_mid_from_bar(bar);
        if let Some(price) = current_price {
            let pnl = direction as f64 * (price - entry_price) * qty;
            state.mae = Some(state.mae.map(|v| v.min(pnl)).unwrap_or(pnl));
            state.mfe = Some(state.mfe.map(|v| v.max(pnl)).unwrap_or(pnl));
        }

        let gas_est = self.gas_est_usd();
        let (target_price, inv_ratio, inv_factor, decay_factor) =
            self.compute_exit_target(state, pm_move, pm_mom, base_pm, gas_est);
        if let Some(target) = target_price {
            state.exit_price_target = Some(target);
        }

        let retrace_ratio = self.retrace_ratio(pm_move, state.pm_peak, direction);
        let mut target = state.exit_price_target.unwrap_or(entry_price);
        let now_ms = now_ts_ms();
        let ttl_expired = self.cfg.exit_ttl_ms > 0
            && state
                .last_exit_place_ms
                .map(|last| now_ms.saturating_sub(last) >= self.cfg.exit_ttl_ms)
                .unwrap_or(false);

        let buy_notional = bar.poly_buy_notional_250ms.unwrap_or(0.0);
        let sell_notional = bar.poly_sell_notional_250ms.unwrap_or(0.0);
        let max_trade = bar.poly_max_trade_notional_250ms.unwrap_or(0.0);
        let bid_delta = bar.poly_bid_delta_notional_250ms.unwrap_or(0.0);
        let ask_delta = bar.poly_ask_delta_notional_250ms.unwrap_or(0.0);
        let mid_slope = bar.poly_mid_slope_250ms.unwrap_or(0.0);
        let ofi_low_trigger = if direction > 0 {
            ofi_value <= self.cfg.ofi_cancel_low
        } else {
            ofi_value >= -self.cfg.ofi_cancel_low
        };
        let ofi_med_trigger = if direction > 0 {
            ofi_value <= self.cfg.ofi_cancel_med
        } else {
            ofi_value >= -self.cfg.ofi_cancel_med
        };
        let mom_bad = if direction > 0 { pm_mom < 0.0 } else { pm_mom > 0.0 };
        let ask_add_trigger = if direction > 0 {
            ask_delta >= self.cfg.ask_add_usd && ofi_low_trigger
        } else {
            bid_delta >= self.cfg.ask_add_usd && ofi_low_trigger
        };
        let slope_against = if direction > 0 {
            mid_slope < 0.0
        } else {
            mid_slope > 0.0
        };
        let aggressive_flow = max_trade >= self.cfg.trade_cancel_usd
            || (direction > 0 && sell_notional >= self.cfg.trade_cancel_usd)
            || (direction < 0 && buy_notional >= self.cfg.trade_cancel_usd)
            || (ofi_low_trigger && (mom_bad || slope_against))
            || ask_add_trigger;
        let bid_withdraw = if direction > 0 {
            bid_delta <= -self.cfg.bid_withdraw_usd && ofi_med_trigger
        } else {
            ask_delta <= -self.cfg.bid_withdraw_usd && ofi_med_trigger
        };
        let book_bad = self
            .book_shape(bar, direction)
            .map(|shape| shape.scale <= 0.0)
            .unwrap_or(true);
        let retrace_hit = retrace_ratio
            .map(|ratio| ratio >= self.cfg.retrace_exit)
            .unwrap_or(false);

        let reverse_shock = state.alpha_type == Some(AlphaType::ShockA)
            && self.reverse_shock_active(pair_key, bar.bar_second, direction);
        let mut exit_reason = None;
        if reverse_shock {
            exit_reason = Some("reverse_shock");
        } else if aggressive_flow {
            exit_reason = Some("aggressive_flow");
        } else if retrace_hit {
            exit_reason = Some("retrace_exit");
        } else if bid_withdraw {
            exit_reason = Some("bid_withdraw");
        } else if book_bad {
            exit_reason = Some("book_shape_deterioration");
        }
        if exit_reason.is_none() && ttl_expired {
            exit_reason = Some("ttl");
        }
        if exit_reason.is_some() && exit_reason != Some("ttl") {
            let edge = (target - entry_price) * 0.7;
            target = entry_price + edge;
        }

        let tick = if self.cfg.tick_px > 0.0 {
            self.cfg.tick_px
        } else {
            1e-4
        };
        let exit_offset = self.cfg.exit_tick_offset.max(0) as f64 * tick;
        let exit_price = if direction > 0 {
            let mut candidate = opi_best_ask_from_bar(bar)
                .map(|ask| ask - exit_offset)
                .unwrap_or(target);
            if let Some(bid) = opi_best_bid_from_bar(bar) {
                if candidate <= bid {
                    candidate = bid + tick;
                }
            }
            candidate.max(target)
        } else {
            let mut candidate = opi_best_bid_from_bar(bar)
                .map(|bid| bid + exit_offset)
                .unwrap_or(target);
            if let Some(ask) = opi_best_ask_from_bar(bar) {
                if candidate >= ask {
                    candidate = ask - tick;
                }
            }
            candidate.min(target)
        };
        let prev_exit_price = state.exit_price;
        state.exit_price = Some(exit_price);
        let reprice_needed = prev_exit_price
            .map(|prev| (prev - exit_price).abs() >= tick)
            .unwrap_or(true)
            || ttl_expired;
        let can_reprice = state
            .last_exit_place_ms
            .map(|last| now_ms - last >= self.order_poll_interval_ms)
            .unwrap_or(true);

        if self.is_live() && state.exit_cancel_pending {
            self.maybe_enqueue_exit_cancel(state);
            if state.exit_place_req_id.is_some() {
                return;
            }
            self.maybe_enqueue_fill_poll(state, pair_key, FillKind::Exit);
            return;
        }

        if exit_reason.is_some()
            && state.state == TradeState::ExitWorking
            && state.exit_price_target.map(|v| (v - target).abs() > 1e-9).unwrap_or(true)
        {
            let row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "EXIT_ACCELERATE".to_string(),
                direction,
                reason: exit_reason.map(|v| v.to_string()),
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: Some(target),
                exit_price: Some(exit_price),
                gas_est_usd: Some(gas_est),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio,
                inv_ratio,
                inv_factor,
                decay_factor,
                pnl_usd_net: None,
                holding_ms: None,
                mae: state.mae,
                mfe: state.mfe,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(state, None),
            };
            self.emit_audit(row).await;
        }

        if self.is_live()
            && state.state == TradeState::ExitWorking
            && reprice_needed
            && can_reprice
            && state.exit_place_req_id.is_none()
        {
            let has_exit_order = state.exit_order_id.is_some() || state.exit_client_id.is_some();
            if has_exit_order {
                let cancel_reason = exit_reason.unwrap_or("reprice");
                if !state.exit_cancel_pending {
                    state.exit_cancel_pending = true;
                    state.exit_cancel_reason = Some(cancel_reason.to_string());
                    self.emit_cancel_snapshot(
                        state,
                        bar,
                        cancel_reason,
                        "EXIT",
                        state.exit_order_id.clone(),
                        state.exit_client_id.clone(),
                        pm_move,
                        ofi_value,
                    )
                    .await;
                }
                self.maybe_enqueue_exit_cancel(state);
                self.maybe_enqueue_fill_poll(state, pair_key, FillKind::Exit);
                return;
            }
        }

        let needs_exit_order = state.state == TradeState::ExitWorking
            && state.exit_order_id.is_none()
            && state.exit_client_id.is_none();
        if self.is_live() && needs_exit_order && can_reprice {
            state.last_exit_place_ms = Some(now_ms);
            let side = if direction > 0 {
                OrderSide::Sell
            } else {
                OrderSide::Buy
            };
            let (order, client_id_used, price_used, chase_count, post_only_rejects, last_err) =
                self.place_exit_with_chase(
                    bar.pair_id,
                    bar.token_side,
                    pair_key,
                    side,
                    exit_price,
                    qty,
                    bar,
                    direction,
                    tick,
                )
                .await;
            if let Some(order) = order {
                state.exit_order_id = Some(order.order_id);
                state.exit_client_id = order.client_order_id.or(client_id_used);
                if let Some(price_used) = price_used {
                    state.exit_price = Some(price_used);
                }
            } else {
                let err_msg = last_err
                    .as_ref()
                    .map(|err| err.to_string())
                    .unwrap_or_else(|| "exit_retry_failed".to_string());
                let row = TradeAuditRow {
                    event_ts_ms: now_ms,
                    pair_id: bar.pair_id,
                    token_side: bar.token_side,
                    phase: "EXIT_RETRY_FAILED".to_string(),
                    direction,
                    reason: Some(err_msg),
                    pm_move: Some(pm_move),
                    pm_mom: Some(pm_mom),
                    ofi_250ms: Some(ofi_value),
                    edge_raw: state.edge_raw,
                    edge_norm: state.edge_norm,
                    entry_price: state.entry_price,
                    entry_qty: state.entry_qty,
                    exit_price_target: Some(target),
                    exit_price: Some(exit_price),
                    gas_est_usd: Some(gas_est),
                    min_profit_usd: state.min_profit_usd,
                    book_decay: state.book_decay,
                    book_support: state.book_support,
                    book_scale: state.book_scale,
                    retrace_ratio,
                    inv_ratio,
                    inv_factor,
                    decay_factor,
                    pnl_usd_net: None,
                    holding_ms: None,
                    mae: state.mae,
                    mfe: state.mfe,
                    obs_latency_poly_p99_ms,
                    obs_latency_opi_p99_ms,
                    bar_gap_flag,
                    poly_bar_gap_flag,
                    opi_bar_gap_flag,
                    poly_staleness_ms,
                    opi_staleness_ms,
                    detail_json: self.detail_json_for_state(
                        state,
                        Some(json!({
                            "exit_chase_count": chase_count,
                            "exit_post_only_rejects": post_only_rejects
                        })),
                    ),
                };
                self.emit_audit(row).await;
            }
        }

        if state.state != TradeState::ExitWorking {
            state.state = TradeState::ExitWorking;
            let row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "EXIT_WORKING".to_string(),
                direction,
                reason: exit_reason.map(|v| v.to_string()),
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: Some(target),
                exit_price: Some(exit_price),
                gas_est_usd: Some(gas_est),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio,
                inv_ratio,
                inv_factor,
                decay_factor,
                pnl_usd_net: None,
                holding_ms: None,
                mae: state.mae,
                mfe: state.mfe,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(state, None),
            };
            self.emit_audit(row).await;
            if self.is_live() {
                state.last_exit_place_ms = Some(now_ms);
                let side = if direction > 0 {
                    OrderSide::Sell
                } else {
                    OrderSide::Buy
                };
                let (order, client_id_used, price_used, chase_count, post_only_rejects, last_err) =
                    self.place_exit_with_chase(
                        bar.pair_id,
                        bar.token_side,
                        pair_key,
                        side,
                        exit_price,
                        qty,
                        bar,
                        direction,
                        tick,
                    )
                    .await;
                if let Some(order) = order {
                    state.exit_order_id = Some(order.order_id);
                    state.exit_client_id = order.client_order_id.or(client_id_used);
                    if let Some(price_used) = price_used {
                        state.exit_price = Some(price_used);
                    }
                } else {
                    state.last_exit_place_ms = Some(now_ms);
                    let err_msg = last_err
                        .as_ref()
                        .map(|err| err.to_string())
                        .unwrap_or_else(|| "exit_place_failed".to_string());
                    let row = TradeAuditRow {
                        event_ts_ms: now_ts_ms(),
                        pair_id: bar.pair_id,
                        token_side: bar.token_side,
                        phase: "EXIT_PLACE_FAILED".to_string(),
                        direction,
                        reason: Some(err_msg),
                        pm_move: Some(pm_move),
                        pm_mom: Some(pm_mom),
                        ofi_250ms: Some(ofi_value),
                        edge_raw: state.edge_raw,
                        edge_norm: state.edge_norm,
                        entry_price: state.entry_price,
                        entry_qty: state.entry_qty,
                        exit_price_target: Some(target),
                        exit_price: Some(exit_price),
                        gas_est_usd: Some(gas_est),
                        min_profit_usd: state.min_profit_usd,
                        book_decay: state.book_decay,
                        book_support: state.book_support,
                        book_scale: state.book_scale,
                        retrace_ratio,
                        inv_ratio,
                        inv_factor,
                        decay_factor,
                        pnl_usd_net: None,
                        holding_ms: None,
                        mae: state.mae,
                        mfe: state.mfe,
                        obs_latency_poly_p99_ms,
                        obs_latency_opi_p99_ms,
                        bar_gap_flag,
                        poly_bar_gap_flag,
                        opi_bar_gap_flag,
                        poly_staleness_ms,
                        opi_staleness_ms,
                        detail_json: self.detail_json_for_state(
                            state,
                            Some(json!({
                                "exit_chase_count": chase_count,
                                "exit_post_only_rejects": post_only_rejects
                            })),
                        ),
                    };
                    self.emit_audit(row).await;
                }
            }
        }

        if self.is_live() {
            if state.exit_place_req_id.is_some() {
                return;
            }
            self.maybe_enqueue_fill_poll(state, pair_key, FillKind::Exit);
            return;
        }

        let fill_price = if direction > 0 {
            opi_best_bid_from_bar(bar)
                .or(bar.opi_last_price)
                .or(opi_mid_from_bar(bar))
        } else {
            opi_best_ask_from_bar(bar)
                .or(bar.opi_last_price)
                .or(opi_mid_from_bar(bar))
        };
        let filled = match (fill_price, direction) {
            (Some(price), 1) => price >= exit_price,
            (Some(price), -1) => price <= exit_price,
            _ => false,
        };

        if filled {
            let exit_price_final = fill_price.unwrap_or(exit_price);
            let pnl = direction as f64 * (exit_price_final - entry_price) * qty;
            let fee_bps = self.fee_bps(Venue::Opinion, now_ts_ms());
            let fee = fee_bps / 10_000.0 * (entry_price * qty + exit_price_final * qty);
            let pnl_net = pnl - fee - gas_est;
            let holding_ms = state
                .entry_ts_ms
                .map(|ts| (now_ts_ms() - ts).max(0));
            let row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "EXIT_FILLED".to_string(),
                direction,
                reason: None,
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: Some(target),
                exit_price: Some(exit_price_final),
                gas_est_usd: Some(gas_est),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio,
                inv_ratio,
                inv_factor,
                decay_factor,
                pnl_usd_net: Some(pnl_net),
                holding_ms,
                mae: state.mae,
                mfe: state.mfe,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(state, None),
            };
            self.emit_audit(row).await;

            if let Some(notional) = state.entry_notional.take() {
                let exposure_key = (pair_key.0, pair_key.1, direction);
                if let Some(alpha_type) = state.alpha_type {
                    self.release_exposure(exposure_key, notional, alpha_type);
                } else {
                    self.release_exposure(exposure_key, notional, AlphaType::GapB);
                }
            }
            state.enter_cooldown(bar.bar_second, self.cfg.cooldown_after_exit_ms);
            let cooldown_row = TradeAuditRow {
                event_ts_ms: now_ts_ms(),
                pair_id: bar.pair_id,
                token_side: bar.token_side,
                phase: "COOLDOWN".to_string(),
                direction,
                reason: Some("after_exit".to_string()),
                pm_move: Some(pm_move),
                pm_mom: Some(pm_mom),
                ofi_250ms: Some(ofi_value),
                edge_raw: state.edge_raw,
                edge_norm: state.edge_norm,
                entry_price: state.entry_price,
                entry_qty: state.entry_qty,
                exit_price_target: Some(target),
                exit_price: Some(exit_price_final),
                gas_est_usd: Some(gas_est),
                min_profit_usd: state.min_profit_usd,
                book_decay: state.book_decay,
                book_support: state.book_support,
                book_scale: state.book_scale,
                retrace_ratio,
                inv_ratio,
                inv_factor,
                decay_factor,
                pnl_usd_net: Some(pnl_net),
                holding_ms,
                mae: state.mae,
                mfe: state.mfe,
                obs_latency_poly_p99_ms,
                obs_latency_opi_p99_ms,
                bar_gap_flag,
                poly_bar_gap_flag,
                opi_bar_gap_flag,
                poly_staleness_ms,
                opi_staleness_ms,
                detail_json: self.detail_json_for_state(state, None),
            };
            self.emit_audit(cooldown_row).await;
        }
    }
}
