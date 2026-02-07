use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Venue {
    Polymarket,
    Opinion,
}

impl Venue {
    pub fn as_str(&self) -> &'static str {
        match self {
            Venue::Polymarket => "pm",
            Venue::Opinion => "opi",
        }
    }
}

impl Default for Venue {
    fn default() -> Self {
        Venue::Polymarket
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum TokenSide {
    Yes,
    No,
    Unknown,
}

impl TokenSide {
    pub fn as_str(&self) -> &'static str {
        match self {
            TokenSide::Yes => "YES",
            TokenSide::No => "NO",
            TokenSide::Unknown => "UNKNOWN",
        }
    }
}

impl Default for TokenSide {
    fn default() -> Self {
        TokenSide::Unknown
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExchangeTsSource {
    VenueWs = 1,
    VenueRest = 2,
    Estimated = 3,
    LocalFallback = 4,
}

impl Default for ExchangeTsSource {
    fn default() -> Self {
        ExchangeTsSource::LocalFallback
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum EventKind {
    BookSnapshot,
    BookDelta,
    Trade,
    LastPrice,
    TickSizeChange,
    Health,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DeltaType {
    Add,
    Cancel,
    Match,
    AddOrUpdate,
    Unknown,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventFlags {
    pub ts_missing: bool,
    pub ts_anomaly: bool,
    pub out_of_order: bool,
    pub trade_side_missing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookLevel {
    pub price: f64,
    pub size: f64,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EventPayload {
    pub side: Option<BookSide>,
    pub delta_type: Option<DeltaType>,
    pub price: Option<f64>,
    pub size: Option<f64>,
    pub bids: Option<Vec<BookLevel>>,
    pub asks: Option<Vec<BookLevel>>,
    pub hash: Option<String>,
    pub best_bid: Option<f64>,
    pub best_ask: Option<f64>,
    pub tick_size: Option<f64>,
    pub outcome_side: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Event {
    pub venue: Venue,
    pub token_key: String,
    pub pair_ids: SmallVec<[i64; 4]>,
    pub token_side: TokenSide,
    pub kind: EventKind,
    pub channel: Option<String>,
    pub exchange_ts_ms: i64,
    pub local_ts_ms: i64,
    pub exchange_ts_source: ExchangeTsSource,
    pub flags: EventFlags,
    pub payload: EventPayload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PairRecord {
    pub pair_id: i64,
    pub watchlist_version: i64,
    pub status: String,
    pub root_market_title: Option<String>,
    pub polymarket_event_title: Option<String>,
    pub opinion_market_id: Option<String>,
    pub opinion_yes_token_id: Option<String>,
    pub opinion_no_token_id: Option<String>,
    pub polymarket_market_id: Option<String>,
    pub polymarket_yes_token_id: Option<String>,
    pub polymarket_no_token_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TokenRegistration {
    pub token_key: String,
    pub venue: Venue,
    pub external_token_id: Option<String>,
    pub market_id: Option<String>,
    pub outcome_side: Option<i32>,
    pub token_side: TokenSide,
    pub pair_ids: SmallVec<[i64; 4]>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Bars1sToken {
    pub venue: Venue,
    pub token_key: String,
    pub token_side: TokenSide,
    pub bar_second: i64,
    pub exchange_ts_source: ExchangeTsSource,
    pub obs_latency_venue_p50_ms: Option<i64>,
    pub obs_latency_venue_p90_ms: Option<i64>,
    pub obs_latency_venue_p99_ms: Option<i64>,
    pub obs_latency_venue_max_ms: Option<i64>,
    pub obs_latency_est_p50_ms: Option<i64>,
    pub obs_latency_est_p90_ms: Option<i64>,
    pub obs_latency_est_p99_ms: Option<i64>,
    pub obs_latency_est_max_ms: Option<i64>,
    pub connection_health_flags: i64,
    pub bar_gap_flag: i64,
    pub bar_stale_flag: i64,
    pub staleness_ms: Option<i64>,
    pub event_count_1s: i64,
    pub ts_missing_count_1s: i64,
    pub ts_anomaly_count_1s: i64,
    pub book_resync_flag: i64,
    pub best_bid_px: Option<f64>,
    pub best_ask_px: Option<f64>,
    pub mid_px: Option<f64>,
    pub best_bid_px_state: Option<f64>,
    pub best_ask_px_state: Option<f64>,
    pub mid_px_state: Option<f64>,
    pub last_price: Option<f64>,
    pub micro_px: Option<f64>,
    pub spread_px: Option<f64>,
    pub tick_px: Option<f64>,
    pub spread_ticks: Option<i64>,
    pub top3_depth_bid_notional: Option<f64>,
    pub top3_depth_ask_notional: Option<f64>,
    pub bid_l1_notional: Option<f64>,
    pub bid_l2_notional: Option<f64>,
    pub bid_l3_notional: Option<f64>,
    pub ask_l1_notional: Option<f64>,
    pub ask_l2_notional: Option<f64>,
    pub ask_l3_notional: Option<f64>,
    pub top10_depth_bid_notional: Option<f64>,
    pub top10_depth_ask_notional: Option<f64>,
    pub depth_1pct_bid_notional: Option<f64>,
    pub depth_1pct_ask_notional: Option<f64>,
    pub imbalance_top10: Option<f64>,
    pub air_pocket_bid_ticks: Option<i64>,
    pub air_pocket_ask_ticks: Option<i64>,
    pub book_updates_1s: i64,
    pub add_updates_1s: i64,
    pub cancel_updates_1s: i64,
    pub match_updates_1s: i64,
    pub remove_unknown_updates_1s: i64,
    pub trade_count_1s: i64,
    pub volume_shares_1s: f64,
    pub volume_notional_1s: f64,
    pub vwap_1s: Option<f64>,
    pub buy_notional_1s: f64,
    pub sell_notional_1s: f64,
    pub cvd_delta_1s: f64,
    pub trade_side_missing_count_1s: i64,
    pub ofi_1s: f64,
    pub ofi_250ms: f64,
    pub bid_delta_notional_250ms: f64,
    pub ask_delta_notional_250ms: f64,
    pub mid_slope_250ms: Option<f64>,
    pub ofi_10s: Option<f64>,
    pub ofi_z_30s: Option<f64>,
    pub levels_crossed_1s: i64,
    pub max_trade_notional_1s: f64,
    pub buy_notional_250ms: f64,
    pub sell_notional_250ms: f64,
    pub max_trade_notional_250ms: f64,
    pub depth_withdrawal_ratio: Option<f64>,
    pub liquidity_pull_flag: i64,
    pub venue_book_hash: Option<String>,
    pub local_topn_hash: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Bars1sPair {
    pub pair_id: i64,
    pub token_side: TokenSide,
    pub bar_second: i64,
    pub poly_bar_gap_flag: i64,
    pub opi_bar_gap_flag: i64,
    pub bar_gap_flag: i64,
    pub poly_bar_stale_flag: i64,
    pub opi_bar_stale_flag: i64,
    pub bar_stale_flag: i64,
    pub poly_staleness_ms: Option<i64>,
    pub opi_staleness_ms: Option<i64>,
    pub poly_best_bid: Option<f64>,
    pub poly_best_ask: Option<f64>,
    pub poly_mid: Option<f64>,
    pub poly_micro: Option<f64>,
    pub poly_best_bid_state: Option<f64>,
    pub poly_best_ask_state: Option<f64>,
    pub poly_mid_state: Option<f64>,
    pub opi_best_bid: Option<f64>,
    pub opi_best_ask: Option<f64>,
    pub opi_mid: Option<f64>,
    pub opi_best_bid_state: Option<f64>,
    pub opi_best_ask_state: Option<f64>,
    pub opi_mid_state: Option<f64>,
    pub opi_last_price: Option<f64>,
    pub opi_taker_buy_notional_1s: Option<f64>,
    pub opi_taker_sell_notional_1s: Option<f64>,
    pub opi_micro: Option<f64>,
    pub opi_bid_l1_notional: Option<f64>,
    pub opi_bid_l2_notional: Option<f64>,
    pub opi_bid_l3_notional: Option<f64>,
    pub opi_ask_l1_notional: Option<f64>,
    pub opi_ask_l2_notional: Option<f64>,
    pub opi_ask_l3_notional: Option<f64>,
    pub poly_fee_bps: Option<i64>,
    pub opi_fee_bps: Option<i64>,
    pub gas_cost: Option<f64>,
    pub network_congestion_flag: i64,
    pub gross_spread_buy: Option<f64>,
    pub gross_spread_sell: Option<f64>,
    pub true_net_spread_buy_opinion: Option<f64>,
    pub true_net_spread_sell_opinion: Option<f64>,
    pub arb_mode_flag: i64,
    pub directional_mode_flag: i64,
    pub poly_cvd_30s: Option<f64>,
    pub poly_buy_sell_ratio_30s: Option<f64>,
    pub price_pressure_index: Option<f64>,
    pub poly_ofi_250ms: Option<f64>,
    pub poly_buy_notional_1s: Option<f64>,
    pub poly_sell_notional_1s: Option<f64>,
    pub poly_max_trade_notional_1s: Option<f64>,
    pub poly_buy_notional_250ms: Option<f64>,
    pub poly_sell_notional_250ms: Option<f64>,
    pub poly_max_trade_notional_250ms: Option<f64>,
    pub poly_bid_delta_notional_250ms: Option<f64>,
    pub poly_ask_delta_notional_250ms: Option<f64>,
    pub poly_mid_slope_250ms: Option<f64>,
    pub poly_obs_latency_p99_ms: Option<i64>,
    pub opi_obs_latency_p99_ms: Option<i64>,
    pub rolling_corr_5m: Option<f64>,
    pub rolling_corr_samples_5m: Option<i64>,
    pub estimated_lag_s: Option<i64>,
    pub lag_confidence: Option<f64>,
    pub beta_1m: Option<f64>,
    pub follow_score_raw: Option<f64>,
    pub follow_score_adj: Option<f64>,
    pub poly_depth_imbalance_top10: Option<f64>,
    pub poly_book_pressure_250ms: Option<f64>,
    pub poly_spread_bps: Option<f64>,
    pub poly_micro_bias_bps: Option<f64>,
    pub book_alpha_raw: Option<f64>,
    pub book_alpha_adj: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClockOffsetSample {
    pub venue: Venue,
    pub local_ts_ms: i64,
    pub exchange_ts_ms: i64,
    pub sample_offset_ms: i64,
    pub smoothed_offset_ms: i64,
    pub delta_clamped_ms: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainMetricsRow {
    pub chain: String,
    pub local_ts_ms: i64,
    pub base_fee: Option<f64>,
    pub priority_fee: Option<f64>,
    pub gas_used_est: Option<f64>,
    pub tx_cost_est: Option<f64>,
    pub congestion_flag: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ShockStatus {
    Pending,
    Final,
}

impl ShockStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            ShockStatus::Pending => "PENDING",
            ShockStatus::Final => "FINAL",
        }
    }
}

impl Default for ShockStatus {
    fn default() -> Self {
        ShockStatus::Pending
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShockEvent {
    pub shock_id: i64,
    pub token_key: String,
    pub pair_id: Option<i64>,
    pub token_side: TokenSide,
    pub shock_type: String,
    pub direction: i64,
    pub noise_flag: i64,
    pub start_ts_ms: i64,
    pub trigger_ts_ms: i64,
    pub peak_ts_ms: i64,
    pub magnitude: f64,
    pub context_json: Option<String>,
    pub obs_latency_ms: Option<i64>,
    pub connection_health_flags: i64,
    pub status: ShockStatus,
    pub peak_finalized: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShockEventUpdate {
    pub shock_id: i64,
    pub start_ts_ms: i64,
    pub peak_ts_ms: i64,
    pub magnitude: f64,
    pub context_json: Option<String>,
    pub direction: i64,
    pub noise_flag: i64,
    pub status: ShockStatus,
    pub peak_finalized: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpinionResponseRow {
    pub shock_id: i64,
    pub horizon_s: i64,
    pub sample_valid_flag: i64,
    pub move_flag: i64,
    pub first_move_lag_s: Option<i64>,
    pub opi_return: Option<f64>,
    pub opi_price_delta: Option<f64>,
    pub opi_volume_sum: Option<f64>,
    pub opi_cvd_sum: Option<f64>,
    pub opi_spread_change: Option<f64>,
    pub opi_depth_change: Option<f64>,
    pub opi_staleness_at_t0_ms: Option<i64>,
    pub opi_liquidity_pull_flag_at_t0: Option<i64>,
    pub true_net_spread_at_t0: Option<f64>,
    pub arb_mode_flag_at_t0: Option<i64>,
    pub estimated_lag_s_at_t0: Option<i64>,
    pub lag_confidence_at_t0: Option<f64>,
    pub obs_latency_poly_p99_at_t0: Option<i64>,
    pub obs_latency_opi_p99_at_t0: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TradeAuditRow {
    pub event_ts_ms: i64,
    pub pair_id: i64,
    pub token_side: TokenSide,
    pub phase: String,
    pub direction: i64,
    pub reason: Option<String>,
    pub pm_move: Option<f64>,
    pub pm_mom: Option<f64>,
    pub ofi_250ms: Option<f64>,
    pub edge_raw: Option<f64>,
    pub edge_norm: Option<f64>,
    pub entry_price: Option<f64>,
    pub entry_qty: Option<f64>,
    pub exit_price_target: Option<f64>,
    pub exit_price: Option<f64>,
    pub gas_est_usd: Option<f64>,
    pub min_profit_usd: Option<f64>,
    pub book_decay: Option<f64>,
    pub book_support: Option<f64>,
    pub book_scale: Option<f64>,
    pub retrace_ratio: Option<f64>,
    pub inv_ratio: Option<f64>,
    pub inv_factor: Option<f64>,
    pub decay_factor: Option<f64>,
    pub pnl_usd_net: Option<f64>,
    pub holding_ms: Option<i64>,
    pub mae: Option<f64>,
    pub mfe: Option<f64>,
    pub obs_latency_poly_p99_ms: Option<i64>,
    pub obs_latency_opi_p99_ms: Option<i64>,
    pub bar_gap_flag: Option<i64>,
    pub poly_bar_gap_flag: Option<i64>,
    pub opi_bar_gap_flag: Option<i64>,
    pub poly_staleness_ms: Option<i64>,
    pub opi_staleness_ms: Option<i64>,
    pub detail_json: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelTriggerSnapshotRow {
    pub event_ts_ms: i64,
    pub pair_id: i64,
    pub token_side: TokenSide,
    pub direction: i64,
    pub order_role: String,
    pub order_id: Option<String>,
    pub client_id: Option<String>,
    pub cancel_reason: String,
    pub cancel_priority: i64,
    pub ofi_250ms: Option<f64>,
    pub pm_sell_trade_notional_250ms: Option<f64>,
    pub pm_retrace_ratio: Option<f64>,
    pub delta_bid_notional_250ms: Option<f64>,
    pub delta_ask_notional_250ms: Option<f64>,
    pub opi_book_decay: Option<f64>,
    pub opi_book_support: Option<f64>,
    pub opi_spread_ticks: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResyncLog {
    pub venue: Venue,
    pub token_key: String,
    pub reason: String,
    pub trigger_local_ts_ms: i64,
    pub trigger_exchange_ts_ms: Option<i64>,
    pub venue_hash_before: Option<String>,
    pub venue_hash_after: Option<String>,
    pub local_topn_hash_before: Option<String>,
    pub local_topn_hash_after: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectionHealthRow {
    pub venue: Venue,
    pub bar_second: i64,
    pub ws_state: i64,
    pub reconnect_count: i64,
    pub last_msg_age_ms: Option<i64>,
    pub heartbeat_sent: Option<i64>,
    pub heartbeat_fail: Option<i64>,
    pub dropped_events: i64,
    pub notes: Option<String>,
}
