use crate::models::{
    Bars1sPair, Bars1sToken, CancelTriggerSnapshotRow, ChainMetricsRow, ClockOffsetSample,
    ConnectionHealthRow, Event, ExchangeTsSource, OpinionResponseRow, ResyncLog, ShockEvent,
    ShockEventUpdate, TradeAuditRow, TokenRegistration, TokenSide, Venue,
};
use crate::time_utils::now_ts_ms;
use serde::{Deserialize, Serialize};
use sqlx::{QueryBuilder, PgPool, Postgres};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::warn;

const DB_FLUSH_INTERVAL_MS: u64 = 20000;
const RAW_EVENT_FLUSH_THRESHOLD: usize = 100000;
const TOKEN_BAR_FLUSH_THRESHOLD: usize = 50000;
const PAIR_BAR_FLUSH_THRESHOLD: usize = 50000;
const CLOCK_FLUSH_THRESHOLD: usize = 20000;
const TOKEN_REGISTRY_FLUSH_THRESHOLD: usize = 5000;
const OVERRIDE_FLUSH_THRESHOLD: usize = 2000;
const RESYNC_FLUSH_THRESHOLD: usize = 2000;
const HEALTH_FLUSH_THRESHOLD: usize = 2000;
const CHAIN_FLUSH_THRESHOLD: usize = 2000;
const SHOCK_FLUSH_THRESHOLD: usize = 2000;
const SHOCK_UPDATE_FLUSH_THRESHOLD: usize = 2000;
const OPINION_RESPONSE_FLUSH_THRESHOLD: usize = 5000;
const TRADE_AUDIT_FLUSH_THRESHOLD: usize = 5000;
const CANCEL_SNAPSHOT_FLUSH_THRESHOLD: usize = 5000;
// Keep batch bind counts under Postgres limits (max 65535 parameters).
const PG_SAFE_BINDS: usize = 60000;
const RAW_EVENT_COLS: usize = 16;
const TOKEN_BAR_COLS: usize = 75;
const PAIR_BAR_COLS: usize = 68;
const RAW_EVENT_BATCH_MAX: usize = PG_SAFE_BINDS / RAW_EVENT_COLS;
const TOKEN_BAR_BATCH_MAX: usize = PG_SAFE_BINDS / TOKEN_BAR_COLS;
const PAIR_BAR_BATCH_MAX: usize = PG_SAFE_BINDS / PAIR_BAR_COLS;
static OPI_LOCAL_FALLBACK_BARS: AtomicUsize = AtomicUsize::new(0);
const OPI_WRITE_ALERT_INTERVAL_MS: i64 = 60_000;
const OPI_WRITE_STALL_MS: i64 = 90_000;

#[derive(Default)]
struct OpiWriteStats {
    last_raw_event_ms: i64,
    last_token_bar_second: i64,
    raw_events: u64,
    token_bars: u64,
    pair_bars: u64,
}

fn dedupe_token_bars<'a>(bars: &'a [(i64, Bars1sToken)]) -> Vec<&'a Bars1sToken> {
    if bars.len() < 2 {
        return bars.iter().map(|(_, bar)| bar).collect();
    }
    let mut map: HashMap<(Venue, String, i64), usize> = HashMap::with_capacity(bars.len());
    let mut deduped: Vec<&'a Bars1sToken> = Vec::with_capacity(bars.len());
    for (_, bar) in bars.iter() {
        let key = (bar.venue, bar.token_key.clone(), bar.bar_second);
        if let Some(&idx) = map.get(&key) {
            deduped[idx] = bar;
        } else {
            let idx = deduped.len();
            map.insert(key, idx);
            deduped.push(bar);
        }
    }
    deduped
}

fn dedupe_pair_bars<'a>(bars: &'a [(i64, Bars1sPair)]) -> Vec<&'a Bars1sPair> {
    if bars.len() < 2 {
        return bars.iter().map(|(_, bar)| bar).collect();
    }
    let mut map: HashMap<(i64, TokenSide, i64), usize> = HashMap::with_capacity(bars.len());
    let mut deduped: Vec<&'a Bars1sPair> = Vec::with_capacity(bars.len());
    for (_, bar) in bars.iter() {
        let key = (bar.pair_id, bar.token_side, bar.bar_second);
        if let Some(&idx) = map.get(&key) {
            deduped[idx] = bar;
        } else {
            let idx = deduped.len();
            map.insert(key, idx);
            deduped.push(bar);
        }
    }
    deduped
}

macro_rules! exec_log {
    ($expr:expr, $ctx:expr) => {{
        match $expr.await {
            Ok(_) => true,
            Err(err) => {
                warn!("db_writer {} err={}", $ctx, err);
                false
            }
        }
    }};
}

macro_rules! flush_heavy_into {
    ($tx:expr, $raw_stage:expr, $token_stage:expr, $pair_stage:expr, $had_error:ident) => {{
        if !$raw_stage.is_empty() && !$had_error {
            let batch_max = RAW_EVENT_BATCH_MAX.max(1);
            for batch in $raw_stage.chunks(batch_max) {
                let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
                    r#"
                INSERT INTO raw_events (
                  venue, token_key, pair_id, token_side, kind, channel,
                  exchange_ts_ms, local_ts_ms, exchange_ts_source, obs_latency_ms,
                  out_of_order_flag, ts_missing_flag, ts_anomaly_flag, trade_side_missing_flag,
                  venue_hash, payload_json
                )
                "#,
                );
                qb.push_values(batch.iter(), |mut b, (_, evt)| {
                    let payload_json: Option<String> = None;
                    let obs_latency_ms = match evt.exchange_ts_source {
                        ExchangeTsSource::VenueWs | ExchangeTsSource::VenueRest => {
                            Some(evt.local_ts_ms - evt.exchange_ts_ms)
                        }
                        _ => None,
                    };
                    let kind = format!("{:?}", evt.kind);
                    let token_key = evt.token_key.clone();
                    let channel = evt.channel.clone();
                    let payload_hash = evt.payload.hash.clone();
                    let pair_id = evt.pair_ids.first().copied();
                    b.push_bind(evt.venue.as_str())
                        .push_bind(token_key)
                        .push_bind(pair_id)
                        .push_bind(evt.token_side.as_str())
                        .push_bind(kind)
                        .push_bind(channel)
                        .push_bind(evt.exchange_ts_ms)
                        .push_bind(evt.local_ts_ms)
                        .push_bind(evt.exchange_ts_source as i64)
                        .push_bind(obs_latency_ms)
                        .push_bind(if evt.flags.out_of_order { 1 } else { 0 })
                        .push_bind(if evt.flags.ts_missing { 1 } else { 0 })
                        .push_bind(if evt.flags.ts_anomaly { 1 } else { 0 })
                        .push_bind(if evt.flags.trade_side_missing { 1 } else { 0 })
                        .push_bind(payload_hash)
                        .push_bind(payload_json);
                });
                if !exec_log!(
                    qb.build().execute(&mut **$tx),
                    format!("raw_events insert batch rows={}", batch.len())
                ) {
                    $had_error = true;
                    break;
                }
            }
        }

        if !$token_stage.is_empty() && !$had_error {
            let deduped = dedupe_token_bars($token_stage);
            let batch_max = TOKEN_BAR_BATCH_MAX.max(1);
            for batch in deduped.chunks(batch_max) {
                let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
                    r#"
                INSERT INTO bars_1s_token (
                  venue, token_key, token_side, bar_second,
                  exchange_ts_source,
                  obs_latency_venue_p50_ms, obs_latency_venue_p90_ms, obs_latency_venue_p99_ms, obs_latency_venue_max_ms,
                  obs_latency_est_p50_ms, obs_latency_est_p90_ms, obs_latency_est_p99_ms, obs_latency_est_max_ms,
                  connection_health_flags, bar_gap_flag, bar_stale_flag, staleness_ms,
                  event_count_1s, ts_missing_count_1s, ts_anomaly_count_1s, book_resync_flag,
                  best_bid_px, best_ask_px, mid_px,
                  best_bid_px_state, best_ask_px_state, mid_px_state,
                  last_price, micro_px, spread_px, tick_px, spread_ticks,
                  top3_depth_bid_notional, top3_depth_ask_notional,
                  bid_l1_notional, bid_l2_notional, bid_l3_notional,
                  ask_l1_notional, ask_l2_notional, ask_l3_notional,
                  top10_depth_bid_notional, top10_depth_ask_notional,
                  depth_1pct_bid_notional, depth_1pct_ask_notional, imbalance_top10, air_pocket_bid_ticks, air_pocket_ask_ticks,
                  book_updates_1s, add_updates_1s, cancel_updates_1s, match_updates_1s, remove_unknown_updates_1s,
                  trade_count_1s, volume_shares_1s, volume_notional_1s, vwap_1s, buy_notional_1s, sell_notional_1s,
                  cvd_delta_1s, trade_side_missing_count_1s, ofi_1s, ofi_250ms,
                  bid_delta_notional_250ms, ask_delta_notional_250ms, mid_slope_250ms,
                  ofi_10s, ofi_z_30s, levels_crossed_1s,
                  max_trade_notional_1s, buy_notional_250ms, sell_notional_250ms, max_trade_notional_250ms,
                  depth_withdrawal_ratio, liquidity_pull_flag, venue_book_hash, local_topn_hash
                )
                "#,
                );
                qb.push_values(batch.iter(), |mut b, bar| {
                    let mut exchange_ts_source = bar.exchange_ts_source;
                    let token_key = bar.token_key.clone();
                    if bar.venue == Venue::Opinion
                        && exchange_ts_source == ExchangeTsSource::LocalFallback
                    {
                        let count = OPI_LOCAL_FALLBACK_BARS.fetch_add(1, Ordering::Relaxed) + 1;
                        if count <= 5 || count % 1000 == 0 {
                            tracing::warn!(
                                "opi bars local_fallback detected count={} token_key={} bar_second={}",
                                count,
                                token_key,
                                bar.bar_second
                            );
                        }
                        exchange_ts_source = ExchangeTsSource::Estimated;
                    }
                    let venue_book_hash = bar.venue_book_hash.clone();
                    let local_topn_hash = bar.local_topn_hash.clone();
                    b.push_bind(bar.venue.as_str())
                        .push_bind(token_key)
                        .push_bind(bar.token_side.as_str())
                        .push_bind(bar.bar_second)
                        .push_bind(exchange_ts_source as i64)
                        .push_bind(bar.obs_latency_venue_p50_ms)
                        .push_bind(bar.obs_latency_venue_p90_ms)
                        .push_bind(bar.obs_latency_venue_p99_ms)
                        .push_bind(bar.obs_latency_venue_max_ms)
                        .push_bind(bar.obs_latency_est_p50_ms)
                        .push_bind(bar.obs_latency_est_p90_ms)
                        .push_bind(bar.obs_latency_est_p99_ms)
                        .push_bind(bar.obs_latency_est_max_ms)
                        .push_bind(bar.connection_health_flags)
                        .push_bind(bar.bar_gap_flag)
                        .push_bind(bar.bar_stale_flag)
                        .push_bind(bar.staleness_ms)
                        .push_bind(bar.event_count_1s)
                        .push_bind(bar.ts_missing_count_1s)
                        .push_bind(bar.ts_anomaly_count_1s)
                        .push_bind(bar.book_resync_flag)
                        .push_bind(bar.best_bid_px)
                        .push_bind(bar.best_ask_px)
                        .push_bind(bar.mid_px)
                        .push_bind(bar.best_bid_px_state)
                        .push_bind(bar.best_ask_px_state)
                        .push_bind(bar.mid_px_state)
                        .push_bind(bar.last_price)
                        .push_bind(bar.micro_px)
                        .push_bind(bar.spread_px)
                        .push_bind(bar.tick_px)
                        .push_bind(bar.spread_ticks)
                        .push_bind(bar.top3_depth_bid_notional)
                        .push_bind(bar.top3_depth_ask_notional)
                        .push_bind(bar.bid_l1_notional)
                        .push_bind(bar.bid_l2_notional)
                        .push_bind(bar.bid_l3_notional)
                        .push_bind(bar.ask_l1_notional)
                        .push_bind(bar.ask_l2_notional)
                        .push_bind(bar.ask_l3_notional)
                        .push_bind(bar.top10_depth_bid_notional)
                        .push_bind(bar.top10_depth_ask_notional)
                        .push_bind(bar.depth_1pct_bid_notional)
                        .push_bind(bar.depth_1pct_ask_notional)
                        .push_bind(bar.imbalance_top10)
                        .push_bind(bar.air_pocket_bid_ticks)
                        .push_bind(bar.air_pocket_ask_ticks)
                        .push_bind(bar.book_updates_1s)
                        .push_bind(bar.add_updates_1s)
                        .push_bind(bar.cancel_updates_1s)
                        .push_bind(bar.match_updates_1s)
                        .push_bind(bar.remove_unknown_updates_1s)
                        .push_bind(bar.trade_count_1s)
                        .push_bind(bar.volume_shares_1s)
                        .push_bind(bar.volume_notional_1s)
                        .push_bind(bar.vwap_1s)
                        .push_bind(bar.buy_notional_1s)
                        .push_bind(bar.sell_notional_1s)
                        .push_bind(bar.cvd_delta_1s)
                        .push_bind(bar.trade_side_missing_count_1s)
                        .push_bind(bar.ofi_1s)
                        .push_bind(bar.ofi_250ms)
                        .push_bind(bar.bid_delta_notional_250ms)
                        .push_bind(bar.ask_delta_notional_250ms)
                        .push_bind(bar.mid_slope_250ms)
                        .push_bind(bar.ofi_10s)
                        .push_bind(bar.ofi_z_30s)
                        .push_bind(bar.levels_crossed_1s)
                        .push_bind(bar.max_trade_notional_1s)
                        .push_bind(bar.buy_notional_250ms)
                        .push_bind(bar.sell_notional_250ms)
                        .push_bind(bar.max_trade_notional_250ms)
                        .push_bind(bar.depth_withdrawal_ratio)
                        .push_bind(bar.liquidity_pull_flag)
                        .push_bind(venue_book_hash)
                        .push_bind(local_topn_hash);
                });
                if !exec_log!(
                    qb.build().execute(&mut **$tx),
                    format!("bars_1s_token insert batch rows={}", batch.len())
                ) {
                    $had_error = true;
                    break;
                }
            }
        }

        if !$pair_stage.is_empty() && !$had_error {
            let deduped = dedupe_pair_bars($pair_stage);
            let batch_max = PAIR_BAR_BATCH_MAX.max(1);
            for batch in deduped.chunks(batch_max) {
                let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
                    r#"
                INSERT INTO bars_1s_pair (
                  pair_id, token_side, bar_second,
                  poly_bar_gap_flag, opi_bar_gap_flag, bar_gap_flag,
                  poly_bar_stale_flag, opi_bar_stale_flag, bar_stale_flag,
                  poly_staleness_ms, opi_staleness_ms,
                  poly_best_bid, poly_best_ask, poly_mid, poly_micro,
                  poly_best_bid_state, poly_best_ask_state, poly_mid_state,
                  opi_best_bid, opi_best_ask, opi_mid,
                  opi_best_bid_state, opi_best_ask_state, opi_mid_state,
                  opi_last_price, opi_taker_buy_notional_1s, opi_taker_sell_notional_1s, opi_micro,
                  opi_bid_l1_notional, opi_bid_l2_notional, opi_bid_l3_notional,
                  opi_ask_l1_notional, opi_ask_l2_notional, opi_ask_l3_notional,
                  poly_fee_bps, opi_fee_bps,
                  gas_cost, network_congestion_flag,
                  gross_spread_buy, gross_spread_sell,
                  true_net_spread_buy_opinion, true_net_spread_sell_opinion,
                  arb_mode_flag, directional_mode_flag,
                  poly_cvd_30s, poly_buy_sell_ratio_30s, price_pressure_index,
                  poly_ofi_250ms,
                  poly_buy_notional_1s, poly_sell_notional_1s, poly_max_trade_notional_1s,
                  poly_buy_notional_250ms, poly_sell_notional_250ms, poly_max_trade_notional_250ms,
                  poly_bid_delta_notional_250ms, poly_ask_delta_notional_250ms, poly_mid_slope_250ms,
                  poly_obs_latency_p99_ms, opi_obs_latency_p99_ms,
                  rolling_corr_5m, rolling_corr_samples_5m, estimated_lag_s, lag_confidence, beta_1m,
                  follow_score_raw, follow_score_adj,
                  poly_depth_imbalance_top10, poly_book_pressure_250ms, poly_spread_bps, poly_micro_bias_bps,
                  book_alpha_raw, book_alpha_adj
                )
                "#,
                );
                qb.push_values(batch.iter(), |mut b, bar| {
                    b.push_bind(bar.pair_id)
                        .push_bind(bar.token_side.as_str())
                        .push_bind(bar.bar_second)
                        .push_bind(bar.poly_bar_gap_flag)
                        .push_bind(bar.opi_bar_gap_flag)
                        .push_bind(bar.bar_gap_flag)
                        .push_bind(bar.poly_bar_stale_flag)
                        .push_bind(bar.opi_bar_stale_flag)
                        .push_bind(bar.bar_stale_flag)
                        .push_bind(bar.poly_staleness_ms)
                        .push_bind(bar.opi_staleness_ms)
                        .push_bind(bar.poly_best_bid)
                        .push_bind(bar.poly_best_ask)
                        .push_bind(bar.poly_mid)
                        .push_bind(bar.poly_micro)
                        .push_bind(bar.poly_best_bid_state)
                        .push_bind(bar.poly_best_ask_state)
                        .push_bind(bar.poly_mid_state)
                        .push_bind(bar.opi_best_bid)
                        .push_bind(bar.opi_best_ask)
                        .push_bind(bar.opi_mid)
                        .push_bind(bar.opi_best_bid_state)
                        .push_bind(bar.opi_best_ask_state)
                        .push_bind(bar.opi_mid_state)
                        .push_bind(bar.opi_last_price)
                        .push_bind(bar.opi_taker_buy_notional_1s)
                        .push_bind(bar.opi_taker_sell_notional_1s)
                        .push_bind(bar.opi_micro)
                        .push_bind(bar.opi_bid_l1_notional)
                        .push_bind(bar.opi_bid_l2_notional)
                        .push_bind(bar.opi_bid_l3_notional)
                        .push_bind(bar.opi_ask_l1_notional)
                        .push_bind(bar.opi_ask_l2_notional)
                        .push_bind(bar.opi_ask_l3_notional)
                        .push_bind(bar.poly_fee_bps)
                        .push_bind(bar.opi_fee_bps)
                        .push_bind(bar.gas_cost)
                        .push_bind(bar.network_congestion_flag)
                        .push_bind(bar.gross_spread_buy)
                        .push_bind(bar.gross_spread_sell)
                        .push_bind(bar.true_net_spread_buy_opinion)
                        .push_bind(bar.true_net_spread_sell_opinion)
                        .push_bind(bar.arb_mode_flag)
                        .push_bind(bar.directional_mode_flag)
                        .push_bind(bar.poly_cvd_30s)
                        .push_bind(bar.poly_buy_sell_ratio_30s)
                        .push_bind(bar.price_pressure_index)
                        .push_bind(bar.poly_ofi_250ms)
                        .push_bind(bar.poly_buy_notional_1s)
                        .push_bind(bar.poly_sell_notional_1s)
                        .push_bind(bar.poly_max_trade_notional_1s)
                        .push_bind(bar.poly_buy_notional_250ms)
                        .push_bind(bar.poly_sell_notional_250ms)
                        .push_bind(bar.poly_max_trade_notional_250ms)
                        .push_bind(bar.poly_bid_delta_notional_250ms)
                        .push_bind(bar.poly_ask_delta_notional_250ms)
                        .push_bind(bar.poly_mid_slope_250ms)
                        .push_bind(bar.poly_obs_latency_p99_ms)
                        .push_bind(bar.opi_obs_latency_p99_ms)
                        .push_bind(bar.rolling_corr_5m)
                        .push_bind(bar.rolling_corr_samples_5m)
                        .push_bind(bar.estimated_lag_s)
                        .push_bind(bar.lag_confidence)
                        .push_bind(bar.beta_1m)
                        .push_bind(bar.follow_score_raw)
                        .push_bind(bar.follow_score_adj)
                        .push_bind(bar.poly_depth_imbalance_top10)
                        .push_bind(bar.poly_book_pressure_250ms)
                        .push_bind(bar.poly_spread_bps)
                        .push_bind(bar.poly_micro_bias_bps)
                        .push_bind(bar.book_alpha_raw)
                        .push_bind(bar.book_alpha_adj);
                });
                qb.push(
                    " ON CONFLICT (pair_id, token_side, bar_second) DO UPDATE SET \
poly_bar_gap_flag=EXCLUDED.poly_bar_gap_flag, \
opi_bar_gap_flag=EXCLUDED.opi_bar_gap_flag, \
bar_gap_flag=EXCLUDED.bar_gap_flag, \
poly_bar_stale_flag=EXCLUDED.poly_bar_stale_flag, \
opi_bar_stale_flag=EXCLUDED.opi_bar_stale_flag, \
bar_stale_flag=EXCLUDED.bar_stale_flag, \
poly_staleness_ms=EXCLUDED.poly_staleness_ms, \
opi_staleness_ms=EXCLUDED.opi_staleness_ms, \
poly_best_bid=EXCLUDED.poly_best_bid, \
poly_best_ask=EXCLUDED.poly_best_ask, \
poly_mid=EXCLUDED.poly_mid, \
poly_micro=EXCLUDED.poly_micro, \
poly_best_bid_state=EXCLUDED.poly_best_bid_state, \
poly_best_ask_state=EXCLUDED.poly_best_ask_state, \
poly_mid_state=EXCLUDED.poly_mid_state, \
opi_best_bid=EXCLUDED.opi_best_bid, \
opi_best_ask=EXCLUDED.opi_best_ask, \
opi_mid=EXCLUDED.opi_mid, \
opi_best_bid_state=EXCLUDED.opi_best_bid_state, \
opi_best_ask_state=EXCLUDED.opi_best_ask_state, \
opi_mid_state=EXCLUDED.opi_mid_state, \
opi_last_price=EXCLUDED.opi_last_price, \
opi_taker_buy_notional_1s=EXCLUDED.opi_taker_buy_notional_1s, \
opi_taker_sell_notional_1s=EXCLUDED.opi_taker_sell_notional_1s, \
opi_micro=EXCLUDED.opi_micro, \
opi_bid_l1_notional=EXCLUDED.opi_bid_l1_notional, \
opi_bid_l2_notional=EXCLUDED.opi_bid_l2_notional, \
opi_bid_l3_notional=EXCLUDED.opi_bid_l3_notional, \
opi_ask_l1_notional=EXCLUDED.opi_ask_l1_notional, \
opi_ask_l2_notional=EXCLUDED.opi_ask_l2_notional, \
opi_ask_l3_notional=EXCLUDED.opi_ask_l3_notional, \
poly_fee_bps=EXCLUDED.poly_fee_bps, \
opi_fee_bps=EXCLUDED.opi_fee_bps, \
gas_cost=EXCLUDED.gas_cost, \
network_congestion_flag=EXCLUDED.network_congestion_flag, \
gross_spread_buy=EXCLUDED.gross_spread_buy, \
gross_spread_sell=EXCLUDED.gross_spread_sell, \
true_net_spread_buy_opinion=EXCLUDED.true_net_spread_buy_opinion, \
true_net_spread_sell_opinion=EXCLUDED.true_net_spread_sell_opinion, \
arb_mode_flag=EXCLUDED.arb_mode_flag, \
directional_mode_flag=EXCLUDED.directional_mode_flag, \
poly_cvd_30s=EXCLUDED.poly_cvd_30s, \
poly_buy_sell_ratio_30s=EXCLUDED.poly_buy_sell_ratio_30s, \
price_pressure_index=EXCLUDED.price_pressure_index, \
poly_ofi_250ms=EXCLUDED.poly_ofi_250ms, \
poly_buy_notional_1s=EXCLUDED.poly_buy_notional_1s, \
poly_sell_notional_1s=EXCLUDED.poly_sell_notional_1s, \
poly_max_trade_notional_1s=EXCLUDED.poly_max_trade_notional_1s, \
poly_buy_notional_250ms=EXCLUDED.poly_buy_notional_250ms, \
poly_sell_notional_250ms=EXCLUDED.poly_sell_notional_250ms, \
poly_max_trade_notional_250ms=EXCLUDED.poly_max_trade_notional_250ms, \
poly_bid_delta_notional_250ms=EXCLUDED.poly_bid_delta_notional_250ms, \
poly_ask_delta_notional_250ms=EXCLUDED.poly_ask_delta_notional_250ms, \
poly_mid_slope_250ms=EXCLUDED.poly_mid_slope_250ms, \
poly_obs_latency_p99_ms=EXCLUDED.poly_obs_latency_p99_ms, \
opi_obs_latency_p99_ms=EXCLUDED.opi_obs_latency_p99_ms, \
rolling_corr_5m=EXCLUDED.rolling_corr_5m, \
estimated_lag_s=EXCLUDED.estimated_lag_s, \
lag_confidence=EXCLUDED.lag_confidence, \
beta_1m=EXCLUDED.beta_1m, \
follow_score_raw=EXCLUDED.follow_score_raw, \
follow_score_adj=EXCLUDED.follow_score_adj, \
poly_depth_imbalance_top10=EXCLUDED.poly_depth_imbalance_top10, \
poly_book_pressure_250ms=EXCLUDED.poly_book_pressure_250ms, \
poly_spread_bps=EXCLUDED.poly_spread_bps, \
poly_micro_bias_bps=EXCLUDED.poly_micro_bias_bps, \
book_alpha_raw=EXCLUDED.book_alpha_raw, \
book_alpha_adj=EXCLUDED.book_alpha_adj",
                );
                if !exec_log!(
                    qb.build().execute(&mut **$tx),
                    format!("bars_1s_pair upsert batch rows={}", batch.len())
                ) {
                    $had_error = true;
                    break;
                }
            }
        }
    }};
}


#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DbMessage {
    RawEvent(Event),
    Bars1sToken(Bars1sToken),
    Bars1sPair(Bars1sPair),
    ClockOffset(ClockOffsetSample),
    TokenRegistryUpsert(TokenRegistration),
    PairTokenOverride {
        pair_id: i64,
        token_side: TokenSide,
        token_id: String,
    },
    ResyncLog(ResyncLog),
    ConnectionHealth(ConnectionHealthRow),
    ChainMetrics(ChainMetricsRow),
    ShockEvent(ShockEvent),
    ShockEventUpdate(ShockEventUpdate),
    OpinionResponse(OpinionResponseRow),
    TradeAudit(TradeAuditRow),
    CancelTriggerSnapshot(CancelTriggerSnapshotRow),
}

#[derive(Debug, Clone)]
pub struct DbEnvelope {
    pub id: i64,
    pub msg: DbMessage,
}

pub type DbAck = i64;

pub async fn run_db_writer(
    mut rx: mpsc::Receiver<DbEnvelope>,
    ack_tx: mpsc::UnboundedSender<DbAck>,
    pool: PgPool,
    alpha_capture_enabled: bool,
) {
    let mut raw_buf: Vec<(i64, Event)> = Vec::new();
    let mut token_buf: Vec<(i64, Bars1sToken)> = Vec::new();
    let mut pair_buf: Vec<(i64, Bars1sPair)> = Vec::new();
    let mut clock_buf: Vec<(i64, ClockOffsetSample)> = Vec::new();
    let mut token_registry_buf: Vec<(i64, TokenRegistration)> = Vec::new();
    let mut override_buf: Vec<(i64, (i64, TokenSide, String))> = Vec::new();
    let mut resync_buf: Vec<(i64, ResyncLog)> = Vec::new();
    let mut health_buf: Vec<(i64, ConnectionHealthRow)> = Vec::new();
    let mut chain_buf: Vec<(i64, ChainMetricsRow)> = Vec::new();
    let mut shock_buf: Vec<(i64, ShockEvent)> = Vec::new();
    let mut shock_update_buf: Vec<(i64, ShockEventUpdate)> = Vec::new();
    let mut response_buf: Vec<(i64, OpinionResponseRow)> = Vec::new();
    let mut trade_audit_buf: Vec<(i64, TradeAuditRow)> = Vec::new();
    let mut cancel_snapshot_buf: Vec<(i64, CancelTriggerSnapshotRow)> = Vec::new();
    let mut opi_stats = OpiWriteStats::default();
    let mut last_opi_alert_ms = 0i64;

    let mut ticker = tokio::time::interval(Duration::from_millis(DB_FLUSH_INTERVAL_MS));
    loop {
        tokio::select! {
            Some(envelope) = rx.recv() => {
                let DbEnvelope { id, msg } = envelope;
                match msg {
                    DbMessage::RawEvent(evt) => {
                        if alpha_capture_enabled {
                            if evt.venue == Venue::Opinion {
                                opi_stats.raw_events += 1;
                                opi_stats.last_raw_event_ms =
                                    opi_stats.last_raw_event_ms.max(evt.local_ts_ms);
                            }
                            raw_buf.push((id, evt));
                        } else {
                            let _ = ack_tx.send(id);
                        }
                    }
                    DbMessage::Bars1sToken(bar) => {
                        if alpha_capture_enabled {
                            if bar.venue == Venue::Opinion {
                                opi_stats.token_bars += 1;
                                opi_stats.last_token_bar_second =
                                    opi_stats.last_token_bar_second.max(bar.bar_second);
                            }
                            token_buf.push((id, bar));
                        } else {
                            let _ = ack_tx.send(id);
                        }
                    }
                    DbMessage::Bars1sPair(bar) => {
                        if alpha_capture_enabled {
                            if bar.opi_mid_state.is_some()
                                || bar.opi_mid.is_some()
                                || bar.opi_last_price.is_some()
                            {
                                opi_stats.pair_bars += 1;
                            }
                            pair_buf.push((id, bar));
                        } else {
                            let _ = ack_tx.send(id);
                        }
                    }
                    DbMessage::ClockOffset(sample) => clock_buf.push((id, sample)),
                    DbMessage::TokenRegistryUpsert(token) => {
                        token_registry_buf.push((id, token));
                    }
                    DbMessage::PairTokenOverride { pair_id, token_side, token_id } => {
                        override_buf.push((id, (pair_id, token_side, token_id)));
                    }
                    DbMessage::ResyncLog(log) => {
                        if alpha_capture_enabled {
                            resync_buf.push((id, log));
                        } else {
                            let _ = ack_tx.send(id);
                        }
                    }
                    DbMessage::ConnectionHealth(row) => health_buf.push((id, row)),
                    DbMessage::ChainMetrics(row) => chain_buf.push((id, row)),
                    DbMessage::ShockEvent(event) => {
                        if alpha_capture_enabled {
                            shock_buf.push((id, event));
                        } else {
                            let _ = ack_tx.send(id);
                        }
                    }
                    DbMessage::ShockEventUpdate(event) => {
                        if alpha_capture_enabled {
                            shock_update_buf.push((id, event));
                        } else {
                            let _ = ack_tx.send(id);
                        }
                    }
                    DbMessage::OpinionResponse(row) => {
                        if alpha_capture_enabled {
                            response_buf.push((id, row));
                        } else {
                            let _ = ack_tx.send(id);
                        }
                    }
                    DbMessage::TradeAudit(row) => trade_audit_buf.push((id, row)),
                    DbMessage::CancelTriggerSnapshot(row) => {
                        cancel_snapshot_buf.push((id, row));
                    }
                }
                if raw_buf.len() >= RAW_EVENT_FLUSH_THRESHOLD
                    || token_buf.len() >= TOKEN_BAR_FLUSH_THRESHOLD
                    || pair_buf.len() >= PAIR_BAR_FLUSH_THRESHOLD
                    || clock_buf.len() >= CLOCK_FLUSH_THRESHOLD
                    || token_registry_buf.len() >= TOKEN_REGISTRY_FLUSH_THRESHOLD
                    || override_buf.len() >= OVERRIDE_FLUSH_THRESHOLD
                    || resync_buf.len() >= RESYNC_FLUSH_THRESHOLD
                    || health_buf.len() >= HEALTH_FLUSH_THRESHOLD
                    || chain_buf.len() >= CHAIN_FLUSH_THRESHOLD
                    || shock_buf.len() >= SHOCK_FLUSH_THRESHOLD
                    || shock_update_buf.len() >= SHOCK_UPDATE_FLUSH_THRESHOLD
                    || response_buf.len() >= OPINION_RESPONSE_FLUSH_THRESHOLD
                    || trade_audit_buf.len() >= TRADE_AUDIT_FLUSH_THRESHOLD
                    || cancel_snapshot_buf.len() >= CANCEL_SNAPSHOT_FLUSH_THRESHOLD
                {
                    flush_buffers(
                        &pool,
                        &ack_tx,
                        &mut raw_buf,
                        &mut token_buf,
                        &mut pair_buf,
                        &mut clock_buf,
                        &mut token_registry_buf,
                        &mut override_buf,
                        &mut resync_buf,
                        &mut health_buf,
                        &mut chain_buf,
                        &mut shock_buf,
                        &mut shock_update_buf,
                        &mut response_buf,
                        &mut trade_audit_buf,
                        &mut cancel_snapshot_buf,
                    )
                    .await;
                }
            }
            _ = ticker.tick() => {
                if alpha_capture_enabled {
                    let now_ms = now_ts_ms();
                    if last_opi_alert_ms == 0 {
                        last_opi_alert_ms = now_ms;
                    } else if now_ms.saturating_sub(last_opi_alert_ms)
                        >= OPI_WRITE_ALERT_INTERVAL_MS
                    {
                        let raw_events = opi_stats.raw_events;
                        let token_bars = opi_stats.token_bars;
                        let pair_bars = opi_stats.pair_bars;
                        let last_raw_ms = opi_stats.last_raw_event_ms;
                        let last_token_second = opi_stats.last_token_bar_second;
                        if raw_events == 0
                            || token_bars == 0
                            || now_ms.saturating_sub(last_raw_ms) > OPI_WRITE_STALL_MS
                        {
                            warn!(
                                "opi_write_alert raw_events={} token_bars={} pair_bars={} last_raw_event_ms={} last_token_bar_second={} window_ms={}",
                                raw_events,
                                token_bars,
                                pair_bars,
                                last_raw_ms,
                                last_token_second,
                                OPI_WRITE_ALERT_INTERVAL_MS
                            );
                        } else {
                            tracing::info!(
                                "opi_write_stats raw_events={} token_bars={} pair_bars={} last_raw_event_ms={} last_token_bar_second={} window_ms={}",
                                raw_events,
                                token_bars,
                                pair_bars,
                                last_raw_ms,
                                last_token_second,
                                OPI_WRITE_ALERT_INTERVAL_MS
                            );
                        }
                        opi_stats = OpiWriteStats::default();
                        last_opi_alert_ms = now_ms;
                    }
                }
                flush_buffers(
                    &pool,
                    &ack_tx,
                    &mut raw_buf,
                    &mut token_buf,
                    &mut pair_buf,
                    &mut clock_buf,
                    &mut token_registry_buf,
                    &mut override_buf,
                    &mut resync_buf,
                    &mut health_buf,
                    &mut chain_buf,
                    &mut shock_buf,
                    &mut shock_update_buf,
                    &mut response_buf,
                    &mut trade_audit_buf,
                    &mut cancel_snapshot_buf,
                )
                .await;
            }
        }
    }
}

async fn flush_buffers(
    pool: &PgPool,
    ack_tx: &mpsc::UnboundedSender<DbAck>,
    raw_buf: &mut Vec<(i64, Event)>,
    token_buf: &mut Vec<(i64, Bars1sToken)>,
    pair_buf: &mut Vec<(i64, Bars1sPair)>,
    clock_buf: &mut Vec<(i64, ClockOffsetSample)>,
    token_registry_buf: &mut Vec<(i64, TokenRegistration)>,
    override_buf: &mut Vec<(i64, (i64, TokenSide, String))>,
    resync_buf: &mut Vec<(i64, ResyncLog)>,
    health_buf: &mut Vec<(i64, ConnectionHealthRow)>,
    chain_buf: &mut Vec<(i64, ChainMetricsRow)>,
    shock_buf: &mut Vec<(i64, ShockEvent)>,
    shock_update_buf: &mut Vec<(i64, ShockEventUpdate)>,
    response_buf: &mut Vec<(i64, OpinionResponseRow)>,
    trade_audit_buf: &mut Vec<(i64, TradeAuditRow)>,
    cancel_snapshot_buf: &mut Vec<(i64, CancelTriggerSnapshotRow)>,
) {
    if raw_buf.is_empty()
        && token_buf.is_empty()
        && pair_buf.is_empty()
        && clock_buf.is_empty()
        && token_registry_buf.is_empty()
        && override_buf.is_empty()
        && resync_buf.is_empty()
        && health_buf.is_empty()
        && chain_buf.is_empty()
        && shock_buf.is_empty()
        && shock_update_buf.is_empty()
        && response_buf.is_empty()
        && trade_audit_buf.is_empty()
        && cancel_snapshot_buf.is_empty()
    {
        return;
    }

    let raw_stage = std::mem::take(raw_buf);
    let token_stage = std::mem::take(token_buf);
    let pair_stage = std::mem::take(pair_buf);
    let clock_stage = std::mem::take(clock_buf);
    let token_registry_stage = std::mem::take(token_registry_buf);
    let override_stage = std::mem::take(override_buf);
    let resync_stage = std::mem::take(resync_buf);
    let health_stage = std::mem::take(health_buf);
    let chain_stage = std::mem::take(chain_buf);
    let shock_stage = std::mem::take(shock_buf);
    let shock_update_stage = std::mem::take(shock_update_buf);
    let response_stage = std::mem::take(response_buf);
    let trade_audit_stage = std::mem::take(trade_audit_buf);
    let cancel_snapshot_stage = std::mem::take(cancel_snapshot_buf);

    let mut tx = match pool.begin().await {
        Ok(tx) => tx,
        Err(err) => {
            warn!("db_writer begin txn failed err={}", err);
            raw_buf.extend(raw_stage);
            token_buf.extend(token_stage);
            pair_buf.extend(pair_stage);
            clock_buf.extend(clock_stage);
            token_registry_buf.extend(token_registry_stage);
            override_buf.extend(override_stage);
            resync_buf.extend(resync_stage);
            health_buf.extend(health_stage);
            chain_buf.extend(chain_stage);
            shock_buf.extend(shock_stage);
            shock_update_buf.extend(shock_update_stage);
            response_buf.extend(response_stage);
            trade_audit_buf.extend(trade_audit_stage);
            cancel_snapshot_buf.extend(cancel_snapshot_stage);
            return;
        }
    };

    let mut had_error = false;

    if !clock_stage.is_empty() && !had_error {
        for (_, sample) in clock_stage.iter() {
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO clock_offsets (
              venue, local_ts_ms, exchange_ts_ms, sample_offset_ms, smoothed_offset_ms, delta_clamped_ms
            ) VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (venue, local_ts_ms) DO UPDATE SET
              exchange_ts_ms=EXCLUDED.exchange_ts_ms,
              sample_offset_ms=EXCLUDED.sample_offset_ms,
              smoothed_offset_ms=EXCLUDED.smoothed_offset_ms,
              delta_clamped_ms=EXCLUDED.delta_clamped_ms
            "#,
                )
                .bind(sample.venue.as_str())
                .bind(sample.local_ts_ms)
                .bind(sample.exchange_ts_ms)
                .bind(sample.sample_offset_ms)
                .bind(sample.smoothed_offset_ms)
                .bind(sample.delta_clamped_ms)
                .execute(&mut *tx),
                "clock_offsets insert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    if !token_registry_stage.is_empty() && !had_error {
        for (_, token) in token_registry_stage.iter() {
            let now_ms = crate::time_utils::now_ts_ms();
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO token_registry (
              token_key, venue, external_token_id, market_id, outcome_side, token_side,
              first_seen_local_ts_ms, last_seen_local_ts_ms
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT(token_key) DO UPDATE SET
              external_token_id=COALESCE(excluded.external_token_id, token_registry.external_token_id),
              market_id=COALESCE(excluded.market_id, token_registry.market_id),
              outcome_side=COALESCE(excluded.outcome_side, token_registry.outcome_side),
              token_side=excluded.token_side,
              last_seen_local_ts_ms=excluded.last_seen_local_ts_ms
            "#,
                )
                .bind(&token.token_key)
                .bind(token.venue.as_str())
                .bind(token.external_token_id.as_deref())
                .bind(token.market_id.as_deref())
                .bind(token.outcome_side)
                .bind(token.token_side.as_str())
                .bind(now_ms)
                .bind(now_ms)
                .execute(&mut *tx),
                "token_registry upsert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    if !override_stage.is_empty() && !had_error {
        for (_, (pair_id, token_side, token_id)) in override_stage.iter() {
            let now_ms = crate::time_utils::now_ts_ms();
            let (yes_override, no_override) = match token_side {
                TokenSide::Yes => (Some(token_id.clone()), None),
                TokenSide::No => (None, Some(token_id.clone())),
                TokenSide::Unknown => (None, None),
            };
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO pair_token_overrides (
              pair_id,
              mapping_overridden_flag,
              opinion_yes_token_id_override,
              opinion_no_token_id_override,
              updated_local_ts_ms
            ) VALUES ($1, 1, $2, $3, $4)
            ON CONFLICT(pair_id) DO UPDATE SET
              mapping_overridden_flag = 1,
              opinion_yes_token_id_override = COALESCE(excluded.opinion_yes_token_id_override, opinion_yes_token_id_override),
              opinion_no_token_id_override = COALESCE(excluded.opinion_no_token_id_override, opinion_no_token_id_override),
              updated_local_ts_ms = excluded.updated_local_ts_ms
            "#,
                )
                .bind(pair_id)
                .bind(yes_override)
                .bind(no_override)
                .bind(now_ms)
                .execute(&mut *tx),
                "pair_token_overrides upsert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    if !resync_stage.is_empty() && !had_error {
        for (_, log) in resync_stage.iter() {
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO orderbook_resync_log (
              venue, token_key, reason, trigger_local_ts_ms, trigger_exchange_ts_ms,
              venue_hash_before, venue_hash_after, local_topn_hash_before, local_topn_hash_after
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
                )
                .bind(log.venue.as_str())
                .bind(&log.token_key)
                .bind(&log.reason)
                .bind(log.trigger_local_ts_ms)
                .bind(log.trigger_exchange_ts_ms)
                .bind(log.venue_hash_before.as_deref())
                .bind(log.venue_hash_after.as_deref())
                .bind(log.local_topn_hash_before.as_deref())
                .bind(log.local_topn_hash_after.as_deref())
                .execute(&mut *tx),
                "orderbook_resync_log insert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    if !health_stage.is_empty() && !had_error {
        for (_, row) in health_stage.iter() {
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO connection_health_1s (
              venue, bar_second, ws_state, reconnect_count, last_msg_age_ms,
              heartbeat_sent, heartbeat_fail, dropped_events, notes
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (venue, bar_second) DO UPDATE SET
              ws_state=EXCLUDED.ws_state,
              reconnect_count=EXCLUDED.reconnect_count,
              last_msg_age_ms=EXCLUDED.last_msg_age_ms,
              heartbeat_sent=EXCLUDED.heartbeat_sent,
              heartbeat_fail=EXCLUDED.heartbeat_fail,
              dropped_events=EXCLUDED.dropped_events,
              notes=EXCLUDED.notes
            "#,
                )
                .bind(row.venue.as_str())
                .bind(row.bar_second)
                .bind(row.ws_state)
                .bind(row.reconnect_count)
                .bind(row.last_msg_age_ms)
                .bind(row.heartbeat_sent)
                .bind(row.heartbeat_fail)
                .bind(row.dropped_events)
                .bind(row.notes.as_deref())
                .execute(&mut *tx),
                "connection_health_1s upsert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    if !chain_stage.is_empty() && !had_error {
        for (_, row) in chain_stage.iter() {
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO chain_metrics (
              chain, local_ts_ms, base_fee, priority_fee, gas_used_est, tx_cost_est, congestion_flag
            ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (chain, local_ts_ms) DO UPDATE SET
              base_fee=EXCLUDED.base_fee,
              priority_fee=EXCLUDED.priority_fee,
              gas_used_est=EXCLUDED.gas_used_est,
              tx_cost_est=EXCLUDED.tx_cost_est,
              congestion_flag=EXCLUDED.congestion_flag
            "#,
                )
                .bind(&row.chain)
                .bind(row.local_ts_ms)
                .bind(row.base_fee)
                .bind(row.priority_fee)
                .bind(row.gas_used_est)
                .bind(row.tx_cost_est)
                .bind(row.congestion_flag)
                .execute(&mut *tx),
                "chain_metrics upsert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    flush_heavy_into!(&mut tx, &raw_stage, &token_stage, &pair_stage, had_error);

    if !shock_stage.is_empty() && !had_error {
        for (_, event) in shock_stage.iter() {
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO poly_shock_events (
              shock_id, token_key, pair_id, token_side, shock_type, direction, noise_flag,
              start_ts_ms, trigger_ts_ms, peak_ts_ms, magnitude, context_json,
              obs_latency_ms, connection_health_flags, status, peak_finalized
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)
            "#,
                )
                .bind(event.shock_id)
                .bind(&event.token_key)
                .bind(event.pair_id)
                .bind(event.token_side.as_str())
                .bind(&event.shock_type)
                .bind(event.direction)
                .bind(event.noise_flag)
                .bind(event.start_ts_ms)
                .bind(event.trigger_ts_ms)
                .bind(event.peak_ts_ms)
                .bind(event.magnitude)
                .bind(event.context_json.as_deref())
                .bind(event.obs_latency_ms)
                .bind(event.connection_health_flags)
                .bind(event.status.as_str())
                .bind(event.peak_finalized)
                .execute(&mut *tx),
                "poly_shock_events insert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    if !shock_update_stage.is_empty() && !had_error {
        for (_, update) in shock_update_stage.iter() {
            if !exec_log!(
                sqlx::query(
                r#"
            UPDATE poly_shock_events
            SET start_ts_ms = $1,
                peak_ts_ms = $2,
                magnitude = $3,
                context_json = $4,
                direction = $5,
                noise_flag = $6,
                status = $7,
                peak_finalized = $8
            WHERE shock_id = $9
            "#,
                )
                .bind(update.start_ts_ms)
                .bind(update.peak_ts_ms)
                .bind(update.magnitude)
                .bind(update.context_json.as_deref())
                .bind(update.direction)
                .bind(update.noise_flag)
                .bind(update.status.as_str())
                .bind(update.peak_finalized)
                .bind(update.shock_id)
                .execute(&mut *tx),
                "poly_shock_events update"
            ) {
                had_error = true;
                break;
            }
        }
    }

    if !response_stage.is_empty() && !had_error {
        for (_, row) in response_stage.iter() {
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO opinion_response (
              shock_id, horizon_s, sample_valid_flag, move_flag, first_move_lag_s,
              opi_return, opi_price_delta, opi_volume_sum, opi_cvd_sum,
              opi_spread_change, opi_depth_change, opi_staleness_at_t0_ms,
              opi_liquidity_pull_flag_at_t0, true_net_spread_at_t0,
              arb_mode_flag_at_t0, estimated_lag_s_at_t0, lag_confidence_at_t0,
              obs_latency_poly_p99_at_t0, obs_latency_opi_p99_at_t0
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            ON CONFLICT (shock_id, horizon_s) DO UPDATE SET
              sample_valid_flag=EXCLUDED.sample_valid_flag,
              move_flag=EXCLUDED.move_flag,
              first_move_lag_s=EXCLUDED.first_move_lag_s,
              opi_return=EXCLUDED.opi_return,
              opi_price_delta=EXCLUDED.opi_price_delta,
              opi_volume_sum=EXCLUDED.opi_volume_sum,
              opi_cvd_sum=EXCLUDED.opi_cvd_sum,
              opi_spread_change=EXCLUDED.opi_spread_change,
              opi_depth_change=EXCLUDED.opi_depth_change,
              opi_staleness_at_t0_ms=EXCLUDED.opi_staleness_at_t0_ms,
              opi_liquidity_pull_flag_at_t0=EXCLUDED.opi_liquidity_pull_flag_at_t0,
              true_net_spread_at_t0=EXCLUDED.true_net_spread_at_t0,
              arb_mode_flag_at_t0=EXCLUDED.arb_mode_flag_at_t0,
              estimated_lag_s_at_t0=EXCLUDED.estimated_lag_s_at_t0,
              lag_confidence_at_t0=EXCLUDED.lag_confidence_at_t0,
              obs_latency_poly_p99_at_t0=EXCLUDED.obs_latency_poly_p99_at_t0,
              obs_latency_opi_p99_at_t0=EXCLUDED.obs_latency_opi_p99_at_t0
            "#,
                )
                .bind(row.shock_id)
                .bind(row.horizon_s)
                .bind(row.sample_valid_flag)
                .bind(row.move_flag)
                .bind(row.first_move_lag_s)
                .bind(row.opi_return)
                .bind(row.opi_price_delta)
                .bind(row.opi_volume_sum)
                .bind(row.opi_cvd_sum)
                .bind(row.opi_spread_change)
                .bind(row.opi_depth_change)
                .bind(row.opi_staleness_at_t0_ms)
                .bind(row.opi_liquidity_pull_flag_at_t0)
                .bind(row.true_net_spread_at_t0)
                .bind(row.arb_mode_flag_at_t0)
                .bind(row.estimated_lag_s_at_t0)
                .bind(row.lag_confidence_at_t0)
                .bind(row.obs_latency_poly_p99_at_t0)
                .bind(row.obs_latency_opi_p99_at_t0)
                .execute(&mut *tx),
                "opinion_response upsert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    if !trade_audit_stage.is_empty() && !had_error {
        for (_, row) in trade_audit_stage.iter() {
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO trade_audit (
              event_ts_ms, pair_id, token_side, phase, direction, reason,
              pm_move, pm_mom, ofi_250ms, edge_raw, edge_norm,
              entry_price, entry_qty, exit_price_target, exit_price,
              gas_est_usd, min_profit_usd,
              book_decay, book_support, book_scale,
              retrace_ratio, inv_ratio, inv_factor, decay_factor,
              pnl_usd_net, holding_ms, mae, mfe,
              obs_latency_poly_p99_ms, obs_latency_opi_p99_ms,
              bar_gap_flag, poly_bar_gap_flag, opi_bar_gap_flag,
              poly_staleness_ms, opi_staleness_ms,
              detail_json
            ) VALUES (
              $1, $2, $3, $4, $5, $6,
              $7, $8, $9, $10, $11,
              $12, $13, $14, $15,
              $16, $17,
              $18, $19, $20,
              $21, $22, $23, $24,
              $25, $26, $27, $28,
              $29, $30,
              $31, $32, $33,
              $34, $35,
              $36
            )
            "#,
                )
                .bind(row.event_ts_ms)
                .bind(row.pair_id)
                .bind(row.token_side.as_str())
                .bind(&row.phase)
                .bind(row.direction)
                .bind(row.reason.as_deref())
                .bind(row.pm_move)
                .bind(row.pm_mom)
                .bind(row.ofi_250ms)
                .bind(row.edge_raw)
                .bind(row.edge_norm)
                .bind(row.entry_price)
                .bind(row.entry_qty)
                .bind(row.exit_price_target)
                .bind(row.exit_price)
                .bind(row.gas_est_usd)
                .bind(row.min_profit_usd)
                .bind(row.book_decay)
                .bind(row.book_support)
                .bind(row.book_scale)
                .bind(row.retrace_ratio)
                .bind(row.inv_ratio)
                .bind(row.inv_factor)
                .bind(row.decay_factor)
                .bind(row.pnl_usd_net)
                .bind(row.holding_ms)
                .bind(row.mae)
                .bind(row.mfe)
                .bind(row.obs_latency_poly_p99_ms)
                .bind(row.obs_latency_opi_p99_ms)
                .bind(row.bar_gap_flag)
                .bind(row.poly_bar_gap_flag)
                .bind(row.opi_bar_gap_flag)
                .bind(row.poly_staleness_ms)
                .bind(row.opi_staleness_ms)
                .bind(row.detail_json.as_deref())
                .execute(&mut *tx),
                "trade_audit insert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    if !cancel_snapshot_stage.is_empty() && !had_error {
        for (_, row) in cancel_snapshot_stage.iter() {
            if !exec_log!(
                sqlx::query(
                r#"
            INSERT INTO cancel_trigger_snapshot (
              event_ts_ms, pair_id, token_side, direction,
              order_role, order_id, client_id,
              cancel_reason, cancel_priority,
              ofi_250ms, pm_sell_trade_notional_250ms, pm_retrace_ratio,
              delta_bid_notional_250ms, delta_ask_notional_250ms,
              opi_book_decay, opi_book_support, opi_spread_ticks
            ) VALUES (
              $1, $2, $3, $4,
              $5, $6, $7,
              $8, $9,
              $10, $11, $12,
              $13, $14,
              $15, $16, $17
            )
            "#,
                )
                .bind(row.event_ts_ms)
                .bind(row.pair_id)
                .bind(row.token_side.as_str())
                .bind(row.direction)
                .bind(&row.order_role)
                .bind(row.order_id.as_deref())
                .bind(row.client_id.as_deref())
                .bind(&row.cancel_reason)
                .bind(row.cancel_priority)
                .bind(row.ofi_250ms)
                .bind(row.pm_sell_trade_notional_250ms)
                .bind(row.pm_retrace_ratio)
                .bind(row.delta_bid_notional_250ms)
                .bind(row.delta_ask_notional_250ms)
                .bind(row.opi_book_decay)
                .bind(row.opi_book_support)
                .bind(row.opi_spread_ticks)
                .execute(&mut *tx),
                "cancel_trigger_snapshot insert"
            ) {
                had_error = true;
                break;
            }
        }
    }

    let commit_ok = if had_error {
        false
    } else {
        match tx.commit().await {
            Ok(_) => true,
            Err(err) => {
                warn!("db_writer commit failed err={}", err);
                false
            }
        }
    };

    if !commit_ok {
        raw_buf.extend(raw_stage);
        token_buf.extend(token_stage);
        pair_buf.extend(pair_stage);
        clock_buf.extend(clock_stage);
        token_registry_buf.extend(token_registry_stage);
        override_buf.extend(override_stage);
        resync_buf.extend(resync_stage);
        health_buf.extend(health_stage);
        chain_buf.extend(chain_stage);
        shock_buf.extend(shock_stage);
        shock_update_buf.extend(shock_update_stage);
        response_buf.extend(response_stage);
        trade_audit_buf.extend(trade_audit_stage);
        cancel_snapshot_buf.extend(cancel_snapshot_stage);
        return;
    }

    for (id, _) in raw_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in token_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in pair_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in clock_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in token_registry_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in override_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in resync_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in health_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in chain_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in shock_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in shock_update_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in response_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in trade_audit_stage.iter() {
        let _ = ack_tx.send(*id);
    }
    for (id, _) in cancel_snapshot_stage.iter() {
        let _ = ack_tx.send(*id);
    }
}
