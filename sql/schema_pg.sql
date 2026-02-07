-- Postgres schema for monitoring

CREATE TABLE IF NOT EXISTS monitor_meta (
  k TEXT PRIMARY KEY,
  v TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS monitor_migrations (
  version BIGINT PRIMARY KEY,
  applied_ts_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS pair_registry (
  pair_id BIGINT PRIMARY KEY,
  watchlist_version BIGINT NOT NULL,
  status TEXT NOT NULL,
  root_market_title TEXT,
  polymarket_event_title TEXT,
  opinion_market_id TEXT,
  opinion_yes_token_id TEXT,
  opinion_no_token_id TEXT,
  polymarket_market_id TEXT,
  polymarket_yes_token_id TEXT,
  polymarket_no_token_id TEXT,
  created_local_ts_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_pair_registry_status
  ON pair_registry(status, watchlist_version);

CREATE TABLE IF NOT EXISTS pair_token_overrides (
  pair_id BIGINT PRIMARY KEY,
  mapping_overridden_flag INTEGER NOT NULL DEFAULT 0,
  opinion_yes_token_id_override TEXT,
  opinion_no_token_id_override TEXT,
  polymarket_yes_token_id_override TEXT,
  polymarket_no_token_id_override TEXT,
  updated_local_ts_ms BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS token_registry (
  token_key TEXT PRIMARY KEY,
  venue TEXT NOT NULL,
  external_token_id TEXT,
  market_id TEXT,
  outcome_side INTEGER,
  token_side TEXT,
  first_seen_local_ts_ms BIGINT NOT NULL,
  last_seen_local_ts_ms BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_token_registry_venue_market
  ON token_registry(venue, market_id);

CREATE TABLE IF NOT EXISTS connection_health_1s (
  venue TEXT NOT NULL,
  bar_second BIGINT NOT NULL,
  ws_state INTEGER NOT NULL,
  reconnect_count INTEGER NOT NULL,
  last_msg_age_ms BIGINT,
  heartbeat_sent INTEGER,
  heartbeat_fail INTEGER,
  dropped_events INTEGER NOT NULL DEFAULT 0,
  notes TEXT,
  PRIMARY KEY (venue, bar_second)
);

CREATE TABLE IF NOT EXISTS clock_offsets (
  venue TEXT NOT NULL,
  local_ts_ms BIGINT NOT NULL,
  exchange_ts_ms BIGINT NOT NULL,
  sample_offset_ms BIGINT NOT NULL,
  smoothed_offset_ms BIGINT NOT NULL,
  delta_clamped_ms BIGINT NOT NULL,
  PRIMARY KEY (venue, local_ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_clock_offsets_venue_time
  ON clock_offsets(venue, local_ts_ms);

CREATE TABLE IF NOT EXISTS raw_events (
  id BIGSERIAL PRIMARY KEY,
  venue TEXT NOT NULL,
  token_key TEXT NOT NULL,
  pair_id BIGINT,
  token_side TEXT,
  kind TEXT NOT NULL,
  channel TEXT,
  exchange_ts_ms BIGINT NOT NULL,
  local_ts_ms BIGINT NOT NULL,
  exchange_ts_source INTEGER NOT NULL,
  obs_latency_ms BIGINT,
  out_of_order_flag INTEGER NOT NULL DEFAULT 0,
  ts_missing_flag INTEGER NOT NULL DEFAULT 0,
  ts_anomaly_flag INTEGER NOT NULL DEFAULT 0,
  trade_side_missing_flag INTEGER NOT NULL DEFAULT 0,
  venue_hash TEXT,
  payload_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_raw_events_token_exchange
  ON raw_events(token_key, exchange_ts_ms);

CREATE INDEX IF NOT EXISTS idx_raw_events_pair_exchange
  ON raw_events(pair_id, exchange_ts_ms);

CREATE INDEX IF NOT EXISTS idx_raw_events_local
  ON raw_events(local_ts_ms);

CREATE TABLE IF NOT EXISTS orderbook_resync_log (
  id BIGSERIAL PRIMARY KEY,
  venue TEXT NOT NULL,
  token_key TEXT NOT NULL,
  reason TEXT NOT NULL,
  trigger_local_ts_ms BIGINT NOT NULL,
  trigger_exchange_ts_ms BIGINT,
  venue_hash_before TEXT,
  venue_hash_after TEXT,
  local_topn_hash_before TEXT,
  local_topn_hash_after TEXT
);

CREATE INDEX IF NOT EXISTS idx_resync_token_time
  ON orderbook_resync_log(token_key, trigger_local_ts_ms);

CREATE TABLE IF NOT EXISTS bars_1s_token (
  venue TEXT NOT NULL,
  token_key TEXT NOT NULL,
  token_side TEXT NOT NULL,
  bar_second BIGINT NOT NULL,
  exchange_ts_source INTEGER NOT NULL,
  obs_latency_venue_p50_ms BIGINT,
  obs_latency_venue_p90_ms BIGINT,
  obs_latency_venue_p99_ms BIGINT,
  obs_latency_venue_max_ms BIGINT,
  obs_latency_est_p50_ms BIGINT,
  obs_latency_est_p90_ms BIGINT,
  obs_latency_est_p99_ms BIGINT,
  obs_latency_est_max_ms BIGINT,
  connection_health_flags INTEGER NOT NULL DEFAULT 0,
  bar_gap_flag INTEGER NOT NULL DEFAULT 0,
  bar_stale_flag INTEGER NOT NULL DEFAULT 0,
  staleness_ms BIGINT,
  event_count_1s INTEGER NOT NULL DEFAULT 0,
  ts_missing_count_1s INTEGER NOT NULL DEFAULT 0,
  ts_anomaly_count_1s INTEGER NOT NULL DEFAULT 0,
  book_resync_flag INTEGER NOT NULL DEFAULT 0,
  best_bid_px DOUBLE PRECISION,
  best_ask_px DOUBLE PRECISION,
  mid_px DOUBLE PRECISION,
  best_bid_px_state DOUBLE PRECISION,
  best_ask_px_state DOUBLE PRECISION,
  mid_px_state DOUBLE PRECISION,
  last_price DOUBLE PRECISION,
  micro_px DOUBLE PRECISION,
  spread_px DOUBLE PRECISION,
  tick_px DOUBLE PRECISION,
  spread_ticks INTEGER,
  top3_depth_bid_notional DOUBLE PRECISION,
  top3_depth_ask_notional DOUBLE PRECISION,
  bid_l1_notional DOUBLE PRECISION,
  bid_l2_notional DOUBLE PRECISION,
  bid_l3_notional DOUBLE PRECISION,
  ask_l1_notional DOUBLE PRECISION,
  ask_l2_notional DOUBLE PRECISION,
  ask_l3_notional DOUBLE PRECISION,
  top10_depth_bid_notional DOUBLE PRECISION,
  top10_depth_ask_notional DOUBLE PRECISION,
  depth_1pct_bid_notional DOUBLE PRECISION,
  depth_1pct_ask_notional DOUBLE PRECISION,
  imbalance_top10 DOUBLE PRECISION,
  air_pocket_bid_ticks INTEGER,
  air_pocket_ask_ticks INTEGER,
  book_updates_1s INTEGER NOT NULL DEFAULT 0,
  add_updates_1s INTEGER NOT NULL DEFAULT 0,
  cancel_updates_1s INTEGER NOT NULL DEFAULT 0,
  match_updates_1s INTEGER NOT NULL DEFAULT 0,
  remove_unknown_updates_1s INTEGER NOT NULL DEFAULT 0,
  trade_count_1s INTEGER NOT NULL DEFAULT 0,
  volume_shares_1s DOUBLE PRECISION NOT NULL DEFAULT 0,
  volume_notional_1s DOUBLE PRECISION NOT NULL DEFAULT 0,
  vwap_1s DOUBLE PRECISION,
  buy_notional_1s DOUBLE PRECISION NOT NULL DEFAULT 0,
  sell_notional_1s DOUBLE PRECISION NOT NULL DEFAULT 0,
  cvd_delta_1s DOUBLE PRECISION NOT NULL DEFAULT 0,
  trade_side_missing_count_1s INTEGER NOT NULL DEFAULT 0,
  ofi_1s DOUBLE PRECISION NOT NULL DEFAULT 0,
  ofi_250ms DOUBLE PRECISION NOT NULL DEFAULT 0,
  bid_delta_notional_250ms DOUBLE PRECISION NOT NULL DEFAULT 0,
  ask_delta_notional_250ms DOUBLE PRECISION NOT NULL DEFAULT 0,
  mid_slope_250ms DOUBLE PRECISION,
  ofi_10s DOUBLE PRECISION,
  ofi_z_30s DOUBLE PRECISION,
  levels_crossed_1s INTEGER NOT NULL DEFAULT 0,
  max_trade_notional_1s DOUBLE PRECISION NOT NULL DEFAULT 0,
  buy_notional_250ms DOUBLE PRECISION NOT NULL DEFAULT 0,
  sell_notional_250ms DOUBLE PRECISION NOT NULL DEFAULT 0,
  max_trade_notional_250ms DOUBLE PRECISION NOT NULL DEFAULT 0,
  depth_withdrawal_ratio DOUBLE PRECISION,
  liquidity_pull_flag INTEGER NOT NULL DEFAULT 0,
  venue_book_hash TEXT,
  local_topn_hash TEXT,
  PRIMARY KEY (venue, token_key, bar_second)
);

CREATE INDEX IF NOT EXISTS idx_bars_1s_token_pm_analysis
  ON bars_1s_token(
    token_key,
    bar_second,
    book_updates_1s,
    volume_notional_1s,
    top10_depth_bid_notional,
    top10_depth_ask_notional
  )
  WHERE venue = 'pm';

CREATE TABLE IF NOT EXISTS bars_1s_pair (
  pair_id BIGINT NOT NULL,
  token_side TEXT NOT NULL,
  bar_second BIGINT NOT NULL,
  poly_bar_gap_flag INTEGER NOT NULL DEFAULT 0,
  opi_bar_gap_flag INTEGER NOT NULL DEFAULT 0,
  bar_gap_flag INTEGER NOT NULL DEFAULT 0,
  poly_bar_stale_flag INTEGER NOT NULL DEFAULT 0,
  opi_bar_stale_flag INTEGER NOT NULL DEFAULT 0,
  bar_stale_flag INTEGER NOT NULL DEFAULT 0,
  poly_staleness_ms BIGINT,
  opi_staleness_ms BIGINT,
  poly_best_bid DOUBLE PRECISION,
  poly_best_ask DOUBLE PRECISION,
  poly_mid DOUBLE PRECISION,
  poly_micro DOUBLE PRECISION,
  poly_best_bid_state DOUBLE PRECISION,
  poly_best_ask_state DOUBLE PRECISION,
  poly_mid_state DOUBLE PRECISION,
  opi_best_bid DOUBLE PRECISION,
  opi_best_ask DOUBLE PRECISION,
  opi_mid DOUBLE PRECISION,
  opi_best_bid_state DOUBLE PRECISION,
  opi_best_ask_state DOUBLE PRECISION,
  opi_mid_state DOUBLE PRECISION,
  opi_last_price DOUBLE PRECISION,
  opi_taker_buy_notional_1s DOUBLE PRECISION,
  opi_taker_sell_notional_1s DOUBLE PRECISION,
  opi_micro DOUBLE PRECISION,
  opi_bid_l1_notional DOUBLE PRECISION,
  opi_bid_l2_notional DOUBLE PRECISION,
  opi_bid_l3_notional DOUBLE PRECISION,
  opi_ask_l1_notional DOUBLE PRECISION,
  opi_ask_l2_notional DOUBLE PRECISION,
  opi_ask_l3_notional DOUBLE PRECISION,
  poly_fee_bps INTEGER,
  opi_fee_bps INTEGER,
  gas_cost DOUBLE PRECISION,
  network_congestion_flag INTEGER NOT NULL DEFAULT 0,
  gross_spread_buy DOUBLE PRECISION,
  gross_spread_sell DOUBLE PRECISION,
  true_net_spread_buy_opinion DOUBLE PRECISION,
  true_net_spread_sell_opinion DOUBLE PRECISION,
  arb_mode_flag INTEGER NOT NULL DEFAULT 0,
  directional_mode_flag INTEGER NOT NULL DEFAULT 0,
  poly_cvd_30s DOUBLE PRECISION,
  poly_buy_sell_ratio_30s DOUBLE PRECISION,
  price_pressure_index DOUBLE PRECISION,
  poly_ofi_250ms DOUBLE PRECISION,
  poly_buy_notional_1s DOUBLE PRECISION,
  poly_sell_notional_1s DOUBLE PRECISION,
  poly_max_trade_notional_1s DOUBLE PRECISION,
  poly_buy_notional_250ms DOUBLE PRECISION,
  poly_sell_notional_250ms DOUBLE PRECISION,
  poly_max_trade_notional_250ms DOUBLE PRECISION,
  poly_bid_delta_notional_250ms DOUBLE PRECISION,
  poly_ask_delta_notional_250ms DOUBLE PRECISION,
  poly_mid_slope_250ms DOUBLE PRECISION,
  poly_obs_latency_p99_ms BIGINT,
  opi_obs_latency_p99_ms BIGINT,
  rolling_corr_5m DOUBLE PRECISION,
  estimated_lag_s BIGINT,
  lag_confidence DOUBLE PRECISION,
  beta_1m DOUBLE PRECISION,
  follow_score_raw DOUBLE PRECISION,
  follow_score_adj DOUBLE PRECISION,
  PRIMARY KEY (pair_id, token_side, bar_second)
);

CREATE INDEX IF NOT EXISTS idx_bars_1s_pair_pair_side_time_cover
  ON bars_1s_pair(pair_id, token_side, bar_second DESC)
  INCLUDE (opi_last_price, opi_mid_state, opi_mid);
CREATE INDEX IF NOT EXISTS idx_bars_1s_pair_opi_any_cover
  ON bars_1s_pair(pair_id, token_side, bar_second DESC)
  INCLUDE (opi_last_price, opi_mid_state, opi_mid)
  WHERE opi_last_price IS NOT NULL OR opi_mid_state IS NOT NULL OR opi_mid IS NOT NULL;

CREATE TABLE IF NOT EXISTS fee_schedule (
  venue TEXT NOT NULL,
  fee_bps INTEGER NOT NULL,
  effective_from_ts_ms BIGINT NOT NULL,
  effective_to_ts_ms BIGINT,
  PRIMARY KEY (venue, effective_from_ts_ms)
);

CREATE TABLE IF NOT EXISTS chain_metrics (
  chain TEXT NOT NULL,
  local_ts_ms BIGINT NOT NULL,
  base_fee DOUBLE PRECISION,
  priority_fee DOUBLE PRECISION,
  gas_used_est DOUBLE PRECISION,
  tx_cost_est DOUBLE PRECISION,
  congestion_flag INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (chain, local_ts_ms)
);

CREATE INDEX IF NOT EXISTS idx_chain_metrics_time
  ON chain_metrics(local_ts_ms);

CREATE TABLE IF NOT EXISTS poly_shock_events (
  shock_id BIGSERIAL PRIMARY KEY,
  token_key TEXT NOT NULL,
  pair_id BIGINT,
  token_side TEXT NOT NULL,
  shock_type TEXT NOT NULL,
  direction INTEGER NOT NULL,
  noise_flag INTEGER NOT NULL DEFAULT 0,
  start_ts_ms BIGINT NOT NULL,
  trigger_ts_ms BIGINT NOT NULL,
  peak_ts_ms BIGINT NOT NULL,
  magnitude DOUBLE PRECISION NOT NULL,
  context_json TEXT,
  obs_latency_ms BIGINT,
  connection_health_flags INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'PENDING',
  peak_finalized INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_shock_time
  ON poly_shock_events(trigger_ts_ms);

CREATE INDEX IF NOT EXISTS idx_shock_token_time
  ON poly_shock_events(token_key, trigger_ts_ms);

CREATE INDEX IF NOT EXISTS idx_shock_type_time
  ON poly_shock_events(shock_type, trigger_ts_ms);

CREATE INDEX IF NOT EXISTS idx_shock_pair_time
  ON poly_shock_events(pair_id, token_side, trigger_ts_ms);

CREATE TABLE IF NOT EXISTS opinion_response (
  shock_id BIGINT NOT NULL,
  horizon_s INTEGER NOT NULL,
  sample_valid_flag INTEGER NOT NULL DEFAULT 1,
  move_flag INTEGER NOT NULL DEFAULT 0,
  first_move_lag_s BIGINT,
  opi_return DOUBLE PRECISION,
  opi_price_delta DOUBLE PRECISION,
  opi_volume_sum DOUBLE PRECISION,
  opi_cvd_sum DOUBLE PRECISION,
  opi_spread_change DOUBLE PRECISION,
  opi_depth_change DOUBLE PRECISION,
  opi_staleness_at_t0_ms BIGINT,
  opi_liquidity_pull_flag_at_t0 INTEGER,
  true_net_spread_at_t0 DOUBLE PRECISION,
  arb_mode_flag_at_t0 INTEGER,
  estimated_lag_s_at_t0 BIGINT,
  lag_confidence_at_t0 DOUBLE PRECISION,
  obs_latency_poly_p99_at_t0 BIGINT,
  obs_latency_opi_p99_at_t0 BIGINT,
  PRIMARY KEY (shock_id, horizon_s)
);

CREATE INDEX IF NOT EXISTS idx_opinion_response_horizon
  ON opinion_response(horizon_s, shock_id, sample_valid_flag);

CREATE TABLE IF NOT EXISTS trade_audit (
  id BIGSERIAL PRIMARY KEY,
  event_ts_ms BIGINT NOT NULL,
  pair_id BIGINT NOT NULL,
  token_side TEXT NOT NULL,
  phase TEXT NOT NULL,
  direction INTEGER NOT NULL,
  reason TEXT,
  pm_move DOUBLE PRECISION,
  pm_mom DOUBLE PRECISION,
  ofi_250ms DOUBLE PRECISION,
  edge_raw DOUBLE PRECISION,
  edge_norm DOUBLE PRECISION,
  entry_price DOUBLE PRECISION,
  entry_qty DOUBLE PRECISION,
  exit_price_target DOUBLE PRECISION,
  exit_price DOUBLE PRECISION,
  gas_est_usd DOUBLE PRECISION,
  min_profit_usd DOUBLE PRECISION,
  book_decay DOUBLE PRECISION,
  book_support DOUBLE PRECISION,
  book_scale DOUBLE PRECISION,
  retrace_ratio DOUBLE PRECISION,
  inv_ratio DOUBLE PRECISION,
  inv_factor DOUBLE PRECISION,
  decay_factor DOUBLE PRECISION,
  pnl_usd_net DOUBLE PRECISION,
  holding_ms BIGINT,
  mae DOUBLE PRECISION,
  mfe DOUBLE PRECISION,
  obs_latency_poly_p99_ms BIGINT,
  obs_latency_opi_p99_ms BIGINT,
  bar_gap_flag INTEGER,
  poly_bar_gap_flag INTEGER,
  opi_bar_gap_flag INTEGER,
  poly_staleness_ms BIGINT,
  opi_staleness_ms BIGINT,
  detail_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_trade_audit_time
  ON trade_audit(event_ts_ms);

CREATE INDEX IF NOT EXISTS idx_trade_audit_pair_time
  ON trade_audit(pair_id, token_side, event_ts_ms);

CREATE TABLE IF NOT EXISTS cancel_trigger_snapshot (
  id BIGSERIAL PRIMARY KEY,
  event_ts_ms BIGINT NOT NULL,
  pair_id BIGINT NOT NULL,
  token_side TEXT NOT NULL,
  direction INTEGER NOT NULL,
  order_role TEXT NOT NULL,
  order_id TEXT,
  client_id TEXT,
  cancel_reason TEXT NOT NULL,
  cancel_priority INTEGER NOT NULL,
  ofi_250ms DOUBLE PRECISION,
  pm_sell_trade_notional_250ms DOUBLE PRECISION,
  pm_retrace_ratio DOUBLE PRECISION,
  delta_bid_notional_250ms DOUBLE PRECISION,
  delta_ask_notional_250ms DOUBLE PRECISION,
  opi_book_decay DOUBLE PRECISION,
  opi_book_support DOUBLE PRECISION,
  opi_spread_ticks INTEGER
);

CREATE INDEX IF NOT EXISTS idx_cancel_trigger_snapshot_time
  ON cancel_trigger_snapshot(event_ts_ms);

CREATE INDEX IF NOT EXISTS idx_cancel_trigger_snapshot_pair_time
  ON cancel_trigger_snapshot(pair_id, token_side, event_ts_ms);

ALTER TABLE bars_1s_token
  ADD COLUMN IF NOT EXISTS bar_stale_flag INTEGER NOT NULL DEFAULT 0;

ALTER TABLE bars_1s_pair
  ADD COLUMN IF NOT EXISTS poly_bar_stale_flag INTEGER NOT NULL DEFAULT 0;
ALTER TABLE bars_1s_pair
  ADD COLUMN IF NOT EXISTS opi_bar_stale_flag INTEGER NOT NULL DEFAULT 0;
ALTER TABLE bars_1s_pair
  ADD COLUMN IF NOT EXISTS bar_stale_flag INTEGER NOT NULL DEFAULT 0;

ALTER TABLE bars_1s_token
  ADD COLUMN IF NOT EXISTS best_bid_px_state DOUBLE PRECISION;
ALTER TABLE bars_1s_token
  ADD COLUMN IF NOT EXISTS best_ask_px_state DOUBLE PRECISION;
ALTER TABLE bars_1s_token
  ADD COLUMN IF NOT EXISTS mid_px_state DOUBLE PRECISION;

ALTER TABLE bars_1s_pair
  ADD COLUMN IF NOT EXISTS poly_best_bid_state DOUBLE PRECISION;
ALTER TABLE bars_1s_pair
  ADD COLUMN IF NOT EXISTS poly_best_ask_state DOUBLE PRECISION;
ALTER TABLE bars_1s_pair
  ADD COLUMN IF NOT EXISTS poly_mid_state DOUBLE PRECISION;
ALTER TABLE bars_1s_pair
  ADD COLUMN IF NOT EXISTS opi_best_bid_state DOUBLE PRECISION;
ALTER TABLE bars_1s_pair
  ADD COLUMN IF NOT EXISTS opi_best_ask_state DOUBLE PRECISION;
ALTER TABLE bars_1s_pair
  ADD COLUMN IF NOT EXISTS opi_mid_state DOUBLE PRECISION;
