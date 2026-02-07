-- Pair follow quality (level alignment) with volatility filters.
-- Usage:
--   psql "postgresql://polymarket:***@127.0.0.1:5432/polymarket" -f monitor/analysis/follow_quality.sql
--
-- Notes:
-- - Uses state prices (carry-forward) when present.
-- - Adjust window/thresholds as needed to avoid low-volatility markets.

WITH w AS (
  SELECT
    pair_id,
    token_side,
    COALESCE(poly_mid_state, poly_mid) AS poly_mid,
    COALESCE(opi_mid_state, opi_mid) AS opi_mid
  FROM bars_1s_pair
  WHERE bar_second >= EXTRACT(EPOCH FROM now())::bigint - 86400
    AND COALESCE(poly_mid_state, poly_mid) IS NOT NULL
    AND COALESCE(opi_mid_state, opi_mid) IS NOT NULL
    AND bar_gap_flag = 0
),
stats AS (
  SELECT
    pair_id,
    token_side,
    COUNT(*) AS n_samples,
    STDDEV_POP(poly_mid) AS poly_mid_std_24h,
    (PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY poly_mid)
     - PERCENTILE_CONT(0.1) WITHIN GROUP (ORDER BY poly_mid)) AS poly_mid_p90_p10,
    COUNT(DISTINCT poly_mid) AS poly_mid_distinct,
    AVG(ABS(opi_mid - poly_mid)) AS avg_abs_diff,
    CORR(poly_mid, opi_mid) AS corr_level
  FROM w
  GROUP BY pair_id, token_side
),
shock_counts AS (
  SELECT
    pair_id,
    token_side,
    COUNT(*) AS poly_shock_events_count
  FROM poly_shock_events
  WHERE trigger_ts_ms >= (EXTRACT(EPOCH FROM now())::bigint * 1000) - 86400000
    AND noise_flag = 0
  GROUP BY pair_id, token_side
)
SELECT
  s.pair_id,
  s.token_side,
  s.n_samples,
  s.poly_mid_std_24h,
  s.poly_mid_p90_p10,
  s.poly_mid_distinct,
  s.avg_abs_diff,
  s.corr_level,
  COALESCE(sc.poly_shock_events_count, 0) AS poly_shock_events_count,
  pr.root_market_title,
  pr.polymarket_event_title
FROM stats s
LEFT JOIN shock_counts sc
  ON sc.pair_id = s.pair_id
  AND sc.token_side = s.token_side
LEFT JOIN pair_registry pr
  ON pr.pair_id = s.pair_id
WHERE s.n_samples >= 1000
  AND s.poly_mid_std_24h >= 0.01
  AND s.poly_mid_p90_p10 >= 0.02
  AND s.poly_mid_distinct >= 10
  AND s.avg_abs_diff <= 0.05
  AND COALESCE(sc.poly_shock_events_count, 0) >= 3
ORDER BY s.corr_level DESC NULLS LAST;
