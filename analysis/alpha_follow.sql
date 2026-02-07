-- Alpha follow summary (valid samples only, noise_flag=0)
-- Usage:
--   psql "postgresql://monitor:***@127.0.0.1:5432/monitor" -f monitor/analysis/alpha_follow.sql

WITH base AS (
  SELECT
    e.shock_type,
    e.direction,
    e.noise_flag,
    r.horizon_s,
    r.sample_valid_flag,
    r.opi_return,
    r.opi_price_delta
  FROM poly_shock_events e
  JOIN opinion_response r ON r.shock_id = e.shock_id
)
SELECT
  shock_type,
  horizon_s,
  COUNT(*) AS n_total,
  SUM(sample_valid_flag) AS n_valid,
  AVG(CASE WHEN sample_valid_flag = 1 THEN opi_return END) AS avg_opi_return,
  AVG(CASE WHEN sample_valid_flag = 1 THEN ABS(opi_return) END) AS avg_abs_return,
  AVG(
    CASE
      WHEN sample_valid_flag = 1 AND direction != 0 AND opi_return IS NOT NULL
        THEN CASE WHEN opi_return * direction > 0 THEN 1.0 ELSE 0.0 END
    END
  ) AS win_rate,
  AVG(CASE WHEN sample_valid_flag = 1 THEN opi_price_delta END) AS avg_price_delta
FROM base
WHERE noise_flag = 0
GROUP BY shock_type, horizon_s
ORDER BY horizon_s, win_rate DESC, avg_opi_return DESC;

-- Optional: split by direction
-- SELECT
--   shock_type,
--   direction,
--   horizon_s,
--   COUNT(*) AS n_total,
--   SUM(sample_valid_flag) AS n_valid,
--   AVG(CASE WHEN sample_valid_flag = 1 THEN opi_return END) AS avg_opi_return,
--   AVG(CASE WHEN sample_valid_flag = 1 THEN ABS(opi_return) END) AS avg_abs_return,
--   AVG(
--     CASE
--       WHEN sample_valid_flag = 1 AND direction != 0 AND opi_return IS NOT NULL
--         THEN CASE WHEN opi_return * direction > 0 THEN 1.0 ELSE 0.0 END
--     END
--   ) AS win_rate,
--   AVG(CASE WHEN sample_valid_flag = 1 THEN opi_price_delta END) AS avg_price_delta
-- FROM base
-- WHERE noise_flag = 0
-- GROUP BY shock_type, direction, horizon_s
-- ORDER BY horizon_s, win_rate DESC, avg_opi_return DESC;
