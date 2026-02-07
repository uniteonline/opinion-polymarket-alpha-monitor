#!/usr/bin/env python3
import argparse
import math
import os
import statistics
import time
from typing import Any, Dict, Iterable, List, Optional, Tuple


def parse_horizons(raw: str) -> List[int]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return [int(p) for p in parts]


def parse_alpha_cols(raw: str) -> List[str]:
    parts = [p.strip() for p in raw.split(",") if p.strip()]
    return parts


def fmt_float(value: Optional[float], width: int = 10, prec: int = 4) -> str:
    if value is None:
        return " " * (width - 4) + "n/a"
    return f"{value:>{width}.{prec}f}"


def mean(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return sum(values) / len(values)


def median(values: List[float]) -> Optional[float]:
    if not values:
        return None
    return statistics.median(values)


def quantile(values: List[float], q: float) -> Optional[float]:
    if not values:
        return None
    if q <= 0.0:
        return min(values)
    if q >= 1.0:
        return max(values)
    values_sorted = sorted(values)
    pos = q * (len(values_sorted) - 1)
    lo = int(math.floor(pos))
    hi = int(math.ceil(pos))
    if lo == hi:
        return values_sorted[lo]
    weight = pos - lo
    return values_sorted[lo] * (1.0 - weight) + values_sorted[hi] * weight


def safe_log_return(start: Optional[float], end: Optional[float]) -> Optional[float]:
    if start is None or end is None:
        return None
    if start <= 0.0 or end <= 0.0:
        return None
    return math.log(end / start)


class LeadLagAgg:
    def __init__(self) -> None:
        self.n = 0
        self.sum_x = 0.0
        self.sum_y = 0.0
        self.sum_z = 0.0
        self.sum_x2 = 0.0
        self.sum_y2 = 0.0
        self.sum_z2 = 0.0
        self.sum_xy = 0.0
        self.sum_xz = 0.0
        self.sum_yz = 0.0

    def add(self, x: float, y: float, z: float) -> None:
        self.n += 1
        self.sum_x += x
        self.sum_y += y
        self.sum_z += z
        self.sum_x2 += x * x
        self.sum_y2 += y * y
        self.sum_z2 += z * z
        self.sum_xy += x * y
        self.sum_xz += x * z
        self.sum_yz += y * z

    def corr_xy(self) -> Optional[float]:
        if self.n < 2:
            return None
        num = self.n * self.sum_xy - self.sum_x * self.sum_y
        den_x = self.n * self.sum_x2 - self.sum_x * self.sum_x
        den_y = self.n * self.sum_y2 - self.sum_y * self.sum_y
        if den_x <= 0.0 or den_y <= 0.0:
            return None
        return num / math.sqrt(den_x * den_y)

    def regression(self) -> Tuple[Optional[float], Optional[float], Optional[float]]:
        # Solve y = a + b*x + c*z
        if self.n < 3:
            return (None, None, None)
        s0 = float(self.n)
        sx = self.sum_x
        sz = self.sum_z
        sxx = self.sum_x2
        szz = self.sum_z2
        sxz = self.sum_xz
        sy = self.sum_y
        sxy = self.sum_xy
        syz = self.sum_yz

        a11, a12, a13 = s0, sx, sz
        a21, a22, a23 = sx, sxx, sxz
        a31, a32, a33 = sz, sxz, szz
        b1, b2, b3 = sy, sxy, syz

        det = (
            a11 * (a22 * a33 - a23 * a32)
            - a12 * (a21 * a33 - a23 * a31)
            + a13 * (a21 * a32 - a22 * a31)
        )
        if det == 0.0:
            return (None, None, None)

        def det3(
            b11: float,
            b12: float,
            b13: float,
            b21: float,
            b22: float,
            b23: float,
            b31: float,
            b32: float,
            b33: float,
        ) -> float:
            return (
                b11 * (b22 * b33 - b23 * b32)
                - b12 * (b21 * b33 - b23 * b31)
                + b13 * (b21 * b32 - b22 * b31)
            )

        det_a = det3(b1, a12, a13, b2, a22, a23, b3, a32, a33)
        det_b = det3(a11, b1, a13, a21, b2, a23, a31, b3, a33)
        det_c = det3(a11, a12, b1, a21, a22, b2, a31, a32, b3)

        a = det_a / det
        b = det_b / det
        c = det_c / det

        sse = (
            self.sum_y2
            + a * a * s0
            + b * b * sxx
            + c * c * szz
            + 2.0 * a * b * sx
            + 2.0 * a * c * sz
            + 2.0 * b * c * sxz
            - 2.0 * a * sy
            - 2.0 * b * sxy
            - 2.0 * c * syz
        )
        sst = self.sum_y2 - (sy * sy) / s0 if s0 > 0.0 else 0.0
        r2 = None
        if sst > 0.0 and sse >= 0.0:
            r2 = 1.0 - (sse / sst)
        return (b, c, r2)


class DbClient:
    def __init__(self, conn: Any, param_style: str) -> None:
        self.conn = conn
        self.param_style = param_style

    def close(self) -> None:
        self.conn.close()

    def execute(self, sql: str, params: Optional[List[object]] = None) -> None:
        sql = adapt_placeholders(sql, self.param_style)
        cursor = self.conn.cursor()
        cursor.execute(sql, params or [])
        cursor.close()

    def query(self, sql: str, params: Optional[List[object]] = None) -> Iterable[Dict[str, Any]]:
        sql = adapt_placeholders(sql, self.param_style)
        cursor = self.conn.cursor()
        cursor.execute(sql, params or [])
        cols = [desc[0] for desc in cursor.description] if cursor.description else []
        for row in cursor:
            yield dict(zip(cols, row))
        cursor.close()

    def fetch_all(
        self, sql: str, params: Optional[List[object]] = None
    ) -> List[Dict[str, Any]]:
        return list(self.query(sql, params))


def adapt_placeholders(sql: str, param_style: str) -> str:
    return sql.replace("?", "%s")


def make_placeholders(count: int, param_style: str) -> str:
    if count <= 0:
        return ""
    token = "%s"
    return ",".join(token for _ in range(count))


def connect_db(target: str) -> DbClient:
    if not (target.startswith("postgres://") or target.startswith("postgresql://")):
        raise SystemExit("--db must be a postgres URL (postgresql://...)")
    try:
        import psycopg  # type: ignore

        conn = psycopg.connect(target)
        conn.autocommit = True
        return DbClient(conn, "pyformat")
    except ImportError:
        try:
            import psycopg2  # type: ignore

            conn = psycopg2.connect(target)
            conn.autocommit = True
            return DbClient(conn, "pyformat")
        except ImportError as exc:
            raise SystemExit(
                "Postgres URL provided but psycopg/psycopg2 is not installed. "
                "Install one of them (e.g. `pip install psycopg[binary]`)."
            ) from exc


def iter_pair_rows(
    rows: Iterable[Dict[str, Any]], alpha_cols: List[str]
) -> Dict[Tuple[int, str], Dict[int, Dict[str, Optional[float]]]]:
    data: Dict[Tuple[int, str], Dict[int, Dict[str, Optional[float]]]] = {}
    for row in rows:
        pair_id = int(row["pair_id"])
        token_side = row["token_side"]
        bar_second = int(row["bar_second"])
        opi_last_price = row.get("opi_last_price")
        opi_mid = row.get("opi_mid_state") or row.get("opi_mid")
        poly_mid = row.get("poly_mid_state") or row.get("poly_mid")
        opi_price = opi_last_price if opi_last_price is not None else opi_mid
        entry = {
            "poly_mid": poly_mid,
            "opi_mid": opi_mid,
            "opi_last_price": opi_last_price,
            "opi_price": opi_price,
        }
        for col in alpha_cols:
            entry[col] = row.get(col)
        data.setdefault((pair_id, token_side), {})[bar_second] = entry
    return data


def build_lead_lag(
    data: Dict[Tuple[int, str], Dict[int, Dict[str, Optional[float]]]],
    alpha_cols: List[str],
    lags: List[int],
) -> Dict[Tuple[str, int], LeadLagAgg]:
    stats: Dict[Tuple[str, int], LeadLagAgg] = {
        (alpha, lag): LeadLagAgg() for alpha in alpha_cols for lag in lags
    }
    for _, series in data.items():
        if not series:
            continue
        seconds = sorted(series.keys())
        for t in seconds:
            row = series[t]
            opi_mid_t = row.get("opi_price")
            poly_mid_t = row.get("poly_mid")
            if opi_mid_t is None or poly_mid_t is None:
                continue
            lag_returns: List[Tuple[int, float, float]] = []
            for lag in lags:
                t2 = t + lag
                row2 = series.get(t2)
                if row2 is None:
                    continue
                r_opi = safe_log_return(opi_mid_t, row2.get("opi_price"))
                r_pm = safe_log_return(poly_mid_t, row2.get("poly_mid"))
                if r_opi is None or r_pm is None:
                    continue
                lag_returns.append((lag, r_opi, r_pm))
            if not lag_returns:
                continue
            for alpha in alpha_cols:
                x = row.get(alpha)
                if x is None:
                    continue
                x_f = float(x)
                for lag, r_opi, r_pm in lag_returns:
                    stats[(alpha, lag)].add(x_f, r_opi, r_pm)
    return stats


def validate_alpha_cols(db: DbClient, alpha_cols: List[str]) -> List[str]:
    available = set()
    rows = db.query(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'public' AND table_name = 'bars_1s_pair'"
    )
    for row in rows:
        name = row.get("column_name")
        if name:
            available.add(name)
    cleaned = []
    for col in alpha_cols:
        if not col.replace("_", "").isalnum():
            continue
        if col not in available:
            continue
        cleaned.append(col)
    return cleaned


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Alpha follow report from monitoring.db (shock_type vs Opinion response)."
    )
    parser.add_argument(
        "--db",
        default=os.environ.get(
            "MONITOR_DB_URL",
            "postgresql://monitor:monitor123@127.0.0.1:5433/monitor",
        ),
        help="Postgres URL (postgresql://...)",
    )
    parser.add_argument(
        "--horizons",
        default="120,300,600",
        help="Comma-separated horizons in seconds",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=30,
        help="Minimum valid samples per group",
    )
    parser.add_argument(
        "--alpha-cols",
        default="follow_score_raw,follow_score_adj,price_pressure_index,poly_cvd_30s,poly_buy_sell_ratio_30s",
        help="Comma-separated alpha columns from bars_1s_pair for lead-lag analysis",
    )
    parser.add_argument(
        "--lead-lag-max",
        type=int,
        default=10,
        help="Lead-lag max seconds (lags from -N..N)",
    )
    parser.add_argument(
        "--lead-lag-min-count",
        type=int,
        default=30,
        help="Minimum samples per lag to print lead-lag stats",
    )
    parser.add_argument(
        "--include-gap-bars",
        action="store_true",
        help="Include bar_gap_flag rows in lead-lag analysis",
    )
    parser.add_argument(
        "--by-direction",
        action="store_true",
        help="Split results by shock direction (+1/-1)",
    )
    parser.add_argument(
        "--include-noise",
        action="store_true",
        help="Include noise_flag=1 shocks",
    )
    parser.add_argument(
        "--liquidity-window",
        type=int,
        default=60,
        help="Window seconds for liquidity filters on shock samples",
    )
    parser.add_argument(
        "--analysis-gap-allow-s",
        type=int,
        default=5,
        help="Lookback seconds for baseline mid in first-move analysis",
    )
    parser.add_argument(
        "--analysis-move-eps",
        type=float,
        default=1e-4,
        help="Minimum absolute price change to count as a move",
    )
    parser.add_argument(
        "--pm-min-book-updates",
        type=int,
        default=20,
        help="Minimum PM book updates in liquidity window (shock filter and lead-lag)",
    )
    parser.add_argument(
        "--pm-min-notional",
        type=float,
        default=0.0,
        help="Minimum PM notional volume in liquidity window (shock filter and lead-lag)",
    )
    parser.add_argument(
        "--opi-min-mid-ratio",
        type=float,
        default=0.5,
        help="Minimum OPI price signal non-null ratio in liquidity window (shock filter and lead-lag)",
    )
    parser.add_argument(
        "--liq-quantile",
        type=float,
        default=None,
        help="Override depth/updates quantiles when explicit ones are not provided (defaults 0.75/0.60)",
    )
    parser.add_argument(
        "--liq-depth-quantile",
        type=float,
        default=0.75,
        help="Quantile for depth threshold (default 0.75)",
    )
    parser.add_argument(
        "--liq-updates-quantile",
        type=float,
        default=0.60,
        help="Quantile for updates threshold (default 0.60)",
    )
    parser.add_argument(
        "--log-sql-timing",
        action="store_true",
        default=True,
        help="Print timing and progress for SQL queries (default: on)",
    )
    parser.add_argument(
        "--no-log-sql-timing",
        action="store_false",
        dest="log_sql_timing",
        help="Disable timing and progress logs",
    )
    parser.add_argument(
        "--log-progress-interval",
        type=int,
        default=20,
        help="Seconds between progress logs when --log-sql-timing is set",
    )
    parser.add_argument(
        "--no-precompute-pm-activity",
        action="store_false",
        dest="precompute_pm_activity",
        help="Disable precomputing pm_activity into a temp table",
    )
    parser.add_argument(
        "--precompute-batch-size",
        type=int,
        default=20000,
        help="Shock_id batch size when precomputing pm_activity",
    )
    parser.set_defaults(precompute_pm_activity=True)
    args = parser.parse_args()

    if not args.db:
        raise SystemExit("--db is required (or set MONITOR_DB_URL)")
    horizons = parse_horizons(args.horizons)
    db = connect_db(args.db)
    horizon_placeholders = make_placeholders(len(horizons), db.param_style)
    filter_pm_updates = args.pm_min_book_updates > 0
    filter_pm_notional = args.pm_min_notional > 0.0
    filter_pm = filter_pm_updates or filter_pm_notional
    filter_opi = args.opi_min_mid_ratio > 0.0
    precompute_opi_coverage = filter_opi
    pm_activity_join = "JOIN bars_1s_token b"
    pm_activity_join_bt = "JOIN bars_1s_token bt"

    pm_activity_cte = f"""
            pm_activity AS (
                SELECT
                    s.shock_id AS shock_id,
                    SUM(COALESCE(b.book_updates_1s, 0)) AS pm_updates_60s,
                    SUM(COALESCE(b.volume_notional_1s, 0.0)) AS pm_notional_60s
                FROM shock_base s
                {pm_activity_join}
                  ON b.venue = 'pm'
                 AND b.token_key = s.token_key
                 AND b.bar_second BETWEEN s.t0 - ? AND s.t0
                GROUP BY s.shock_id
            )
    """

    pm_activity_cte_move = f"""
        pm_activity AS (
            SELECT
                s.shock_id AS shock_id,
                SUM(COALESCE(bt.book_updates_1s, 0)) AS pm_updates_60s,
                SUM(COALESCE(bt.volume_notional_1s, 0.0)) AS pm_notional_60s
            FROM shock_base s
            JOIN pm_map m
              ON m.pair_id = s.pair_id
             AND m.token_side = s.token_side
            {pm_activity_join_bt}
              ON bt.venue = 'pm'
             AND bt.token_key = m.pm_token_key
             AND bt.bar_second BETWEEN s.t0 - ? AND s.t0
            GROUP BY s.shock_id
        )
    """

    opi_coverage_cte = """
            opi_coverage AS (
                SELECT
                    s.shock_id AS shock_id,
                    AVG(
                        CASE
                            WHEN COALESCE(p.opi_last_price, p.opi_mid_state, p.opi_mid) IS NOT NULL THEN 1.0
                            ELSE 0.0
                        END
                    ) AS opi_mid_ratio_60s
                FROM shock_base s
                JOIN bars_1s_pair p
                  ON p.pair_id = s.pair_id
                 AND p.token_side = s.token_side
                 AND p.bar_second BETWEEN s.t0 - ? AND s.t0
                GROUP BY s.shock_id
            )
    """

    if args.precompute_pm_activity:
        if args.log_sql_timing:
            print("precomputing pm_activity...", flush=True)
        pre_t0 = time.monotonic()
        db.execute("DROP TABLE IF EXISTS pm_activity_tmp")
        db.execute("DROP TABLE IF EXISTS pm_activity_tmp_move")
        db.execute(
            """
            CREATE TEMP TABLE pm_activity_tmp (
                shock_id BIGINT NOT NULL,
                pm_updates_60s BIGINT,
                pm_notional_60s DOUBLE PRECISION
            )
            """
        )
        db.execute(
            """
            CREATE TEMP TABLE pm_activity_tmp_move (
                shock_id BIGINT NOT NULL,
                pm_updates_60s BIGINT,
                pm_notional_60s DOUBLE PRECISION
            )
            """
        )
        total_shocks: Optional[int] = None
        if args.log_sql_timing:
            count_sql = f"""
                SELECT COUNT(DISTINCT shock_id) AS cnt
                FROM opinion_response
                WHERE horizon_s IN ({horizon_placeholders})
            """
            rows = db.fetch_all(count_sql, list(horizons))
            if rows:
                total_shocks = int(rows[0]["cnt"] or 0)
        pre_last_log = pre_t0
        last_shock_id = 0
        batch = 0
        total_inserted = 0
        while True:
            batch += 1
            batch_sql = f"""
                WITH resp_batch AS (
                    SELECT DISTINCT shock_id
                    FROM opinion_response
                    WHERE horizon_s IN ({horizon_placeholders})
                      AND shock_id > ?
                    ORDER BY shock_id
                    LIMIT ?
                ),
                shock_base AS (
                    SELECT
                        e.shock_id,
                        e.token_key,
                        CAST(e.trigger_ts_ms / 1000 AS INTEGER) AS t0
                    FROM resp_batch r
                    JOIN poly_shock_events e
                      ON e.shock_id = r.shock_id
                ),
                ins AS (
                    INSERT INTO pm_activity_tmp (shock_id, pm_updates_60s, pm_notional_60s)
                    SELECT
                        s.shock_id AS shock_id,
                        SUM(COALESCE(b.book_updates_1s, 0)) AS pm_updates_60s,
                        SUM(COALESCE(b.volume_notional_1s, 0.0)) AS pm_notional_60s
                    FROM shock_base s
                    JOIN bars_1s_token b
                      ON b.venue = 'pm'
                     AND b.token_key = s.token_key
                     AND b.bar_second BETWEEN s.t0 - ? AND s.t0
                    GROUP BY s.shock_id
                    RETURNING shock_id
                )
                SELECT
                    (SELECT MAX(shock_id) FROM resp_batch) AS max_id,
                    (SELECT COUNT(*) FROM resp_batch) AS batch_count,
                    (SELECT COUNT(*) FROM ins) AS inserted
            """
            batch_params: List[object] = []
            batch_params.extend(horizons)
            batch_params.append(last_shock_id)
            batch_params.append(args.precompute_batch_size)
            batch_params.append(args.liquidity_window)
            result_rows = db.fetch_all(batch_sql, batch_params)
            if not result_rows:
                break
            batch_count = int(result_rows[0]["batch_count"] or 0)
            inserted = int(result_rows[0]["inserted"] or 0)
            max_id = result_rows[0]["max_id"]
            if batch_count <= 0 or max_id is None:
                break
            last_shock_id = int(max_id)
            total_inserted += inserted
            if args.log_sql_timing:
                now = time.monotonic()
                if now - pre_last_log >= max(args.log_progress_interval, 1) or (
                    batch_count < args.precompute_batch_size
                ):
                    elapsed = now - pre_t0
                    rate = (total_inserted / elapsed) if elapsed > 0 else 0.0
                    if total_shocks is not None and total_shocks > 0:
                        progress = f"{total_inserted}/{total_shocks}"
                    else:
                        progress = f"{total_inserted}"
                    print(
                        f"pm_activity batch={batch} shocks={batch_count} inserted={inserted} "
                        f"total={progress} elapsed_s={elapsed:.1f} rate_per_s={rate:.1f}",
                        flush=True,
                    )
                    pre_last_log = now
            if batch_count < args.precompute_batch_size:
                break
        db.execute("CREATE INDEX ON pm_activity_tmp(shock_id)")
        db.execute("ANALYZE pm_activity_tmp")
        if args.log_sql_timing:
            pre_elapsed = time.monotonic() - pre_t0
            print(f"pm_activity precompute done elapsed_s={pre_elapsed:.1f}", flush=True)

        if args.log_sql_timing:
            print("precomputing pm_activity_move...", flush=True)
        move_t0 = time.monotonic()
        move_last_log = move_t0
        last_shock_id = 0
        batch = 0
        total_inserted = 0
        while True:
            batch += 1
            batch_sql_move = f"""
                WITH pm_map AS (
                    SELECT pair_id,
                           'YES' AS token_side,
                           ('pm:asset:' || polymarket_yes_token_id) AS pm_token_key
                    FROM pair_registry
                    UNION ALL
                    SELECT pair_id,
                           'NO' AS token_side,
                           ('pm:asset:' || polymarket_no_token_id) AS pm_token_key
                    FROM pair_registry
                ),
                resp_batch AS (
                    SELECT DISTINCT shock_id
                    FROM opinion_response
                    WHERE horizon_s IN ({horizon_placeholders})
                      AND shock_id > ?
                    ORDER BY shock_id
                    LIMIT ?
                ),
                shock_base AS (
                    SELECT
                        e.shock_id,
                        e.pair_id,
                        e.token_side,
                        CAST(e.trigger_ts_ms / 1000 AS INTEGER) AS t0
                    FROM resp_batch r
                    JOIN poly_shock_events e
                      ON e.shock_id = r.shock_id
                ),
                shock_map AS (
                    SELECT
                        s.shock_id,
                        m.pm_token_key AS pm_token_key,
                        s.t0
                    FROM shock_base s
                    JOIN pm_map m
                      ON m.pair_id = s.pair_id
                     AND m.token_side = s.token_side
                ),
                ins AS (
                    INSERT INTO pm_activity_tmp_move (shock_id, pm_updates_60s, pm_notional_60s)
                    SELECT
                        s.shock_id AS shock_id,
                        SUM(COALESCE(b.book_updates_1s, 0)) AS pm_updates_60s,
                        SUM(COALESCE(b.volume_notional_1s, 0.0)) AS pm_notional_60s
                    FROM shock_map s
                    JOIN bars_1s_token b
                      ON b.venue = 'pm'
                     AND b.token_key = s.pm_token_key
                     AND b.bar_second BETWEEN s.t0 - ? AND s.t0
                    GROUP BY s.shock_id
                    RETURNING shock_id
                )
                SELECT
                    (SELECT MAX(shock_id) FROM resp_batch) AS max_id,
                    (SELECT COUNT(*) FROM resp_batch) AS batch_count,
                    (SELECT COUNT(*) FROM ins) AS inserted
            """
            batch_params = []
            batch_params.extend(horizons)
            batch_params.append(last_shock_id)
            batch_params.append(args.precompute_batch_size)
            batch_params.append(args.liquidity_window)
            result_rows = db.fetch_all(batch_sql_move, batch_params)
            if not result_rows:
                break
            batch_count = int(result_rows[0]["batch_count"] or 0)
            inserted = int(result_rows[0]["inserted"] or 0)
            max_id = result_rows[0]["max_id"]
            if batch_count <= 0 or max_id is None:
                break
            last_shock_id = int(max_id)
            total_inserted += inserted
            if args.log_sql_timing:
                now = time.monotonic()
                if now - move_last_log >= max(args.log_progress_interval, 1) or (
                    batch_count < args.precompute_batch_size
                ):
                    elapsed = now - move_t0
                    rate = (total_inserted / elapsed) if elapsed > 0 else 0.0
                    if total_shocks is not None and total_shocks > 0:
                        progress = f"{total_inserted}/{total_shocks}"
                    else:
                        progress = f"{total_inserted}"
                    print(
                        f"pm_activity_move batch={batch} shocks={batch_count} inserted={inserted} "
                        f"total={progress} elapsed_s={elapsed:.1f} rate_per_s={rate:.1f}",
                        flush=True,
                    )
                    move_last_log = now
            if batch_count < args.precompute_batch_size:
                break
        db.execute("CREATE INDEX ON pm_activity_tmp_move(shock_id)")
        db.execute("ANALYZE pm_activity_tmp_move")
        if args.log_sql_timing:
            move_elapsed = time.monotonic() - move_t0
            print(f"pm_activity_move precompute done elapsed_s={move_elapsed:.1f}", flush=True)

        pm_activity_cte = """
            pm_activity AS (
                SELECT shock_id, pm_updates_60s, pm_notional_60s
                FROM pm_activity_tmp
            )
        """
        pm_activity_cte_move = """
        pm_activity AS (
            SELECT shock_id, pm_updates_60s, pm_notional_60s
            FROM pm_activity_tmp_move
        )
    """

    if precompute_opi_coverage:
        if args.log_sql_timing:
            print("precomputing opi_coverage...", flush=True)
        opi_t0 = time.monotonic()
        db.execute("DROP TABLE IF EXISTS opi_coverage_tmp")
        db.execute(
            """
            CREATE TEMP TABLE opi_coverage_tmp (
                shock_id BIGINT NOT NULL,
                opi_mid_ratio_60s DOUBLE PRECISION
            )
            """
        )
        total_shocks: Optional[int] = None
        if args.log_sql_timing:
            count_sql = f"""
                SELECT COUNT(DISTINCT shock_id) AS cnt
                FROM opinion_response
                WHERE horizon_s IN ({horizon_placeholders})
            """
            rows = db.fetch_all(count_sql, list(horizons))
            if rows:
                total_shocks = int(rows[0]["cnt"] or 0)
        opi_last_log = opi_t0
        last_shock_id = 0
        batch = 0
        total_inserted = 0
        while True:
            batch += 1
            batch_sql = f"""
                WITH resp_batch AS (
                    SELECT DISTINCT shock_id
                    FROM opinion_response
                    WHERE horizon_s IN ({horizon_placeholders})
                      AND shock_id > ?
                    ORDER BY shock_id
                    LIMIT ?
                ),
                shock_base AS (
                    SELECT
                        e.shock_id,
                        e.pair_id,
                        e.token_side,
                        CAST(e.trigger_ts_ms / 1000 AS INTEGER) AS t0
                    FROM resp_batch r
                    JOIN poly_shock_events e
                      ON e.shock_id = r.shock_id
                ),
                ins AS (
                    INSERT INTO opi_coverage_tmp (shock_id, opi_mid_ratio_60s)
                    SELECT
                        s.shock_id AS shock_id,
                        AVG(
                            CASE
                                WHEN COALESCE(p.opi_last_price, p.opi_mid_state, p.opi_mid) IS NOT NULL THEN 1.0
                                ELSE 0.0
                            END
                        ) AS opi_mid_ratio_60s
                    FROM shock_base s
                    JOIN bars_1s_pair p
                      ON p.pair_id = s.pair_id
                     AND p.token_side = s.token_side
                     AND p.bar_second BETWEEN s.t0 - ? AND s.t0
                    GROUP BY s.shock_id
                    RETURNING shock_id
                )
                SELECT
                    (SELECT MAX(shock_id) FROM resp_batch) AS max_id,
                    (SELECT COUNT(*) FROM resp_batch) AS batch_count,
                    (SELECT COUNT(*) FROM ins) AS inserted
            """
            batch_params: List[object] = []
            batch_params.extend(horizons)
            batch_params.append(last_shock_id)
            batch_params.append(args.precompute_batch_size)
            batch_params.append(args.liquidity_window)
            result_rows = db.fetch_all(batch_sql, batch_params)
            if not result_rows:
                break
            batch_count = int(result_rows[0]["batch_count"] or 0)
            inserted = int(result_rows[0]["inserted"] or 0)
            max_id = result_rows[0]["max_id"]
            if batch_count <= 0 or max_id is None:
                break
            last_shock_id = int(max_id)
            total_inserted += inserted
            if args.log_sql_timing:
                now = time.monotonic()
                if now - opi_last_log >= max(args.log_progress_interval, 1) or (
                    batch_count < args.precompute_batch_size
                ):
                    elapsed = now - opi_t0
                    rate = (total_inserted / elapsed) if elapsed > 0 else 0.0
                    if total_shocks is not None and total_shocks > 0:
                        progress = f"{total_inserted}/{total_shocks}"
                    else:
                        progress = f"{total_inserted}"
                    print(
                        f"opi_coverage batch={batch} shocks={batch_count} inserted={inserted} "
                        f"total={progress} elapsed_s={elapsed:.1f} rate_per_s={rate:.1f}",
                        flush=True,
                    )
                    opi_last_log = now
            if batch_count < args.precompute_batch_size:
                break
        db.execute("CREATE INDEX ON opi_coverage_tmp(shock_id)")
        db.execute("ANALYZE opi_coverage_tmp")
        if args.log_sql_timing:
            opi_elapsed = time.monotonic() - opi_t0
            print(f"opi_coverage precompute done elapsed_s={opi_elapsed:.1f}", flush=True)

        opi_coverage_cte = """
            opi_coverage AS (
                SELECT shock_id, opi_mid_ratio_60s
                FROM opi_coverage_tmp
            )
        """

    use_liquidity = filter_pm or filter_opi
    if use_liquidity:
        ctes = [
            f"""
            resp AS NOT MATERIALIZED (
                SELECT
                    shock_id,
                    horizon_s,
                    sample_valid_flag,
                    opi_return,
                    opi_price_delta,
                    estimated_lag_s_at_t0,
                    lag_confidence_at_t0
                FROM opinion_response
                WHERE horizon_s IN ({horizon_placeholders})
            )
            """.strip(),
            """
            shock_base AS (
                SELECT DISTINCT
                    e.shock_id,
                    e.shock_type,
                    e.direction,
                    e.noise_flag,
                    e.pair_id,
                    e.token_side,
                    e.token_key,
                    CAST(e.trigger_ts_ms / 1000 AS INTEGER) AS t0
                FROM resp r
                JOIN poly_shock_events e
                  ON e.shock_id = r.shock_id
            )
            """.strip(),
        ]
        if filter_pm:
            ctes.append(pm_activity_cte.strip())
        if filter_opi:
            ctes.append(opi_coverage_cte.strip())
        cte_sql = ",\n".join(ctes)

        pm_join = ""
        if filter_pm:
            pm_on = "pm.shock_id = s.shock_id"
            if filter_pm_updates:
                pm_on += " AND pm.pm_updates_60s >= ?"
            if filter_pm_notional:
                pm_on += " AND pm.pm_notional_60s >= ?"
            pm_join = f"JOIN pm_activity pm ON {pm_on}"

        opi_join = ""
        if filter_opi:
            opi_join = (
                "JOIN opi_coverage oc ON oc.shock_id = s.shock_id "
                "AND oc.opi_mid_ratio_60s >= ?"
            )

        query = f"""
            WITH {cte_sql}
            SELECT
                s.shock_type,
                s.direction,
                s.noise_flag,
                r.horizon_s,
                r.sample_valid_flag,
                r.opi_return,
                r.opi_price_delta,
                r.estimated_lag_s_at_t0,
                r.lag_confidence_at_t0
            FROM shock_base s
            JOIN resp r ON r.shock_id = s.shock_id
            {pm_join}
            {opi_join}
        """
    else:
        query = f"""
            WITH resp AS NOT MATERIALIZED (
                SELECT
                    shock_id,
                    horizon_s,
                    sample_valid_flag,
                    opi_return,
                    opi_price_delta,
                    estimated_lag_s_at_t0,
                    lag_confidence_at_t0
                FROM opinion_response
                WHERE horizon_s IN ({horizon_placeholders})
            )
            SELECT
                e.shock_type,
                e.direction,
                e.noise_flag,
                r.horizon_s,
                r.sample_valid_flag,
                r.opi_return,
                r.opi_price_delta,
                r.estimated_lag_s_at_t0,
                r.lag_confidence_at_t0
            FROM resp r
            JOIN poly_shock_events e
              ON e.shock_id = r.shock_id
        """

    print("loading shock stats...", flush=True)
    shock_t0 = time.monotonic()
    shock_last_log = shock_t0
    params: List[object] = []
    params.extend(horizons)
    if use_liquidity:
        if filter_pm and not args.precompute_pm_activity:
            params.append(args.liquidity_window)
        if filter_opi and not precompute_opi_coverage:
            params.append(args.liquidity_window)
        if filter_pm_updates:
            params.append(args.pm_min_book_updates)
        if filter_pm_notional:
            params.append(args.pm_min_notional)
        if filter_opi:
            params.append(args.opi_min_mid_ratio)
    row_count = 0
    rows_iter = db.query(query, params)

    groups: Dict[Tuple, Dict] = {}
    for row in rows_iter:
        row_count += 1
        if not args.include_noise and row["noise_flag"]:
            continue
        shock_type = row["shock_type"]
        direction = int(row["direction"] or 0)
        horizon = int(row["horizon_s"])
        valid = int(row["sample_valid_flag"] or 0)
        ret = row["opi_return"]
        delta = row["opi_price_delta"]
        lag_s = row["estimated_lag_s_at_t0"]
        lag_conf = row["lag_confidence_at_t0"]

        key = (shock_type, horizon)
        if args.by_direction:
            key = (shock_type, direction, horizon)

        bucket = groups.setdefault(
            key,
            {
                "shock_type": shock_type,
                "direction": direction,
                "horizon": horizon,
                "n_total": 0,
                "n_valid": 0,
                "n_dir": 0,
                "hits": 0,
                "returns": [],
                "price_delta": [],
                "lag_s": [],
                "lag_conf": [],
            },
        )

        bucket["n_total"] += 1
        if valid:
            bucket["n_valid"] += 1
        if valid and ret is not None:
            bucket["returns"].append(float(ret))
            if delta is not None:
                bucket["price_delta"].append(float(delta))
            if direction != 0:
                bucket["n_dir"] += 1
                if float(ret) * direction > 0:
                    bucket["hits"] += 1
        if valid and lag_s is not None:
            bucket["lag_s"].append(float(lag_s))
        if valid and lag_conf is not None:
            bucket["lag_conf"].append(float(lag_conf))
        if args.log_sql_timing:
            now = time.monotonic()
            if now - shock_last_log >= max(args.log_progress_interval, 1):
                elapsed = now - shock_t0
                rate = (row_count / elapsed) if elapsed > 0 else 0.0
                print(
                    f"shock stats progress rows={row_count} elapsed_s={elapsed:.1f} rate_per_s={rate:.1f}",
                    flush=True,
                )
                shock_last_log = now

    report_rows = []
    for bucket in groups.values():
        n_used = len(bucket["returns"])
        if n_used < args.min_count:
            continue
        avg_ret = mean(bucket["returns"])
        med_ret = median(bucket["returns"])
        avg_abs = mean([abs(v) for v in bucket["returns"]])
        avg_delta = mean(bucket["price_delta"])
        avg_lag = mean(bucket["lag_s"])
        avg_lag_conf = mean(bucket["lag_conf"])
        win_rate = None
        if bucket["n_dir"] > 0:
            win_rate = bucket["hits"] / bucket["n_dir"]

        report_rows.append(
            {
                "shock_type": bucket["shock_type"],
                "direction": bucket["direction"],
                "horizon": bucket["horizon"],
                "n_total": bucket["n_total"],
                "n_valid": bucket["n_valid"],
                "n_used": n_used,
                "win_rate": win_rate,
                "avg_ret_bps": avg_ret * 10000.0 if avg_ret is not None else None,
                "med_ret_bps": med_ret * 10000.0 if med_ret is not None else None,
                "avg_abs_ret_bps": avg_abs * 10000.0 if avg_abs is not None else None,
                "avg_price_delta": avg_delta,
                "avg_lag_s": avg_lag,
                "avg_lag_conf": avg_lag_conf,
            }
        )

    def sort_key(item: Dict) -> Tuple:
        win = item["win_rate"]
        win_sort = -1.0 if win is None else -win
        avg_ret = item["avg_ret_bps"]
        avg_ret_sort = -1.0 if avg_ret is None else -avg_ret
        return (item["horizon"], win_sort, avg_ret_sort)

    report_rows.sort(key=sort_key)
    shock_elapsed = time.monotonic() - shock_t0
    if args.log_sql_timing:
        print(
            f"shock stats query done rows={row_count} elapsed_s={shock_elapsed:.1f}",
            flush=True,
        )
    else:
        print(f"loaded shock stats rows={row_count}", flush=True)

    if args.by_direction:
        header = (
            "shock_type direction horizon n_total n_valid n_used "
            "win_rate avg_ret_bps med_ret_bps avg_abs_ret_bps "
            "avg_px_delta avg_lag_s avg_lag_conf"
        )
    else:
        header = (
            "shock_type horizon n_total n_valid n_used "
            "win_rate avg_ret_bps med_ret_bps avg_abs_ret_bps "
            "avg_px_delta avg_lag_s avg_lag_conf"
        )
    print(header)

    for item in report_rows:
        fields = []
        fields.append(f"{item['shock_type']:<20}")
        if args.by_direction:
            fields.append(f"{item['direction']:>9}")
        fields.append(f"{item['horizon']:>7}")
        fields.append(f"{item['n_total']:>7}")
        fields.append(f"{item['n_valid']:>7}")
        fields.append(f"{item['n_used']:>6}")
        win = item["win_rate"]
        fields.append(fmt_float(win, width=8, prec=3))
        fields.append(fmt_float(item["avg_ret_bps"], width=12, prec=3))
        fields.append(fmt_float(item["med_ret_bps"], width=12, prec=3))
        fields.append(fmt_float(item["avg_abs_ret_bps"], width=14, prec=3))
        fields.append(fmt_float(item["avg_price_delta"], width=12, prec=6))
        fields.append(fmt_float(item["avg_lag_s"], width=10, prec=2))
        fields.append(fmt_float(item["avg_lag_conf"], width=12, prec=3))
        print(" ".join(fields))

    print("\nFIRST-MOVE RESPONSE (event-driven lag distribution)")
    move_window = max(args.liquidity_window - 1, 0)
    noise_filter = ""
    if not args.include_noise:
        noise_filter = "AND e.noise_flag = 0"

    move_ctes = [
        """
        pm_map AS (
            SELECT pair_id,
                   'YES' AS token_side,
                   ('pm:asset:' || polymarket_yes_token_id) AS pm_token_key
            FROM pair_registry
            UNION ALL
            SELECT pair_id,
                   'NO' AS token_side,
                   ('pm:asset:' || polymarket_no_token_id) AS pm_token_key
            FROM pair_registry
        )
        """.strip(),
        f"""
        resp AS NOT MATERIALIZED (
            SELECT
                shock_id,
                horizon_s,
                sample_valid_flag,
                opi_return,
                opi_price_delta
            FROM opinion_response
            WHERE horizon_s IN ({horizon_placeholders})
        )
        """.strip(),
        f"""
        base AS (
            SELECT
                e.shock_id,
                e.shock_type,
                e.direction,
                e.noise_flag,
                e.pair_id,
                e.token_side,
                CAST(e.trigger_ts_ms / 1000 AS INTEGER) AS t0,
                r.horizon_s,
                r.sample_valid_flag,
                r.opi_return,
                r.opi_price_delta
            FROM resp r
            JOIN poly_shock_events e
              ON e.shock_id = r.shock_id
            WHERE 1 = 1
            {noise_filter}
        )
        """.strip(),
        """
        shock_base AS (
            SELECT DISTINCT
                shock_id,
                shock_type,
                direction,
                noise_flag,
                pair_id,
                token_side,
                t0
            FROM base
        )
        """.strip(),
        """
        pair_opi_all AS NOT MATERIALIZED (
            SELECT
                pair_id,
                token_side,
                bar_second,
                COALESCE(opi_last_price, opi_mid_state, opi_mid) AS opi_price
            FROM bars_1s_pair
        )
        """.strip(),
        """
        pair_opi_nonnull AS NOT MATERIALIZED (
            SELECT
                pair_id,
                token_side,
                bar_second,
                COALESCE(opi_last_price, opi_mid_state, opi_mid) AS opi_price
            FROM bars_1s_pair
            WHERE opi_last_price IS NOT NULL OR opi_mid_state IS NOT NULL OR opi_mid IS NOT NULL
        )
        """.strip(),
        """
        base_mid AS (
            SELECT
                s.shock_id,
                (
                    SELECT p.opi_price
                    FROM pair_opi_nonnull p
                    WHERE p.pair_id = s.pair_id
                      AND p.token_side = s.token_side
                      AND p.bar_second BETWEEN s.t0 - ? AND s.t0
                    ORDER BY p.bar_second DESC
                    LIMIT 1
                ) AS base_mid
            FROM shock_base s
        )
        """.strip(),
        pm_activity_cte_move.strip(),
    ]
    if filter_opi:
        move_ctes.append(opi_coverage_cte.strip())
    move_ctes.extend(
        [
            """
            pm_depth AS (
                SELECT
                    s.shock_id AS shock_id,
                    (bt.top10_depth_bid_notional + bt.top10_depth_ask_notional) AS pm_top10_depth_t0
                FROM shock_base s
                JOIN pm_map m
                  ON m.pair_id = s.pair_id
                 AND m.token_side = s.token_side
                JOIN bars_1s_token bt
                  ON bt.venue = 'pm'
                 AND bt.token_key = m.pm_token_key
                 AND bt.bar_second = s.t0
            )
            """.strip(),
            """
            moves AS (
                SELECT
                    s.shock_id,
                    mv.move_second
                FROM shock_base s
                JOIN base_mid bm
                  ON bm.shock_id = s.shock_id
                 AND bm.base_mid IS NOT NULL
                LEFT JOIN LATERAL (
                    SELECT p2.bar_second AS move_second
                    FROM pair_opi_nonnull p2
                    WHERE p2.pair_id = s.pair_id
                      AND p2.token_side = s.token_side
                      AND p2.bar_second > s.t0
                      AND p2.bar_second <= s.t0 + ?
                      AND ABS(p2.opi_price - bm.base_mid) > ?
                    ORDER BY p2.bar_second
                    LIMIT 1
                ) mv ON true
            )
            """.strip(),
        ]
    )
    move_cte_sql = ",\n".join(move_ctes)

    move_select_cols = [
        "b.shock_type",
        "b.direction",
        "b.noise_flag",
        "b.horizon_s",
        "b.sample_valid_flag",
        "b.opi_return",
        "b.opi_price_delta",
        "s.t0",
        "mv.move_second",
        "pm.pm_updates_60s",
        "pm.pm_notional_60s",
    ]
    if filter_opi:
        move_select_cols.append("oc.opi_mid_ratio_60s")
    move_select_cols.append("pd.pm_top10_depth_t0")
    move_select_sql = ",\n            ".join(move_select_cols)

    pm_join = "LEFT JOIN pm_activity pm ON pm.shock_id = b.shock_id"
    if filter_pm:
        pm_on = "pm.shock_id = b.shock_id"
        if filter_pm_updates:
            pm_on += " AND pm.pm_updates_60s >= ?"
        if filter_pm_notional:
            pm_on += " AND pm.pm_notional_60s >= ?"
        pm_join = f"JOIN pm_activity pm ON {pm_on}"

    opi_join = ""
    if filter_opi:
        opi_join = (
            "JOIN opi_coverage oc ON oc.shock_id = b.shock_id "
            "AND oc.opi_mid_ratio_60s >= ?"
        )

    move_query = f"""
        WITH {move_cte_sql}
        SELECT
            {move_select_sql}
        FROM base b
        JOIN shock_base s ON s.shock_id = b.shock_id
        LEFT JOIN moves mv ON mv.shock_id = b.shock_id
        {pm_join}
        {opi_join}
        LEFT JOIN pm_depth pd ON pd.shock_id = b.shock_id
        WHERE 1 = 1
    """
    move_params: List[object] = []
    move_params.extend(horizons)
    move_params.append(args.analysis_gap_allow_s)
    if not args.precompute_pm_activity:
        move_params.append(args.liquidity_window)
    if filter_opi and not precompute_opi_coverage:
        move_params.append(args.liquidity_window)
    move_params.append(move_window)
    move_params.append(args.analysis_move_eps)
    if filter_pm_updates:
        move_params.append(args.pm_min_book_updates)
    if filter_pm_notional:
        move_params.append(args.pm_min_notional)
    if filter_opi:
        move_params.append(args.opi_min_mid_ratio)

    print("loading first-move data...", flush=True)
    move_t0 = time.monotonic()
    last_log = move_t0
    move_row_count = 0
    move_cache: List[
        Tuple[str, int, int, int, int, Optional[int], int, Optional[float], float, float]
    ] = []
    depth_values: List[float] = []
    update_values: List[float] = []
    for row in db.query(move_query, move_params):
        move_row_count += 1
        shock_type = row["shock_type"]
        horizon = int(row["horizon_s"])
        direction = int(row["direction"] or 0)
        noise_flag = int(row["noise_flag"] or 0)
        sample_valid = int(row["sample_valid_flag"] or 0)
        move_second = row["move_second"]
        t0 = int(row["t0"])
        opi_return = row["opi_return"]
        depth = float(row["pm_top10_depth_t0"] or 0.0)
        updates = float(row["pm_updates_60s"] or 0.0)
        depth_values.append(depth)
        update_values.append(updates)
        move_cache.append(
            (
                shock_type,
                horizon,
                direction,
                noise_flag,
                sample_valid,
                move_second,
                t0,
                opi_return,
                depth,
                updates,
            )
        )
        if args.log_sql_timing:
            now = time.monotonic()
            if now - last_log >= max(args.log_progress_interval, 1):
                elapsed = now - move_t0
                rate = (move_row_count / elapsed) if elapsed > 0 else 0.0
                print(
                    f"first-move progress rows={move_row_count} elapsed_s={elapsed:.1f} rate_per_s={rate:.1f}",
                    flush=True,
                )
                last_log = now
    db.close()
    move_elapsed = time.monotonic() - move_t0
    if args.log_sql_timing:
        print(
            f"first-move query done rows={move_row_count} elapsed_s={move_elapsed:.1f}",
            flush=True,
        )
    else:
        print(f"loaded first-move rows={move_row_count}", flush=True)

    depth_q = args.liq_depth_quantile
    updates_q = args.liq_updates_quantile
    if args.liq_quantile is not None and depth_q == 0.75 and updates_q == 0.60:
        depth_q = args.liq_quantile
        updates_q = args.liq_quantile
    depth_q = min(max(depth_q, 0.0), 1.0)
    updates_q = min(max(updates_q, 0.0), 1.0)
    depth_p50 = quantile(depth_values, depth_q)
    updates_p50 = quantile(update_values, updates_q)
    if depth_p50 is not None and updates_p50 is not None:
        print(
            f"high_liq threshold: pm_top10_depth_t0 >= {depth_p50:.2f} (q={depth_q:.2f}), "
            f"pm_updates_60s >= {updates_p50:.1f} (q={updates_q:.2f})"
        )

    buckets: Dict[Tuple[str, int, str], Dict[str, object]] = {}
    for row in move_cache:
        shock_type, horizon, direction, noise_flag, sample_valid, move_second, t0, ret, depth, updates = row
        if not args.include_noise and noise_flag:
            continue

        is_high_liq = False
        if depth_p50 is not None and updates_p50 is not None:
            is_high_liq = depth >= depth_p50 and updates >= updates_p50

        for bucket_name, enabled in (("all", True), ("high_liq", is_high_liq)):
            if not enabled:
                continue
            key = (shock_type, horizon, bucket_name)
            bucket = buckets.setdefault(
                key,
                {
                    "shock_type": shock_type,
                    "horizon": horizon,
                    "bucket": bucket_name,
                    "n_total": 0,
                    "n_move": 0,
                    "n_dir": 0,
                    "dir_hits": 0,
                    "lags": [],
                    "abs_rets": [],
                },
            )

            bucket["n_total"] += 1
            move_within = move_second is not None and int(move_second) <= t0 + horizon
            if sample_valid and move_within and ret is not None:
                lag_s = int(move_second) - t0
                bucket["n_move"] += 1
                bucket["lags"].append(float(lag_s))
                bucket["abs_rets"].append(abs(float(ret)) * 10000.0)
                if direction != 0:
                    bucket["n_dir"] += 1
                    if float(ret) * direction > 0:
                        bucket["dir_hits"] += 1

    header = (
        "shock_type horizon bucket n_total n_move move_rate "
        "same_dir_rate lag_p50 lag_p90 abs_ret_p50_bps abs_ret_p90_bps"
    )
    print(header)
    for bucket in sorted(buckets.values(), key=lambda x: (x["horizon"], x["bucket"])):
        n_total = bucket["n_total"]
        n_move = bucket["n_move"]
        move_rate = (n_move / n_total) if n_total > 0 else None
        same_dir = None
        if bucket["n_dir"] > 0:
            same_dir = bucket["dir_hits"] / bucket["n_dir"]
        lag_p50 = quantile(bucket["lags"], 0.5)
        lag_p90 = quantile(bucket["lags"], 0.9)
        abs_p50 = quantile(bucket["abs_rets"], 0.5)
        abs_p90 = quantile(bucket["abs_rets"], 0.9)

        fields = [
            f"{bucket['shock_type']:<20}",
            f"{bucket['horizon']:>7}",
            f"{bucket['bucket']:<8}",
            f"{n_total:>7}",
            f"{n_move:>7}",
            fmt_float(move_rate, width=9, prec=3),
            fmt_float(same_dir, width=11, prec=3),
            fmt_float(lag_p50, width=8, prec=1),
            fmt_float(lag_p90, width=8, prec=1),
            fmt_float(abs_p50, width=14, prec=2),
            fmt_float(abs_p90, width=14, prec=2),
        ]
        print(" ".join(fields))


if __name__ == "__main__":
    main()
