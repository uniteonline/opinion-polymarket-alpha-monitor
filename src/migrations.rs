use anyhow::Context;
use sqlx::{Row, SqlitePool};
use std::collections::HashSet;

pub async fn run_migrations(pool: &SqlitePool) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS monitor_migrations (\
         version INTEGER PRIMARY KEY,\
         applied_ts_ms INTEGER NOT NULL\
         );",
    )
    .execute(pool)
    .await?;

    let rows = sqlx::query("SELECT version FROM monitor_migrations")
        .fetch_all(pool)
        .await?;
    let mut applied = HashSet::new();
    for row in rows {
        let version: i64 = row.try_get("version").context("version")?;
        applied.insert(version);
    }

    if !applied.contains(&1) {
        migrate_v1(pool).await?;
        sqlx::query("INSERT INTO monitor_migrations(version, applied_ts_ms) VALUES (?, ?)")
            .bind(1_i64)
            .bind(crate::time_utils::now_ts_ms())
            .execute(pool)
            .await?;
    }
    if !applied.contains(&2) {
        migrate_v2(pool).await?;
        sqlx::query("INSERT INTO monitor_migrations(version, applied_ts_ms) VALUES (?, ?)")
            .bind(2_i64)
            .bind(crate::time_utils::now_ts_ms())
            .execute(pool)
            .await?;
    }
    if !applied.contains(&3) {
        migrate_v3(pool).await?;
        sqlx::query("INSERT INTO monitor_migrations(version, applied_ts_ms) VALUES (?, ?)")
            .bind(3_i64)
            .bind(crate::time_utils::now_ts_ms())
            .execute(pool)
            .await?;
    }
    if !applied.contains(&4) {
        migrate_v4(pool).await?;
        sqlx::query("INSERT INTO monitor_migrations(version, applied_ts_ms) VALUES (?, ?)")
            .bind(4_i64)
            .bind(crate::time_utils::now_ts_ms())
            .execute(pool)
            .await?;
    }
    if !applied.contains(&5) {
        migrate_v5(pool).await?;
        sqlx::query("INSERT INTO monitor_migrations(version, applied_ts_ms) VALUES (?, ?)")
            .bind(5_i64)
            .bind(crate::time_utils::now_ts_ms())
            .execute(pool)
            .await?;
    }
    if !applied.contains(&6) {
        migrate_v6(pool).await?;
        sqlx::query("INSERT INTO monitor_migrations(version, applied_ts_ms) VALUES (?, ?)")
            .bind(6_i64)
            .bind(crate::time_utils::now_ts_ms())
            .execute(pool)
            .await?;
    }
    if !applied.contains(&7) {
        migrate_v7(pool).await?;
        sqlx::query("INSERT INTO monitor_migrations(version, applied_ts_ms) VALUES (?, ?)")
            .bind(7_i64)
            .bind(crate::time_utils::now_ts_ms())
            .execute(pool)
            .await?;
    }
    if !applied.contains(&8) {
        migrate_v8(pool).await?;
        sqlx::query("INSERT INTO monitor_migrations(version, applied_ts_ms) VALUES (?, ?)")
            .bind(8_i64)
            .bind(crate::time_utils::now_ts_ms())
            .execute(pool)
            .await?;
    }

    Ok(())
}

async fn migrate_v1(pool: &SqlitePool) -> anyhow::Result<()> {
    ensure_column(
        pool,
        "bars_1s_token",
        "event_count_1s",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(
        pool,
        "bars_1s_token",
        "ts_missing_count_1s",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(
        pool,
        "bars_1s_token",
        "ts_anomaly_count_1s",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(
        pool,
        "bars_1s_token",
        "book_resync_flag",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    ensure_column(pool, "raw_events", "channel", "TEXT").await?;
    ensure_column(
        pool,
        "poly_shock_events",
        "status",
        "TEXT NOT NULL DEFAULT 'PENDING'",
    )
    .await?;
    ensure_column(
        pool,
        "poly_shock_events",
        "peak_finalized",
        "INTEGER NOT NULL DEFAULT 0",
    )
    .await?;
    Ok(())
}

async fn migrate_v2(pool: &SqlitePool) -> anyhow::Result<()> {
    ensure_column(pool, "bars_1s_token", "last_price", "REAL").await?;
    ensure_column(pool, "bars_1s_pair", "opi_last_price", "REAL").await?;
    ensure_index(
        pool,
        "idx_bars_1s_pair_opi_price_cover",
        "CREATE INDEX IF NOT EXISTS idx_bars_1s_pair_opi_price_cover \
         ON bars_1s_pair(pair_id, token_side, bar_second, opi_last_price) \
         WHERE opi_last_price IS NOT NULL",
    )
    .await?;
    Ok(())
}

async fn migrate_v3(pool: &SqlitePool) -> anyhow::Result<()> {
    for column in [
        "bid_l1_notional",
        "bid_l2_notional",
        "bid_l3_notional",
        "ask_l1_notional",
        "ask_l2_notional",
        "ask_l3_notional",
    ] {
        ensure_column(pool, "bars_1s_token", column, "REAL").await?;
    }
    ensure_column(pool, "bars_1s_token", "ofi_250ms", "REAL NOT NULL DEFAULT 0").await?;
    ensure_column(
        pool,
        "bars_1s_token",
        "max_trade_notional_1s",
        "REAL NOT NULL DEFAULT 0",
    )
    .await?;

    for column in [
        "opi_bid_l1_notional",
        "opi_bid_l2_notional",
        "opi_bid_l3_notional",
        "opi_ask_l1_notional",
        "opi_ask_l2_notional",
        "opi_ask_l3_notional",
        "poly_ofi_250ms",
        "poly_buy_notional_1s",
        "poly_sell_notional_1s",
        "poly_max_trade_notional_1s",
    ] {
        ensure_column(pool, "bars_1s_pair", column, "REAL").await?;
    }

    sqlx::query(
        "CREATE TABLE IF NOT EXISTS trade_audit (\
         id INTEGER PRIMARY KEY AUTOINCREMENT,\
         event_ts_ms INTEGER NOT NULL,\
         pair_id INTEGER NOT NULL,\
         token_side TEXT NOT NULL,\
         phase TEXT NOT NULL,\
         direction INTEGER NOT NULL,\
         reason TEXT,\
         pm_move REAL,\
         pm_mom REAL,\
         ofi_250ms REAL,\
         edge_raw REAL,\
         edge_norm REAL,\
         entry_price REAL,\
         entry_qty REAL,\
         exit_price_target REAL,\
         exit_price REAL,\
         gas_est_usd REAL,\
         min_profit_usd REAL,\
         book_decay REAL,\
         book_support REAL,\
         book_scale REAL,\
         retrace_ratio REAL,\
         inv_ratio REAL,\
         inv_factor REAL,\
         decay_factor REAL,\
         pnl_usd_net REAL,\
         holding_ms INTEGER,\
         mae REAL,\
         mfe REAL,\
         obs_latency_poly_p99_ms INTEGER,\
         obs_latency_opi_p99_ms INTEGER,\
         bar_gap_flag INTEGER,\
         poly_bar_gap_flag INTEGER,\
         opi_bar_gap_flag INTEGER,\
         poly_staleness_ms INTEGER,\
         opi_staleness_ms INTEGER,\
         detail_json TEXT\
         );",
    )
    .execute(pool)
    .await?;
    ensure_index(
        pool,
        "idx_trade_audit_time",
        "CREATE INDEX IF NOT EXISTS idx_trade_audit_time ON trade_audit(event_ts_ms)",
    )
    .await?;
    ensure_index(
        pool,
        "idx_trade_audit_pair_time",
        "CREATE INDEX IF NOT EXISTS idx_trade_audit_pair_time \
         ON trade_audit(pair_id, token_side, event_ts_ms)",
    )
    .await?;

    Ok(())
}

async fn migrate_v4(pool: &SqlitePool) -> anyhow::Result<()> {
    for column in [
        "bid_delta_notional_250ms",
        "ask_delta_notional_250ms",
        "mid_slope_250ms",
        "buy_notional_250ms",
        "sell_notional_250ms",
        "max_trade_notional_250ms",
    ] {
        let definition = match column {
            "mid_slope_250ms" => "REAL",
            _ => "REAL NOT NULL DEFAULT 0",
        };
        ensure_column(pool, "bars_1s_token", column, definition).await?;
    }

    for column in [
        "poly_buy_notional_250ms",
        "poly_sell_notional_250ms",
        "poly_max_trade_notional_250ms",
        "poly_bid_delta_notional_250ms",
        "poly_ask_delta_notional_250ms",
        "poly_mid_slope_250ms",
        "poly_obs_latency_p99_ms",
        "opi_obs_latency_p99_ms",
    ] {
        let definition = match column {
            "poly_mid_slope_250ms" => "REAL",
            "poly_obs_latency_p99_ms" | "opi_obs_latency_p99_ms" => "INTEGER",
            _ => "REAL",
        };
        ensure_column(pool, "bars_1s_pair", column, definition).await?;
    }

    Ok(())
}

async fn migrate_v5(pool: &SqlitePool) -> anyhow::Result<()> {
    for column in [
        "obs_latency_poly_p99_ms",
        "obs_latency_opi_p99_ms",
        "bar_gap_flag",
        "poly_bar_gap_flag",
        "opi_bar_gap_flag",
        "poly_staleness_ms",
        "opi_staleness_ms",
    ] {
        ensure_column(pool, "trade_audit", column, "INTEGER").await?;
    }
    Ok(())
}

async fn migrate_v6(pool: &SqlitePool) -> anyhow::Result<()> {
    for column in [
        "opi_taker_buy_notional_1s",
        "opi_taker_sell_notional_1s",
    ] {
        ensure_column(pool, "bars_1s_pair", column, "REAL").await?;
    }
    Ok(())
}

async fn migrate_v7(pool: &SqlitePool) -> anyhow::Result<()> {
    ensure_column(pool, "bars_1s_token", "bar_stale_flag", "INTEGER").await?;
    for column in ["poly_bar_stale_flag", "opi_bar_stale_flag", "bar_stale_flag"] {
        ensure_column(pool, "bars_1s_pair", column, "INTEGER").await?;
    }
    Ok(())
}

async fn migrate_v8(pool: &SqlitePool) -> anyhow::Result<()> {
    for column in ["best_bid_px_state", "best_ask_px_state", "mid_px_state"] {
        ensure_column(pool, "bars_1s_token", column, "REAL").await?;
    }
    for column in [
        "poly_best_bid_state",
        "poly_best_ask_state",
        "poly_mid_state",
        "opi_best_bid_state",
        "opi_best_ask_state",
        "opi_mid_state",
    ] {
        ensure_column(pool, "bars_1s_pair", column, "REAL").await?;
    }
    Ok(())
}

async fn ensure_column(
    pool: &SqlitePool,
    table: &str,
    column: &str,
    definition: &str,
) -> anyhow::Result<()> {
    if column_exists(pool, table, column).await? {
        return Ok(());
    }
    let sql = format!("ALTER TABLE {table} ADD COLUMN {column} {definition}");
    sqlx::query(&sql)
        .execute(pool)
        .await
        .with_context(|| format!("adding column {column} to {table}"))?;
    Ok(())
}

async fn column_exists(pool: &SqlitePool, table: &str, column: &str) -> anyhow::Result<bool> {
    let sql = format!("PRAGMA table_info({table})");
    let rows = sqlx::query(&sql).fetch_all(pool).await?;
    for row in rows {
        let name: String = row.try_get("name").context("column name")?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn ensure_index(pool: &SqlitePool, name: &str, sql: &str) -> anyhow::Result<()> {
    if index_exists(pool, name).await? {
        return Ok(());
    }
    sqlx::query(sql).execute(pool).await?;
    Ok(())
}

async fn index_exists(pool: &SqlitePool, index: &str) -> anyhow::Result<bool> {
    let row = sqlx::query(
        "SELECT name FROM sqlite_master WHERE type='index' AND name = ? LIMIT 1",
    )
    .bind(index)
    .fetch_optional(pool)
    .await?;
    Ok(row.is_some())
}
