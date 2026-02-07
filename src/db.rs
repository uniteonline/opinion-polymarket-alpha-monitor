use crate::models::{PairRecord, TokenRegistration, TokenSide, Venue};
use anyhow::Context;
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct MonitorDb {
    pub pool: PgPool,
}

impl MonitorDb {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(url)
            .await?;
        let db = Self { pool };
        db.apply_schema().await?;
        Ok(db)
    }

    pub async fn apply_schema(&self) -> anyhow::Result<()> {
        let schema_sql = include_str!("../sql/schema_pg.sql");
        for statement in split_sql(schema_sql) {
            let stmt = statement.trim();
            if stmt.is_empty() {
                continue;
            }
            sqlx::query(stmt).execute(&self.pool).await?;
        }
        Ok(())
    }

    pub async fn upsert_pairs(
        &self,
        watchlist_version: i64,
        pairs: &[PairRecord],
    ) -> anyhow::Result<()> {
        let now_ms = now_ts_ms();
        let mut tx = self.pool.begin().await?;
        for pair in pairs {
            sqlx::query(
                r#"
                INSERT INTO pair_registry (
                  pair_id, watchlist_version, status, root_market_title, polymarket_event_title,
                  opinion_market_id, opinion_yes_token_id, opinion_no_token_id,
                  polymarket_market_id, polymarket_yes_token_id, polymarket_no_token_id,
                  created_local_ts_ms
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
                ON CONFLICT(pair_id) DO UPDATE SET
                  watchlist_version=excluded.watchlist_version,
                  status=excluded.status,
                  root_market_title=excluded.root_market_title,
                  polymarket_event_title=excluded.polymarket_event_title,
                  opinion_market_id=excluded.opinion_market_id,
                  opinion_yes_token_id=excluded.opinion_yes_token_id,
                  opinion_no_token_id=excluded.opinion_no_token_id,
                  polymarket_market_id=excluded.polymarket_market_id,
                  polymarket_yes_token_id=excluded.polymarket_yes_token_id,
                  polymarket_no_token_id=excluded.polymarket_no_token_id
                "#,
            )
            .bind(pair.pair_id)
            .bind(watchlist_version)
            .bind(&pair.status)
            .bind(&pair.root_market_title)
            .bind(&pair.polymarket_event_title)
            .bind(&pair.opinion_market_id)
            .bind(&pair.opinion_yes_token_id)
            .bind(&pair.opinion_no_token_id)
            .bind(&pair.polymarket_market_id)
            .bind(&pair.polymarket_yes_token_id)
            .bind(&pair.polymarket_no_token_id)
            .bind(now_ms)
            .execute(&mut *tx)
            .await?;
        }
        sqlx::query(
            "INSERT INTO monitor_meta(k, v) VALUES('watchlist_version', $1) \
             ON CONFLICT(k) DO UPDATE SET v=EXCLUDED.v",
        )
            .bind(watchlist_version.to_string())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub async fn upsert_tokens(&self, tokens: &[TokenRegistration]) -> anyhow::Result<()> {
        let now_ms = now_ts_ms();
        let mut tx = self.pool.begin().await?;
        for token in tokens {
            sqlx::query(
                r#"
                INSERT INTO token_registry (
                  token_key, venue, external_token_id, market_id, outcome_side, token_side,
                  first_seen_local_ts_ms, last_seen_local_ts_ms
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT(token_key) DO UPDATE SET
                  external_token_id=excluded.external_token_id,
                  market_id=excluded.market_id,
                  outcome_side=excluded.outcome_side,
                  token_side=excluded.token_side,
                  last_seen_local_ts_ms=excluded.last_seen_local_ts_ms
                "#,
            )
            .bind(&token.token_key)
            .bind(token.venue.as_str())
            .bind(&token.external_token_id)
            .bind(&token.market_id)
            .bind(token.outcome_side)
            .bind(token.token_side.as_str())
            .bind(now_ms)
            .bind(now_ms)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn load_pair_overrides(
        &self,
    ) -> anyhow::Result<HashMap<i64, (Option<String>, Option<String>)>> {
        let rows = sqlx::query(
            r#"
            SELECT pair_id,
                   mapping_overridden_flag,
                   opinion_yes_token_id_override,
                   opinion_no_token_id_override
            FROM pair_token_overrides
            WHERE mapping_overridden_flag = 1
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .context("load pair_token_overrides failed")?;
        let mut map = HashMap::new();
        for row in rows {
            let pair_id: i64 = row.try_get("pair_id")?;
            let yes: Option<String> = row.try_get("opinion_yes_token_id_override")?;
            let no: Option<String> = row.try_get("opinion_no_token_id_override")?;
            map.insert(pair_id, (yes, no));
        }
        Ok(map)
    }

    pub async fn get_existing_tokens(&self) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query("SELECT token_key FROM token_registry")
            .fetch_all(&self.pool)
            .await?;
        let mut keys = Vec::with_capacity(rows.len());
        for row in rows {
            let key: String = row.try_get("token_key").context("token_key")?;
            keys.push(key);
        }
        Ok(keys)
    }
}

fn split_sql(input: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    for line in input.lines() {
        buf.push_str(line);
        buf.push('\n');
        if line.trim_end().ends_with(';') {
            out.push(buf.clone());
            buf.clear();
        }
    }
    if !buf.trim().is_empty() {
        out.push(buf);
    }
    out
}

fn now_ts_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn parse_token_side(value: &str) -> TokenSide {
    match value {
        "YES" => TokenSide::Yes,
        "NO" => TokenSide::No,
        _ => TokenSide::Unknown,
    }
}

pub fn parse_venue(value: &str) -> Venue {
    match value {
        "pm" => Venue::Polymarket,
        "opi" => Venue::Opinion,
        _ => Venue::Polymarket,
    }
}
