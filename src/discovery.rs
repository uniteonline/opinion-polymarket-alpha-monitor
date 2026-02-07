use crate::models::PairRecord;
use anyhow::Context;
use sqlx::{sqlite::SqliteConnectOptions, Row, SqlitePool};

pub struct DiscoveryLoader {
    pool: SqlitePool,
}

impl DiscoveryLoader {
    pub async fn connect(path: &str) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::new()
            .filename(path)
            .create_if_missing(false);
        let pool = SqlitePool::connect_with(options).await?;
        Ok(Self { pool })
    }

    pub async fn load_latest_pairs(&self) -> anyhow::Result<(i64, Vec<PairRecord>)> {
        let watchlist_version = self.load_watchlist_version().await?;
        let rows = sqlx::query(
            r#"
            SELECT pair_id, watchlist_version, status, root_market_title, polymarket_event_title,
                   opinion_market_id, opinion_yes_token_id, opinion_no_token_id,
                   polymarket_market_id, polymarket_yes_token_id, polymarket_no_token_id
            FROM market_pairs
            WHERE watchlist_version = ? AND status = 'active'
            "#,
        )
        .bind(watchlist_version)
        .fetch_all(&self.pool)
        .await?;

        let mut pairs = Vec::with_capacity(rows.len());
        for row in rows {
            let opinion_market_id: Option<String> = row.try_get("opinion_market_id").ok();
            let polymarket_market_id: Option<String> = row.try_get("polymarket_market_id").ok();
            let pair_id_raw: Option<String> = row.try_get("pair_id").ok();
            let pair_id = compute_pair_id(
                &opinion_market_id,
                &polymarket_market_id,
                pair_id_raw.as_deref(),
            );

            let record = PairRecord {
                pair_id,
                watchlist_version: row
                    .try_get::<i64, _>("watchlist_version")
                    .unwrap_or(watchlist_version),
                status: row
                    .try_get::<String, _>("status")
                    .unwrap_or_else(|_| "active".to_string()),
                root_market_title: row.try_get("root_market_title").ok(),
                polymarket_event_title: row.try_get("polymarket_event_title").ok(),
                opinion_market_id,
                opinion_yes_token_id: row.try_get("opinion_yes_token_id").ok(),
                opinion_no_token_id: row.try_get("opinion_no_token_id").ok(),
                polymarket_market_id,
                polymarket_yes_token_id: row.try_get("polymarket_yes_token_id").ok(),
                polymarket_no_token_id: row.try_get("polymarket_no_token_id").ok(),
            };
            pairs.push(record);
        }
        Ok((watchlist_version, pairs))
    }

    async fn load_watchlist_version(&self) -> anyhow::Result<i64> {
        let row = sqlx::query("SELECT value FROM watchlist_meta WHERE key = 'watchlist_version'")
            .fetch_optional(&self.pool)
            .await?;
        if let Some(row) = row {
            let value: String = row.try_get("value").context("watchlist_meta.value")?;
            return value.parse::<i64>().context("watchlist_version parse");
        }
        Ok(0)
    }
}

fn compute_pair_id(
    opinion_market_id: &Option<String>,
    polymarket_market_id: &Option<String>,
    pair_id_raw: Option<&str>,
) -> i64 {
    let base = if let (Some(op), Some(pm)) = (opinion_market_id, polymarket_market_id) {
        format!("{op}:{pm}")
    } else if let Some(raw) = pair_id_raw {
        raw.to_string()
    } else {
        "unknown".to_string()
    };
    let hash = fnv1a_64(base.as_bytes());
    hash as i64
}

fn fnv1a_64(data: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in data {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}
