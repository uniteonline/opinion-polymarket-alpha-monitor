use crate::models::Venue;
use sqlx::{PgPool, Row};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::time::Duration;

#[derive(Debug, Clone)]
pub struct FeeRow {
    pub venue: Venue,
    pub fee_bps: i64,
    pub effective_from_ts_ms: i64,
    pub effective_to_ts_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct FeeScheduleState {
    defaults: (i64, i64),
    rows: Vec<FeeRow>,
}

impl FeeScheduleState {
    pub fn new(default_pm_bps: i64, default_opi_bps: i64) -> Self {
        FeeScheduleState {
            defaults: (default_pm_bps, default_opi_bps),
            rows: Vec::new(),
        }
    }

    pub fn update(&mut self, rows: Vec<FeeRow>) {
        self.rows = rows;
    }

    pub fn fee_bps(&self, venue: Venue, ts_ms: i64) -> i64 {
        let mut best: Option<&FeeRow> = None;
        for row in &self.rows {
            if row.venue != venue {
                continue;
            }
            if row.effective_from_ts_ms > ts_ms {
                continue;
            }
            if let Some(end) = row.effective_to_ts_ms {
                if ts_ms >= end {
                    continue;
                }
            }
            if best
                .as_ref()
                .map(|b| row.effective_from_ts_ms > b.effective_from_ts_ms)
                .unwrap_or(true)
            {
                best = Some(row);
            }
        }
        if let Some(row) = best {
            return row.fee_bps;
        }
        match venue {
            Venue::Polymarket => self.defaults.0,
            Venue::Opinion => self.defaults.1,
        }
    }
}

pub type SharedFeeSchedule = Arc<RwLock<FeeScheduleState>>;

pub async fn run_fee_schedule_refresher(
    pool: PgPool,
    state: SharedFeeSchedule,
    interval_sec: u64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
    loop {
        interval.tick().await;
        let rows = match sqlx::query(
            r#"
            SELECT venue, fee_bps, effective_from_ts_ms, effective_to_ts_ms
            FROM fee_schedule
            "#,
        )
        .fetch_all(&pool)
        .await
        {
            Ok(rows) => rows,
            Err(_) => continue,
        };
        let mut schedule_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let venue_raw: String = row.get("venue");
            let venue = crate::db::parse_venue(&venue_raw);
            let fee_bps: i64 = row.get("fee_bps");
            let effective_from_ts_ms: i64 = row.get("effective_from_ts_ms");
            let effective_to_ts_ms: Option<i64> = row.try_get("effective_to_ts_ms").ok();
            schedule_rows.push(FeeRow {
                venue,
                fee_bps,
                effective_from_ts_ms,
                effective_to_ts_ms,
            });
        }
        {
            let mut guard = state.write().await;
            guard.update(schedule_rows);
        }
    }
}
