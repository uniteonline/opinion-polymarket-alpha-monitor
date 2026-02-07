use crate::db_queue::{try_send_db, DbMessage, DbSender};
use crate::time_utils::now_ts_ms;
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::time::Duration;

#[derive(Debug, Default)]
pub struct ChainMetricsState {
    tx_cost_values: VecDeque<(i64, f64)>,
    base_fee_values: VecDeque<f64>,
    max_len: usize,
    last_congestion_flag: i64,
}

impl ChainMetricsState {
    pub fn new(max_len: usize) -> Self {
        ChainMetricsState {
            tx_cost_values: VecDeque::with_capacity(max_len),
            base_fee_values: VecDeque::with_capacity(max_len),
            max_len,
            last_congestion_flag: 0,
        }
    }

    pub fn record(&mut self, ts_ms: i64, tx_cost: f64, base_fee: f64, congestion_flag: i64) {
        if self.tx_cost_values.len() >= self.max_len {
            self.tx_cost_values.pop_front();
        }
        if self.base_fee_values.len() >= self.max_len {
            self.base_fee_values.pop_front();
        }
        self.tx_cost_values.push_back((ts_ms, tx_cost));
        self.base_fee_values.push_back(base_fee);
        self.last_congestion_flag = congestion_flag;
    }

    pub fn p90_recent(&self, since_ms: i64) -> Option<f64> {
        let mut data: Vec<f64> = self
            .tx_cost_values
            .iter()
            .filter(|(ts, _)| *ts >= since_ms)
            .map(|(_, v)| *v)
            .collect();
        if data.is_empty() {
            return None;
        }
        data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((data.len() as f64 - 1.0) * 0.9).round() as usize;
        data.get(idx).copied()
    }

    pub fn p95_base_fee(&self) -> Option<f64> {
        if self.base_fee_values.is_empty() {
            return None;
        }
        let mut data: Vec<f64> = self.base_fee_values.iter().copied().collect();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((data.len() as f64 - 1.0) * 0.95).round() as usize;
        data.get(idx).copied()
    }

    pub fn last_congestion_flag(&self) -> i64 {
        self.last_congestion_flag
    }
}

pub type SharedChain = Arc<Mutex<ChainMetricsState>>;

pub fn new_shared_chain(max_len: usize) -> SharedChain {
    Arc::new(Mutex::new(ChainMetricsState::new(max_len)))
}

pub async fn run_chain_metrics(
    db_sender: DbSender,
    state: SharedChain,
    interval_sec: u64,
    base_fee: f64,
    priority_fee: f64,
    gas_used_est: f64,
) {
    let mut interval = tokio::time::interval(Duration::from_secs(interval_sec));
    loop {
        interval.tick().await;
        let now_ms = now_ts_ms();
        let tx_cost = (base_fee + priority_fee) * gas_used_est;
        let congestion_flag = {
            let mut guard = state.lock().await;
            let p95 = guard.p95_base_fee().unwrap_or(base_fee);
            let congestion = if p95 > 0.0 && base_fee >= p95 { 1 } else { 0 };
            guard.record(now_ms, tx_cost, base_fee, congestion);
            congestion
        };
        try_send_db(
            &db_sender,
            DbMessage::ChainMetrics(crate::models::ChainMetricsRow {
                chain: "bnb".to_string(),
                local_ts_ms: now_ms,
                base_fee: Some(base_fee),
                priority_fee: Some(priority_fee),
                gas_used_est: Some(gas_used_est),
                tx_cost_est: Some(tx_cost),
                congestion_flag,
            }),
            "chain_metrics",
        );
    }
}
