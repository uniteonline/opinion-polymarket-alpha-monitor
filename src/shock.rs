use crate::models::BookSide;
use serde_json::json;
use std::collections::{HashMap, VecDeque};

const COOLDOWN_MS: i64 = 10_000;
const PEAK_WINDOW_MS: i64 = 10_000;
const WEAK_THRESHOLD_FACTOR: f64 = 0.7;
const METRIC_HISTORY_MAX: usize = 120;
const MICRO_GAP_WINDOW: usize = 30;
const TRADE_NOTIONAL_WINDOW: usize = 3_600;

const SHOCK_PRICE_VELOCITY_5S: &str = "PRICE_VELOCITY_5S";
const SHOCK_PRICE_ACCEL_10S: &str = "PRICE_ACCEL_10S";
const SHOCK_RV_JUMP_5S: &str = "RV_JUMP_5S";
const SHOCK_RV_JUMP_1M: &str = "RV_JUMP_1M";
const SHOCK_LARGE_TRADE: &str = "LARGE_TRADE";
const SHOCK_TAKER_IMBALANCE_30S: &str = "TAKER_IMBALANCE_30S";
const SHOCK_CVD_SPIKE_30S: &str = "CVD_SPIKE_30S";
const SHOCK_QUOTE_BURST_ADD: &str = "QUOTE_BURST_ADD";
const SHOCK_QUOTE_BURST_CANCEL: &str = "QUOTE_BURST_CANCEL";
const SHOCK_OFI_SPIKE_10S: &str = "OFI_SPIKE_10S";
const SHOCK_SWEEP: &str = "SWEEP";
const SHOCK_FAKE_WALL_PULL: &str = "FAKE_WALL_PULL";
const SHOCK_WASH_TRADE: &str = "WASH_TRADE";

#[derive(Debug, Clone)]
pub struct WallPullSignal {
    pub side: BookSide,
    pub price: f64,
    pub size_before: f64,
    pub pull_ratio: f64,
}

#[derive(Debug, Clone)]
pub struct ShockPending {
    pub shock_type: &'static str,
    pub direction: i64,
    pub noise_flag: i64,
    pub start_ts_ms: i64,
    pub trigger_ts_ms: i64,
    pub magnitude: f64,
    pub context_json: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ShockFinal {
    pub shock_type: &'static str,
    pub direction: i64,
    pub noise_flag: i64,
    pub start_ts_ms: i64,
    pub trigger_ts_ms: i64,
    pub peak_ts_ms: i64,
    pub magnitude: f64,
    pub context_json: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ShockSignal {
    Pending(ShockPending),
    Final(ShockFinal),
}

#[derive(Debug, Clone)]
struct MetricPoint {
    ts_ms: i64,
    value: f64,
}

#[derive(Debug, Clone)]
struct PendingShock {
    shock_type: &'static str,
    direction: i64,
    noise_flag: i64,
    start_ts_ms: i64,
    trigger_ts_ms: i64,
    peak_ts_ms: i64,
    peak_value: f64,
    context_json: Option<String>,
    expires_ts_ms: i64,
}

pub struct ShockEngine {
    last_trigger_ms: HashMap<&'static str, i64>,
    pending: HashMap<&'static str, PendingShock>,
    metric_history: HashMap<&'static str, VecDeque<MetricPoint>>,
    micro_history: VecDeque<f64>,
    return_history: VecDeque<f64>,
    ofi_history: VecDeque<f64>,
    buy_history: VecDeque<f64>,
    sell_history: VecDeque<f64>,
    cvd_history: VecDeque<f64>,
    vol_history: VecDeque<f64>,
    trade_count_history: VecDeque<i64>,
    trade_missing_history: VecDeque<i64>,
    micro_gap_history: VecDeque<f64>,
    abs_gap_z_window: RollingWindow,
    abs_r5_window: RollingWindow,
    abs_a10_window: RollingWindow,
    rv5_window: RollingWindow,
    rv60_window: RollingWindow,
    vol1_window: RollingWindow,
    vol30_window: RollingWindow,
    abs_cvd30_window: RollingWindow,
    abs_ofi10_window: RollingWindow,
    abs_ofi30_window: RollingWindow,
    abs_r3_window: RollingWindow,
    abs_r30_window: RollingWindow,
    q_add_window: RollingWindow,
    q_cancel_window: RollingWindow,
    trade_notional_window: RollingWindow,
    q_add_stats: EwmaStats,
    q_cancel_stats: EwmaStats,
}

impl ShockEngine {
    pub fn new() -> Self {
        ShockEngine {
            last_trigger_ms: HashMap::new(),
            pending: HashMap::new(),
            metric_history: HashMap::new(),
            micro_history: VecDeque::with_capacity(64),
            return_history: VecDeque::with_capacity(64),
            ofi_history: VecDeque::with_capacity(64),
            buy_history: VecDeque::with_capacity(64),
            sell_history: VecDeque::with_capacity(64),
            cvd_history: VecDeque::with_capacity(64),
            vol_history: VecDeque::with_capacity(64),
            trade_count_history: VecDeque::with_capacity(64),
            trade_missing_history: VecDeque::with_capacity(64),
            micro_gap_history: VecDeque::with_capacity(MICRO_GAP_WINDOW),
            abs_gap_z_window: RollingWindow::new(21_600),
            abs_r5_window: RollingWindow::new(21_600),
            abs_a10_window: RollingWindow::new(21_600),
            rv5_window: RollingWindow::new(21_600),
            rv60_window: RollingWindow::new(21_600),
            vol1_window: RollingWindow::new(21_600),
            vol30_window: RollingWindow::new(21_600),
            abs_cvd30_window: RollingWindow::new(21_600),
            abs_ofi10_window: RollingWindow::new(21_600),
            abs_ofi30_window: RollingWindow::new(21_600),
            abs_r3_window: RollingWindow::new(21_600),
            abs_r30_window: RollingWindow::new(21_600),
            q_add_window: RollingWindow::new(21_600),
            q_cancel_window: RollingWindow::new(21_600),
            trade_notional_window: RollingWindow::new(TRADE_NOTIONAL_WINDOW),
            q_add_stats: EwmaStats::new(0.1),
            q_cancel_stats: EwmaStats::new(0.1),
        }
    }

    pub fn on_bar(
        &mut self,
        bar_second: i64,
        micro_px: Option<f64>,
        tick_px: Option<f64>,
        micro_mid_gap: Option<f64>,
        add_updates_1s: i64,
        cancel_updates_1s: i64,
        volume_notional_1s: f64,
        buy_notional_1s: f64,
        sell_notional_1s: f64,
        cvd_delta_1s: f64,
        ofi_1s: f64,
        levels_crossed_1s: i64,
        trade_count_1s: i64,
        trade_side_missing_count_1s: i64,
        max_trade_notional_1s: f64,
        wall_pull: Option<WallPullSignal>,
    ) -> Vec<ShockSignal> {
        let ts_ms = bar_second * 1000;
        let micro = match micro_px {
            Some(value) if value > 0.0 => value,
            _ => return Vec::new(),
        };

        let mut signals = Vec::new();
        let mut current_metrics: HashMap<&'static str, f64> = HashMap::new();

        self.micro_history.push_back(micro);
        trim_vec(&mut self.micro_history, 64);

        if let Some(prev) = self.micro_history.iter().rev().nth(1).copied() {
            if prev > 0.0 {
                let r_t = (micro / prev).ln();
                self.return_history.push_back(r_t);
                trim_vec(&mut self.return_history, 64);
            }
        }

        if let Some(gap) = micro_mid_gap {
            self.micro_gap_history.push_back(gap);
            trim_vec(&mut self.micro_gap_history, MICRO_GAP_WINDOW);
        }
        let micro_gap_z =
            micro_mid_gap.and_then(|gap| zscore_from_vec(&self.micro_gap_history, gap));
        if let Some(gap_z) = micro_gap_z {
            self.abs_gap_z_window.push(gap_z.abs());
        }

        self.ofi_history.push_back(ofi_1s);
        trim_vec(&mut self.ofi_history, 64);
        self.buy_history.push_back(buy_notional_1s);
        self.sell_history.push_back(sell_notional_1s);
        self.cvd_history.push_back(cvd_delta_1s);
        self.vol_history.push_back(volume_notional_1s);
        self.trade_count_history.push_back(trade_count_1s);
        self.trade_missing_history
            .push_back(trade_side_missing_count_1s);
        trim_vec(&mut self.buy_history, 64);
        trim_vec(&mut self.sell_history, 64);
        trim_vec(&mut self.cvd_history, 64);
        trim_vec(&mut self.vol_history, 64);
        trim_vec(&mut self.trade_count_history, 64);
        trim_vec(&mut self.trade_missing_history, 64);

        let r3 = log_return(&self.micro_history, 3);
        let r5 = log_return(&self.micro_history, 5);
        let r30 = log_return(&self.micro_history, 30);
        let r5_prev = log_return_at(&self.micro_history, 5, 5);
        let a10 = match (r5, r5_prev) {
            (Some(cur), Some(prev)) => Some(cur - prev),
            _ => None,
        };

        let rv5 = realized_vol(&self.return_history, 5);
        let rv60 = realized_vol(&self.return_history, 60);
        let ofi_10 = sum_last(&self.ofi_history, 10);
        let ofi_30 = sum_last(&self.ofi_history, 30);
        let cvd_30 = sum_last(&self.cvd_history, 30);
        let buy_30 = sum_last(&self.buy_history, 30);
        let sell_30 = sum_last(&self.sell_history, 30);
        let vol_30 = sum_last(&self.vol_history, 30);

        if let Some(value) = r5 {
            self.abs_r5_window.push(value.abs());
            self.record_metric(SHOCK_PRICE_VELOCITY_5S, ts_ms, value.abs());
            current_metrics.insert(SHOCK_PRICE_VELOCITY_5S, value.abs());
        }
        if let Some(value) = a10 {
            self.abs_a10_window.push(value.abs());
            self.record_metric(SHOCK_PRICE_ACCEL_10S, ts_ms, value.abs());
            current_metrics.insert(SHOCK_PRICE_ACCEL_10S, value.abs());
        }
        if let Some(value) = rv5 {
            self.rv5_window.push(value);
            self.record_metric(SHOCK_RV_JUMP_5S, ts_ms, value);
            current_metrics.insert(SHOCK_RV_JUMP_5S, value);
        }
        if let Some(value) = rv60 {
            self.rv60_window.push(value);
            self.record_metric(SHOCK_RV_JUMP_1M, ts_ms, value);
            current_metrics.insert(SHOCK_RV_JUMP_1M, value);
        }
        self.vol1_window.push(volume_notional_1s);
        if let Some(value) = vol_30 {
            self.vol30_window.push(value);
            self.record_metric(SHOCK_WASH_TRADE, ts_ms, value);
            current_metrics.insert(SHOCK_WASH_TRADE, value);
        }
        if let Some(value) = cvd_30 {
            self.abs_cvd30_window.push(value.abs());
            self.record_metric(SHOCK_CVD_SPIKE_30S, ts_ms, value.abs());
            current_metrics.insert(SHOCK_CVD_SPIKE_30S, value.abs());
        }
        if let Some(value) = ofi_10 {
            self.abs_ofi10_window.push(value.abs());
            self.record_metric(SHOCK_OFI_SPIKE_10S, ts_ms, value.abs());
            current_metrics.insert(SHOCK_OFI_SPIKE_10S, value.abs());
        }
        if let Some(value) = ofi_30 {
            self.abs_ofi30_window.push(value.abs());
        }
        if let Some(value) = r3 {
            self.abs_r3_window.push(value.abs());
        }
        if let Some(value) = r30 {
            self.abs_r30_window.push(value.abs());
        }
        self.q_add_window.push(add_updates_1s as f64);
        self.q_cancel_window.push(cancel_updates_1s as f64);
        self.trade_notional_window.push(max_trade_notional_1s);
        self.q_add_stats.update(add_updates_1s as f64);
        self.q_cancel_stats.update(cancel_updates_1s as f64);
        self.record_metric(SHOCK_QUOTE_BURST_ADD, ts_ms, add_updates_1s as f64);
        self.record_metric(SHOCK_QUOTE_BURST_CANCEL, ts_ms, cancel_updates_1s as f64);
        current_metrics.insert(SHOCK_QUOTE_BURST_ADD, add_updates_1s as f64);
        current_metrics.insert(SHOCK_QUOTE_BURST_CANCEL, cancel_updates_1s as f64);

        if max_trade_notional_1s > 0.0 {
            self.record_metric(SHOCK_LARGE_TRADE, ts_ms, max_trade_notional_1s);
            current_metrics.insert(SHOCK_LARGE_TRADE, max_trade_notional_1s);
        }
        self.record_metric(SHOCK_SWEEP, ts_ms, volume_notional_1s);
        current_metrics.insert(SHOCK_SWEEP, volume_notional_1s);

        // PRICE_VELOCITY_5S
        if let Some(value) = r5 {
            let metric = value.abs();
            let p99 = self.abs_r5_window.quantile(0.99).unwrap_or(0.0);
            let tick_floor = tick_px
                .map(|tick| if tick > 0.0 { 5.0 * tick / micro } else { 0.0 })
                .unwrap_or(0.0);
            let threshold = p99.max(tick_floor);
            if metric >= threshold {
                let direction = if value > 0.0 { 1 } else { -1 };
                let context = json!({
                    "r5": value,
                    "threshold": threshold
                });
                if let Some(pending) = self.schedule_shock(
                    SHOCK_PRICE_VELOCITY_5S,
                    direction,
                    0,
                    ts_ms,
                    metric,
                    threshold,
                    Some(context),
                ) {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        // PRICE_ACCEL_10S
        if let Some(value) = a10 {
            let metric = value.abs();
            let p99 = self.abs_a10_window.quantile(0.99).unwrap_or(0.0);
            if metric >= p99 {
                let direction = if value > 0.0 { 1 } else { -1 };
                let context = json!({ "a10": value, "threshold": p99 });
                if let Some(pending) = self.schedule_shock(
                    SHOCK_PRICE_ACCEL_10S,
                    direction,
                    0,
                    ts_ms,
                    metric,
                    p99,
                    Some(context),
                ) {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        // RV_JUMP_5S
        if let Some(value) = rv5 {
            let p99 = self.rv5_window.quantile(0.99).unwrap_or(0.0);
            let p50 = self.rv5_window.quantile(0.50).unwrap_or(0.0);
            if value >= p99 && value >= 3.0 * p50 {
                let context = json!({ "rv5": value, "p99": p99, "p50": p50 });
                if let Some(pending) =
                    self.schedule_shock(SHOCK_RV_JUMP_5S, 0, 0, ts_ms, value, p99, Some(context))
                {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        // RV_JUMP_1M
        if let Some(value) = rv60 {
            let p99 = self.rv60_window.quantile(0.99).unwrap_or(0.0);
            if value >= p99 {
                let context = json!({ "rv60": value, "p99": p99 });
                if let Some(pending) =
                    self.schedule_shock(SHOCK_RV_JUMP_1M, 0, 0, ts_ms, value, p99, Some(context))
                {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        // LARGE_TRADE
        if max_trade_notional_1s > 0.0 {
            let p99 = self.trade_notional_window.quantile(0.99).unwrap_or(0.0);
            let threshold = p99.max(1000.0);
            if max_trade_notional_1s >= threshold {
                let direction = if cvd_delta_1s >= 0.0 { 1 } else { -1 };
                let r3_ok = r3.map(|r| r.signum() == direction as f64).unwrap_or(false);
                if r3_ok {
                    let context = json!({ "max_trade_notional": max_trade_notional_1s, "threshold": threshold });
                    if let Some(pending) = self.schedule_shock(
                        SHOCK_LARGE_TRADE,
                        direction,
                        0,
                        ts_ms,
                        max_trade_notional_1s,
                        threshold,
                        Some(context),
                    ) {
                        signals.push(ShockSignal::Pending(pending));
                    }
                }
            }
        }

        // TAKER_IMBALANCE_30S
        if let (Some(buy), Some(sell)) = (buy_30, sell_30) {
            let ratio = (buy + 1e-9) / (sell + 1e-9);
            let imbalance = if ratio >= 1.0 { ratio } else { 1.0 / ratio };
            self.record_metric(SHOCK_TAKER_IMBALANCE_30S, ts_ms, imbalance);
            current_metrics.insert(SHOCK_TAKER_IMBALANCE_30S, imbalance);
            if ratio >= 3.0 || ratio <= (1.0 / 3.0) {
                let direction = if ratio >= 1.0 { 1 } else { -1 };
                let context = json!({ "ratio": ratio, "buy_30": buy, "sell_30": sell });
                if let Some(pending) = self.schedule_shock(
                    SHOCK_TAKER_IMBALANCE_30S,
                    direction,
                    0,
                    ts_ms,
                    imbalance,
                    3.0,
                    Some(context),
                ) {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        // CVD_SPIKE_30S
        if let Some(cvd) = cvd_30 {
            let p99 = self.abs_cvd30_window.quantile(0.99).unwrap_or(0.0);
            let missing_ratio =
                missing_ratio_30s(&self.trade_count_history, &self.trade_missing_history);
            if missing_ratio < 0.5 && cvd.abs() >= p99 {
                let direction = if cvd > 0.0 { 1 } else { -1 };
                let context =
                    json!({ "cvd_30": cvd, "threshold": p99, "missing_ratio": missing_ratio });
                if let Some(pending) = self.schedule_shock(
                    SHOCK_CVD_SPIKE_30S,
                    direction,
                    0,
                    ts_ms,
                    cvd.abs(),
                    p99,
                    Some(context),
                ) {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        // QUOTE_BURST_ADD/CANCEL
        let (add_mean, add_std) = self.q_add_stats.snapshot();
        let (cancel_mean, cancel_std) = self.q_cancel_stats.snapshot();
        let add_z = zscore(add_updates_1s as f64, add_mean, add_std);
        let cancel_z = zscore(cancel_updates_1s as f64, cancel_mean, cancel_std);
        let add_p99 = self.q_add_window.quantile(0.99).unwrap_or(0.0);
        let cancel_p99 = self.q_cancel_window.quantile(0.99).unwrap_or(0.0);
        let add_threshold = add_p99.max(add_mean + 5.0 * add_std);
        let cancel_threshold = cancel_p99.max(cancel_mean + 5.0 * cancel_std);
        if add_z >= 5.0 || (add_updates_1s as f64) >= add_p99 {
            let context = json!({ "q_add": add_updates_1s, "z": add_z, "p99": add_p99 });
            if let Some(pending) = self.schedule_shock(
                SHOCK_QUOTE_BURST_ADD,
                0,
                0,
                ts_ms,
                add_updates_1s as f64,
                add_threshold,
                Some(context),
            ) {
                signals.push(ShockSignal::Pending(pending));
            }
        }
        if cancel_z >= 5.0 || (cancel_updates_1s as f64) >= cancel_p99 {
            let context =
                json!({ "q_cancel": cancel_updates_1s, "z": cancel_z, "p99": cancel_p99 });
            if let Some(pending) = self.schedule_shock(
                SHOCK_QUOTE_BURST_CANCEL,
                0,
                0,
                ts_ms,
                cancel_updates_1s as f64,
                cancel_threshold,
                Some(context),
            ) {
                signals.push(ShockSignal::Pending(pending));
            }
        }

        // OFI_SPIKE_10S
        if let Some(ofi_10) = ofi_10 {
            let p99 = self.abs_ofi10_window.quantile(0.99).unwrap_or(0.0);
            let metric = ofi_10.abs();
            if metric >= p99 {
                let direction = if ofi_10 > 0.0 { 1 } else { -1 };
                let context = json!({ "ofi_10": ofi_10, "threshold": p99 });
                if let Some(pending) = self.schedule_shock(
                    SHOCK_OFI_SPIKE_10S,
                    direction,
                    0,
                    ts_ms,
                    metric,
                    p99,
                    Some(context),
                ) {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        // SWEEP
        if levels_crossed_1s >= 3 {
            let p95 = self.vol1_window.quantile(0.95).unwrap_or(0.0);
            if volume_notional_1s >= p95 {
                let direction = if cvd_delta_1s >= 0.0 { 1 } else { -1 };
                let context =
                    json!({ "levels_crossed": levels_crossed_1s, "vol_1s": volume_notional_1s });
                if let Some(pending) = self.schedule_shock(
                    SHOCK_SWEEP,
                    direction,
                    0,
                    ts_ms,
                    volume_notional_1s,
                    p95,
                    Some(context),
                ) {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        // FAKE_WALL_PULL
        if let Some(signal) = wall_pull {
            let abs_r3 = r3.map(|v| v.abs()).unwrap_or(0.0);
            let p20_r3 = self.abs_r3_window.quantile(0.20).unwrap_or(0.0);
            let abs_ofi10 = ofi_10.map(|v| v.abs()).unwrap_or(0.0);
            let p20_ofi = self.abs_ofi10_window.quantile(0.20).unwrap_or(0.0);
            if abs_r3 <= p20_r3 && abs_ofi10 <= p20_ofi {
                let context = json!({
                    "wall_price": signal.price,
                    "wall_size": signal.size_before,
                    "pull_ratio": signal.pull_ratio,
                    "side": format!("{:?}", signal.side),
                });
                if let Some(pending) = self.schedule_shock(
                    SHOCK_FAKE_WALL_PULL,
                    0,
                    1,
                    ts_ms,
                    signal.size_before,
                    signal.size_before,
                    Some(context),
                ) {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        // WASH_TRADE
        if let (Some(vol_30), Some(r30), Some(ofi_10)) = (vol_30, r30, ofi_10) {
            let p99_vol = self.vol30_window.quantile(0.99).unwrap_or(0.0);
            let p20_r30 = self.abs_r30_window.quantile(0.20).unwrap_or(0.0);
            let abs_ofi10 = ofi_10.abs();
            let p20_ofi10 = self.abs_ofi10_window.quantile(0.20).unwrap_or(0.0);
            if vol_30 >= p99_vol && r30.abs() <= p20_r30 && abs_ofi10 <= p20_ofi10 {
                let context = json!({ "vol_30": vol_30, "r30": r30, "ofi_10": ofi_10 });
                if let Some(pending) = self.schedule_shock(
                    SHOCK_WASH_TRADE,
                    0,
                    1,
                    ts_ms,
                    vol_30,
                    p99_vol,
                    Some(context),
                ) {
                    signals.push(ShockSignal::Pending(pending));
                }
            }
        }

        self.finalize_pending(ts_ms, &current_metrics, &mut signals);

        signals
    }

    fn record_metric(&mut self, key: &'static str, ts_ms: i64, value: f64) {
        let entry = self
            .metric_history
            .entry(key)
            .or_insert_with(|| VecDeque::with_capacity(METRIC_HISTORY_MAX));
        if entry.len() >= METRIC_HISTORY_MAX {
            entry.pop_front();
        }
        entry.push_back(MetricPoint { ts_ms, value });
    }

    fn find_start_ts(&self, key: &'static str, weak_threshold: f64, fallback: i64) -> i64 {
        let history = match self.metric_history.get(key) {
            Some(history) => history,
            None => return fallback,
        };
        let mut start = fallback;
        for point in history.iter().rev() {
            if point.value >= weak_threshold {
                start = point.ts_ms;
            } else {
                break;
            }
        }
        start
    }

    fn schedule_shock(
        &mut self,
        shock_type: &'static str,
        direction: i64,
        noise_flag: i64,
        ts_ms: i64,
        magnitude: f64,
        strong_threshold: f64,
        context: Option<serde_json::Value>,
    ) -> Option<ShockPending> {
        if let Some(last) = self.last_trigger_ms.get(shock_type) {
            if ts_ms - *last < COOLDOWN_MS {
                return None;
            }
        }
        self.last_trigger_ms.insert(shock_type, ts_ms);
        let start_ts = if strong_threshold > 0.0 {
            self.find_start_ts(shock_type, strong_threshold * WEAK_THRESHOLD_FACTOR, ts_ms)
        } else {
            ts_ms
        };
        let context_json = context.map(|value| value.to_string());
        let pending = PendingShock {
            shock_type,
            direction,
            noise_flag,
            start_ts_ms: start_ts,
            trigger_ts_ms: ts_ms,
            peak_ts_ms: ts_ms,
            peak_value: magnitude,
            context_json: context_json.clone(),
            expires_ts_ms: ts_ms + PEAK_WINDOW_MS,
        };
        self.pending.insert(shock_type, pending);
        Some(ShockPending {
            shock_type,
            direction,
            noise_flag,
            start_ts_ms: start_ts,
            trigger_ts_ms: ts_ms,
            magnitude,
            context_json,
        })
    }

    fn finalize_pending(
        &mut self,
        ts_ms: i64,
        current_metrics: &HashMap<&'static str, f64>,
        out: &mut Vec<ShockSignal>,
    ) {
        let keys: Vec<&'static str> = self.pending.keys().copied().collect();
        for key in keys {
            let mut should_finalize = false;
            if let Some(pending) = self.pending.get_mut(key) {
                if ts_ms <= pending.expires_ts_ms {
                    if let Some(value) = current_metrics.get(key) {
                        if *value > pending.peak_value {
                            pending.peak_value = *value;
                            pending.peak_ts_ms = ts_ms;
                        }
                    }
                }
                if ts_ms >= pending.expires_ts_ms {
                    should_finalize = true;
                }
            }
            if should_finalize {
                if let Some(pending) = self.pending.remove(key) {
                    out.push(ShockSignal::Final(ShockFinal {
                        shock_type: pending.shock_type,
                        direction: pending.direction,
                        noise_flag: pending.noise_flag,
                        start_ts_ms: pending.start_ts_ms,
                        trigger_ts_ms: pending.trigger_ts_ms,
                        peak_ts_ms: pending.peak_ts_ms,
                        magnitude: pending.peak_value,
                        context_json: pending.context_json,
                    }));
                }
            }
        }
    }
}

#[derive(Default)]
struct RollingWindow {
    values: VecDeque<f64>,
    max_len: usize,
}

impl RollingWindow {
    fn new(max_len: usize) -> Self {
        RollingWindow {
            values: VecDeque::new(),
            max_len,
        }
    }

    fn push(&mut self, value: f64) {
        if self.max_len == 0 {
            return;
        }
        if self.values.len() >= self.max_len {
            self.values.pop_front();
        }
        self.values.push_back(value);
    }

    fn quantile(&self, q: f64) -> Option<f64> {
        if self.values.is_empty() {
            return None;
        }
        let mut data: Vec<f64> = self.values.iter().copied().collect();
        data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let idx = ((data.len() as f64 - 1.0) * q).round() as usize;
        data.get(idx).copied()
    }
}

#[derive(Debug, Clone)]
struct EwmaStats {
    alpha: f64,
    mean: Option<f64>,
    var: Option<f64>,
}

impl EwmaStats {
    fn new(alpha: f64) -> Self {
        EwmaStats {
            alpha,
            mean: None,
            var: None,
        }
    }

    fn update(&mut self, value: f64) {
        if let Some(mean) = self.mean {
            let delta = value - mean;
            let new_mean = mean + self.alpha * delta;
            let var = self.var.unwrap_or(0.0);
            let new_var = (1.0 - self.alpha) * (var + self.alpha * delta * delta);
            self.mean = Some(new_mean);
            self.var = Some(new_var);
        } else {
            self.mean = Some(value);
            self.var = Some(0.0);
        }
    }

    fn snapshot(&self) -> (f64, f64) {
        let mean = self.mean.unwrap_or(0.0);
        let std = self.var.unwrap_or(0.0).sqrt();
        (mean, std)
    }
}

fn sum_last(values: &VecDeque<f64>, count: usize) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    let mut sum = 0.0;
    let mut seen = 0usize;
    for value in values.iter().rev() {
        sum += *value;
        seen += 1;
        if seen >= count {
            break;
        }
    }
    Some(sum)
}

fn sum_last_i64(values: &VecDeque<i64>, count: usize) -> i64 {
    if values.is_empty() {
        return 0;
    }
    let mut sum = 0i64;
    let mut seen = 0usize;
    for value in values.iter().rev() {
        sum += *value;
        seen += 1;
        if seen >= count {
            break;
        }
    }
    sum
}

fn realized_vol(returns: &VecDeque<f64>, count: usize) -> Option<f64> {
    if returns.len() < count {
        return None;
    }
    let mut sum = 0.0;
    for value in returns.iter().rev().take(count) {
        sum += value * value;
    }
    Some(sum.sqrt())
}

fn log_return(history: &VecDeque<f64>, lag: usize) -> Option<f64> {
    if history.len() <= lag {
        return None;
    }
    let current = history.back().copied()?;
    let past = history.get(history.len() - 1 - lag).copied()?;
    if current > 0.0 && past > 0.0 {
        Some((current / past).ln())
    } else {
        None
    }
}

fn log_return_at(history: &VecDeque<f64>, lag: usize, offset: usize) -> Option<f64> {
    if history.len() <= lag + offset {
        return None;
    }
    let current_idx = history.len() - 1 - offset;
    let past_idx = history.len() - 1 - offset - lag;
    let current = history.get(current_idx).copied()?;
    let past = history.get(past_idx).copied()?;
    if current > 0.0 && past > 0.0 {
        Some((current / past).ln())
    } else {
        None
    }
}

fn trim_vec<T>(values: &mut VecDeque<T>, max_len: usize) {
    while values.len() > max_len {
        values.pop_front();
    }
}

fn zscore(value: f64, mean: f64, std: f64) -> f64 {
    if std <= 0.0 {
        0.0
    } else {
        (value - mean) / std
    }
}

fn zscore_from_vec(values: &VecDeque<f64>, latest: f64) -> Option<f64> {
    if values.len() < 5 {
        return None;
    }
    let mean = values.iter().copied().sum::<f64>() / values.len() as f64;
    let mut var = 0.0;
    for value in values {
        var += (value - mean) * (value - mean);
    }
    let std = (var / values.len() as f64).sqrt();
    if std <= 0.0 {
        None
    } else {
        Some((latest - mean) / std)
    }
}

fn missing_ratio_30s(trade_counts: &VecDeque<i64>, missing_counts: &VecDeque<i64>) -> f64 {
    let trades = sum_last_i64(trade_counts, 30);
    let missing = sum_last_i64(missing_counts, 30);
    if trades <= 0 {
        return 1.0;
    }
    missing as f64 / trades as f64
}
