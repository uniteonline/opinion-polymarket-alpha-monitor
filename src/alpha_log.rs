use crate::config::TradeConfig;
use crate::models::{Bars1sPair, TokenSide};
use crate::time_utils::now_ts_ms;
use serde::Serialize;
use std::fs::{create_dir_all, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::OnceLock;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

#[derive(Debug, Clone, Serialize)]
pub struct AlphaLogEntry {
    pub ts_ms: i64,
    pub bar_second: i64,
    pub phase: String,
    pub state: String,
    pub pair_id: i64,
    pub token_side: String,
    pub direction: i64,
    pub alpha_type: Option<String>,
    pub alpha_source: Option<String>,
    pub alpha_source_cfg: Option<String>,
    pub alpha_value: Option<f64>,
    pub alpha_min: Option<f64>,
    pub pm_move: Option<f64>,
    pub pm_mom: Option<f64>,
    pub ofi_250ms: Option<f64>,
    pub edge_raw: Option<f64>,
    pub edge_norm: Option<f64>,
    pub base_pm: Option<f64>,
    pub pm_mid: Option<f64>,
    pub opi_mid: Option<f64>,
    pub pm_best_bid: Option<f64>,
    pub pm_best_ask: Option<f64>,
    pub opi_best_bid: Option<f64>,
    pub opi_best_ask: Option<f64>,
    pub pm_staleness_ms: Option<i64>,
    pub opi_staleness_ms: Option<i64>,
    pub follow_score_raw: Option<f64>,
    pub follow_score_adj: Option<f64>,
    pub book_alpha_raw: Option<f64>,
    pub book_alpha_adj: Option<f64>,
    pub true_net_spread_buy_opinion: Option<f64>,
    pub true_net_spread_sell_opinion: Option<f64>,
    pub arb_mode_flag: Option<i64>,
    pub entry_price: Option<f64>,
    pub entry_qty: Option<f64>,
    pub entry_notional: Option<f64>,
    pub exit_price_target: Option<f64>,
    pub exit_price: Option<f64>,
    pub entry_order_id: Option<String>,
    pub exit_order_id: Option<String>,
    pub entry_ts_ms: Option<i64>,
    pub entry_bar_second: Option<i64>,
    pub holding_ms: Option<i64>,
    pub opi_last_price: Option<f64>,
    pub opi_bid_l1: Option<f64>,
    pub opi_ask_l1: Option<f64>,
    pub opi_bid_l3: Option<f64>,
    pub opi_ask_l3: Option<f64>,
    pub opi_bar_second: Option<i64>,
}

#[derive(Clone)]
pub struct AlphaLogger {
    tx: UnboundedSender<AlphaLogEntry>,
}

impl AlphaLogger {
    pub fn log(&self, entry: AlphaLogEntry) {
        let _ = self.tx.send(entry);
    }
}

struct AlphaLogConfig {
    jsonl_path: Option<String>,
    csv_path: Option<String>,
}

static ALPHA_LOG_TX: OnceLock<UnboundedSender<AlphaLogEntry>> = OnceLock::new();

pub fn init_alpha_logger(cfg: &TradeConfig) -> Option<AlphaLogger> {
    if !cfg.alpha_log_enabled {
        return None;
    }
    if let Some(tx) = ALPHA_LOG_TX.get() {
        return Some(AlphaLogger { tx: tx.clone() });
    }
    let jsonl_path = if cfg.alpha_log_jsonl_path.trim().is_empty() {
        None
    } else {
        Some(cfg.alpha_log_jsonl_path.clone())
    };
    let csv_path = if cfg.alpha_log_csv_path.trim().is_empty() {
        None
    } else {
        Some(cfg.alpha_log_csv_path.clone())
    };
    if jsonl_path.is_none() && csv_path.is_none() {
        return None;
    }
    let (tx, rx) = unbounded_channel();
    let _ = ALPHA_LOG_TX.set(tx.clone());
    let config = AlphaLogConfig { jsonl_path, csv_path };
    tokio::spawn(alpha_log_worker(rx, config));
    Some(AlphaLogger { tx })
}

async fn alpha_log_worker(mut rx: UnboundedReceiver<AlphaLogEntry>, cfg: AlphaLogConfig) {
    let mut jsonl_writer = open_writer(cfg.jsonl_path.as_deref(), true);
    let mut csv_writer = open_writer(cfg.csv_path.as_deref(), true);
    let mut csv_header_written = false;
    if let Some((_, ref mut writer)) = csv_writer {
        csv_header_written = writer.get_ref().metadata().map(|m| m.len() > 0).unwrap_or(false);
    }
    while let Some(entry) = rx.recv().await {
        if let Some((_, ref mut writer)) = jsonl_writer {
            if let Ok(line) = serde_json::to_string(&entry) {
                let _ = writeln!(writer, "{}", line);
                let _ = writer.flush();
            }
        }
        if let Some((_, ref mut writer)) = csv_writer {
            if !csv_header_written {
                let _ = writeln!(writer, "{}", csv_header());
                let _ = writer.flush();
                csv_header_written = true;
            }
            let row = csv_row(&entry);
            let _ = writeln!(writer, "{}", row);
            let _ = writer.flush();
        }
    }
}

fn open_writer(path: Option<&str>, create_dir: bool) -> Option<(String, BufWriter<std::fs::File>)> {
    let path = path?;
    if create_dir {
        if let Some(parent) = Path::new(path).parent() {
            let _ = create_dir_all(parent);
        }
    }
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .ok()?;
    Some((path.to_string(), BufWriter::new(file)))
}

fn csv_header() -> String {
    [
        "ts_ms",
        "bar_second",
        "phase",
        "state",
        "pair_id",
        "token_side",
        "direction",
        "alpha_type",
        "alpha_source",
        "alpha_source_cfg",
        "alpha_value",
        "alpha_min",
        "pm_move",
        "pm_mom",
        "ofi_250ms",
        "edge_raw",
        "edge_norm",
        "base_pm",
        "pm_mid",
        "opi_mid",
        "pm_best_bid",
        "pm_best_ask",
        "opi_best_bid",
        "opi_best_ask",
        "pm_staleness_ms",
        "opi_staleness_ms",
        "follow_score_raw",
        "follow_score_adj",
        "book_alpha_raw",
        "book_alpha_adj",
        "true_net_spread_buy_opinion",
        "true_net_spread_sell_opinion",
        "arb_mode_flag",
        "entry_price",
        "entry_qty",
        "entry_notional",
        "exit_price_target",
        "exit_price",
        "entry_order_id",
        "exit_order_id",
        "entry_ts_ms",
        "entry_bar_second",
        "holding_ms",
        "opi_last_price",
        "opi_bid_l1",
        "opi_ask_l1",
        "opi_bid_l3",
        "opi_ask_l3",
        "opi_bar_second",
    ]
    .join(",")
}

fn csv_row(entry: &AlphaLogEntry) -> String {
    let mut fields: Vec<String> = Vec::new();
    fields.push(entry.ts_ms.to_string());
    fields.push(entry.bar_second.to_string());
    fields.push(csv_escape(&entry.phase));
    fields.push(csv_escape(&entry.state));
    fields.push(entry.pair_id.to_string());
    fields.push(csv_escape(&entry.token_side));
    fields.push(entry.direction.to_string());
    fields.push(opt_str(&entry.alpha_type));
    fields.push(opt_str(&entry.alpha_source));
    fields.push(opt_str(&entry.alpha_source_cfg));
    fields.push(opt_f64(entry.alpha_value));
    fields.push(opt_f64(entry.alpha_min));
    fields.push(opt_f64(entry.pm_move));
    fields.push(opt_f64(entry.pm_mom));
    fields.push(opt_f64(entry.ofi_250ms));
    fields.push(opt_f64(entry.edge_raw));
    fields.push(opt_f64(entry.edge_norm));
    fields.push(opt_f64(entry.base_pm));
    fields.push(opt_f64(entry.pm_mid));
    fields.push(opt_f64(entry.opi_mid));
    fields.push(opt_f64(entry.pm_best_bid));
    fields.push(opt_f64(entry.pm_best_ask));
    fields.push(opt_f64(entry.opi_best_bid));
    fields.push(opt_f64(entry.opi_best_ask));
    fields.push(opt_i64(entry.pm_staleness_ms));
    fields.push(opt_i64(entry.opi_staleness_ms));
    fields.push(opt_f64(entry.follow_score_raw));
    fields.push(opt_f64(entry.follow_score_adj));
    fields.push(opt_f64(entry.book_alpha_raw));
    fields.push(opt_f64(entry.book_alpha_adj));
    fields.push(opt_f64(entry.true_net_spread_buy_opinion));
    fields.push(opt_f64(entry.true_net_spread_sell_opinion));
    fields.push(opt_i64(entry.arb_mode_flag));
    fields.push(opt_f64(entry.entry_price));
    fields.push(opt_f64(entry.entry_qty));
    fields.push(opt_f64(entry.entry_notional));
    fields.push(opt_f64(entry.exit_price_target));
    fields.push(opt_f64(entry.exit_price));
    fields.push(opt_str(&entry.entry_order_id));
    fields.push(opt_str(&entry.exit_order_id));
    fields.push(opt_i64(entry.entry_ts_ms));
    fields.push(opt_i64(entry.entry_bar_second));
    fields.push(opt_i64(entry.holding_ms));
    fields.push(opt_f64(entry.opi_last_price));
    fields.push(opt_f64(entry.opi_bid_l1));
    fields.push(opt_f64(entry.opi_ask_l1));
    fields.push(opt_f64(entry.opi_bid_l3));
    fields.push(opt_f64(entry.opi_ask_l3));
    fields.push(opt_i64(entry.opi_bar_second));
    fields.join(",")
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn opt_str(value: &Option<String>) -> String {
    value.as_deref().map(csv_escape).unwrap_or_default()
}

fn opt_f64(value: Option<f64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

fn opt_i64(value: Option<i64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

pub fn build_alpha_entry_base(
    bar: &Bars1sPair,
    token_side: TokenSide,
    pair_id: i64,
    phase: &str,
    state: &str,
    direction: i64,
) -> AlphaLogEntry {
    AlphaLogEntry {
        ts_ms: now_ts_ms(),
        bar_second: bar.bar_second,
        phase: phase.to_string(),
        state: state.to_string(),
        pair_id,
        token_side: token_side.as_str().to_string(),
        direction,
        alpha_type: None,
        alpha_source: None,
        alpha_source_cfg: None,
        alpha_value: None,
        alpha_min: None,
        pm_move: None,
        pm_mom: None,
        ofi_250ms: None,
        edge_raw: None,
        edge_norm: None,
        base_pm: None,
        pm_mid: bar.poly_mid_state.or(bar.poly_mid),
        opi_mid: bar.opi_mid_state.or(bar.opi_mid),
        pm_best_bid: bar.poly_best_bid_state.or(bar.poly_best_bid),
        pm_best_ask: bar.poly_best_ask_state.or(bar.poly_best_ask),
        opi_best_bid: bar.opi_best_bid_state.or(bar.opi_best_bid),
        opi_best_ask: bar.opi_best_ask_state.or(bar.opi_best_ask),
        pm_staleness_ms: bar.poly_staleness_ms,
        opi_staleness_ms: bar.opi_staleness_ms,
        follow_score_raw: bar.follow_score_raw,
        follow_score_adj: bar.follow_score_adj,
        book_alpha_raw: bar.book_alpha_raw,
        book_alpha_adj: bar.book_alpha_adj,
        true_net_spread_buy_opinion: bar.true_net_spread_buy_opinion,
        true_net_spread_sell_opinion: bar.true_net_spread_sell_opinion,
        arb_mode_flag: Some(bar.arb_mode_flag),
        entry_price: None,
        entry_qty: None,
        entry_notional: None,
        exit_price_target: None,
        exit_price: None,
        entry_order_id: None,
        exit_order_id: None,
        entry_ts_ms: None,
        entry_bar_second: None,
        holding_ms: None,
        opi_last_price: bar.opi_last_price,
        opi_bid_l1: bar.opi_bid_l1_notional,
        opi_ask_l1: bar.opi_ask_l1_notional,
        opi_bid_l3: bar.opi_bid_l3_notional,
        opi_ask_l3: bar.opi_ask_l3_notional,
        opi_bar_second: Some(bar.bar_second),
    }
}
