use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::Path;

#[derive(Debug, Clone, Deserialize)]
pub struct MonitorConfig {
    pub discovery_config_path: String,
    pub discovery_db_path: String,
    #[serde(default)]
    pub monitoring_db_url: String,
    #[serde(default = "default_db_queue_path")]
    pub db_queue_path: String,
    #[serde(default = "default_alpha_capture_enabled")]
    pub alpha_capture_enabled: bool,
    pub shard_count: usize,
    pub polymarket_ws_url: String,
    pub polymarket_rest_base: String,
    #[serde(default = "default_polymarket_rest_refresh_interval_sec")]
    pub polymarket_rest_refresh_interval_sec: u64,
    #[serde(default = "default_polymarket_rest_stale_threshold_sec")]
    pub polymarket_rest_stale_threshold_sec: u64,
    #[serde(default = "default_polymarket_rest_batch_size")]
    pub polymarket_rest_batch_size: usize,
    pub opinion_ws_url: String,
    pub opinion_api_key: String,
    #[serde(skip)]
    pub opinion_api_key_source: String,
    pub opinion_rest_base: String,
    #[serde(default = "default_opinion_rest_bootstrap_concurrency")]
    pub opinion_rest_bootstrap_concurrency: usize,
    #[serde(default = "default_opinion_rest_bootstrap_batch_size")]
    pub opinion_rest_bootstrap_batch_size: usize,
    #[serde(default = "default_opinion_rest_bootstrap_batch_delay_ms")]
    pub opinion_rest_bootstrap_batch_delay_ms: u64,
    #[serde(default = "default_opinion_rest_bootstrap_max_rps")]
    pub opinion_rest_bootstrap_max_rps: f64,
    #[serde(default = "default_opinion_rest_max_rps")]
    pub opinion_rest_max_rps: f64,
    #[serde(default = "default_opinion_rest_trigger_batch_size")]
    pub opinion_rest_trigger_batch_size: usize,
    #[serde(default = "default_opinion_rest_trigger_batch_delay_ms")]
    pub opinion_rest_trigger_batch_delay_ms: u64,
    #[serde(default = "default_opinion_rest_trigger_max_rps")]
    pub opinion_rest_trigger_max_rps: f64,
    #[serde(default = "default_opinion_rest_ws_connect_grace_ms")]
    pub opinion_rest_ws_connect_grace_ms: u64,
    #[serde(default = "default_opinion_rest_fast_interval_sec")]
    pub opinion_rest_fast_interval_sec: u64,
    #[serde(default = "default_opinion_rest_inactive_sec")]
    pub opinion_rest_inactive_sec: u64,
    #[serde(default = "default_opinion_rest_latest_price_interval_sec")]
    pub opinion_rest_latest_price_interval_sec: u64,
    #[serde(default)]
    pub opinion_rest_latest_price_all: bool,
    #[serde(default = "default_opinion_rest_latest_price_ws_stale_sec")]
    pub opinion_rest_latest_price_ws_stale_sec: u64,
    #[serde(default = "default_opinion_rest_latest_price_book_stale_sec")]
    pub opinion_rest_latest_price_book_stale_sec: u64,
    #[serde(default = "default_opinion_rest_orderbook_ws_stale_sec")]
    pub opinion_rest_orderbook_ws_stale_sec: u64,
    #[serde(default = "default_opinion_rest_orderbook_staleness_ms")]
    pub opinion_rest_orderbook_staleness_ms: i64,
    #[serde(default = "default_opinion_rest_active_only")]
    pub opinion_rest_active_only: bool,
    #[serde(default = "default_opinion_rest_ws_primary")]
    pub opinion_rest_ws_primary: bool,
    #[serde(default = "default_opinion_rest_stale_cooldown_threshold_ms")]
    pub opinion_rest_stale_cooldown_threshold_ms: i64,
    #[serde(default = "default_opinion_rest_stale_cooldown_duration_sec")]
    pub opinion_rest_stale_cooldown_duration_sec: u64,
    #[serde(default = "default_opinion_active_window_sec")]
    pub opinion_active_window_sec: u64,
    #[serde(default = "default_opinion_ts_max_skew_ms")]
    pub opinion_ts_max_skew_ms: i64,
    pub heartbeat_interval_sec: u64,
    pub snapshot_interval_min: u64,
    pub tick_interval_ms: u64,
    #[serde(default = "default_ws_stale_alert_sec")]
    pub ws_stale_alert_sec: u64,
    #[serde(default = "default_ws_reconnect_stale_sec")]
    pub ws_reconnect_stale_sec: u64,
    #[serde(default = "default_ws_error_reconnect_window_sec")]
    pub ws_error_reconnect_window_sec: u64,
    #[serde(default = "default_ws_error_reconnect_threshold")]
    pub ws_error_reconnect_threshold: u64,
    #[serde(default = "default_pm_staleness_ms")]
    pub pm_staleness_ms: i64,
    #[serde(default = "default_other_staleness_ms")]
    pub other_staleness_ms: i64,
    #[serde(default = "default_hard_clear_ms")]
    pub hard_clear_ms: i64,
    #[serde(default = "default_analysis_staleness_ms")]
    pub analysis_staleness_ms: i64,
    #[serde(default = "default_analysis_gap_allow")]
    pub analysis_gap_allow: usize,
    #[serde(default = "default_analysis_move_eps")]
    pub analysis_move_eps: f64,
    #[serde(default = "default_pair_flush_late_arrival_s")]
    pub pair_flush_late_arrival_s: i64,
    pub clock_offset_alpha: f64,
    pub clock_offset_max_step_ms: i64,
    pub fee_bps_polymarket: i64,
    pub fee_bps_opinion: i64,
    pub chain_metrics_interval_sec: u64,
    pub chain_metrics_base_fee: f64,
    pub chain_metrics_priority_fee: f64,
    pub chain_metrics_gas_used_est: f64,
    #[serde(default)]
    pub trade: TradeConfig,
}

impl MonitorConfig {
    pub fn load() -> anyhow::Result<Self> {
        let default_config_path =
            env::var("MONITOR_CONFIG").unwrap_or_else(|_| "./monitor/config.yaml".to_string());
        let mut cfg = if Path::new(&default_config_path).exists() {
            let raw = fs::read_to_string(&default_config_path)?;
            serde_yaml::from_str::<MonitorConfig>(&raw)?
        } else {
            MonitorConfig::default()
        };
        cfg.opinion_api_key = normalize_key(&cfg.opinion_api_key);
        let mut key_source = if cfg.opinion_api_key.is_empty() {
            String::new()
        } else {
            format!("config:{}", default_config_path)
        };

        if let Ok(path) = env::var("DISCOVERY_CONFIG_PATH") {
            cfg.discovery_config_path = path;
        }
        if let Ok(path) = env::var("DISCOVERY_DB_PATH") {
            cfg.discovery_db_path = path;
        }
        if let Ok(url) = env::var("MONITOR_DB_URL") {
            cfg.monitoring_db_url = url;
        }
        if let Ok(path) = env::var("MONITOR_DB_QUEUE_PATH") {
            cfg.db_queue_path = path;
        }
        if let Ok(value) = env::var("ALPHA_CAPTURE_ENABLED") {
            if let Ok(parsed) = value.parse::<bool>() {
                cfg.alpha_capture_enabled = parsed;
            }
        }
        if let Ok(url) = env::var("PM_WS_URL") {
            cfg.polymarket_ws_url = url;
        }
        if let Ok(url) = env::var("POLYMARKET_REST_BASE") {
            cfg.polymarket_rest_base = url;
        }
        if let Ok(value) = env::var("POLYMARKET_REST_REFRESH_INTERVAL_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.polymarket_rest_refresh_interval_sec = parsed;
            }
        }
        if let Ok(value) = env::var("POLYMARKET_REST_STALE_THRESHOLD_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.polymarket_rest_stale_threshold_sec = parsed;
            }
        }
        if let Ok(value) = env::var("POLYMARKET_REST_BATCH_SIZE") {
            if let Ok(parsed) = value.parse::<usize>() {
                cfg.polymarket_rest_batch_size = parsed;
            }
        }
        if let Ok(url) = env::var("OPI_WS_URL") {
            cfg.opinion_ws_url = url;
        }
        if let Ok(key) = env::var("OPINION_API_KEY") {
            let key = normalize_key(&key);
            if !key.is_empty() {
                cfg.opinion_api_key = key;
                key_source = "env:OPINION_API_KEY".to_string();
            }
        }
        if let Ok(url) = env::var("OPINION_REST_BASE") {
            cfg.opinion_rest_base = url;
        }
        if let Ok(value) = env::var("OPINION_REST_BOOTSTRAP_CONCURRENCY") {
            if let Ok(parsed) = value.parse::<usize>() {
                cfg.opinion_rest_bootstrap_concurrency = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_BOOTSTRAP_BATCH_SIZE") {
            if let Ok(parsed) = value.parse::<usize>() {
                cfg.opinion_rest_bootstrap_batch_size = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_BOOTSTRAP_BATCH_DELAY_MS") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_bootstrap_batch_delay_ms = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_BOOTSTRAP_MAX_RPS") {
            if let Ok(parsed) = value.parse::<f64>() {
                cfg.opinion_rest_bootstrap_max_rps = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_MAX_RPS") {
            if let Ok(parsed) = value.parse::<f64>() {
                cfg.opinion_rest_max_rps = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_TRIGGER_BATCH_SIZE") {
            if let Ok(parsed) = value.parse::<usize>() {
                cfg.opinion_rest_trigger_batch_size = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_TRIGGER_BATCH_DELAY_MS") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_trigger_batch_delay_ms = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_TRIGGER_MAX_RPS") {
            if let Ok(parsed) = value.parse::<f64>() {
                cfg.opinion_rest_trigger_max_rps = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_WS_CONNECT_GRACE_MS") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_ws_connect_grace_ms = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_FAST_INTERVAL_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_fast_interval_sec = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_INACTIVE_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_inactive_sec = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_LATEST_PRICE_INTERVAL_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_latest_price_interval_sec = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_LATEST_PRICE_ALL") {
            if let Ok(parsed) = value.parse::<bool>() {
                cfg.opinion_rest_latest_price_all = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_LATEST_PRICE_WS_STALE_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_latest_price_ws_stale_sec = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_LATEST_PRICE_BOOK_STALE_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_latest_price_book_stale_sec = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_ORDERBOOK_WS_STALE_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_orderbook_ws_stale_sec = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_ORDERBOOK_STALENESS_MS") {
            if let Ok(parsed) = value.parse::<i64>() {
                cfg.opinion_rest_orderbook_staleness_ms = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_ACTIVE_ONLY") {
            if let Ok(parsed) = value.parse::<bool>() {
                cfg.opinion_rest_active_only = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_WS_PRIMARY") {
            if let Ok(parsed) = value.parse::<bool>() {
                cfg.opinion_rest_ws_primary = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_STALE_COOLDOWN_THRESHOLD_MS") {
            if let Ok(parsed) = value.parse::<i64>() {
                cfg.opinion_rest_stale_cooldown_threshold_ms = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_REST_STALE_COOLDOWN_DURATION_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_rest_stale_cooldown_duration_sec = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_ACTIVE_WINDOW_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.opinion_active_window_sec = parsed;
            }
        }
        if let Ok(value) = env::var("OPINION_TS_MAX_SKEW_MS") {
            if let Ok(parsed) = value.parse::<i64>() {
                cfg.opinion_ts_max_skew_ms = parsed;
            }
        }
        if let Ok(value) = env::var("CHAIN_METRICS_INTERVAL_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.chain_metrics_interval_sec = parsed;
            }
        }
        if let Ok(value) = env::var("CHAIN_BASE_FEE") {
            if let Ok(parsed) = value.parse::<f64>() {
                cfg.chain_metrics_base_fee = parsed;
            }
        }
        if let Ok(value) = env::var("CHAIN_PRIORITY_FEE") {
            if let Ok(parsed) = value.parse::<f64>() {
                cfg.chain_metrics_priority_fee = parsed;
            }
        }
        if let Ok(value) = env::var("CHAIN_GAS_USED_EST") {
            if let Ok(parsed) = value.parse::<f64>() {
                cfg.chain_metrics_gas_used_est = parsed;
            }
        }
        if let Ok(value) = env::var("WS_STALE_ALERT_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.ws_stale_alert_sec = parsed;
            }
        }
        if let Ok(value) = env::var("WS_RECONNECT_STALE_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.ws_reconnect_stale_sec = parsed;
            }
        }
        if let Ok(value) = env::var("WS_ERROR_RECONNECT_WINDOW_SEC") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.ws_error_reconnect_window_sec = parsed;
            }
        }
        if let Ok(value) = env::var("WS_ERROR_RECONNECT_THRESHOLD") {
            if let Ok(parsed) = value.parse::<u64>() {
                cfg.ws_error_reconnect_threshold = parsed;
            }
        }
        if let Ok(value) = env::var("PM_STALENESS_MS") {
            if let Ok(parsed) = value.parse::<i64>() {
                cfg.pm_staleness_ms = parsed;
            }
        }
        if let Ok(value) = env::var("OTHER_STALENESS_MS") {
            if let Ok(parsed) = value.parse::<i64>() {
                cfg.other_staleness_ms = parsed;
            }
        }
        if let Ok(value) = env::var("HARD_CLEAR_MS") {
            if let Ok(parsed) = value.parse::<i64>() {
                cfg.hard_clear_ms = parsed;
            }
        }
        if let Ok(value) = env::var("ANALYSIS_STALENESS_MS") {
            if let Ok(parsed) = value.parse::<i64>() {
                cfg.analysis_staleness_ms = parsed;
            }
        }
        if let Ok(value) = env::var("ANALYSIS_GAP_ALLOW") {
            if let Ok(parsed) = value.parse::<usize>() {
                cfg.analysis_gap_allow = parsed;
            }
        }
        if let Ok(value) = env::var("ANALYSIS_MOVE_EPS") {
            if let Ok(parsed) = value.parse::<f64>() {
                cfg.analysis_move_eps = parsed;
            }
        }
        if let Ok(value) = env::var("PAIR_FLUSH_LATE_ARRIVAL_S") {
            if let Ok(parsed) = value.parse::<i64>() {
                cfg.pair_flush_late_arrival_s = parsed.max(0);
            }
        }

        if cfg.opinion_api_key.is_empty() || cfg.discovery_db_path.is_empty() {
            if let Ok(discovery_cfg) = read_discovery_config(&cfg.discovery_config_path) {
                if let Some(key) = discovery_cfg.opinion_api_key {
                    let key = normalize_key(&key);
                    if !key.is_empty() {
                        cfg.opinion_api_key = key;
                        key_source = format!("discovery_config:{}", cfg.discovery_config_path);
                    }
                }
                if cfg.discovery_db_path.is_empty() {
                    if let Some(db_path) = discovery_cfg.database_path {
                        cfg.discovery_db_path = db_path;
                    }
                }
                if let Some(opinion_base) = discovery_cfg.opinion_base_url {
                    cfg.opinion_rest_base = normalize_opinion_base(&opinion_base);
                }
            }
        }
        if cfg.opinion_api_key.is_empty() {
            let accounts_path = env::var("OPINION_ACCOUNTS_PATH").unwrap_or_else(|_| {
                "/data/polymarket/polymarket_opinion_bot/config/accounts.json".to_string()
            });
            if let Some(key) = read_accounts_key(&accounts_path) {
                cfg.opinion_api_key = key;
                key_source = format!("accounts_file:{}", accounts_path);
            } else {
                let fallback_path =
                    "/data/polymarket/polymarket_opinion_bot/config/accounts.example.json";
                if let Some(key) = read_accounts_key(fallback_path) {
                    cfg.opinion_api_key = key;
                    key_source = format!("accounts_file:{}", fallback_path);
                }
            }
        }
        cfg.opinion_api_key_source = key_source;
        Ok(cfg)
    }
}

impl Default for MonitorConfig {
    fn default() -> Self {
        MonitorConfig {
            discovery_config_path: "/data/polymarket/polymarket_opinion_bot/discovery/config.yaml"
                .to_string(),
            discovery_db_path: "/data/polymarket/polymarket_opinion_bot/discovery/discovery.db"
                .to_string(),
            monitoring_db_url: String::new(),
            db_queue_path: default_db_queue_path(),
            alpha_capture_enabled: default_alpha_capture_enabled(),
            shard_count: 8,
            polymarket_ws_url: "wss://ws-subscriptions-clob.polymarket.com/ws/market".to_string(),
            polymarket_rest_base: "https://clob.polymarket.com".to_string(),
            polymarket_rest_refresh_interval_sec: default_polymarket_rest_refresh_interval_sec(),
            polymarket_rest_stale_threshold_sec: default_polymarket_rest_stale_threshold_sec(),
            polymarket_rest_batch_size: default_polymarket_rest_batch_size(),
            opinion_ws_url: "wss://ws.opinion.trade".to_string(),
            opinion_api_key: String::new(),
            opinion_api_key_source: String::new(),
            opinion_rest_base: "https://openapi.opinion.trade/openapi".to_string(),
            opinion_rest_bootstrap_concurrency: default_opinion_rest_bootstrap_concurrency(),
            opinion_rest_bootstrap_batch_size: default_opinion_rest_bootstrap_batch_size(),
            opinion_rest_bootstrap_batch_delay_ms: default_opinion_rest_bootstrap_batch_delay_ms(),
            opinion_rest_bootstrap_max_rps: default_opinion_rest_bootstrap_max_rps(),
            opinion_rest_max_rps: default_opinion_rest_max_rps(),
            opinion_rest_trigger_batch_size: default_opinion_rest_trigger_batch_size(),
            opinion_rest_trigger_batch_delay_ms: default_opinion_rest_trigger_batch_delay_ms(),
            opinion_rest_trigger_max_rps: default_opinion_rest_trigger_max_rps(),
            opinion_rest_ws_connect_grace_ms: default_opinion_rest_ws_connect_grace_ms(),
            opinion_rest_fast_interval_sec: default_opinion_rest_fast_interval_sec(),
            opinion_rest_inactive_sec: default_opinion_rest_inactive_sec(),
            opinion_rest_latest_price_interval_sec: default_opinion_rest_latest_price_interval_sec(
            ),
            opinion_rest_latest_price_all: false,
            opinion_rest_latest_price_ws_stale_sec: default_opinion_rest_latest_price_ws_stale_sec(
            ),
            opinion_rest_latest_price_book_stale_sec:
                default_opinion_rest_latest_price_book_stale_sec(),
            opinion_rest_orderbook_ws_stale_sec: default_opinion_rest_orderbook_ws_stale_sec(),
            opinion_rest_orderbook_staleness_ms: default_opinion_rest_orderbook_staleness_ms(),
            opinion_rest_active_only: default_opinion_rest_active_only(),
            opinion_rest_ws_primary: default_opinion_rest_ws_primary(),
            opinion_rest_stale_cooldown_threshold_ms: default_opinion_rest_stale_cooldown_threshold_ms(),
            opinion_rest_stale_cooldown_duration_sec: default_opinion_rest_stale_cooldown_duration_sec(),
            opinion_active_window_sec: default_opinion_active_window_sec(),
            opinion_ts_max_skew_ms: default_opinion_ts_max_skew_ms(),
            heartbeat_interval_sec: 30,
            snapshot_interval_min: 15,
            tick_interval_ms: 1000,
            ws_stale_alert_sec: default_ws_stale_alert_sec(),
            ws_reconnect_stale_sec: default_ws_reconnect_stale_sec(),
            ws_error_reconnect_window_sec: default_ws_error_reconnect_window_sec(),
            ws_error_reconnect_threshold: default_ws_error_reconnect_threshold(),
            pm_staleness_ms: default_pm_staleness_ms(),
            other_staleness_ms: default_other_staleness_ms(),
            hard_clear_ms: default_hard_clear_ms(),
            analysis_staleness_ms: default_analysis_staleness_ms(),
            analysis_gap_allow: default_analysis_gap_allow(),
            analysis_move_eps: default_analysis_move_eps(),
            pair_flush_late_arrival_s: default_pair_flush_late_arrival_s(),
            clock_offset_alpha: 0.1,
            clock_offset_max_step_ms: 50,
            fee_bps_polymarket: 350,
            fee_bps_opinion: 0,
            chain_metrics_interval_sec: 10,
            chain_metrics_base_fee: 0.0,
            chain_metrics_priority_fee: 0.0,
            chain_metrics_gas_used_est: 0.0,
            trade: TradeConfig::default(),
        }
    }
}

fn default_opinion_rest_bootstrap_concurrency() -> usize {
    8
}

fn default_opinion_rest_bootstrap_batch_size() -> usize {
    50
}

fn default_opinion_rest_bootstrap_batch_delay_ms() -> u64 {
    300
}

fn default_opinion_rest_bootstrap_max_rps() -> f64 {
    6.0
}

fn default_opinion_rest_trigger_batch_size() -> usize {
    50
}

fn default_opinion_rest_trigger_batch_delay_ms() -> u64 {
    300
}

fn default_opinion_rest_trigger_max_rps() -> f64 {
    6.0
}

fn default_opinion_rest_ws_connect_grace_ms() -> u64 {
    3000
}

fn default_db_queue_path() -> String {
    "/data/polymarket/records/db_queue.sqlite".to_string()
}

fn default_alpha_capture_enabled() -> bool {
    false
}

fn default_opinion_rest_max_rps() -> f64 {
    10.0
}

fn default_opinion_rest_fast_interval_sec() -> u64 {
    120
}

fn default_opinion_rest_inactive_sec() -> u64 {
    60
}

fn default_opinion_rest_latest_price_interval_sec() -> u64 {
    120
}

fn default_opinion_rest_latest_price_ws_stale_sec() -> u64 {
    15
}

fn default_opinion_rest_latest_price_book_stale_sec() -> u64 {
    15
}

fn default_opinion_rest_orderbook_ws_stale_sec() -> u64 {
    0
}

fn default_opinion_rest_orderbook_staleness_ms() -> i64 {
    0
}

fn default_opinion_rest_active_only() -> bool {
    false
}

fn default_opinion_rest_ws_primary() -> bool {
    false
}

fn default_opinion_rest_stale_cooldown_threshold_ms() -> i64 {
    0
}

fn default_opinion_rest_stale_cooldown_duration_sec() -> u64 {
    0
}

fn default_opinion_active_window_sec() -> u64 {
    600
}

fn default_opinion_ts_max_skew_ms() -> i64 {
    30_000
}

fn default_ws_stale_alert_sec() -> u64 {
    15
}

fn default_ws_reconnect_stale_sec() -> u64 {
    60
}

fn default_ws_error_reconnect_window_sec() -> u64 {
    60
}

fn default_ws_error_reconnect_threshold() -> u64 {
    3
}

fn default_polymarket_rest_refresh_interval_sec() -> u64 {
    60
}

fn default_polymarket_rest_stale_threshold_sec() -> u64 {
    120
}

fn default_polymarket_rest_batch_size() -> usize {
    500
}

fn default_pm_staleness_ms() -> i64 {
    10_000
}

fn default_other_staleness_ms() -> i64 {
    30_000
}

fn default_hard_clear_ms() -> i64 {
    120_000
}

fn default_analysis_staleness_ms() -> i64 {
    60_000
}

fn default_analysis_gap_allow() -> usize {
    5
}

fn default_analysis_move_eps() -> f64 {
    1e-6
}

fn default_pair_flush_late_arrival_s() -> i64 {
    60
}

#[derive(Debug, Clone, Deserialize)]
pub struct TradeConfig {
    #[serde(default = "default_trade_enabled")]
    pub enabled: bool,
    #[serde(default = "default_trade_dry_run")]
    pub dry_run: bool,
    #[serde(default = "default_trade_engine_shards")]
    pub engine_shards: usize,
    #[serde(default = "default_trade_queue_capacity")]
    pub queue_capacity: usize,
    #[serde(default = "default_trade_order_executor_concurrency")]
    pub order_executor_concurrency: usize,
    #[serde(default = "default_trade_opinion_account_file")]
    pub opinion_account_file: String,
    #[serde(default = "default_trade_opinion_account_id")]
    pub opinion_account_id: String,
    #[serde(default = "default_trade_opinion_private_base")]
    pub opinion_private_base: String,
    #[serde(default = "default_trade_opinion_prefetch_base")]
    pub opinion_prefetch_base: String,
    #[serde(default = "default_trade_opinion_prefetch_interval_ms")]
    pub opinion_prefetch_interval_ms: u64,
    #[serde(default = "default_trade_opinion_check_approval")]
    pub opinion_check_approval: bool,
    #[serde(default = "default_trade_opinion_enable_trading_on_startup")]
    pub opinion_enable_trading_on_startup: bool,
    #[serde(default = "default_trade_opinion_enable_retry_ms")]
    pub opinion_enable_retry_ms: i64,
    #[serde(default = "default_trade_opinion_force_eoa_orders")]
    pub opinion_force_eoa_orders: bool,
    #[serde(default = "default_trade_order_poll_interval_ms")]
    pub order_poll_interval_ms: u64,
    #[serde(default = "default_trade_order_client_prefix")]
    pub order_client_prefix: String,
    #[serde(default = "default_trade_ws_user_channels_enabled")]
    pub ws_user_channels_enabled: bool,
    #[serde(default = "default_trade_ws_user_channel_stale_ms")]
    pub ws_user_channel_stale_ms: i64,
    #[serde(default = "default_trade_post_only")]
    pub post_only: bool,
    #[serde(default = "default_trade_entry_ttl_ms")]
    pub entry_ttl_ms: i64,
    #[serde(default = "default_trade_alpha_a_entry_ttl_ms")]
    pub alpha_a_entry_ttl_ms: i64,
    #[serde(default = "default_trade_alpha_a_entry_ttl_frac")]
    pub alpha_a_entry_ttl_frac: f64,
    #[serde(default = "default_trade_alpha_a_entry_ttl_cap_ms")]
    pub alpha_a_entry_ttl_cap_ms: i64,
    #[serde(default = "default_trade_alpha_b_entry_ttl_frac")]
    pub alpha_b_entry_ttl_frac: f64,
    #[serde(default = "default_trade_alpha_b_entry_ttl_cap_ms")]
    pub alpha_b_entry_ttl_cap_ms: i64,
    #[serde(default = "default_trade_exit_ttl_ms")]
    pub exit_ttl_ms: i64,
    #[serde(default = "default_trade_alpha_source")]
    pub alpha_source: String,
    #[serde(default = "default_trade_alpha_log_enabled")]
    pub alpha_log_enabled: bool,
    #[serde(default = "default_trade_alpha_log_jsonl_path")]
    pub alpha_log_jsonl_path: String,
    #[serde(default = "default_trade_alpha_log_csv_path")]
    pub alpha_log_csv_path: String,
    #[serde(default = "default_trade_alpha_log_interval_sec")]
    pub alpha_log_interval_sec: i64,
    #[serde(default = "default_trade_alpha_min")]
    pub alpha_min: f64,
    #[serde(default = "default_trade_move_min")]
    pub move_min: f64,
    #[serde(default = "default_trade_mom_min")]
    pub mom_min: f64,
    #[serde(default = "default_trade_edge_min")]
    pub edge_min: f64,
    #[serde(default = "default_trade_book_decay_block")]
    pub book_decay_block: f64,
    #[serde(default = "default_trade_book_support_block")]
    pub book_support_block: f64,
    #[serde(default = "default_trade_book_decay_scale")]
    pub book_decay_scale: f64,
    #[serde(default = "default_trade_book_support_scale")]
    pub book_support_scale: f64,
    #[serde(default = "default_trade_book_scale_min")]
    pub book_scale_min: f64,
    #[serde(default = "default_trade_entry_fraction")]
    pub entry_fraction: f64,
    #[serde(default = "default_trade_entry_fraction_free")]
    pub entry_fraction_free: f64,
    #[serde(default = "default_trade_entry_pool_frac")]
    pub entry_pool_frac: f64,
    #[serde(default = "default_trade_min_order_notional_usd")]
    pub min_order_notional_usd: f64,
    #[serde(default = "default_trade_max_open_positions")]
    pub max_open_positions: usize,
    #[serde(default = "default_trade_max_global_exposure_frac")]
    pub max_global_exposure_frac: f64,
    #[serde(default = "default_trade_max_per_pair_exposure_frac")]
    pub max_per_pair_exposure_frac: f64,
    #[serde(default = "default_trade_gas_mult")]
    pub gas_mult: f64,
    #[serde(default = "default_trade_gas_buffer_usd")]
    pub gas_buffer_usd: f64,
    #[serde(default = "default_trade_gas_est_usd")]
    pub gas_est_usd: f64,
    #[serde(default = "default_trade_gas_lookback_ms")]
    pub gas_lookback_ms: i64,
    #[serde(default = "default_trade_gas_include_cancel_allowance")]
    pub gas_include_cancel_allowance: bool,
    #[serde(default = "default_trade_gas_cancel_allowance_factor")]
    pub gas_cancel_allowance_factor: f64,
    #[serde(default = "default_trade_gas_emergency_pause_enabled")]
    pub gas_emergency_pause_enabled: bool,
    #[serde(default = "default_trade_gas_emergency_max_usd")]
    pub gas_emergency_max_usd: f64,
    #[serde(default = "default_trade_cancel_check_interval_ms")]
    pub cancel_check_interval_ms: i64,
    #[serde(default = "default_trade_entry_cancel_shock_mom_ratio_min")]
    pub entry_cancel_shock_mom_ratio_min: f64,
    #[serde(default = "default_trade_entry_cancel_mom_floor")]
    pub entry_cancel_mom_floor: f64,
    #[serde(default = "default_trade_entry_cancel_shock_floor")]
    pub entry_cancel_shock_floor: f64,
    #[serde(default = "default_trade_entry_cancel_mom_decay_min")]
    pub entry_cancel_mom_decay_min: f64,
    #[serde(default = "default_trade_retrace_cancel")]
    pub retrace_cancel: f64,
    #[serde(default = "default_trade_retrace_exit")]
    pub retrace_exit: f64,
    #[serde(default = "default_trade_cooldown_sec")]
    pub cooldown_sec: i64,
    #[serde(default = "default_trade_cooldown_after_cancel_ms")]
    pub cooldown_after_cancel_ms: i64,
    #[serde(default = "default_trade_cooldown_after_exit_ms")]
    pub cooldown_after_exit_ms: i64,
    #[serde(default = "default_trade_cooldown_after_postonly_reject_ms")]
    pub cooldown_after_postonly_reject_ms: i64,
    #[serde(default = "default_trade_per_pair_min_interval_ms")]
    pub per_pair_min_interval_ms: i64,
    #[serde(default = "default_trade_base_alpha")]
    pub base_alpha: f64,
    #[serde(default = "default_trade_move_eps")]
    pub move_eps: f64,
    #[serde(default = "default_trade_tick_px")]
    pub tick_px: f64,
    #[serde(default = "default_trade_entry_tick_offset")]
    pub entry_tick_offset: i64,
    #[serde(default = "default_trade_entry_spread_max_ticks")]
    pub entry_spread_max_ticks: i64,
    #[serde(default = "default_trade_entry_spread_max_pct")]
    pub entry_spread_max_pct: f64,
    #[serde(default = "default_trade_entry_spread_size_scale")]
    pub entry_spread_size_scale: f64,
    #[serde(default = "default_trade_entry_aggr_enabled")]
    pub entry_aggr_enabled: bool,
    #[serde(default = "default_trade_entry_aggr_shock_max")]
    pub entry_aggr_shock_max: f64,
    #[serde(default = "default_trade_entry_aggr_shock_min")]
    pub entry_aggr_shock_min: f64,
    #[serde(default = "default_trade_entry_aggr_shock_early_ratio")]
    pub entry_aggr_shock_early_ratio: f64,
    #[serde(default = "default_trade_entry_aggr_gap")]
    pub entry_aggr_gap: f64,
    #[serde(default = "default_trade_entry_aggr_default")]
    pub entry_aggr_default: f64,
    #[serde(default = "default_trade_exit_tick_offset")]
    pub exit_tick_offset: i64,
    #[serde(default = "default_trade_max_chase_ticks")]
    pub max_chase_ticks: i64,
    #[serde(default = "default_trade_chase_step_ticks")]
    pub chase_step_ticks: i64,
    #[serde(default = "default_trade_inv_k")]
    pub inv_k: f64,
    #[serde(default = "default_trade_target_decay_per_sec")]
    pub target_decay_per_sec: f64,
    #[serde(default = "default_trade_target_floor_gas_mult")]
    pub target_floor_gas_mult: f64,
    #[serde(default = "default_trade_ofi_entry_min")]
    pub ofi_entry_min: f64,
    #[serde(default = "default_trade_ofi_cancel_low")]
    pub ofi_cancel_low: f64,
    #[serde(default = "default_trade_ofi_cancel_med")]
    pub ofi_cancel_med: f64,
    #[serde(default = "default_trade_trade_cancel_usd")]
    pub trade_cancel_usd: f64,
    #[serde(default = "default_trade_ask_add_usd")]
    pub ask_add_usd: f64,
    #[serde(default = "default_trade_bid_withdraw_usd")]
    pub bid_withdraw_usd: f64,
    #[serde(default = "default_trade_opi_mid_ret_cancel_bps")]
    pub opi_mid_ret_cancel_bps: f64,
    #[serde(default = "default_trade_opi_spread_bps_min")]
    pub opi_spread_bps_min: f64,
    #[serde(default = "default_trade_opi_edge_min_bps")]
    pub opi_edge_min_bps: f64,
    #[serde(default = "default_trade_opi_imbalance_cancel")]
    pub opi_imbalance_cancel: f64,
    #[serde(default = "default_trade_opi_bid_withdraw_usd")]
    pub opi_bid_withdraw_usd: f64,
    #[serde(default = "default_trade_opi_ask_add_usd")]
    pub opi_ask_add_usd: f64,
    #[serde(default = "default_trade_opi_taker_buy_cancel_usd")]
    pub opi_taker_buy_cancel_usd: f64,
    #[serde(default = "default_trade_opi_taker_sell_cancel_usd")]
    pub opi_taker_sell_cancel_usd: f64,
    #[serde(default = "default_trade_opi_trade_imbalance_cancel")]
    pub opi_trade_imbalance_cancel: f64,
    #[serde(default = "default_trade_opi_cancel_staleness_ms")]
    pub opi_cancel_staleness_ms: i64,
    #[serde(default = "default_trade_signal_staleness_ms")]
    pub signal_staleness_ms: i64,
    #[serde(default = "default_trade_opi_snapshot_on_demand_min_interval_ms")]
    pub opi_snapshot_on_demand_min_interval_ms: i64,
    #[serde(default = "default_trade_log_blocked_signals")]
    pub log_blocked_signals: bool,
    #[serde(default = "default_trade_log_blocked_interval_ms")]
    pub log_blocked_interval_ms: i64,
    #[serde(default = "default_trade_max_bar_gap_ratio_1m")]
    pub max_bar_gap_ratio_1m: f64,
    #[serde(default = "default_trade_max_opi_staleness_p90_ms")]
    pub max_opi_staleness_p90_ms: i64,
    #[serde(default = "default_trade_max_poly_latency_p99_ms")]
    pub max_poly_latency_p99_ms: i64,
    #[serde(default = "default_trade_max_opi_latency_p99_ms")]
    pub max_opi_latency_p99_ms: i64,
    #[serde(default = "default_trade_balance_total_usd")]
    pub balance_total_usd: f64,
    #[serde(default = "default_trade_balance_free_usd")]
    pub balance_free_usd: f64,
    #[serde(default = "default_trade_k_edge")]
    pub k_edge: f64,
    #[serde(default = "default_trade_k_mom")]
    pub k_mom: f64,
    #[serde(default = "default_trade_k_shock")]
    pub k_shock: f64,
    #[serde(default = "default_trade_tp_a1")]
    pub tp_a1: f64,
    #[serde(default = "default_trade_tp_a2")]
    pub tp_a2: f64,
    #[serde(default = "default_trade_tp_a3")]
    pub tp_a3: f64,
    #[serde(default = "default_trade_tp_a4_shock")]
    pub tp_a4_shock: f64,
    #[serde(default = "default_trade_alpha_a_enabled")]
    pub alpha_a_enabled: bool,
    #[serde(default = "default_trade_alpha_a_opi_stale_max_ms")]
    pub alpha_a_opi_stale_max_ms: i64,
    #[serde(default = "default_trade_alpha_a_opi_stale_soft_start_ms")]
    pub alpha_a_opi_stale_soft_start_ms: i64,
    #[serde(default = "default_trade_alpha_a_opi_stale_soft_tau_ms")]
    pub alpha_a_opi_stale_soft_tau_ms: i64,
    #[serde(default = "default_trade_alpha_a_opi_stale_soft_min_scale")]
    pub alpha_a_opi_stale_soft_min_scale: f64,
    #[serde(default = "default_trade_alpha_a_use_pair_quantile")]
    pub alpha_a_use_pair_quantile: bool,
    #[serde(default = "default_trade_alpha_a_pair_p50_plus_ms")]
    pub alpha_a_pair_p50_plus_ms: i64,
    #[serde(default = "default_trade_alpha_a_edge_norm_min")]
    pub alpha_a_edge_norm_min: f64,
    #[serde(default = "default_trade_alpha_a_edge_raw_min_abs")]
    pub alpha_a_edge_raw_min_abs: f64,
    #[serde(default = "default_trade_alpha_a_cost_mult")]
    pub alpha_a_cost_mult: f64,
    #[serde(default = "default_trade_alpha_a_max_age_s")]
    pub alpha_a_max_age_s: i64,
    #[serde(default = "default_trade_alpha_a_allowed_shock_types")]
    pub alpha_a_allowed_shock_types: Vec<String>,
    #[serde(default = "default_trade_alpha_a_require_noise_zero")]
    pub alpha_a_require_noise_zero: bool,
    #[serde(default = "default_trade_alpha_a_decay_per_sec")]
    pub alpha_a_decay_per_sec: f64,
    #[serde(default = "default_trade_alpha_a_horizon_default_s")]
    pub alpha_a_horizon_default_s: i64,
    #[serde(default = "default_trade_alpha_a_horizon_by_shock_type")]
    pub alpha_a_horizon_by_shock_type: HashMap<String, i64>,
    #[serde(default = "default_trade_alpha_b_enabled")]
    pub alpha_b_enabled: bool,
    #[serde(default = "default_trade_alpha_b_opi_stale_max_ms")]
    pub alpha_b_opi_stale_max_ms: i64,
    #[serde(default = "default_trade_alpha_b_use_pair_quantile")]
    pub alpha_b_use_pair_quantile: bool,
    #[serde(default = "default_trade_alpha_b_pair_p50_plus_ms")]
    pub alpha_b_pair_p50_plus_ms: i64,
    #[serde(default = "default_trade_alpha_b_move_min")]
    pub alpha_b_move_min: f64,
    #[serde(default = "default_trade_alpha_b_mom_min")]
    pub alpha_b_mom_min: f64,
    #[serde(default = "default_trade_alpha_b_ofi_entry_min")]
    pub alpha_b_ofi_entry_min: f64,
    #[serde(default = "default_trade_alpha_b_edge_norm_min")]
    pub alpha_b_edge_norm_min: f64,
    #[serde(default = "default_trade_alpha_b_cost_mult")]
    pub alpha_b_cost_mult: f64,
    #[serde(default = "default_trade_alpha_b_retrace_cancel_ratio")]
    pub alpha_b_retrace_cancel_ratio: f64,
    #[serde(default = "default_trade_alpha_b_ofi_flip_cancel_usd")]
    pub alpha_b_ofi_flip_cancel_usd: f64,
    #[serde(default = "default_trade_alpha_b_decay_per_sec")]
    pub alpha_b_decay_per_sec: f64,
    #[serde(default = "default_trade_alpha_b_horizon_default_s")]
    pub alpha_b_horizon_default_s: i64,
    #[serde(default = "default_trade_alpha_c_enabled")]
    pub alpha_c_enabled: bool,
    #[serde(default = "default_trade_alpha_c_use_lag_clamp")]
    pub alpha_c_use_lag_clamp: bool,
    #[serde(default = "default_trade_alpha_c_lookback_fixed_s")]
    pub alpha_c_lookback_fixed_s: i64,
    #[serde(default = "default_trade_alpha_c_lookback_min_s")]
    pub alpha_c_lookback_min_s: i64,
    #[serde(default = "default_trade_alpha_c_lookback_max_s")]
    pub alpha_c_lookback_max_s: i64,
    #[serde(default = "default_trade_alpha_c_min_abs_return")]
    pub alpha_c_min_abs_return: f64,
    #[serde(default = "default_trade_alpha_c_align_bonus")]
    pub alpha_c_align_bonus: f64,
    #[serde(default = "default_trade_alpha_c_conflict_penalty")]
    pub alpha_c_conflict_penalty: f64,
    #[serde(default = "default_trade_alpha_c_block_b_on_conflict")]
    pub alpha_c_block_b_on_conflict: bool,
    #[serde(default = "default_trade_rank_top_k_a")]
    pub rank_top_k_a: usize,
    #[serde(default = "default_trade_rank_top_k_b")]
    pub rank_top_k_b: usize,
    #[serde(default = "default_trade_rank_w_edge")]
    pub rank_w_edge: f64,
    #[serde(default = "default_trade_rank_w_driver")]
    pub rank_w_driver: f64,
    #[serde(default = "default_trade_rank_w_stale")]
    pub rank_w_stale: f64,
    #[serde(default = "default_trade_rank_w_cost")]
    pub rank_w_cost: f64,
    #[serde(default = "default_trade_rank_w_bookshape")]
    pub rank_w_bookshape: f64,
    #[serde(default = "default_trade_rank_ofi_scale")]
    pub rank_ofi_scale: f64,
    #[serde(default = "default_trade_strategy_cap_a")]
    pub strategy_cap_a: f64,
    #[serde(default = "default_trade_strategy_cap_b")]
    pub strategy_cap_b: f64,
    #[serde(default = "default_trade_strategy_cap_c")]
    pub strategy_cap_c: f64,
    #[serde(default = "default_trade_per_trade_cap_a")]
    pub per_trade_cap_a: f64,
    #[serde(default = "default_trade_per_trade_cap_b")]
    pub per_trade_cap_b: f64,
    #[serde(default = "default_trade_per_trade_cap_c")]
    pub per_trade_cap_c: f64,
    #[serde(default = "default_trade_follow_min_corr")]
    pub follow_min_corr: f64,
    #[serde(default = "default_trade_follow_min_lag_conf")]
    pub follow_min_lag_conf: f64,
    #[serde(default = "default_trade_follow_min_range")]
    pub follow_min_range: f64,
    #[serde(default = "default_trade_follow_corr_min_samples")]
    pub follow_corr_min_samples: usize,
    #[serde(default = "default_trade_follow_corr_min_range")]
    pub follow_corr_min_range: f64,
}

impl Default for TradeConfig {
    fn default() -> Self {
        TradeConfig {
            enabled: default_trade_enabled(),
            dry_run: default_trade_dry_run(),
            engine_shards: default_trade_engine_shards(),
            queue_capacity: default_trade_queue_capacity(),
            order_executor_concurrency: default_trade_order_executor_concurrency(),
            opinion_account_file: default_trade_opinion_account_file(),
            opinion_account_id: default_trade_opinion_account_id(),
            opinion_private_base: default_trade_opinion_private_base(),
            opinion_prefetch_base: default_trade_opinion_prefetch_base(),
            opinion_prefetch_interval_ms: default_trade_opinion_prefetch_interval_ms(),
            opinion_check_approval: default_trade_opinion_check_approval(),
            opinion_enable_trading_on_startup: default_trade_opinion_enable_trading_on_startup(),
            opinion_enable_retry_ms: default_trade_opinion_enable_retry_ms(),
            opinion_force_eoa_orders: default_trade_opinion_force_eoa_orders(),
            order_poll_interval_ms: default_trade_order_poll_interval_ms(),
            order_client_prefix: default_trade_order_client_prefix(),
            ws_user_channels_enabled: default_trade_ws_user_channels_enabled(),
            ws_user_channel_stale_ms: default_trade_ws_user_channel_stale_ms(),
            post_only: default_trade_post_only(),
            entry_ttl_ms: default_trade_entry_ttl_ms(),
            alpha_a_entry_ttl_ms: default_trade_alpha_a_entry_ttl_ms(),
            alpha_a_entry_ttl_frac: default_trade_alpha_a_entry_ttl_frac(),
            alpha_a_entry_ttl_cap_ms: default_trade_alpha_a_entry_ttl_cap_ms(),
            alpha_b_entry_ttl_frac: default_trade_alpha_b_entry_ttl_frac(),
            alpha_b_entry_ttl_cap_ms: default_trade_alpha_b_entry_ttl_cap_ms(),
            exit_ttl_ms: default_trade_exit_ttl_ms(),
            alpha_source: default_trade_alpha_source(),
            alpha_log_enabled: default_trade_alpha_log_enabled(),
            alpha_log_jsonl_path: default_trade_alpha_log_jsonl_path(),
            alpha_log_csv_path: default_trade_alpha_log_csv_path(),
            alpha_log_interval_sec: default_trade_alpha_log_interval_sec(),
            alpha_min: default_trade_alpha_min(),
            move_min: default_trade_move_min(),
            mom_min: default_trade_mom_min(),
            edge_min: default_trade_edge_min(),
            book_decay_block: default_trade_book_decay_block(),
            book_support_block: default_trade_book_support_block(),
            book_decay_scale: default_trade_book_decay_scale(),
            book_support_scale: default_trade_book_support_scale(),
            book_scale_min: default_trade_book_scale_min(),
            entry_fraction: default_trade_entry_fraction(),
            entry_fraction_free: default_trade_entry_fraction_free(),
            entry_pool_frac: default_trade_entry_pool_frac(),
            min_order_notional_usd: default_trade_min_order_notional_usd(),
            max_open_positions: default_trade_max_open_positions(),
            max_global_exposure_frac: default_trade_max_global_exposure_frac(),
            max_per_pair_exposure_frac: default_trade_max_per_pair_exposure_frac(),
            gas_mult: default_trade_gas_mult(),
            gas_buffer_usd: default_trade_gas_buffer_usd(),
            gas_est_usd: default_trade_gas_est_usd(),
            gas_lookback_ms: default_trade_gas_lookback_ms(),
            gas_include_cancel_allowance: default_trade_gas_include_cancel_allowance(),
            gas_cancel_allowance_factor: default_trade_gas_cancel_allowance_factor(),
            gas_emergency_pause_enabled: default_trade_gas_emergency_pause_enabled(),
            gas_emergency_max_usd: default_trade_gas_emergency_max_usd(),
            cancel_check_interval_ms: default_trade_cancel_check_interval_ms(),
            entry_cancel_shock_mom_ratio_min: default_trade_entry_cancel_shock_mom_ratio_min(),
            entry_cancel_mom_floor: default_trade_entry_cancel_mom_floor(),
            entry_cancel_shock_floor: default_trade_entry_cancel_shock_floor(),
            entry_cancel_mom_decay_min: default_trade_entry_cancel_mom_decay_min(),
            retrace_cancel: default_trade_retrace_cancel(),
            retrace_exit: default_trade_retrace_exit(),
            cooldown_sec: default_trade_cooldown_sec(),
            cooldown_after_cancel_ms: default_trade_cooldown_after_cancel_ms(),
            cooldown_after_exit_ms: default_trade_cooldown_after_exit_ms(),
            cooldown_after_postonly_reject_ms: default_trade_cooldown_after_postonly_reject_ms(),
            per_pair_min_interval_ms: default_trade_per_pair_min_interval_ms(),
            base_alpha: default_trade_base_alpha(),
            move_eps: default_trade_move_eps(),
            tick_px: default_trade_tick_px(),
            entry_tick_offset: default_trade_entry_tick_offset(),
            entry_spread_max_ticks: default_trade_entry_spread_max_ticks(),
            entry_spread_max_pct: default_trade_entry_spread_max_pct(),
            entry_spread_size_scale: default_trade_entry_spread_size_scale(),
            entry_aggr_enabled: default_trade_entry_aggr_enabled(),
            entry_aggr_shock_max: default_trade_entry_aggr_shock_max(),
            entry_aggr_shock_min: default_trade_entry_aggr_shock_min(),
            entry_aggr_shock_early_ratio: default_trade_entry_aggr_shock_early_ratio(),
            entry_aggr_gap: default_trade_entry_aggr_gap(),
            entry_aggr_default: default_trade_entry_aggr_default(),
            exit_tick_offset: default_trade_exit_tick_offset(),
            max_chase_ticks: default_trade_max_chase_ticks(),
            chase_step_ticks: default_trade_chase_step_ticks(),
            inv_k: default_trade_inv_k(),
            target_decay_per_sec: default_trade_target_decay_per_sec(),
            target_floor_gas_mult: default_trade_target_floor_gas_mult(),
            ofi_entry_min: default_trade_ofi_entry_min(),
            ofi_cancel_low: default_trade_ofi_cancel_low(),
            ofi_cancel_med: default_trade_ofi_cancel_med(),
            trade_cancel_usd: default_trade_trade_cancel_usd(),
            ask_add_usd: default_trade_ask_add_usd(),
            bid_withdraw_usd: default_trade_bid_withdraw_usd(),
            opi_mid_ret_cancel_bps: default_trade_opi_mid_ret_cancel_bps(),
            opi_spread_bps_min: default_trade_opi_spread_bps_min(),
            opi_edge_min_bps: default_trade_opi_edge_min_bps(),
            opi_imbalance_cancel: default_trade_opi_imbalance_cancel(),
            opi_bid_withdraw_usd: default_trade_opi_bid_withdraw_usd(),
            opi_ask_add_usd: default_trade_opi_ask_add_usd(),
            opi_taker_buy_cancel_usd: default_trade_opi_taker_buy_cancel_usd(),
            opi_taker_sell_cancel_usd: default_trade_opi_taker_sell_cancel_usd(),
            opi_trade_imbalance_cancel: default_trade_opi_trade_imbalance_cancel(),
            opi_cancel_staleness_ms: default_trade_opi_cancel_staleness_ms(),
            signal_staleness_ms: default_trade_signal_staleness_ms(),
            opi_snapshot_on_demand_min_interval_ms:
                default_trade_opi_snapshot_on_demand_min_interval_ms(),
            log_blocked_signals: default_trade_log_blocked_signals(),
            log_blocked_interval_ms: default_trade_log_blocked_interval_ms(),
            max_bar_gap_ratio_1m: default_trade_max_bar_gap_ratio_1m(),
            max_opi_staleness_p90_ms: default_trade_max_opi_staleness_p90_ms(),
            max_poly_latency_p99_ms: default_trade_max_poly_latency_p99_ms(),
            max_opi_latency_p99_ms: default_trade_max_opi_latency_p99_ms(),
            balance_total_usd: default_trade_balance_total_usd(),
            balance_free_usd: default_trade_balance_free_usd(),
            k_edge: default_trade_k_edge(),
            k_mom: default_trade_k_mom(),
            k_shock: default_trade_k_shock(),
            tp_a1: default_trade_tp_a1(),
            tp_a2: default_trade_tp_a2(),
            tp_a3: default_trade_tp_a3(),
            tp_a4_shock: default_trade_tp_a4_shock(),
            alpha_a_enabled: default_trade_alpha_a_enabled(),
            alpha_a_opi_stale_max_ms: default_trade_alpha_a_opi_stale_max_ms(),
            alpha_a_opi_stale_soft_start_ms: default_trade_alpha_a_opi_stale_soft_start_ms(),
            alpha_a_opi_stale_soft_tau_ms: default_trade_alpha_a_opi_stale_soft_tau_ms(),
            alpha_a_opi_stale_soft_min_scale: default_trade_alpha_a_opi_stale_soft_min_scale(),
            alpha_a_use_pair_quantile: default_trade_alpha_a_use_pair_quantile(),
            alpha_a_pair_p50_plus_ms: default_trade_alpha_a_pair_p50_plus_ms(),
            alpha_a_edge_norm_min: default_trade_alpha_a_edge_norm_min(),
            alpha_a_edge_raw_min_abs: default_trade_alpha_a_edge_raw_min_abs(),
            alpha_a_cost_mult: default_trade_alpha_a_cost_mult(),
            alpha_a_max_age_s: default_trade_alpha_a_max_age_s(),
            alpha_a_allowed_shock_types: default_trade_alpha_a_allowed_shock_types(),
            alpha_a_require_noise_zero: default_trade_alpha_a_require_noise_zero(),
            alpha_a_decay_per_sec: default_trade_alpha_a_decay_per_sec(),
            alpha_a_horizon_default_s: default_trade_alpha_a_horizon_default_s(),
            alpha_a_horizon_by_shock_type: default_trade_alpha_a_horizon_by_shock_type(),
            alpha_b_enabled: default_trade_alpha_b_enabled(),
            alpha_b_opi_stale_max_ms: default_trade_alpha_b_opi_stale_max_ms(),
            alpha_b_use_pair_quantile: default_trade_alpha_b_use_pair_quantile(),
            alpha_b_pair_p50_plus_ms: default_trade_alpha_b_pair_p50_plus_ms(),
            alpha_b_move_min: default_trade_alpha_b_move_min(),
            alpha_b_mom_min: default_trade_alpha_b_mom_min(),
            alpha_b_ofi_entry_min: default_trade_alpha_b_ofi_entry_min(),
            alpha_b_edge_norm_min: default_trade_alpha_b_edge_norm_min(),
            alpha_b_cost_mult: default_trade_alpha_b_cost_mult(),
            alpha_b_retrace_cancel_ratio: default_trade_alpha_b_retrace_cancel_ratio(),
            alpha_b_ofi_flip_cancel_usd: default_trade_alpha_b_ofi_flip_cancel_usd(),
            alpha_b_decay_per_sec: default_trade_alpha_b_decay_per_sec(),
            alpha_b_horizon_default_s: default_trade_alpha_b_horizon_default_s(),
            alpha_c_enabled: default_trade_alpha_c_enabled(),
            alpha_c_use_lag_clamp: default_trade_alpha_c_use_lag_clamp(),
            alpha_c_lookback_fixed_s: default_trade_alpha_c_lookback_fixed_s(),
            alpha_c_lookback_min_s: default_trade_alpha_c_lookback_min_s(),
            alpha_c_lookback_max_s: default_trade_alpha_c_lookback_max_s(),
            alpha_c_min_abs_return: default_trade_alpha_c_min_abs_return(),
            alpha_c_align_bonus: default_trade_alpha_c_align_bonus(),
            alpha_c_conflict_penalty: default_trade_alpha_c_conflict_penalty(),
            alpha_c_block_b_on_conflict: default_trade_alpha_c_block_b_on_conflict(),
            rank_top_k_a: default_trade_rank_top_k_a(),
            rank_top_k_b: default_trade_rank_top_k_b(),
            rank_w_edge: default_trade_rank_w_edge(),
            rank_w_driver: default_trade_rank_w_driver(),
            rank_w_stale: default_trade_rank_w_stale(),
            rank_w_cost: default_trade_rank_w_cost(),
            rank_w_bookshape: default_trade_rank_w_bookshape(),
            rank_ofi_scale: default_trade_rank_ofi_scale(),
            strategy_cap_a: default_trade_strategy_cap_a(),
            strategy_cap_b: default_trade_strategy_cap_b(),
            strategy_cap_c: default_trade_strategy_cap_c(),
            per_trade_cap_a: default_trade_per_trade_cap_a(),
            per_trade_cap_b: default_trade_per_trade_cap_b(),
            per_trade_cap_c: default_trade_per_trade_cap_c(),
            follow_min_corr: default_trade_follow_min_corr(),
            follow_min_lag_conf: default_trade_follow_min_lag_conf(),
            follow_min_range: default_trade_follow_min_range(),
            follow_corr_min_samples: default_trade_follow_corr_min_samples(),
            follow_corr_min_range: default_trade_follow_corr_min_range(),
        }
    }
}

fn default_trade_enabled() -> bool {
    false
}

fn default_trade_dry_run() -> bool {
    true
}

fn default_trade_engine_shards() -> usize {
    8
}

fn default_trade_queue_capacity() -> usize {
    8192
}

fn default_trade_order_executor_concurrency() -> usize {
    16
}

fn default_trade_opinion_account_file() -> String {
    "/data/polymarket/polymarket_opinion_bot/config/accounts.example.json".to_string()
}

fn default_trade_opinion_account_id() -> String {
    "opinion_acc".to_string()
}

fn default_trade_opinion_private_base() -> String {
    "https://proxy.opinion.trade:8443/openapi".to_string()
}

fn default_trade_opinion_prefetch_base() -> String {
    String::new()
}

fn default_trade_opinion_prefetch_interval_ms() -> u64 {
    250
}

fn default_trade_opinion_check_approval() -> bool {
    true
}

fn default_trade_opinion_enable_trading_on_startup() -> bool {
    true
}

fn default_trade_opinion_enable_retry_ms() -> i64 {
    60_000
}

fn default_trade_opinion_force_eoa_orders() -> bool {
    false
}

fn default_trade_order_poll_interval_ms() -> u64 {
    2_000
}

fn default_trade_order_client_prefix() -> String {
    "monitor".to_string()
}

fn default_trade_ws_user_channels_enabled() -> bool {
    true
}

fn default_trade_ws_user_channel_stale_ms() -> i64 {
    30_000
}

fn default_trade_post_only() -> bool {
    true
}

fn default_trade_entry_ttl_ms() -> i64 {
    6_000
}

fn default_trade_alpha_a_entry_ttl_ms() -> i64 {
    0
}

fn default_trade_alpha_a_entry_ttl_frac() -> f64 {
    0.25
}

fn default_trade_alpha_a_entry_ttl_cap_ms() -> i64 {
    3_000
}

fn default_trade_alpha_b_entry_ttl_frac() -> f64 {
    0.35
}

fn default_trade_alpha_b_entry_ttl_cap_ms() -> i64 {
    6_000
}

fn default_trade_exit_ttl_ms() -> i64 {
    15_000
}

fn default_trade_alpha_source() -> String {
    "follow_score_raw".to_string()
}

fn default_trade_alpha_log_enabled() -> bool {
    false
}

fn default_trade_alpha_log_jsonl_path() -> String {
    "/data/polymarket/records/alpha_log.jsonl".to_string()
}

fn default_trade_alpha_log_csv_path() -> String {
    "/data/polymarket/records/alpha_log.csv".to_string()
}

fn default_trade_alpha_log_interval_sec() -> i64 {
    60
}

fn default_trade_alpha_min() -> f64 {
    0.0
}

fn default_trade_move_min() -> f64 {
    0.0
}

fn default_trade_mom_min() -> f64 {
    0.0
}

fn default_trade_edge_min() -> f64 {
    0.0
}

fn default_trade_book_decay_block() -> f64 {
    0.95
}

fn default_trade_book_support_block() -> f64 {
    0.05
}

fn default_trade_book_decay_scale() -> f64 {
    0.75
}

fn default_trade_book_support_scale() -> f64 {
    0.2
}

fn default_trade_book_scale_min() -> f64 {
    0.2
}

fn default_trade_entry_fraction() -> f64 {
    1.0
}

fn default_trade_entry_fraction_free() -> f64 {
    1.0
}

fn default_trade_entry_pool_frac() -> f64 {
    0.25
}

fn default_trade_min_order_notional_usd() -> f64 {
    5.0
}

fn default_trade_max_open_positions() -> usize {
    60
}

fn default_trade_max_global_exposure_frac() -> f64 {
    0.6
}

fn default_trade_max_per_pair_exposure_frac() -> f64 {
    0.02
}

fn default_trade_gas_mult() -> f64 {
    2.5
}

fn default_trade_gas_buffer_usd() -> f64 {
    0.2
}

fn default_trade_gas_est_usd() -> f64 {
    0.0
}

fn default_trade_gas_lookback_ms() -> i64 {
    60_000
}

fn default_trade_gas_include_cancel_allowance() -> bool {
    false
}

fn default_trade_gas_cancel_allowance_factor() -> f64 {
    1.0
}

fn default_trade_gas_emergency_pause_enabled() -> bool {
    false
}

fn default_trade_gas_emergency_max_usd() -> f64 {
    0.0
}

fn default_trade_cancel_check_interval_ms() -> i64 {
    100
}

fn default_trade_entry_cancel_shock_mom_ratio_min() -> f64 {
    0.0
}

fn default_trade_entry_cancel_mom_floor() -> f64 {
    1e-6
}

fn default_trade_entry_cancel_shock_floor() -> f64 {
    1e-6
}

fn default_trade_entry_cancel_mom_decay_min() -> f64 {
    0.0
}

fn default_trade_retrace_cancel() -> f64 {
    0.4
}

fn default_trade_retrace_exit() -> f64 {
    0.5
}

fn default_trade_cooldown_sec() -> i64 {
    10
}

fn default_trade_cooldown_after_cancel_ms() -> i64 {
    15_000
}

fn default_trade_cooldown_after_exit_ms() -> i64 {
    5_000
}

fn default_trade_cooldown_after_postonly_reject_ms() -> i64 {
    5_000
}

fn default_trade_per_pair_min_interval_ms() -> i64 {
    2_000
}

fn default_trade_base_alpha() -> f64 {
    0.033
}

fn default_trade_move_eps() -> f64 {
    1e-6
}

fn default_trade_tick_px() -> f64 {
    0.0001
}

fn default_trade_entry_tick_offset() -> i64 {
    1
}

fn default_trade_entry_spread_max_ticks() -> i64 {
    0
}

fn default_trade_entry_spread_max_pct() -> f64 {
    0.05
}

fn default_trade_entry_spread_size_scale() -> f64 {
    0.0
}

fn default_trade_entry_aggr_enabled() -> bool {
    true
}

fn default_trade_entry_aggr_shock_max() -> f64 {
    1.0
}

fn default_trade_entry_aggr_shock_min() -> f64 {
    0.3
}

fn default_trade_entry_aggr_shock_early_ratio() -> f64 {
    0.3
}

fn default_trade_entry_aggr_gap() -> f64 {
    0.2
}

fn default_trade_entry_aggr_default() -> f64 {
    0.3
}

fn default_trade_exit_tick_offset() -> i64 {
    1
}

fn default_trade_max_chase_ticks() -> i64 {
    3
}

fn default_trade_chase_step_ticks() -> i64 {
    1
}

fn default_trade_inv_k() -> f64 {
    0.8
}

fn default_trade_target_decay_per_sec() -> f64 {
    0.008
}

fn default_trade_target_floor_gas_mult() -> f64 {
    1.2
}

fn default_trade_ofi_entry_min() -> f64 {
    0.0
}

fn default_trade_ofi_cancel_low() -> f64 {
    -1000.0
}

fn default_trade_ofi_cancel_med() -> f64 {
    -5000.0
}

fn default_trade_trade_cancel_usd() -> f64 {
    1000.0
}

fn default_trade_ask_add_usd() -> f64 {
    1000.0
}

fn default_trade_bid_withdraw_usd() -> f64 {
    2000.0
}

fn default_trade_opi_mid_ret_cancel_bps() -> f64 {
    5.0
}

fn default_trade_opi_spread_bps_min() -> f64 {
    1.0
}

fn default_trade_opi_edge_min_bps() -> f64 {
    0.0
}

fn default_trade_opi_imbalance_cancel() -> f64 {
    0.3
}

fn default_trade_opi_bid_withdraw_usd() -> f64 {
    2000.0
}

fn default_trade_opi_ask_add_usd() -> f64 {
    2000.0
}

fn default_trade_opi_taker_buy_cancel_usd() -> f64 {
    2000.0
}

fn default_trade_opi_taker_sell_cancel_usd() -> f64 {
    2000.0
}

fn default_trade_opi_trade_imbalance_cancel() -> f64 {
    0.6
}

fn default_trade_opi_cancel_staleness_ms() -> i64 {
    15_000
}

fn default_trade_signal_staleness_ms() -> i64 {
    10_000
}

fn default_trade_opi_snapshot_on_demand_min_interval_ms() -> i64 {
    5_000
}

fn default_trade_log_blocked_signals() -> bool {
    false
}

fn default_trade_log_blocked_interval_ms() -> i64 {
    30_000
}

fn default_trade_max_bar_gap_ratio_1m() -> f64 {
    0.80
}

fn default_trade_max_opi_staleness_p90_ms() -> i64 {
    120_000
}

fn default_trade_max_poly_latency_p99_ms() -> i64 {
    5_000
}

fn default_trade_max_opi_latency_p99_ms() -> i64 {
    120_000
}

fn default_trade_balance_total_usd() -> f64 {
    0.0
}

fn default_trade_balance_free_usd() -> f64 {
    0.0
}

fn default_trade_k_edge() -> f64 {
    0.6
}

fn default_trade_k_mom() -> f64 {
    0.4
}

fn default_trade_k_shock() -> f64 {
    0.6
}

fn default_trade_tp_a1() -> f64 {
    1.0
}

fn default_trade_tp_a2() -> f64 {
    0.5
}

fn default_trade_tp_a3() -> f64 {
    0.7
}

fn default_trade_tp_a4_shock() -> f64 {
    1.0
}

fn default_trade_alpha_a_enabled() -> bool {
    true
}

fn default_trade_alpha_a_opi_stale_max_ms() -> i64 {
    120_000
}

fn default_trade_alpha_a_opi_stale_soft_start_ms() -> i64 {
    50_000
}

fn default_trade_alpha_a_opi_stale_soft_tau_ms() -> i64 {
    30_000
}

fn default_trade_alpha_a_opi_stale_soft_min_scale() -> f64 {
    0.2
}

fn default_trade_alpha_a_use_pair_quantile() -> bool {
    true
}

fn default_trade_alpha_a_pair_p50_plus_ms() -> i64 {
    10_000
}

fn default_trade_alpha_a_edge_norm_min() -> f64 {
    0.0
}

fn default_trade_alpha_a_edge_raw_min_abs() -> f64 {
    0.0
}

fn default_trade_alpha_a_cost_mult() -> f64 {
    1.0
}

fn default_trade_alpha_a_max_age_s() -> i64 {
    120
}

fn default_trade_alpha_a_allowed_shock_types() -> Vec<String> {
    vec![
        "PRICE_VELOCITY_5S".to_string(),
        "PRICE_ACCEL_10S".to_string(),
        "RV_JUMP_5S".to_string(),
        "SWEEP".to_string(),
        "OFI_SPIKE_10S".to_string(),
        "LARGE_TRADE".to_string(),
    ]
}

fn default_trade_alpha_a_require_noise_zero() -> bool {
    true
}

fn default_trade_alpha_a_decay_per_sec() -> f64 {
    0.015
}

fn default_trade_alpha_a_horizon_default_s() -> i64 {
    60
}

fn default_trade_alpha_a_horizon_by_shock_type() -> HashMap<String, i64> {
    let mut map = HashMap::new();
    map.insert("PRICE_VELOCITY_5S".to_string(), 30);
    map.insert("PRICE_ACCEL_10S".to_string(), 60);
    map.insert("RV_JUMP_5S".to_string(), 60);
    map.insert("SWEEP".to_string(), 30);
    map.insert("OFI_SPIKE_10S".to_string(), 30);
    map.insert("LARGE_TRADE".to_string(), 30);
    map
}

fn default_trade_alpha_b_enabled() -> bool {
    true
}

fn default_trade_alpha_b_opi_stale_max_ms() -> i64 {
    20_000
}

fn default_trade_alpha_b_use_pair_quantile() -> bool {
    true
}

fn default_trade_alpha_b_pair_p50_plus_ms() -> i64 {
    5_000
}

fn default_trade_alpha_b_move_min() -> f64 {
    0.0015
}

fn default_trade_alpha_b_mom_min() -> f64 {
    0.0006
}

fn default_trade_alpha_b_ofi_entry_min() -> f64 {
    1000.0
}

fn default_trade_alpha_b_edge_norm_min() -> f64 {
    0.0030
}

fn default_trade_alpha_b_cost_mult() -> f64 {
    2.0
}

fn default_trade_alpha_b_retrace_cancel_ratio() -> f64 {
    0.40
}

fn default_trade_alpha_b_ofi_flip_cancel_usd() -> f64 {
    1000.0
}

fn default_trade_alpha_b_decay_per_sec() -> f64 {
    0.008
}

fn default_trade_alpha_b_horizon_default_s() -> i64 {
    60
}

fn default_trade_alpha_c_enabled() -> bool {
    true
}

fn default_trade_alpha_c_use_lag_clamp() -> bool {
    true
}

fn default_trade_alpha_c_lookback_fixed_s() -> i64 {
    30
}

fn default_trade_alpha_c_lookback_min_s() -> i64 {
    10
}

fn default_trade_alpha_c_lookback_max_s() -> i64 {
    60
}

fn default_trade_alpha_c_min_abs_return() -> f64 {
    0.0008
}

fn default_trade_alpha_c_align_bonus() -> f64 {
    0.20
}

fn default_trade_alpha_c_conflict_penalty() -> f64 {
    0.30
}

fn default_trade_alpha_c_block_b_on_conflict() -> bool {
    true
}

fn default_trade_rank_top_k_a() -> usize {
    20
}

fn default_trade_rank_top_k_b() -> usize {
    30
}

fn default_trade_rank_w_edge() -> f64 {
    1.0
}

fn default_trade_rank_w_driver() -> f64 {
    0.8
}

fn default_trade_rank_w_stale() -> f64 {
    0.6
}

fn default_trade_rank_w_cost() -> f64 {
    0.4
}

fn default_trade_rank_w_bookshape() -> f64 {
    0.3
}

fn default_trade_rank_ofi_scale() -> f64 {
    1000.0
}

fn default_trade_strategy_cap_a() -> f64 {
    0.10
}

fn default_trade_strategy_cap_b() -> f64 {
    0.05
}

fn default_trade_strategy_cap_c() -> f64 {
    0.01
}

fn default_trade_per_trade_cap_a() -> f64 {
    0.02
}

fn default_trade_per_trade_cap_b() -> f64 {
    0.01
}

fn default_trade_per_trade_cap_c() -> f64 {
    0.002
}

fn default_trade_follow_min_corr() -> f64 {
    0.80
}

fn default_trade_follow_min_lag_conf() -> f64 {
    0.25
}

fn default_trade_follow_min_range() -> f64 {
    0.02
}

fn default_trade_follow_corr_min_samples() -> usize {
    30
}

fn default_trade_follow_corr_min_range() -> f64 {
    0.02
}

#[derive(Debug, Deserialize)]
struct DiscoveryConfigFile {
    #[serde(default)]
    database_path: Option<String>,
    #[serde(default)]
    opinion: Option<DiscoveryOpinionConfig>,
}

#[derive(Debug, Deserialize)]
struct DiscoveryOpinionConfig {
    #[serde(default)]
    api_key: Option<String>,
    #[serde(default)]
    base_url: Option<String>,
}

struct DiscoveryConfigParsed {
    database_path: Option<String>,
    opinion_api_key: Option<String>,
    opinion_base_url: Option<String>,
}

fn read_discovery_config(path: &str) -> anyhow::Result<DiscoveryConfigParsed> {
    if !Path::new(path).exists() {
        return Ok(DiscoveryConfigParsed {
            database_path: None,
            opinion_api_key: None,
            opinion_base_url: None,
        });
    }
    let raw = fs::read_to_string(path)?;
    let parsed: DiscoveryConfigFile = serde_yaml::from_str(&raw)?;
    let opinion_api_key = parsed.opinion.as_ref().and_then(|o| o.api_key.clone());
    let opinion_base_url = parsed.opinion.as_ref().and_then(|o| o.base_url.clone());
    Ok(DiscoveryConfigParsed {
        database_path: parsed.database_path,
        opinion_api_key,
        opinion_base_url: opinion_base_url.map(|value| normalize_opinion_base(&value)),
    })
}

fn normalize_opinion_base(value: &str) -> String {
    let trimmed = value.trim_end_matches('/');
    if trimmed.ends_with("/market") {
        return trimmed.trim_end_matches("/market").to_string();
    }
    trimmed.to_string()
}

#[derive(Debug, Deserialize)]
struct AccountsFile {
    accounts: Vec<AccountEntry>,
}

#[derive(Debug, Deserialize)]
struct AccountEntry {
    exchange: String,
    api_key: Option<String>,
}

fn read_accounts_key(path: &str) -> Option<String> {
    if !Path::new(path).exists() {
        return None;
    }
    let raw = fs::read_to_string(path).ok()?;
    let parsed: AccountsFile = serde_json::from_str(&raw).ok()?;
    for account in parsed.accounts {
        if account.exchange.eq_ignore_ascii_case("Opinion") {
            if let Some(key) = account.api_key {
                let key = normalize_key(&key);
                if !key.is_empty() {
                    return Some(key);
                }
            }
        }
    }
    None
}

fn normalize_key(value: &str) -> String {
    value.trim().to_string()
}
