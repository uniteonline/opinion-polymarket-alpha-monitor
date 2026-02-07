use crate::clock::ClockOffset;
use crate::db_queue::{try_send_db, DbMessage, DbSender};
use crate::dispatcher::{DispatcherMessage, DISPATCH_QUEUE_CAPACITY};
use crate::health::{SharedHealth, WsState};
use crate::mem_stats;
use crate::models::{
    BookSide, DeltaType, Event, EventFlags, EventKind, EventPayload, ExchangeTsSource, TokenSide,
    Venue,
};
use crate::rest::opinion::SnapshotCommand;
use crate::token_registry::{token_key_opinion, RegistryState};
use crate::trade::{OpinionUserOrderUpdate, OpinionUserTradeRecord, TradeMessage};
use crate::time_utils::now_ts_ms;
use futures_util::{SinkExt, StreamExt};
use http::Uri;
use serde_json::Value;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Instant;
use tokio::sync::{mpsc, Mutex as AsyncMutex, RwLock};
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::client::ClientRequestBuilder;
use tokio_tungstenite::tungstenite::Error as WsError;
use tracing::{debug, info, warn};

static FIRST_OPI_EVENT: AtomicBool = AtomicBool::new(false);
static NON_MARKET_COUNTER: AtomicUsize = AtomicUsize::new(0);
static MISSING_OPI_TOKEN_SIDE: OnceLock<StdMutex<HashSet<String>>> = OnceLock::new();
static LAST_OPI_GAPFILL_TRIGGER_MS: AtomicI64 = AtomicI64::new(0);
const OPI_WS_TS_LOG_INTERVAL_MS: i64 = 60_000;
const OPI_WS_AGE_LOG_INTERVAL_MS: i64 = 60_000;
const OPI_WS_AGE_WARN_MS: i64 = 5_000;
const OPI_WS_DISPATCH_WARN_MS: u128 = 200;
const OPI_WS_SNAPSHOT_BACKPRESSURE_LOG_MS: i64 = 60_000;
const OPI_WS_AGE_SUMMARY_INTERVAL_MS: i64 = 60_000;
static OPI_WS_TS_LOG_CACHE: OnceLock<StdMutex<HashMap<String, i64>>> = OnceLock::new();
static OPI_WS_AGE_LOG_CACHE: OnceLock<StdMutex<HashMap<String, i64>>> = OnceLock::new();
static OPI_WS_SNAPSHOT_BACKPRESSURE_CACHE: OnceLock<StdMutex<HashMap<String, i64>>> = OnceLock::new();

#[derive(Default)]
struct WsAgeStats {
    conn_start_ms: i64,
    window_start_ms: i64,
    total_count: u64,
    warn_count: u64,
    age_sum_ms: u64,
    max_age_ms: i64,
    first_msg_logged: bool,
    first_warn_logged: bool,
}

async fn log_opinion_ws_health(health: &SharedHealth, reason: &str) {
    let snapshot = {
        let state = health.lock().await;
        state.snapshot(Venue::Opinion, now_ts_ms() / 1000)
    };
    warn!(
        "opinion ws health reason={} last_msg_age_ms={:?} reconnect_count={} ws_state={}",
        reason,
        snapshot.last_msg_age_ms,
        snapshot.reconnect_count,
        snapshot.ws_state
    );
}

fn log_opinion_ws_reconnect(
    reason: &str,
    last_msg_ts_ms: i64,
    last_market_msg_ts_ms: i64,
    backoff_ms: u64,
    error_count: u64,
    error_window_ms: i64,
) {
    let now_ms = now_ts_ms();
    let last_msg_age_ms = now_ms.saturating_sub(last_msg_ts_ms);
    let last_market_msg_age_ms = now_ms.saturating_sub(last_market_msg_ts_ms);
    warn!(
        "opinion ws reconnect reason={} last_msg_age_ms={} last_market_msg_age_ms={} backoff_ms={} error_count={} error_window_ms={}",
        reason,
        last_msg_age_ms,
        last_market_msg_age_ms,
        backoff_ms,
        error_count,
        error_window_ms
    );
}

#[derive(Default)]
struct OpiWsStats {
    total: u64,
    depth: u64,
    last_price: u64,
    last_price_changed: u64,
    last_trade: u64,
    last_trade_changed: u64,
    order_update: u64,
    trade_record: u64,
    non_market: u64,
    msg_type_missing: u64,
    non_json: u64,
}

pub async fn run_opinion_ws(
    base_url: String,
    api_key: String,
    market_ids: Vec<String>,
    registry: Arc<RwLock<RegistryState>>,
    clock_offset: Arc<ClockOffset>,
    dispatcher: mpsc::Sender<DispatcherMessage>,
    trade_sender: Option<mpsc::Sender<TradeMessage>>,
    db_sender: DbSender,
    health: SharedHealth,
    snapshot_trigger: mpsc::Sender<SnapshotCommand>,
    heartbeat_interval_sec: u64,
    stale_reconnect_ms: i64,
    hard_clear_ms: i64,
    error_reconnect_window_ms: i64,
    error_reconnect_threshold: u64,
    opinion_ts_max_skew_ms: i64,
) {
    let mut backoff_ms = 1000u64;
    let (base_url, had_path, had_query) = normalize_opinion_ws_base(&base_url);
    if had_path || had_query {
        warn!(
            "opinion ws base_url normalized url={} had_path={} had_query={}",
            redact_url(&base_url, &api_key),
            had_path,
            had_query
        );
    }
    let url = format!("{}/?apikey={}", base_url, api_key);
    let mut has_connected = false;
    loop {
        {
            let mut state = health.lock().await;
            state.set_state(Venue::Opinion, WsState::Reconnecting);
        }
        let request = build_opinion_ws_request(&url, &api_key);
        let connect = connect_async(request).await;
        let (mut ws, _) = match connect {
            Ok(value) => value,
            Err(err) => {
                log_opinion_ws_connect_error(&err, &url, &api_key);
                log_opinion_ws_health(&health, "connect_failed").await;
                try_trigger_opinion_gapfill(
                    &snapshot_trigger,
                    "connect_failed",
                    hard_clear_ms,
                );
                let mut state = health.lock().await;
                state.record_reconnect(Venue::Opinion);
                state.set_state(Venue::Opinion, WsState::Down);
                sleep_with_jitter(backoff_ms).await;
                backoff_ms = (backoff_ms * 2).min(30_000);
                continue;
            }
        };
        {
            let mut state = health.lock().await;
            state.set_state(Venue::Opinion, WsState::Up);
        }
        let conn_start_ms = Arc::new(AtomicI64::new(now_ts_ms()));
        let ws_age_stats = Arc::new(AsyncMutex::new(WsAgeStats {
            conn_start_ms: conn_start_ms.load(Ordering::Relaxed),
            window_start_ms: now_ts_ms(),
            total_count: 0,
            warn_count: 0,
            age_sum_ms: 0,
            max_age_ms: 0,
            first_msg_logged: false,
            first_warn_logged: false,
        }));
        info!(
            "opinion ws connected url={} markets_count={}",
            url,
            market_ids.len()
        );
        mem_stats::snapshot("after_opi_ws_connected");

        let non_numeric_market_ids = market_ids
            .iter()
            .filter(|id| id.parse::<i64>().is_err())
            .count();
        if non_numeric_market_ids > 0 {
            warn!(
                "opinion ws market_ids non_numeric_count={} using rootMarketId",
                non_numeric_market_ids
            );
        }

        for market_id in &market_ids {
            let market_id_num = market_id.parse::<i64>().ok();
            for channel in [
                "market.depth.diff",
                "market.last.price",
                "market.last.trade",
                "trade.order.update",
                "trade.record.new",
            ] {
                let payload = if let Some(market_id_num) = market_id_num {
                    serde_json::json!({
                        "action": "SUBSCRIBE",
                        "channel": channel,
                        "marketId": market_id_num,
                    })
                } else {
                    serde_json::json!({
                        "action": "SUBSCRIBE",
                        "channel": channel,
                        "rootMarketId": market_id,
                    })
                };
                if let Err(err) = ws
                    .send(tokio_tungstenite::tungstenite::Message::Text(
                        payload.to_string(),
                    ))
                    .await
                {
                    warn!(
                        "opinion ws subscribe failed market_id={} channel={} err={}",
                        market_id, channel, err
                    );
                }
            }
        }
        info!(
            "opinion ws subscribed markets_count={} channels_per_market={}",
            market_ids.len(),
            5
        );
        log_opinion_subscription_coverage(&market_ids, &registry).await;
        mem_stats::snapshot("after_opi_ws_subscribe");
        let refresh_reason = if has_connected {
            "ws_reconnect"
        } else {
            "ws_connect"
        };
        if snapshot_trigger
            .try_send(SnapshotCommand::TriggerAll {
                reason: refresh_reason.to_string(),
            })
            .is_ok()
        {
            info!("opinion_rest refresh triggered source={}", refresh_reason);
        }
        has_connected = true;

        let mut heartbeat =
            tokio::time::interval(std::time::Duration::from_secs(heartbeat_interval_sec));
        let mut stats_tick = tokio::time::interval(std::time::Duration::from_secs(60));
        stats_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        stats_tick.tick().await;
        let mut ws_stats = OpiWsStats::default();
        let (subscribed_unique, subscribed_numeric) = summarize_market_ids(&market_ids);
        let mut last_price_by_token: HashMap<String, f64> = HashMap::new();
        let price_eps = 1e-6_f64;
        let mut last_msg_ts_ms = now_ts_ms();
        let mut last_market_msg_ts_ms = last_msg_ts_ms;
        let mut error_window_start_ms = now_ts_ms();
        let mut error_count: u64 = 0;

        'ws_loop: loop {
            tokio::select! {
                _ = heartbeat.tick() => {
                    let payload = serde_json::json!({"action": "HEARTBEAT"});
                    match ws
                        .send(tokio_tungstenite::tungstenite::Message::Text(
                            payload.to_string(),
                        ))
                        .await
                    {
                        Ok(_) => {
                            let mut state = health.lock().await;
                            state.record_heartbeat_sent(Venue::Opinion);
                        }
                        Err(err) => {
                            warn!("opinion ws heartbeat send failed err={}", err);
                            log_opinion_ws_reconnect(
                                "heartbeat_send_failed",
                                last_msg_ts_ms,
                                last_market_msg_ts_ms,
                                backoff_ms,
                                0,
                                error_reconnect_window_ms,
                            );
                            let mut state = health.lock().await;
                            state.record_heartbeat_fail(Venue::Opinion);
                            state.record_reconnect(Venue::Opinion);
                            break;
                        }
                    }
                    if stale_reconnect_ms > 0 || hard_clear_ms > 0 {
                        let now_ms = now_ts_ms();
                        let age_ms = now_ms.saturating_sub(last_msg_ts_ms);
                        let market_age_ms = now_ms.saturating_sub(last_market_msg_ts_ms);
                        if hard_clear_ms > 0 && market_age_ms > hard_clear_ms {
                            try_trigger_opinion_gapfill(
                                &snapshot_trigger,
                                "market_silent_hard_clear",
                                hard_clear_ms,
                            );
                        }
                        if stale_reconnect_ms > 0 && age_ms > stale_reconnect_ms {
                            warn!(
                                "opinion ws stale reconnect age_ms={} threshold_ms={}",
                                age_ms, stale_reconnect_ms
                            );
                            log_opinion_ws_reconnect(
                                "stale_reconnect",
                                last_msg_ts_ms,
                                last_market_msg_ts_ms,
                                backoff_ms,
                                0,
                                error_reconnect_window_ms,
                            );
                            log_opinion_ws_health(&health, "stale_reconnect").await;
                            try_trigger_opinion_gapfill(
                                &snapshot_trigger,
                                "stale_reconnect",
                                hard_clear_ms,
                            );
                            let mut state = health.lock().await;
                            state.record_reconnect(Venue::Opinion);
                            break;
                        }
                    }
                }
                _ = stats_tick.tick() => {
                    let now_ms = now_ts_ms();
                    let last_msg_age_ms = now_ms.saturating_sub(last_msg_ts_ms);
                    let last_market_msg_age_ms = now_ms.saturating_sub(last_market_msg_ts_ms);
                    let dispatch_remaining = dispatcher.capacity();
                    let dispatch_used = DISPATCH_QUEUE_CAPACITY.saturating_sub(dispatch_remaining);
                    if ws_stats.total > 0 || ws_stats.non_market > 0 || ws_stats.non_json > 0 || ws_stats.msg_type_missing > 0 || ws_stats.order_update > 0 || ws_stats.trade_record > 0 {
                        info!(
                            "opinion ws msg_stats window_s=60 total={} depth={} last_price={} last_price_changed={} last_trade={} last_trade_changed={} order_update={} trade_record={} non_market={} msg_type_missing={} non_json={} subscribed_unique={} subscribed_numeric={} last_msg_age_ms={} last_market_msg_age_ms={} dispatch_used={} dispatch_capacity={}",
                            ws_stats.total,
                            ws_stats.depth,
                            ws_stats.last_price,
                            ws_stats.last_price_changed,
                            ws_stats.last_trade,
                            ws_stats.last_trade_changed,
                            ws_stats.order_update,
                            ws_stats.trade_record,
                            ws_stats.non_market,
                            ws_stats.msg_type_missing,
                            ws_stats.non_json,
                            subscribed_unique,
                            subscribed_numeric,
                            last_msg_age_ms,
                            last_market_msg_age_ms,
                            dispatch_used,
                            DISPATCH_QUEUE_CAPACITY,
                        );
                    } else {
                        warn!(
                            "opinion ws msg_stats window_s=60 total=0 last_msg_age_ms={} last_market_msg_age_ms={} dispatch_used={} dispatch_capacity={}",
                            last_msg_age_ms,
                            last_market_msg_age_ms,
                            dispatch_used,
                            DISPATCH_QUEUE_CAPACITY
                        );
                    }
                    ws_stats = OpiWsStats::default();
                }
                msg = ws.next() => {
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(err)) => {
                            warn!("opinion ws recv error err={}", err);
                            log_opinion_ws_reconnect(
                                "recv_error",
                                last_msg_ts_ms,
                                last_market_msg_ts_ms,
                                backoff_ms,
                                0,
                                error_reconnect_window_ms,
                            );
                            log_opinion_ws_health(&health, "recv_error").await;
                            try_trigger_opinion_gapfill(
                                &snapshot_trigger,
                                "recv_error",
                                hard_clear_ms,
                            );
                            let mut state = health.lock().await;
                            state.record_reconnect(Venue::Opinion);
                            break;
                        }
                        None => {
                            warn!("opinion ws stream ended");
                            log_opinion_ws_reconnect(
                                "stream_end",
                                last_msg_ts_ms,
                                last_market_msg_ts_ms,
                                backoff_ms,
                                0,
                                error_reconnect_window_ms,
                            );
                            try_trigger_opinion_gapfill(
                                &snapshot_trigger,
                                "stream_end",
                                hard_clear_ms,
                            );
                            let mut state = health.lock().await;
                            state.record_reconnect(Venue::Opinion);
                            break;
                        }
                    };
                    let payload_bytes = match msg {
                        tokio_tungstenite::tungstenite::Message::Close(frame) => {
                            if let Some(frame) = frame {
                                warn!(
                                    "opinion ws closed code={:?} reason={}",
                                    frame.code, frame.reason
                                );
                            } else {
                                warn!("opinion ws closed");
                            }
                            log_opinion_ws_reconnect(
                                "ws_closed",
                                last_msg_ts_ms,
                                last_market_msg_ts_ms,
                                backoff_ms,
                                0,
                                error_reconnect_window_ms,
                            );
                            try_trigger_opinion_gapfill(
                                &snapshot_trigger,
                                "ws_closed",
                                hard_clear_ms,
                            );
                            let mut state = health.lock().await;
                            state.record_reconnect(Venue::Opinion);
                            break;
                        }
                        tokio_tungstenite::tungstenite::Message::Ping(_)
                        | tokio_tungstenite::tungstenite::Message::Pong(_) => {
                            last_msg_ts_ms = now_ts_ms();
                            continue;
                        }
                        tokio_tungstenite::tungstenite::Message::Text(text) => {
                            last_msg_ts_ms = now_ts_ms();
                            text.into_bytes()
                        }
                        tokio_tungstenite::tungstenite::Message::Binary(bytes) => {
                            last_msg_ts_ms = now_ts_ms();
                            bytes
                        }
                        _ => {
                            continue;
                        }
                    };
                    let payload: Value = match serde_json::from_slice(&payload_bytes) {
                        Ok(value) => value,
                        Err(_) => {
                            ws_stats.non_json += 1;
                            info!(
                                "opinion ws non_json payload bytes={}",
                                truncate_bytes_lossy(&payload_bytes, 200)
                            );
                            continue;
                        }
                    };
                    let payloads = match payload {
                        Value::Array(items) => items,
                        value => vec![value],
                    };
                    let mut force_reconnect = false;
                    for item in payloads {
                        let msg_type = extract_msg_type_any(&item).map(|value| value.to_string());
                        let msg_type = match msg_type {
                            Some(value) => value,
                            None => {
                                if should_reconnect_on_non_market(
                                    &item,
                                    error_reconnect_window_ms,
                                    error_reconnect_threshold,
                                    &mut error_window_start_ms,
                                    &mut error_count,
                                ) {
                                    force_reconnect = true;
                                    break;
                                }
                                log_non_market_payload(&item, None);
                                ws_stats.non_market += 1;
                                ws_stats.msg_type_missing += 1;
                                continue;
                            }
                        };
                        if is_user_msg_type(msg_type.as_str()) {
                            if backoff_ms != 1000 {
                                backoff_ms = 1000;
                            }
                            handle_user_message(
                                &trade_sender,
                                &registry,
                                msg_type.as_str(),
                                &item,
                                &mut ws_stats,
                            )
                            .await;
                            continue;
                        }
                        if !is_market_msg_type(msg_type.as_str()) {
                            if should_reconnect_on_non_market(
                                &item,
                                error_reconnect_window_ms,
                                error_reconnect_threshold,
                                &mut error_window_start_ms,
                                &mut error_count,
                            ) {
                                force_reconnect = true;
                                break;
                            }
                            log_non_market_payload(&item, Some(msg_type.as_str()));
                            ws_stats.non_market += 1;
                            continue;
                        }
                        // Successful market message: treat connection as healthy and reset backoff.
                        if backoff_ms != 1000 {
                            backoff_ms = 1000;
                        }
                        last_market_msg_ts_ms = now_ts_ms();
                        ws_stats.total += 1;
                        match msg_type.as_str() {
                            "market.depth.diff" => ws_stats.depth += 1,
                            "market.last.price" => ws_stats.last_price += 1,
                            "market.last.trade" => ws_stats.last_trade += 1,
                            _ => {}
                        }
                        if matches!(msg_type.as_str(), "market.last.price" | "market.last.trade") {
                            let data = item.get("data").unwrap_or(&item);
                            if let Some(price) = extract_price(data) {
                                if let Some(token_id) =
                                    extract_token_id(data).or_else(|| extract_token_id(&item))
                                {
                                    let changed = match last_price_by_token.get(&token_id) {
                                        Some(prev) => (price - prev).abs() > price_eps,
                                        None => true,
                                    };
                                    last_price_by_token.insert(token_id, price);
                                    if changed {
                                        match msg_type.as_str() {
                                            "market.last.price" => {
                                                ws_stats.last_price_changed += 1;
                                            }
                                            "market.last.trade" => {
                                                ws_stats.last_trade_changed += 1;
                                            }
                                            _ => {}
                                        }
                                    }
                                }
                            }
                        }
                        {
                            let mut state = health.lock().await;
                            state.record_message(Venue::Opinion);
                        }
                        handle_message(
                            item,
                            msg_type,
                            &registry,
                            &clock_offset,
                            &dispatcher,
                            &db_sender,
                            &snapshot_trigger,
                            &conn_start_ms,
                            &ws_age_stats,
                            opinion_ts_max_skew_ms,
                        )
                        .await;
                    }
                    if force_reconnect {
                        warn!(
                            "opinion ws server_error reconnect triggered count={} window_ms={}",
                            error_count, error_reconnect_window_ms
                        );
                        log_opinion_ws_reconnect(
                            "server_error_reconnect",
                            last_msg_ts_ms,
                            last_market_msg_ts_ms,
                            backoff_ms,
                            error_count,
                            error_reconnect_window_ms,
                        );
                        log_opinion_ws_health(&health, "server_error_reconnect").await;
                        try_trigger_opinion_gapfill(
                            &snapshot_trigger,
                            "server_error_reconnect",
                            hard_clear_ms,
                        );
                        let mut state = health.lock().await;
                        state.record_reconnect(Venue::Opinion);
                        break 'ws_loop;
                    }
                }
            }
        }
        sleep_with_jitter(backoff_ms).await;
        backoff_ms = (backoff_ms * 2).min(30_000);
    }
}

async fn handle_message(
    payload: Value,
    msg_type: String,
    registry: &Arc<RwLock<RegistryState>>,
    clock_offset: &Arc<ClockOffset>,
    dispatcher: &mpsc::Sender<DispatcherMessage>,
    db_sender: &DbSender,
    snapshot_trigger: &mpsc::Sender<SnapshotCommand>,
    conn_start_ms: &Arc<AtomicI64>,
    ws_age_stats: &Arc<AsyncMutex<WsAgeStats>>,
    opinion_ts_max_skew_ms: i64,
) {
    let data = payload.get("data").unwrap_or(&payload);
    let market_id = extract_market_id(data).or_else(|| extract_market_id(&payload));
    let outcome_side = extract_outcome_side(data).unwrap_or(0);
    let token_id = extract_token_id(data).or_else(|| extract_token_id(&payload));

    let mut resolved: Option<(String, TokenSide, SmallVec<[i64; 4]>)> = None;
    if let (Some(token), Some(market)) = (token_id.as_deref(), market_id.as_deref()) {
        if outcome_side > 0 {
            let mut reg = registry.write().await;
            let (token_key, mismatch, is_new, pair_ids) =
                reg.ensure_opi_token(market, outcome_side, token);
            if mismatch || is_new {
                for pair_id in &pair_ids {
                    try_send_db(
                        db_sender,
                        DbMessage::PairTokenOverride {
                            pair_id: *pair_id,
                            token_side: RegistryState::token_side_for_opi(outcome_side),
                            token_id: token.to_string(),
                        },
                        "opinion_pair_override",
                    );
                }
                let registration = crate::models::TokenRegistration {
                    token_key: token_key.clone(),
                    venue: Venue::Opinion,
                    external_token_id: Some(token.to_string()),
                    market_id: Some(market.to_string()),
                    outcome_side: Some(outcome_side),
                    token_side: RegistryState::token_side_for_opi(outcome_side),
                    pair_ids: SmallVec::new(),
                };
                try_send_db(
                    db_sender,
                    DbMessage::TokenRegistryUpsert(registration),
                    "opinion_token_registry",
                );
            }
            let token_side = RegistryState::token_side_for_opi(outcome_side);
            resolved = Some((token_key, token_side, pair_ids));
        }
    }
    if resolved.is_none() {
        if let Some(token) = token_id.as_deref() {
            let token_key = format!("opi:token:{token}");
            let reg = registry.read().await;
            let token_side = reg
                .token_sides
                .get(&token_key)
                .copied()
                .unwrap_or(TokenSide::Unknown);
            let pair_ids = reg.token_pairs.get(&token_key).cloned().unwrap_or_default();
            if token_side == TokenSide::Unknown {
                log_missing_token_side_once(
                    &token_key,
                    market_id.as_deref(),
                    outcome_side,
                    &msg_type,
                );
            }
            resolved = Some((token_key, token_side, pair_ids));
        }
    }
    if resolved.is_none() {
        if let Some(market) = market_id.as_deref() {
            if outcome_side > 0 {
                let reg = registry.read().await;
                let token_key = reg
                    .token_key_for_opi_market(market, outcome_side)
                    .unwrap_or_else(|| format!("opi:market:{}:{}", market, outcome_side));
                let token_side = RegistryState::token_side_for_opi(outcome_side);
                let pair_ids = reg
                    .opi_market_pairs
                    .get(market)
                    .cloned()
                    .unwrap_or_default();
                resolved = Some((token_key, token_side, pair_ids));
            }
        }
    }
    let (token_key, token_side, pair_ids) = match resolved {
        Some(values) => values,
        None => return,
    };

    let local_ts_ms = now_ts_ms();
    let ts_raw = extract_ts(data).or_else(|| extract_ts(&payload));
    let mut payload_ts_ms: Option<i64> = None;
    let mut flags = EventFlags::default();
    if ts_raw.is_none() {
        flags.ts_missing = true;
    }
    if let Some(raw_ts) = ts_raw {
        if let Some(norm_ts) = normalize_ts_ms(raw_ts) {
            payload_ts_ms = Some(norm_ts);
            let diff_ms = local_ts_ms - norm_ts;
            let skew_ms = diff_ms.unsigned_abs() as i64;
            if skew_ms > opinion_ts_max_skew_ms {
                flags.ts_anomaly = true;
                if should_log_ws_ts_skew(&token_key, local_ts_ms) {
                    info!(
                        "opinion_ws ts_skew token_key={} ts_raw={} ts_norm={} diff_ms={} skew_ms={} max_skew_ms={} unit_guess={}",
                        token_key,
                        raw_ts,
                        norm_ts,
                        diff_ms,
                        skew_ms,
                        opinion_ts_max_skew_ms,
                        ts_unit_hint(raw_ts),
                    );
                }
            }
            {
                let age_ms = diff_ms.max(0);
                let conn_start = conn_start_ms.load(Ordering::Relaxed);
                let conn_age_ms = local_ts_ms.saturating_sub(conn_start);
                let mut stats = ws_age_stats.lock().await;
                if !stats.first_msg_logged {
                    stats.first_msg_logged = true;
                    stats.conn_start_ms = conn_start;
                    stats.window_start_ms = local_ts_ms;
                    info!(
                        "opinion_ws msg_age_first token_key={} msg_type={} age_ms={} conn_age_ms={}",
                        token_key, msg_type, age_ms, conn_age_ms
                    );
                }
                stats.total_count += 1;
                stats.age_sum_ms = stats.age_sum_ms.saturating_add(age_ms as u64);
                if age_ms > stats.max_age_ms {
                    stats.max_age_ms = age_ms;
                }
                if age_ms >= OPI_WS_AGE_WARN_MS && !stats.first_warn_logged {
                    stats.first_warn_logged = true;
                    warn!(
                        "opinion_ws msg_age_first_high token_key={} msg_type={} age_ms={} conn_age_ms={}",
                        token_key, msg_type, age_ms, conn_age_ms
                    );
                }
                if age_ms > OPI_WS_AGE_WARN_MS {
                    stats.warn_count += 1;
                }
                if local_ts_ms.saturating_sub(stats.window_start_ms)
                    >= OPI_WS_AGE_SUMMARY_INTERVAL_MS
                {
                    let avg_ms = if stats.total_count > 0 {
                        (stats.age_sum_ms / stats.total_count) as i64
                    } else {
                        0
                    };
                    info!(
                        "opinion_ws msg_age_summary conn_age_ms={} total={} avg_ms={} max_ms={} warn_count={}",
                        conn_age_ms,
                        stats.total_count,
                        avg_ms,
                        stats.max_age_ms,
                        stats.warn_count
                    );
                    stats.window_start_ms = local_ts_ms;
                    stats.total_count = 0;
                    stats.age_sum_ms = 0;
                    stats.max_age_ms = 0;
                    stats.warn_count = 0;
                }
            }
            if diff_ms > OPI_WS_AGE_WARN_MS {
                if should_log_ws_msg_age(&token_key, local_ts_ms) {
                    let dispatch_remaining = dispatcher.capacity();
                    let dispatch_used = DISPATCH_QUEUE_CAPACITY.saturating_sub(dispatch_remaining);
                    let snapshot_remaining = snapshot_trigger.capacity();
                    warn!(
                        "opinion_ws msg_age_high token_key={} msg_type={} age_ms={} ts_raw={} ts_norm={} dispatch_used={} dispatch_capacity={} snapshot_remaining={}",
                        token_key,
                        msg_type,
                        diff_ms,
                        raw_ts,
                        norm_ts,
                        dispatch_used,
                        DISPATCH_QUEUE_CAPACITY,
                        snapshot_remaining
                    );
                }
            }
        } else {
            flags.ts_anomaly = true;
        }
    } else if should_log_ws_msg_age(&token_key, local_ts_ms) {
        warn!(
            "opinion_ws msg_ts_missing token_key={} msg_type={} local_ts_ms={}",
            token_key, msg_type, local_ts_ms
        );
    }
    let exchange_ts_ms = local_ts_ms - clock_offset.current();
    let exchange_ts_source = ExchangeTsSource::Estimated;

    let mut event = Event {
        venue: Venue::Opinion,
        token_key: token_key.clone(),
        pair_ids,
        token_side,
        kind: EventKind::Health,
        channel: Some(msg_type.clone()),
        exchange_ts_ms,
        local_ts_ms,
        exchange_ts_source,
        flags,
        payload: EventPayload::default(),
    };

    let mut parsed = true;
    let mut ws_book_update = false;
    let mut ws_last_price = false;
    match msg_type.as_str() {
        "market.depth.diff" => {
            let side = match parse_side(data.get("side").and_then(|v| v.as_str())) {
                Some(side) => side,
                None => {
                    parsed = false;
                    BookSide::Bid
                }
            };
            let price = match data.get("price").and_then(extract_f64) {
                Some(price) => price,
                None => {
                    parsed = false;
                    0.0
                }
            };
            let size = data
                .get("size")
                .or_else(|| data.get("amount"))
                .and_then(extract_f64)
                .unwrap_or(0.0);
            if parsed {
                let delta_type = if size <= 0.0 {
                    DeltaType::Cancel
                } else {
                    DeltaType::AddOrUpdate
                };
                event.kind = EventKind::BookDelta;
                event.payload.side = Some(side);
                event.payload.price = Some(price);
                event.payload.size = Some(size);
                event.payload.delta_type = Some(delta_type);
                ws_book_update = true;
            }
        }
        "market.last.price" => {
            let price = match extract_price(data) {
                Some(price) => price,
                None => {
                    parsed = false;
                    0.0
                }
            };
            if parsed {
                event.kind = EventKind::LastPrice;
                event.payload.price = Some(price);
                ws_last_price = true;
            }
        }
        "market.last.trade" => {
            let price = match extract_price(data) {
                Some(price) => price,
                None => {
                    parsed = false;
                    0.0
                }
            };
            let size = data
                .get("size")
                .or_else(|| data.get("amount"))
                .and_then(extract_f64)
                .unwrap_or(0.0);
            let side = parse_side(data.get("side").and_then(|v| v.as_str()));
            if parsed {
                event.kind = EventKind::Trade;
                event.payload.price = Some(price);
                event.payload.size = Some(size);
                event.payload.side = side;
                ws_last_price = true;
                if is_split_merge(data) {
                    event.payload.side = None;
                    event.flags.trade_side_missing = true;
                }
                if event.payload.side.is_none() {
                    event.flags.trade_side_missing = true;
                }
            }
        }
        _ => return,
    }

    if let Err(err) = snapshot_trigger.try_send(SnapshotCommand::WsActivity {
        token_key: token_key.clone(),
        last_price: ws_last_price,
        book_update: ws_book_update,
        payload_ts_ms,
    }) {
        if should_log_ws_snapshot_backpressure(&token_key, local_ts_ms) {
            let snapshot_remaining = snapshot_trigger.capacity();
            warn!(
                "opinion_ws snapshot_backpressure token_key={} msg_type={} err={:?} snapshot_remaining={}",
                token_key,
                msg_type,
                err,
                snapshot_remaining
            );
        }
    }

    log_first_event(&event);
    let send_start = Instant::now();
    let send_result = dispatcher.send(DispatcherMessage::Event(event)).await;
    let send_ms = send_start.elapsed().as_millis();
    if send_ms > OPI_WS_DISPATCH_WARN_MS {
        let dispatch_remaining = dispatcher.capacity();
        let dispatch_used = DISPATCH_QUEUE_CAPACITY.saturating_sub(dispatch_remaining);
        warn!(
            "opinion_ws dispatch_backpressure token_key={} msg_type={} send_ms={} dispatch_used={} dispatch_capacity={}",
            token_key,
            msg_type,
            send_ms,
            dispatch_used,
            DISPATCH_QUEUE_CAPACITY
        );
    }
    if let Err(err) = send_result {
        warn!(
            "opinion_ws dispatch_failed token_key={} msg_type={} err={:?}",
            token_key,
            msg_type,
            err
        );
    }
}

fn is_market_msg_type(msg_type: &str) -> bool {
    matches!(
        msg_type,
        "market.depth.diff" | "market.last.price" | "market.last.trade"
    )
}

fn is_user_msg_type(msg_type: &str) -> bool {
    matches!(msg_type, "trade.order.update" | "trade.record.new")
}

async fn handle_user_message(
    trade_sender: &Option<mpsc::Sender<TradeMessage>>,
    registry: &Arc<RwLock<RegistryState>>,
    msg_type: &str,
    item: &Value,
    ws_stats: &mut OpiWsStats,
) {
    let sender = match trade_sender {
        Some(sender) => sender,
        None => return,
    };
    let data = item.get("data").unwrap_or(item);
    let market_id = extract_market_id(data).or_else(|| extract_market_id(item));
    let outcome_side = extract_outcome_side(data)
        .or_else(|| extract_outcome_side(item))
        .unwrap_or(0);
    let market_id = match market_id {
        Some(value) => value,
        None => {
            warn!(
                "opinion ws user msg missing market_id msg_type={} payload={}",
                msg_type,
                truncate_text(&item.to_string(), 200)
            );
            return;
        }
    };
    let (token_key, token_side, pair_ids) = {
        let state = registry.read().await;
        resolve_opi_user_target(&state, &market_id, outcome_side)
    };
    if pair_ids.is_empty() {
        warn!(
            "opinion ws user msg unmapped msg_type={} market_id={} outcome_side={} token_key={}",
            msg_type, market_id, outcome_side, token_key
        );
        return;
    }
    match msg_type {
        "trade.order.update" => {
            ws_stats.order_update += 1;
            let order_id = extract_order_id_ws(data).unwrap_or_default();
            if order_id.is_empty() {
                warn!(
                    "opinion ws order_update missing order_id market_id={} outcome_side={} payload={}",
                    market_id,
                    outcome_side,
                    truncate_text(&item.to_string(), 200)
                );
                return;
            }
            let order_update_type = data
                .get("orderUpdateType")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string());
            let status = data.get("status").and_then(extract_i64);
            let side = parse_order_side(data.get("side"));
            let price = data.get("price").and_then(extract_f64);
            let shares = data.get("shares").and_then(extract_f64);
            let amount = data.get("amount").and_then(extract_f64);
            let filled_shares = data.get("filledShares").and_then(extract_f64);
            let filled_amount = data.get("filledAmount").and_then(extract_f64);
            let ts_ms = extract_user_ts_ms(data);
            for pair_id in pair_ids {
                let update = OpinionUserOrderUpdate {
                    pair_id,
                    token_side,
                    order_id: order_id.clone(),
                    order_update_type: order_update_type.clone(),
                    status,
                    side: side.clone(),
                    outcome_side: Some(outcome_side),
                    price,
                    shares,
                    amount,
                    filled_shares,
                    filled_amount,
                    ts_ms,
                    market_id: Some(market_id.clone()),
                };
                if let Err(err) = sender
                    .send(TradeMessage::OpinionOrderUpdate(update))
                    .await
                {
                    warn!(
                        "opinion ws order_update send failed pair_id={} err={}",
                        pair_id, err
                    );
                }
            }
        }
        "trade.record.new" => {
            ws_stats.trade_record += 1;
            let order_id = extract_order_id_ws(data).unwrap_or_default();
            if order_id.is_empty() {
                warn!(
                    "opinion ws trade_record missing order_id market_id={} outcome_side={} payload={}",
                    market_id,
                    outcome_side,
                    truncate_text(&item.to_string(), 200)
                );
                return;
            }
            let side = data
                .get("side")
                .and_then(|v| v.as_str())
                .map(|v| v.to_string());
            let price = data.get("price").and_then(extract_f64);
            let shares = data.get("shares").and_then(extract_f64);
            let amount = data.get("amount").and_then(extract_f64);
            let ts_ms = extract_user_ts_ms(data);
            for pair_id in pair_ids {
                let record = OpinionUserTradeRecord {
                    pair_id,
                    token_side,
                    order_id: order_id.clone(),
                    side: side.clone(),
                    outcome_side: Some(outcome_side),
                    price,
                    shares,
                    amount,
                    ts_ms,
                    market_id: Some(market_id.clone()),
                };
                if let Err(err) = sender
                    .send(TradeMessage::OpinionTradeRecord(record))
                    .await
                {
                    warn!(
                        "opinion ws trade_record send failed pair_id={} err={}",
                        pair_id, err
                    );
                }
            }
        }
        _ => {}
    }
}

fn resolve_opi_user_target(
    state: &RegistryState,
    market_id: &str,
    outcome_side: i32,
) -> (String, TokenSide, SmallVec<[i64; 4]>) {
    let token_key = state
        .token_key_for_opi_market(market_id, outcome_side)
        .unwrap_or_else(|| token_key_opinion(None, Some(market_id), outcome_side));
    let token_side = state
        .token_sides
        .get(&token_key)
        .copied()
        .unwrap_or_else(|| RegistryState::token_side_for_opi(outcome_side));
    let pairs = state
        .token_pairs
        .get(&token_key)
        .cloned()
        .unwrap_or_else(SmallVec::new);
    (token_key, token_side, pairs)
}

fn extract_order_id_ws(value: &Value) -> Option<String> {
    for key in ["orderId", "order_id", "id"] {
        if let Some(val) = value.get(key) {
            if let Some(text) = val.as_str() {
                return Some(text.to_string());
            }
        }
    }
    None
}

fn parse_order_side(value: Option<&Value>) -> Option<String> {
    let value = value?;
    if let Some(num) = extract_i64(value) {
        return match num {
            1 => Some("BUY".to_string()),
            2 => Some("SELL".to_string()),
            _ => None,
        };
    }
    if let Some(text) = value.as_str() {
        let upper = text.to_ascii_uppercase();
        if upper == "BUY" || upper == "SELL" {
            return Some(upper);
        }
    }
    None
}

fn extract_user_ts_ms(value: &Value) -> Option<i64> {
    let ts_raw = value
        .get("createdAt")
        .or_else(|| value.get("created_at"))
        .or_else(|| value.get("timestamp"))
        .or_else(|| value.get("ts"))
        .or_else(|| value.get("time"))
        .and_then(extract_i64);
    ts_raw.and_then(normalize_ts_ms)
}

fn should_reconnect_on_non_market(
    payload: &Value,
    window_ms: i64,
    threshold: u64,
    window_start_ms: &mut i64,
    counter: &mut u64,
) -> bool {
    if window_ms <= 0 || threshold == 0 {
        return false;
    }
    if !is_server_error_payload(payload) {
        return false;
    }
    let now_ms = now_ts_ms();
    if now_ms.saturating_sub(*window_start_ms) > window_ms {
        *window_start_ms = now_ms;
        *counter = 0;
    }
    *counter += 1;
    *counter >= threshold
}

fn is_server_error_payload(payload: &Value) -> bool {
    if let Some(code) = extract_code(payload) {
        if code >= 500 {
            return true;
        }
    }
    if let Some(message) = extract_message(payload) {
        if message.to_lowercase().contains("internal server error") {
            return true;
        }
    }
    false
}

fn extract_msg_type(payload: &Value) -> Option<&str> {
    payload
        .get("msgType")
        .or_else(|| payload.get("msg_type"))
        .and_then(|v| v.as_str())
}

fn extract_msg_type_any(payload: &Value) -> Option<&str> {
    extract_msg_type(payload).or_else(|| payload.get("data").and_then(extract_msg_type))
}

fn log_non_market_payload(payload: &Value, channel: Option<&str>) {
    let message = extract_message(payload);
    let code = extract_code(payload);
    if is_ws_heartbeat_or_ack(code, message.as_deref()) {
        return;
    }
    let mut warn_flag = code.map(|c| c >= 400).unwrap_or(false);
    if let Some(msg) = message.as_ref() {
        let msg_lower = msg.to_lowercase();
        if msg_lower.contains("rate") || msg_lower.contains("limit") || msg_lower.contains("error")
        {
            warn_flag = true;
        }
    }
    let payload_str = truncate_text(&payload.to_string(), 400);
    let channel = channel.unwrap_or("unknown");
    if warn_flag {
        warn!(
            "opinion ws non_market channel={} code={:?} message={:?} payload={}",
            channel, code, message, payload_str
        );
    } else {
        let count = NON_MARKET_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        if count == 1 || count % 200 == 0 {
            debug!(
                "opinion ws non_market channel={} code={:?} message={:?} payload={}",
                channel, code, message, payload_str
            );
        }
    }
}

fn is_ws_heartbeat_or_ack(code: Option<i64>, message: Option<&str>) -> bool {
    if code != Some(200) {
        return false;
    }
    let msg = match message {
        Some(text) => text.trim().to_lowercase(),
        None => return false,
    };
    msg == "heartbeat" || msg == "message processed"
}

async fn log_opinion_subscription_coverage(
    market_ids: &[String],
    registry: &Arc<RwLock<RegistryState>>,
) {
    let (subscribed_unique, subscribed_numeric) = summarize_market_ids(market_ids);
    let reg = registry.read().await;
    let registry_total = reg.opi_market_ids.len() as u64;
    let subscribed_set: std::collections::HashSet<String> = market_ids.iter().cloned().collect();
    let mut missing = Vec::new();
    for market_id in reg.opi_market_ids.iter() {
        if !subscribed_set.contains(market_id) {
            missing.push(market_id.clone());
            if missing.len() >= 3 {
                break;
            }
        }
    }
    let mut extra = Vec::new();
    for market_id in subscribed_set.iter() {
        if !reg.opi_market_ids.contains(market_id) {
            extra.push(market_id.clone());
            if extra.len() >= 3 {
                break;
            }
        }
    }
    info!(
        "opinion ws subscribe coverage registry_market_ids={} subscribed_unique={} subscribed_total={} subscribed_numeric={} subscribed_root={} missing_count={} extra_count={} missing_sample={:?} extra_sample={:?}",
        registry_total,
        subscribed_unique,
        market_ids.len(),
        subscribed_numeric,
        subscribed_unique.saturating_sub(subscribed_numeric),
        registry_total.saturating_sub(subscribed_unique),
        subscribed_unique.saturating_sub(registry_total),
        missing,
        extra
    );
}

fn summarize_market_ids(market_ids: &[String]) -> (u64, u64) {
    let mut unique = std::collections::HashSet::new();
    let mut numeric = 0u64;
    for market_id in market_ids {
        unique.insert(market_id.clone());
    }
    for market_id in unique.iter() {
        if market_id.parse::<i64>().is_ok() {
            numeric += 1;
        }
    }
    (unique.len() as u64, numeric)
}

fn extract_message(payload: &Value) -> Option<String> {
    for key in ["message", "msg", "error", "reason", "detail"] {
        if let Some(val) = payload.get(key) {
            if let Some(text) = val.as_str() {
                return Some(text.to_string());
            }
        }
    }
    None
}

fn extract_code(payload: &Value) -> Option<i64> {
    for key in ["code", "status", "status_code", "errno"] {
        if let Some(val) = payload.get(key) {
            if let Some(num) = val.as_i64() {
                return Some(num);
            }
        }
    }
    None
}

fn truncate_text(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        return text.to_string();
    }
    let mut out = text.chars().take(max_len).collect::<String>();
    out.push_str("...");
    out
}

fn truncate_bytes_lossy(bytes: &[u8], max_len: usize) -> String {
    if bytes.len() <= max_len {
        return String::from_utf8_lossy(bytes).to_string();
    }
    let mut out = String::from_utf8_lossy(&bytes[..max_len]).to_string();
    out.push_str("...");
    out
}

fn redact_url(url: &str, api_key: &str) -> String {
    if api_key.is_empty() {
        return url.to_string();
    }
    url.replace(api_key, "REDACTED")
}

fn normalize_opinion_ws_base(value: &str) -> (String, bool, bool) {
    let trimmed = value.trim();
    let mut had_query = false;
    let mut had_path = false;
    let mut base = trimmed;
    if let Some((head, _)) = base.split_once('?') {
        base = head;
        had_query = true;
    }
    base = base.trim_end_matches('/');
    if let Some(scheme_idx) = base.find("://") {
        let scheme = &base[..scheme_idx + 3];
        let rest = &base[scheme_idx + 3..];
        if let Some(slash_idx) = rest.find('/') {
            had_path = true;
            let host = &rest[..slash_idx];
            return (format!("{}{}", scheme, host), had_path, had_query);
        }
        return (base.to_string(), had_path, had_query);
    }
    if let Some(slash_idx) = base.find('/') {
        had_path = true;
        base = &base[..slash_idx];
    }
    (base.to_string(), had_path, had_query)
}

fn log_first_event(event: &Event) {
    if FIRST_OPI_EVENT
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        info!(
            "ws first event venue=opinion local_ts_ms={} channel={}",
            event.local_ts_ms,
            event.channel.as_deref().unwrap_or("unknown")
        );
        mem_stats::snapshot("after_first_opi_ws_event");
    }
}

fn log_missing_token_side_once(
    token_key: &str,
    market_id: Option<&str>,
    outcome_side: i32,
    msg_type: &str,
) {
    let cache = MISSING_OPI_TOKEN_SIDE.get_or_init(|| StdMutex::new(HashSet::new()));
    let mut cache = match cache.lock() {
        Ok(guard) => guard,
        Err(_) => return,
    };
    if !cache.insert(token_key.to_string()) {
        return;
    }
    warn!(
        "opinion ws token side missing; using UNKNOWN token_key={} market_id={:?} outcome_side={} msg_type={}",
        token_key,
        market_id,
        outcome_side,
        msg_type
    );
}

fn log_opinion_ws_connect_error(err: &WsError, url: &str, api_key: &str) {
    match err {
        WsError::Http(resp) => {
            let status = resp.status();
            let content_type = resp
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok());
            let body_len = resp.body().as_ref().map(|body| body.len()).unwrap_or(0);
            let body_prefix = resp.body().as_ref().map(|body| {
                let mut text = String::from_utf8_lossy(body).into_owned();
                text = text.replace('\n', " ").replace('\r', " ");
                truncate_text(&text, 1024)
            });
            warn!(
                "opinion ws connect failed request_url={} status={} content_type={:?} body_len={} body_prefix={:?} err={}",
                redact_url(url, api_key),
                status,
                content_type,
                body_len,
                body_prefix,
                err
            );
        }
        _ => {
            warn!(
                "opinion ws connect failed request_url={} err={}",
                redact_url(url, api_key),
                err
            );
        }
    }
}

fn try_trigger_opinion_gapfill(
    snapshot_trigger: &mpsc::Sender<SnapshotCommand>,
    reason: &str,
    cutoff_ms: i64,
) {
    if cutoff_ms <= 0 {
        return;
    }
    let now_ms = now_ts_ms();
    let last_ms = LAST_OPI_GAPFILL_TRIGGER_MS.load(Ordering::Relaxed);
    if last_ms > 0 && now_ms.saturating_sub(last_ms) < 30_000 {
        return;
    }
    if LAST_OPI_GAPFILL_TRIGGER_MS
        .compare_exchange(last_ms, now_ms, Ordering::Relaxed, Ordering::Relaxed)
        .is_err()
    {
        return;
    }
    let _ = snapshot_trigger.try_send(SnapshotCommand::TriggerInactive {
        reason: reason.to_string(),
        cutoff_ms,
    });
    info!(
        "opinion_rest gapfill request sent reason={} cutoff_ms={}",
        reason, cutoff_ms
    );
}

async fn sleep_with_jitter(base_ms: u64) {
    let jitter = jitter_ms(base_ms);
    tokio::time::sleep(std::time::Duration::from_millis(
        base_ms.saturating_add(jitter),
    ))
    .await;
}

fn jitter_ms(base_ms: u64) -> u64 {
    let max_jitter = 250u64.min((base_ms / 4).max(1));
    let now = now_ts_ms() as u64;
    now % (max_jitter + 1)
}

fn build_opinion_ws_request(url: &str, api_key: &str) -> ClientRequestBuilder {
    let uri = match url.parse::<Uri>() {
        Ok(uri) => uri,
        Err(err) => {
            warn!(
                "opinion ws url parse failed url={} err={}",
                redact_url(url, api_key),
                err
            );
            Uri::from_static("wss://ws.opinion.trade/?apikey=invalid")
        }
    };
    ClientRequestBuilder::new(uri)
        .with_header("Origin", "https://opinion.trade")
        .with_header("User-Agent", "monitor/1.0")
        .with_header("Accept", "application/json")
}

fn extract_market_id(value: &Value) -> Option<String> {
    for key in ["marketId", "market_id", "rootMarketId", "root_market_id"] {
        if let Some(val) = value.get(key) {
            if let Some(text) = val.as_str() {
                return Some(text.to_string());
            }
            if let Some(num) = val.as_i64() {
                return Some(num.to_string());
            }
        }
    }
    None
}

fn extract_token_id(value: &Value) -> Option<String> {
    for key in ["tokenId", "token_id"] {
        if let Some(val) = value.get(key) {
            if let Some(text) = val.as_str() {
                return Some(text.to_string());
            }
            if let Some(num) = val.as_i64() {
                return Some(num.to_string());
            }
            if let Some(num) = val.as_u64() {
                return Some(num.to_string());
            }
        }
    }
    None
}

fn extract_outcome_side(value: &Value) -> Option<i32> {
    for key in ["outcomeSide", "outcome_side"] {
        if let Some(val) = value.get(key) {
            if let Some(num) = extract_i64(val) {
                return Some(num as i32);
            }
            if let Some(text) = val.as_str() {
                let lower = text.trim().to_lowercase();
                if lower == "yes" {
                    return Some(1);
                }
                if lower == "no" {
                    return Some(2);
                }
                if let Ok(num) = lower.parse::<i32>() {
                    return Some(num);
                }
            }
        }
    }
    None
}

fn extract_ts(value: &Value) -> Option<i64> {
    for key in ["timestamp", "ts", "time", "eventTime", "event_time"] {
        if let Some(val) = value.get(key) {
            if let Some(ts) = extract_i64(val) {
                return Some(ts);
            }
        }
    }
    None
}

fn normalize_ts_ms(ts_raw: i64) -> Option<i64> {
    if ts_raw < 0 {
        return None;
    }
    if ts_raw < 100_000_000_000 {
        return Some(ts_raw * 1000);
    }
    if ts_raw < 100_000_000_000_000 {
        return Some(ts_raw);
    }
    if ts_raw < 100_000_000_000_000_000 {
        return Some(ts_raw / 1000);
    }
    Some(ts_raw / 1_000_000)
}

fn ts_unit_hint(ts_raw: i64) -> &'static str {
    if ts_raw < 100_000_000_000 {
        return "s";
    }
    if ts_raw < 100_000_000_000_000 {
        return "ms";
    }
    if ts_raw < 100_000_000_000_000_000 {
        return "us";
    }
    "ns"
}

fn should_log_ws_ts_skew(token_key: &str, now_ms: i64) -> bool {
    let cache = OPI_WS_TS_LOG_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = cache.lock().unwrap_or_else(|err| err.into_inner());
    let last = guard.get(token_key).copied().unwrap_or(0);
    if now_ms.saturating_sub(last) >= OPI_WS_TS_LOG_INTERVAL_MS {
        guard.insert(token_key.to_string(), now_ms);
        true
    } else {
        false
    }
}

fn should_log_ws_msg_age(token_key: &str, now_ms: i64) -> bool {
    let cache = OPI_WS_AGE_LOG_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = cache.lock().unwrap_or_else(|err| err.into_inner());
    let last = guard.get(token_key).copied().unwrap_or(0);
    if now_ms.saturating_sub(last) >= OPI_WS_AGE_LOG_INTERVAL_MS {
        guard.insert(token_key.to_string(), now_ms);
        true
    } else {
        false
    }
}

fn should_log_ws_snapshot_backpressure(token_key: &str, now_ms: i64) -> bool {
    let cache =
        OPI_WS_SNAPSHOT_BACKPRESSURE_CACHE.get_or_init(|| StdMutex::new(HashMap::new()));
    let mut guard = cache.lock().unwrap_or_else(|err| err.into_inner());
    let last = guard.get(token_key).copied().unwrap_or(0);
    if now_ms.saturating_sub(last) >= OPI_WS_SNAPSHOT_BACKPRESSURE_LOG_MS {
        guard.insert(token_key.to_string(), now_ms);
        true
    } else {
        false
    }
}

fn extract_i64(value: &Value) -> Option<i64> {
    if let Some(val) = value.as_i64() {
        return Some(val);
    }
    if let Some(val) = value.as_u64() {
        return Some(val as i64);
    }
    if let Some(text) = value.as_str() {
        return text.parse::<i64>().ok();
    }
    None
}

fn extract_price(value: &Value) -> Option<f64> {
    value
        .get("price")
        .or_else(|| value.get("lastPrice"))
        .or_else(|| value.get("last_price"))
        .and_then(extract_f64)
}

fn extract_f64(value: &Value) -> Option<f64> {
    if let Some(text) = value.as_str() {
        return text.parse::<f64>().ok();
    }
    if let Some(val) = value.as_f64() {
        return Some(val);
    }
    None
}

fn parse_side(value: Option<&str>) -> Option<BookSide> {
    let side = value?.to_ascii_uppercase();
    match side.as_str() {
        "BUY" | "BID" | "BIDS" => Some(BookSide::Bid),
        "SELL" | "ASK" | "ASKS" => Some(BookSide::Ask),
        _ => None,
    }
}

fn is_split_merge(data: &Value) -> bool {
    let kind = data
        .get("tradeType")
        .or_else(|| data.get("type"))
        .and_then(|v| v.as_str())
        .unwrap_or("");
    matches!(kind.to_ascii_uppercase().as_str(), "SPLIT" | "MERGE")
}
