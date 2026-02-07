use crate::dispatcher::{DispatcherMessage, DISPATCH_QUEUE_CAPACITY};
use crate::health::{SharedHealth, WsState};
use crate::mem_stats;
use crate::models::{
    BookLevel, BookSide, DeltaType, Event, EventFlags, EventKind, EventPayload, ExchangeTsSource,
    TokenSide, Venue,
};
use crate::rest::polymarket::PmSnapshotCommand;
use crate::time_utils::{now_ts_ms, validate_exchange_ts};
use crate::token_registry::RegistryState;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Instant;
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tokio_tungstenite::connect_async;
use tracing::{info, warn};

static FIRST_PM_EVENT: AtomicBool = AtomicBool::new(false);
static BEST_BID_ASK_SAMPLE: AtomicUsize = AtomicUsize::new(0);
static BEST_BID_ASK_INVALID_SAMPLE: AtomicUsize = AtomicUsize::new(0);
static DISPATCH_BACKPRESSURE_COUNT: AtomicUsize = AtomicUsize::new(0);
static UNMAPPED_ASSET_STATS: LazyLock<Mutex<UnmappedAssetStats>> =
    LazyLock::new(|| Mutex::new(UnmappedAssetStats::default()));
const MIN_TOP_SIZE: f64 = 1.0 / 1_000_000.0;
const BEST_BID_ASK_SAMPLE_RATE: usize = 200;
const BEST_BID_ASK_INVALID_SAMPLE_RATE: usize = 200;
const PM_PRICE_SCALE: f64 = 10_000.0;
const PM_PRICE_MAX_IDX: i64 = 10_000;
const UNMAPPED_ASSET_LOG_INTERVAL_MS: i64 = 60_000;
const PM_DISPATCH_WARN_MS: u128 = 200;

#[derive(Default)]
struct UnmappedAssetStats {
    last_log_ms: i64,
    total: u64,
    per_asset: HashMap<String, u64>,
}

pub async fn run_polymarket_ws(
    url: String,
    asset_ids: Vec<String>,
    registry: Arc<RwLock<RegistryState>>,
    dispatcher: mpsc::Sender<DispatcherMessage>,
    health: SharedHealth,
    stale_reconnect_ms: i64,
    pm_snapshot_trigger: Option<mpsc::Sender<PmSnapshotCommand>>,
) {
    let mut backoff_ms = 1000u64;
    loop {
        {
            let mut state = health.lock().await;
            state.set_state(Venue::Polymarket, WsState::Reconnecting);
        }
        let connect = connect_async(&url).await;
        let (mut ws, _) = match connect {
            Ok(value) => value,
            Err(_) => {
                let mut state = health.lock().await;
                state.record_reconnect(Venue::Polymarket);
                state.set_state(Venue::Polymarket, WsState::Down);
                tokio::time::sleep(std::time::Duration::from_millis(backoff_ms)).await;
                backoff_ms = (backoff_ms * 2).min(30_000);
                continue;
            }
        };
        backoff_ms = 1000;
        {
            let mut state = health.lock().await;
            state.set_state(Venue::Polymarket, WsState::Up);
        }
        info!("polymarket ws connected assets_count={}", asset_ids.len());
        mem_stats::snapshot("after_pm_ws_connected");

        let subscribe = serde_json::json!({
            "type": "market",
            "assets_ids": asset_ids,
            "custom_feature_enabled": true,
            "event_types": [
                "book",
                "price_change",
                "last_trade_price",
                "tick_size_change",
                "best_bid_ask",
            ],
        });
        if let Err(err) = ws
            .send(tokio_tungstenite::tungstenite::Message::Text(
                subscribe.to_string(),
            ))
            .await
        {
            warn!("polymarket ws subscribe failed err={}", err);
        }
        info!("polymarket ws subscribed assets_count={}", asset_ids.len());
        mem_stats::snapshot("after_pm_ws_subscribe");

        let mut last_good_ts: HashMap<String, i64> = HashMap::new();
        let mut last_best_bid_ask: HashMap<String, (Option<f64>, Option<f64>)> = HashMap::new();
        let mut last_msg_ts_ms = now_ts_ms();
        let mut stale_tick = tokio::time::interval(std::time::Duration::from_secs(5));
        stale_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = stale_tick.tick() => {
                    if stale_reconnect_ms > 0 {
                        let now_ms = now_ts_ms();
                        let age_ms = now_ms.saturating_sub(last_msg_ts_ms);
                        if age_ms > stale_reconnect_ms {
                            warn!(
                                "polymarket ws stale reconnect age_ms={} threshold_ms={}",
                                age_ms, stale_reconnect_ms
                            );
                            let mut state = health.lock().await;
                            state.record_reconnect(Venue::Polymarket);
                            break;
                        }
                    }
                }
                msg = ws.next() => {
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(err)) => {
                            warn!("polymarket ws recv error err={}", err);
                            let mut state = health.lock().await;
                            state.record_reconnect(Venue::Polymarket);
                            break;
                        }
                        None => {
                            warn!("polymarket ws stream ended");
                            let mut state = health.lock().await;
                            state.record_reconnect(Venue::Polymarket);
                            break;
                        }
                    };
                    if let tokio_tungstenite::tungstenite::Message::Close(frame) = &msg {
                        if let Some(frame) = frame {
                            warn!(
                                "polymarket ws closed code={:?} reason={}",
                                frame.code, frame.reason
                            );
                        } else {
                            warn!("polymarket ws closed");
                        }
                        let mut state = health.lock().await;
                        state.record_reconnect(Venue::Polymarket);
                        break;
                    }
                    if msg.is_ping() || msg.is_pong() {
                        last_msg_ts_ms = now_ts_ms();
                        continue;
                    }
                    let text = match msg.to_text() {
                        Ok(text) => text,
                        Err(_) => continue,
                    };
                    if text == "PING" || text == "PONG" {
                        last_msg_ts_ms = now_ts_ms();
                        continue;
                    }
                    last_msg_ts_ms = now_ts_ms();
                    let payload: Value = match serde_json::from_str(text) {
                        Ok(value) => value,
                        Err(_) => {
                            info!(
                                "polymarket ws non_json payload text={}",
                                truncate_text(text, 200)
                            );
                            continue;
                        }
                    };
                    let (event_payload, event_type) = extract_event(&payload);
                    let event_type = match event_type {
                        Some(t) => t,
                        None => {
                            log_non_market_payload(&payload, None);
                            continue;
                        }
                    };

                    if matches!(
                        event_type,
                        "book" | "price_change" | "last_trade_price" | "tick_size_change" | "best_bid_ask"
                    ) {
                        let mut state = health.lock().await;
                        state.record_message(Venue::Polymarket);
                    }

                    if event_type == "book" {
                        if let Some(event) = parse_book(event_payload, &registry, &mut last_good_ts).await {
                            log_first_event(&event);
                            if let Some(trigger) = &pm_snapshot_trigger {
                                let _ = trigger.try_send(PmSnapshotCommand::WsActivity {
                                    token_key: event.token_key.clone(),
                                });
                            }
                            dispatch_event(&dispatcher, event, "book").await;
                        }
                    } else if event_type == "price_change" {
                        let events = parse_price_change(event_payload, &registry, &mut last_good_ts).await;
                        for event in events {
                            log_first_event(&event);
                            if let Some(trigger) = &pm_snapshot_trigger {
                                let _ = trigger.try_send(PmSnapshotCommand::WsActivity {
                                    token_key: event.token_key.clone(),
                                });
                            }
                            dispatch_event(&dispatcher, event, "price_change").await;
                        }
                    } else if event_type == "last_trade_price" {
                        if let Some(event) =
                            parse_last_trade(event_payload, &registry, &mut last_good_ts).await
                        {
                            log_first_event(&event);
                            if let Some(trigger) = &pm_snapshot_trigger {
                                let _ = trigger.try_send(PmSnapshotCommand::WsActivity {
                                    token_key: event.token_key.clone(),
                                });
                            }
                            dispatch_event(&dispatcher, event, "last_trade_price").await;
                        }
                    } else if event_type == "tick_size_change" {
                        if let Some(event) =
                            parse_tick_size(event_payload, &registry, &mut last_good_ts).await
                        {
                            log_first_event(&event);
                            if let Some(trigger) = &pm_snapshot_trigger {
                                let _ = trigger.try_send(PmSnapshotCommand::WsActivity {
                                    token_key: event.token_key.clone(),
                                });
                            }
                            dispatch_event(&dispatcher, event, "tick_size_change").await;
                        }
                    } else if event_type == "best_bid_ask" {
                        let events = parse_best_bid_ask(
                            event_payload,
                            &registry,
                            &mut last_good_ts,
                            &mut last_best_bid_ask,
                        )
                        .await;
                        for event in events {
                            log_first_event(&event);
                            if let Some(trigger) = &pm_snapshot_trigger {
                                let _ = trigger.try_send(PmSnapshotCommand::WsActivity {
                                    token_key: event.token_key.clone(),
                                });
                            }
                            dispatch_event(&dispatcher, event, "best_bid_ask").await;
                        }
                    } else {
                        log_non_market_payload(&payload, Some(event_type));
                    }
                }
            }
        }
    }
}

fn log_first_event(event: &Event) {
    if FIRST_PM_EVENT
        .compare_exchange(false, true, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
    {
        info!(
            "ws first event venue=polymarket local_ts_ms={} channel={}",
            event.local_ts_ms,
            event.channel.as_deref().unwrap_or("unknown")
        );
        mem_stats::snapshot("after_first_pm_ws_event");
    }
}

async fn dispatch_event(
    dispatcher: &mpsc::Sender<DispatcherMessage>,
    event: Event,
    msg_type: &str,
) {
    let token_key = event.token_key.clone();
    let venue = event.venue;
    let kind = event.kind;
    let start = Instant::now();
    let send_result = dispatcher.send(DispatcherMessage::Event(event)).await;
    let send_ms = start.elapsed().as_millis();
    let dispatch_remaining = dispatcher.capacity();
    let dispatch_used = DISPATCH_QUEUE_CAPACITY.saturating_sub(dispatch_remaining);
    if send_ms > PM_DISPATCH_WARN_MS {
        let count = DISPATCH_BACKPRESSURE_COUNT.fetch_add(1, Ordering::Relaxed) + 1;
        if count == 1 || count % 100 == 0 {
            warn!(
                "pm_ws dispatch_backpressure count={} msg_type={} send_ms={} dispatch_used={} dispatch_capacity={} venue={:?} kind={:?} token_key={}",
                count,
                msg_type,
                send_ms,
                dispatch_used,
                DISPATCH_QUEUE_CAPACITY,
                venue,
                kind,
                token_key
            );
        }
    }
    if let Err(err) = send_result {
        warn!(
            "pm_ws dispatch_failed msg_type={} err={:?} token_key={} dispatch_used={} dispatch_capacity={}",
            msg_type,
            err,
            token_key,
            dispatch_used,
            DISPATCH_QUEUE_CAPACITY
        );
    }
}

fn log_non_market_payload(payload: &Value, event_type: Option<&str>) {
    let message = extract_message(payload);
    let code = extract_code(payload);
    let mut warn_flag = code.map(|c| c >= 400).unwrap_or(false);
    if let Some(msg) = message.as_ref() {
        let msg_lower = msg.to_lowercase();
        if msg_lower.contains("rate") || msg_lower.contains("limit") || msg_lower.contains("error")
        {
            warn_flag = true;
        }
    }
    let payload_str = truncate_text(&payload.to_string(), 400);
    let event_type = event_type.unwrap_or("unknown");
    if warn_flag {
        warn!(
            "polymarket ws non_market event_type={} code={:?} message={:?} payload={}",
            event_type, code, message, payload_str
        );
    } else {
        info!(
            "polymarket ws non_market event_type={} code={:?} message={:?} payload={}",
            event_type, code, message, payload_str
        );
    }
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

fn best_bid_ask_value_valid(price: f64, size: f64) -> bool {
    if !price.is_finite() || !size.is_finite() {
        return false;
    }
    let idx = (price * PM_PRICE_SCALE).round() as i64;
    if idx < 0 || idx > PM_PRICE_MAX_IDX {
        return false;
    }
    true
}

fn sample_best_bid_ask_invalid(
    asset_id: &str,
    token_key: &str,
    price: f64,
    size: f64,
    payload: &Value,
) {
    let count = BEST_BID_ASK_INVALID_SAMPLE.fetch_add(1, Ordering::Relaxed);
    if count % BEST_BID_ASK_INVALID_SAMPLE_RATE != 0 {
        return;
    }
    let payload_str = truncate_text(&payload.to_string(), 400);
    warn!(
        "polymarket best_bid_ask invalid_price_or_size asset_id={} token_key={} price={} size={} payload={}",
        asset_id, token_key, price, size, payload_str
    );
}

fn sample_best_bid_ask_payload(
    asset_id: &str,
    token_key: &str,
    bid: Option<f64>,
    ask: Option<f64>,
    payload: &Value,
) {
    let count = BEST_BID_ASK_SAMPLE.fetch_add(1, Ordering::Relaxed);
    if count % BEST_BID_ASK_SAMPLE_RATE != 0 {
        return;
    }
    let payload_str = truncate_text(&payload.to_string(), 400);
    info!(
        "polymarket best_bid_ask parse missing_side asset_id={} token_key={} bid={:?} ask={:?} payload={}",
        asset_id, token_key, bid, ask, payload_str
    );
}

fn record_unmapped_asset(asset_id: &str, payload: &Value, pair_ids: Option<&[i64]>) {
    let now_ms = now_ts_ms();
    let mut stats = match UNMAPPED_ASSET_STATS.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    };
    stats.total = stats.total.saturating_add(1);
    let entry = stats.per_asset.entry(asset_id.to_string()).or_insert(0);
    *entry = entry.saturating_add(1);
    if *entry == 1 {
        let payload_str = truncate_text(&payload.to_string(), 400);
        warn!(
            "polymarket ws unmapped asset_id={} pair_ids={:?} payload={}",
            asset_id,
            pair_ids,
            payload_str
        );
    }
    if stats.last_log_ms == 0 || now_ms.saturating_sub(stats.last_log_ms) >= UNMAPPED_ASSET_LOG_INTERVAL_MS {
        stats.last_log_ms = now_ms;
        let mut top: Vec<(&String, &u64)> = stats.per_asset.iter().collect();
        top.sort_by(|a, b| b.1.cmp(a.1));
        let mut top_str = String::new();
        for (idx, (key, val)) in top.iter().take(5).enumerate() {
            if idx > 0 {
                top_str.push_str(", ");
            }
            top_str.push_str(&format!("{}:{}", key, val));
        }
        warn!(
            "polymarket ws unmapped asset summary total={} unique={} top=[{}]",
            stats.total,
            stats.per_asset.len(),
            top_str
        );
    }
}

async fn parse_book(
    payload: &Value,
    registry: &Arc<RwLock<RegistryState>>,
    last_good_ts: &mut HashMap<String, i64>,
) -> Option<Event> {
    let asset_id = extract_asset_id(payload)?;
    let (token_key, token_side, pair_ids) = {
        let state = registry.read().await;
        let key = match state.pm_asset_key.get(&asset_id) {
            Some(key) => key.to_string(),
            None => {
                let pair_ids = state.pm_market_pairs.get(&asset_id).map(|v| v.as_slice());
                record_unmapped_asset(&asset_id, payload, pair_ids);
                return None;
            }
        };
        let side = state
            .token_sides
            .get(&key)
            .copied()
            .unwrap_or(TokenSide::Unknown);
        let pairs = state.token_pairs.get(&key).cloned().unwrap_or_default();
        (key, side, pairs)
    };
    let local_ts = now_ts_ms();
    let ts_raw = extract_ts(payload);
    let last_good = last_good_ts.get(&token_key).copied();
    let check = validate_exchange_ts(ts_raw, local_ts, last_good);
    if check.source == ExchangeTsSource::VenueWs {
        last_good_ts.insert(token_key.clone(), check.exchange_ts_ms);
    }
    let bids = parse_levels(payload.get("bids"));
    let asks = parse_levels(payload.get("asks"));
    if bids.is_none() && asks.is_none() {
        return None;
    }

    Some(Event {
        venue: Venue::Polymarket,
        token_key: token_key.clone(),
        pair_ids,
        token_side,
        kind: EventKind::BookSnapshot,
        channel: Some("book".to_string()),
        exchange_ts_ms: check.exchange_ts_ms,
        local_ts_ms: local_ts,
        exchange_ts_source: check.source,
        flags: EventFlags {
            ts_missing: check.ts_missing,
            ts_anomaly: check.ts_anomaly,
            out_of_order: check.out_of_order,
            trade_side_missing: false,
        },
        payload: EventPayload {
            bids,
            asks,
            hash: extract_hash(payload),
            ..Default::default()
        },
    })
}

async fn parse_price_change(
    payload: &Value,
    registry: &Arc<RwLock<RegistryState>>,
    last_good_ts: &mut HashMap<String, i64>,
) -> Vec<Event> {
    let mut out = Vec::new();
    let changes = payload
        .get("price_changes")
        .or_else(|| payload.get("changes"));
    let changes = match changes.and_then(|v| v.as_array()) {
        Some(list) => list,
        None => return out,
    };
    for change in changes {
        let asset_id = extract_asset_id(change).or_else(|| extract_asset_id(payload));
        let asset_id = match asset_id {
            Some(id) => id,
            None => continue,
        };
        let (token_key, token_side, pair_ids) = {
            let state = registry.read().await;
            let key = match state.pm_asset_key.get(&asset_id) {
                Some(key) => key.clone(),
                None => {
                    let pair_ids = state.pm_market_pairs.get(&asset_id).map(|v| v.as_slice());
                    record_unmapped_asset(&asset_id, change, pair_ids);
                    continue;
                }
            };
            let side = state
                .token_sides
                .get(&key)
                .copied()
                .unwrap_or(TokenSide::Unknown);
            let pairs = state.token_pairs.get(&key).cloned().unwrap_or_default();
            (key, side, pairs)
        };
        let local_ts = now_ts_ms();
        let ts_raw = extract_ts(change).or_else(|| extract_ts(payload));
        let last_good = last_good_ts.get(&token_key).copied();
        let check = validate_exchange_ts(ts_raw, local_ts, last_good);
        if check.source == ExchangeTsSource::VenueWs {
            last_good_ts.insert(token_key.clone(), check.exchange_ts_ms);
        }
        let side = match parse_side(
            change
                .get("side")
                .or_else(|| change.get("type"))
                .and_then(|v| v.as_str()),
        ) {
            Some(side) => side,
            None => continue,
        };
        let price = match change.get("price").and_then(extract_f64) {
            Some(price) => price,
            None => continue,
        };
        let size = change
            .get("size")
            .or_else(|| change.get("amount"))
            .and_then(extract_f64)
            .unwrap_or(0.0);
        let delta_type = if size <= 0.0 {
            DeltaType::Cancel
        } else {
            DeltaType::AddOrUpdate
        };

        let hash = extract_hash(change).or_else(|| extract_hash(payload));
        let (mut best_bid, mut best_ask) = extract_best_bid_ask(payload);
        if best_bid.is_none() || best_ask.is_none() {
            let (change_bid, change_ask) = extract_best_bid_ask(change);
            if best_bid.is_none() {
                best_bid = change_bid;
            }
            if best_ask.is_none() {
                best_ask = change_ask;
            }
        }
        out.push(Event {
            venue: Venue::Polymarket,
            token_key: token_key.clone(),
            pair_ids,
            token_side,
            kind: EventKind::BookDelta,
            channel: Some("price_change".to_string()),
            exchange_ts_ms: check.exchange_ts_ms,
            local_ts_ms: local_ts,
            exchange_ts_source: check.source,
            flags: EventFlags {
                ts_missing: check.ts_missing,
                ts_anomaly: check.ts_anomaly,
                out_of_order: check.out_of_order,
                trade_side_missing: false,
            },
            payload: EventPayload {
                side: Some(side),
                delta_type: Some(delta_type),
                price: Some(price),
                size: Some(size),
                hash,
                best_bid,
                best_ask,
                ..Default::default()
            },
        });
    }
    out
}

async fn parse_last_trade(
    payload: &Value,
    registry: &Arc<RwLock<RegistryState>>,
    last_good_ts: &mut HashMap<String, i64>,
) -> Option<Event> {
    let asset_id = extract_asset_id(payload)?;
    let (token_key, token_side, pair_ids) = {
        let state = registry.read().await;
        let key = match state.pm_asset_key.get(&asset_id) {
            Some(key) => key.to_string(),
            None => {
                let pair_ids = state.pm_market_pairs.get(&asset_id).map(|v| v.as_slice());
                record_unmapped_asset(&asset_id, payload, pair_ids);
                return None;
            }
        };
        let side = state
            .token_sides
            .get(&key)
            .copied()
            .unwrap_or(TokenSide::Unknown);
        let pairs = state.token_pairs.get(&key).cloned().unwrap_or_default();
        (key, side, pairs)
    };
    let local_ts = now_ts_ms();
    let ts_raw = extract_ts(payload);
    let last_good = last_good_ts.get(&token_key).copied();
    let check = validate_exchange_ts(ts_raw, local_ts, last_good);
    if check.source == ExchangeTsSource::VenueWs {
        last_good_ts.insert(token_key.clone(), check.exchange_ts_ms);
    }
    let price = payload.get("price").and_then(extract_f64)?;
    let size = payload
        .get("size")
        .or_else(|| payload.get("amount"))
        .and_then(extract_f64)
        .unwrap_or(0.0);

    let side = parse_side(payload.get("side").and_then(|v| v.as_str()));
    let mut flags = EventFlags {
        ts_missing: check.ts_missing,
        ts_anomaly: check.ts_anomaly,
        out_of_order: check.out_of_order,
        trade_side_missing: false,
    };
    if side.is_none() {
        flags.trade_side_missing = true;
    }

    Some(Event {
        venue: Venue::Polymarket,
        token_key: token_key.clone(),
        pair_ids,
        token_side,
        kind: EventKind::Trade,
        channel: Some("last_trade_price".to_string()),
        exchange_ts_ms: check.exchange_ts_ms,
        local_ts_ms: local_ts,
        exchange_ts_source: check.source,
        flags,
        payload: EventPayload {
            side,
            price: Some(price),
            size: Some(size),
            ..Default::default()
        },
    })
}

async fn parse_tick_size(
    payload: &Value,
    registry: &Arc<RwLock<RegistryState>>,
    last_good_ts: &mut HashMap<String, i64>,
) -> Option<Event> {
    let asset_id = extract_asset_id(payload)?;
    let (token_key, token_side, pair_ids) = {
        let state = registry.read().await;
        let key = match state.pm_asset_key.get(&asset_id) {
            Some(key) => key.to_string(),
            None => {
                let pair_ids = state.pm_market_pairs.get(&asset_id).map(|v| v.as_slice());
                record_unmapped_asset(&asset_id, payload, pair_ids);
                return None;
            }
        };
        let side = state
            .token_sides
            .get(&key)
            .copied()
            .unwrap_or(TokenSide::Unknown);
        let pairs = state.token_pairs.get(&key).cloned().unwrap_or_default();
        (key, side, pairs)
    };
    let local_ts = now_ts_ms();
    let ts_raw = extract_ts(payload);
    let last_good = last_good_ts.get(&token_key).copied();
    let check = validate_exchange_ts(ts_raw, local_ts, last_good);
    if check.source == ExchangeTsSource::VenueWs {
        last_good_ts.insert(token_key.clone(), check.exchange_ts_ms);
    }
    let tick = payload
        .get("tick_size")
        .or_else(|| payload.get("tickSize"))
        .and_then(extract_f64)?;

    Some(Event {
        venue: Venue::Polymarket,
        token_key: token_key.clone(),
        pair_ids,
        token_side,
        kind: EventKind::TickSizeChange,
        channel: Some("tick_size_change".to_string()),
        exchange_ts_ms: check.exchange_ts_ms,
        local_ts_ms: local_ts,
        exchange_ts_source: check.source,
        flags: EventFlags {
            ts_missing: check.ts_missing,
            ts_anomaly: check.ts_anomaly,
            out_of_order: check.out_of_order,
            trade_side_missing: false,
        },
        payload: EventPayload {
            tick_size: Some(tick),
            ..Default::default()
        },
    })
}

async fn parse_best_bid_ask(
    payload: &Value,
    registry: &Arc<RwLock<RegistryState>>,
    last_good_ts: &mut HashMap<String, i64>,
    last_best: &mut HashMap<String, (Option<f64>, Option<f64>)>,
) -> Vec<Event> {
    let mut out = Vec::new();
    let asset_id = match extract_asset_id(payload) {
        Some(asset_id) => asset_id,
        None => return out,
    };
    let (token_key, token_side, pair_ids) = {
        let state = registry.read().await;
        let key = match state.pm_asset_key.get(&asset_id) {
            Some(key) => key.to_string(),
            None => {
                let pair_ids = state.pm_market_pairs.get(&asset_id).map(|v| v.as_slice());
                record_unmapped_asset(&asset_id, payload, pair_ids);
                return out;
            }
        };
        let side = state
            .token_sides
            .get(&key)
            .copied()
            .unwrap_or(TokenSide::Unknown);
        let pairs = state.token_pairs.get(&key).cloned().unwrap_or_default();
        (key, side, pairs)
    };
    let (incoming_bid, incoming_ask) = extract_best_bid_ask(payload);
    if incoming_bid.is_none() || incoming_ask.is_none() {
        sample_best_bid_ask_payload(&asset_id, &token_key, incoming_bid, incoming_ask, payload);
    }
    if incoming_bid.is_none() && incoming_ask.is_none() {
        return out;
    }

    let local_ts = now_ts_ms();
    let ts_raw = extract_ts(payload);
    let last_good = last_good_ts.get(&token_key).copied();
    let check = validate_exchange_ts(ts_raw, local_ts, last_good);
    if check.source == ExchangeTsSource::VenueWs {
        last_good_ts.insert(token_key.clone(), check.exchange_ts_ms);
    }

    let (prev_bid, prev_ask) = last_best.get(&token_key).copied().unwrap_or((None, None));
    let merged_bid = incoming_bid.or(prev_bid);
    let merged_ask = incoming_ask.or(prev_ask);

    let payload_ref = payload;
    let asset_id_ref = asset_id.as_str();
    let token_key_ref = token_key.as_str();
    let mut push_event = |side: BookSide, price: f64, size: f64, delta_type: DeltaType| {
        if !price.is_finite() {
            return;
        }
        if !best_bid_ask_value_valid(price, size) {
            sample_best_bid_ask_invalid(asset_id_ref, token_key_ref, price, size, payload_ref);
        }
        out.push(Event {
            venue: Venue::Polymarket,
            token_key: token_key.clone(),
            pair_ids: pair_ids.clone(),
            token_side,
            kind: EventKind::BookDelta,
            channel: Some("best_bid_ask".to_string()),
            exchange_ts_ms: check.exchange_ts_ms,
            local_ts_ms: local_ts,
            exchange_ts_source: check.source,
            flags: EventFlags {
                ts_missing: check.ts_missing,
                ts_anomaly: check.ts_anomaly,
                out_of_order: check.out_of_order,
                trade_side_missing: false,
            },
            payload: EventPayload {
                side: Some(side),
                delta_type: Some(delta_type),
                price: Some(price),
                size: Some(size),
                best_bid: merged_bid,
                best_ask: merged_ask,
                ..Default::default()
            },
        });
    };

    if let Some(new_bid) = incoming_bid {
        if prev_bid != Some(new_bid) {
            if let Some(prev) = prev_bid {
                push_event(BookSide::Bid, prev, 0.0, DeltaType::Cancel);
            }
            push_event(BookSide::Bid, new_bid, MIN_TOP_SIZE, DeltaType::AddOrUpdate);
        }
    }
    if let Some(new_ask) = incoming_ask {
        if prev_ask != Some(new_ask) {
            if let Some(prev) = prev_ask {
                push_event(BookSide::Ask, prev, 0.0, DeltaType::Cancel);
            }
            push_event(BookSide::Ask, new_ask, MIN_TOP_SIZE, DeltaType::AddOrUpdate);
        }
    }

    let mut updated = (prev_bid, prev_ask);
    if incoming_bid.is_some() {
        updated.0 = incoming_bid;
    }
    if incoming_ask.is_some() {
        updated.1 = incoming_ask;
    }
    if updated != (prev_bid, prev_ask) {
        last_best.insert(token_key, updated);
    }

    out
}

fn extract_event<'a>(value: &'a Value) -> (&'a Value, Option<&'a str>) {
    let event_type = value
        .get("event_type")
        .or_else(|| value.get("eventType"))
        .or_else(|| value.get("type"))
        .and_then(|v| v.as_str());
    let payload = value
        .get("data")
        .or_else(|| value.get("event"))
        .unwrap_or(value);
    (payload, event_type)
}

fn extract_asset_id(value: &Value) -> Option<String> {
    for key in [
        "asset_id",
        "assetId",
        "token_id",
        "tokenId",
        "market_id",
        "marketId",
    ] {
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

fn extract_hash(value: &Value) -> Option<String> {
    value
        .get("hash")
        .or_else(|| value.get("book_hash"))
        .or_else(|| value.get("bookHash"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}

const BEST_BID_KEYS: [&str; 6] = [
    "best_bid",
    "bestBid",
    "best_bid_price",
    "bestBidPrice",
    "best_bid_px",
    "bestBidPx",
];
const BEST_ASK_KEYS: [&str; 6] = [
    "best_ask",
    "bestAsk",
    "best_ask_price",
    "bestAskPrice",
    "best_ask_px",
    "bestAskPx",
];
const PRICE_KEYS: [&str; 3] = ["price", "px", "p"];

fn extract_best_bid_ask(value: &Value) -> (Option<f64>, Option<f64>) {
    let bid = extract_best_side_price(value, true);
    let ask = extract_best_side_price(value, false);
    if bid.is_some() || ask.is_some() {
        return (bid, ask);
    }
    if let Some(obj) = value.as_object() {
        for key in [
            "data",
            "event",
            "change",
            "payload",
            "best_bid_ask",
            "bestBidAsk",
        ] {
            if let Some(nested) = obj.get(key) {
                let result = extract_best_bid_ask(nested);
                if result.0.is_some() || result.1.is_some() {
                    return result;
                }
            }
        }
    }
    (None, None)
}

fn extract_best_side_price(value: &Value, is_bid: bool) -> Option<f64> {
    if let Some(obj) = value.as_object() {
        let keys = if is_bid {
            &BEST_BID_KEYS
        } else {
            &BEST_ASK_KEYS
        };
        for key in keys {
            if let Some(val) = obj.get(*key) {
                if let Some(price) = extract_price_from_value(val, is_bid) {
                    return Some(price);
                }
            }
        }
        let side_key = if is_bid { "bid" } else { "ask" };
        if let Some(val) = obj.get(side_key) {
            if let Some(price) = extract_price_from_value(val, is_bid) {
                return Some(price);
            }
        }
        let side_levels = if is_bid { "bids" } else { "asks" };
        if let Some(val) = obj.get(side_levels) {
            if let Some(price) = extract_price_from_levels(val, is_bid) {
                return Some(price);
            }
        }
    }
    None
}

fn extract_price_from_levels(value: &Value, is_bid: bool) -> Option<f64> {
    let list = value.as_array()?;
    let first = list.first()?;
    extract_price_from_value(first, is_bid)
}

fn extract_price_from_value(value: &Value, is_bid: bool) -> Option<f64> {
    if let Some(num) = value.as_f64() {
        return Some(num);
    }
    if let Some(text) = value.as_str() {
        return text.parse::<f64>().ok();
    }
    if let Some(list) = value.as_array() {
        if let Some(first) = list.first() {
            return extract_price_from_value(first, is_bid);
        }
        return None;
    }
    if let Some(obj) = value.as_object() {
        if let Some(price) = extract_number_from_object(obj, &PRICE_KEYS) {
            return Some(price);
        }
        let keys = if is_bid {
            &BEST_BID_KEYS
        } else {
            &BEST_ASK_KEYS
        };
        return extract_number_from_object(obj, keys);
    }
    None
}

fn extract_number(value: &Value, keys: &[&str]) -> Option<f64> {
    let obj = value.as_object()?;
    extract_number_from_object(obj, keys)
}

fn extract_f64(value: &Value) -> Option<f64> {
    if let Some(num) = value.as_f64() {
        return Some(num);
    }
    if let Some(text) = value.as_str() {
        return text.parse::<f64>().ok();
    }
    None
}

fn extract_number_from_object(obj: &Map<String, Value>, keys: &[&str]) -> Option<f64> {
    for key in keys {
        if let Some(val) = obj.get(*key) {
            if let Some(num) = extract_f64(val) {
                return Some(num);
            }
        }
    }
    None
}

fn parse_levels(value: Option<&Value>) -> Option<Vec<BookLevel>> {
    let mut levels = Vec::new();
    let list = value?.as_array()?;
    for entry in list {
        if let Some(arr) = entry.as_array() {
            if arr.len() >= 2 {
                if let (Some(price), Some(size)) = (extract_f64(&arr[0]), extract_f64(&arr[1])) {
                    levels.push(BookLevel { price, size });
                }
            }
            continue;
        }
        if let Some(obj) = entry.as_object() {
            let price = obj.get("price").and_then(extract_f64);
            let size = obj
                .get("size")
                .or_else(|| obj.get("amount"))
                .and_then(extract_f64);
            if let (Some(price), Some(size)) = (price, size) {
                levels.push(BookLevel { price, size });
            }
        }
    }
    Some(levels)
}

fn parse_side(value: Option<&str>) -> Option<BookSide> {
    let side = value?.to_ascii_uppercase();
    match side.as_str() {
        "BUY" | "BID" | "BIDS" => Some(BookSide::Bid),
        "SELL" | "ASK" | "ASKS" => Some(BookSide::Ask),
        _ => None,
    }
}

fn extract_ts(value: &Value) -> Option<i64> {
    let val = value
        .get("timestamp")
        .or_else(|| value.get("ts"))
        .or_else(|| value.get("time"))?;
    if let Some(num) = val.as_i64() {
        return Some(num);
    }
    if let Some(num) = val.as_f64() {
        return Some(num.round() as i64);
    }
    if let Some(text) = val.as_str() {
        return text.parse::<i64>().ok();
    }
    None
}
