use crate::dispatcher::DispatcherMessage;
use crate::models::{
    BookLevel, Event, EventFlags, EventKind, EventPayload, ExchangeTsSource, TokenSide, Venue,
};
use crate::time_utils::now_ts_ms;
use reqwest::Client;
use serde_json::Value;
use smallvec::SmallVec;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::time::{Duration, Instant};
use tracing::{info, warn};

const BOOKS_TRACE_SAMPLES: usize = 3;
static BOOKS_TRACE_COUNTER: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct PolymarketSnapshotTarget {
    pub token_key: String,
    pub token_id: String,
    pub token_side: TokenSide,
    pub market_id: Option<String>,
    pub pair_ids: SmallVec<[i64; 4]>,
}

#[derive(Debug)]
pub enum PmSnapshotCommand {
    WsActivity { token_key: String },
}

pub async fn run_polymarket_snapshotter(
    base_url: String,
    targets: Vec<PolymarketSnapshotTarget>,
    dispatcher: mpsc::Sender<DispatcherMessage>,
    refresh_interval_sec: u64,
    stale_threshold_sec: u64,
    batch_size: usize,
    mut trigger_rx: mpsc::Receiver<PmSnapshotCommand>,
) {
    if targets.is_empty() {
        warn!("pm_rest bootstrap skipped: no polymarket targets");
        return;
    }
    let client = Client::builder()
        .timeout(Duration::from_secs(15))
        .connect_timeout(Duration::from_secs(5))
        .build()
        .unwrap_or_else(|_| Client::new());
    let mut last_ws_seen_ms: HashMap<String, i64> = HashMap::new();
    let mut target_lookup: HashMap<String, PolymarketSnapshotTarget> = HashMap::new();
    for target in &targets {
        if target_lookup
            .insert(target.token_id.clone(), target.clone())
            .is_some()
        {
            warn!(
                "pm_rest duplicate token_id in targets token_id={} token_key={}",
                target.token_id, target.token_key
            );
        }
    }

    let batch_size = batch_size.max(1).min(500);
    info!(
        "pm_rest bootstrap start targets_len={} batch_size={} base_url={}",
        targets.len(),
        batch_size,
        base_url
    );
    let mut missing_bootstrap: HashSet<String> = HashSet::new();
    let mut bootstrap_ok = 0usize;
    for chunk in targets.chunks(batch_size) {
        match fetch_books_batch(&client, &base_url, chunk).await {
            Ok((books, missing)) => {
                for token_id in missing {
                    missing_bootstrap.insert(token_id);
                }
                for target in chunk {
                    if let Some(book) = books.get(&target.token_id) {
                        let event = build_snapshot_event(target, book);
                        let _ = dispatcher.send(DispatcherMessage::Event(event)).await;
                        bootstrap_ok += 1;
                    }
                }
            }
            Err(err) => {
                warn!(
                    "pm_rest bootstrap batch_failed size={} err={:?}",
                    chunk.len(),
                    err
                );
            }
        }
    }
    if !missing_bootstrap.is_empty() {
        let sample: Vec<String> = missing_bootstrap.iter().take(20).cloned().collect();
        warn!(
            "pm_rest bootstrap_404_token_ids count={} sample={:?}",
            missing_bootstrap.len(),
            sample
        );
        let missing_list: Vec<String> = missing_bootstrap.iter().cloned().collect();
        let details = build_missing_details(&missing_list, &target_lookup, 20);
        warn!(
            "pm_rest bootstrap_missing_details count={} sample={:?}",
            missing_bootstrap.len(),
            details
        );
    }
    info!(
        "pm_rest bootstrap done snapshots_ok={} missing={}",
        bootstrap_ok,
        missing_bootstrap.len()
    );

    let mut refresh_tick = tokio::time::interval(Duration::from_secs(refresh_interval_sec.max(5)));
    refresh_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let stale_threshold_ms = (stale_threshold_sec as i64).max(0) * 1000;

    loop {
        tokio::select! {
            _ = refresh_tick.tick() => {
                let now_ms = now_ts_ms();
                let mut refresh_targets = Vec::new();
                let mut unseen = 0usize;
                let mut stale = 0usize;
                for target in &targets {
                    let last_seen = last_ws_seen_ms.get(&target.token_key).copied();
                    let is_unseen = last_seen.is_none();
                    let is_stale = last_seen
                        .map(|ts| now_ms.saturating_sub(ts) > stale_threshold_ms)
                        .unwrap_or(true);
                    if is_unseen {
                        unseen += 1;
                    }
                    if is_stale {
                        stale += 1;
                        refresh_targets.push(target.clone());
                    }
                }
                info!(
                    "pm_rest refresh tick total={} refresh_targets={} unseen={} stale_threshold_ms={}",
                    targets.len(),
                    refresh_targets.len(),
                    unseen,
                    stale_threshold_ms
                );
                for chunk in refresh_targets.chunks(batch_size) {
                    match fetch_books_batch(&client, &base_url, chunk).await {
                        Ok((books, missing)) => {
                            if !missing.is_empty() {
                                let sample: Vec<String> = missing.iter().take(20).cloned().collect();
                                warn!(
                                    "pm_rest refresh_missing_token_ids count={} sample={:?}",
                                    missing.len(),
                                    sample
                                );
                            }
                            for target in chunk {
                                if let Some(book) = books.get(&target.token_id) {
                                    let event = build_snapshot_event(target, book);
                                    let _ = dispatcher.send(DispatcherMessage::Event(event)).await;
                                }
                            }
                        }
                        Err(err) => {
                            warn!(
                                "pm_rest refresh batch_failed size={} err={:?}",
                                chunk.len(),
                                err
                            );
                        }
                    }
                }
            }
            cmd = trigger_rx.recv() => {
                match cmd {
                    Some(PmSnapshotCommand::WsActivity { token_key }) => {
                        last_ws_seen_ms.insert(token_key, now_ts_ms());
                    }
                    None => break,
                }
            }
        }
    }
}

struct BookSnapshot {
    bids: Option<Vec<BookLevel>>,
    asks: Option<Vec<BookLevel>>,
    exchange_ts_ms: Option<i64>,
}

fn build_snapshot_event(target: &PolymarketSnapshotTarget, book: &BookSnapshot) -> Event {
    let local_ts_ms = now_ts_ms();
    let mut flags = EventFlags::default();
    let (exchange_ts_ms, exchange_ts_source) = if let Some(ts) = book.exchange_ts_ms {
        (ts, ExchangeTsSource::VenueRest)
    } else {
        flags.ts_missing = true;
        (local_ts_ms, ExchangeTsSource::Estimated)
    };
    Event {
        venue: Venue::Polymarket,
        token_key: target.token_key.clone(),
        pair_ids: target.pair_ids.clone(),
        token_side: target.token_side,
        kind: EventKind::BookSnapshot,
        channel: Some("rest.book".to_string()),
        exchange_ts_ms,
        local_ts_ms,
        exchange_ts_source,
        flags,
        payload: EventPayload {
            bids: book.bids.clone(),
            asks: book.asks.clone(),
            ..Default::default()
        },
    }
}

async fn fetch_books_batch(
    client: &Client,
    base_url: &str,
    batch: &[PolymarketSnapshotTarget],
) -> anyhow::Result<(HashMap<String, BookSnapshot>, Vec<String>)> {
    let trace = BOOKS_TRACE_COUNTER.fetch_add(1, Ordering::Relaxed) < BOOKS_TRACE_SAMPLES;
    let start = Instant::now();
    let url = format!("{}/books", base_url.trim_end_matches('/'));
    let body: Vec<Value> = batch
        .iter()
        .map(|t| serde_json::json!({ "token_id": t.token_id }))
        .collect();
    let resp = client.post(&url).json(&body).send().await?;
    let status = resp.status();
    let raw = resp.bytes().await?;
    if trace {
        info!(
            "pm_rest books_fetch status={} body_len={} elapsed_ms={}",
            status.as_u16(),
            raw.len(),
            start.elapsed().as_millis()
        );
    }
    if !status.is_success() {
        let preview = truncate_text(&String::from_utf8_lossy(&raw), 300);
        return Err(anyhow::anyhow!(
            "pm_rest books http_error status={} body_prefix=\"{}\"",
            status.as_u16(),
            preview
        ));
    }
    let envelope: Value = serde_json::from_slice(&raw)?;
    let empty = Vec::new();
    let books = extract_books_array(&envelope).unwrap_or(&empty);
    let mut map: HashMap<String, BookSnapshot> = HashMap::new();
    for entry in books {
        if let Some(token_id) = extract_token_id(entry) {
            let bids = parse_levels(extract_book_side(entry, "bids"));
            let asks = parse_levels(extract_book_side(entry, "asks"));
            let exchange_ts_ms = extract_ts_ms(entry);
            map.insert(
                token_id,
                BookSnapshot {
                    bids,
                    asks,
                    exchange_ts_ms,
                },
            );
        }
    }
    let mut missing = Vec::new();
    for target in batch {
        if !map.contains_key(&target.token_id) {
            missing.push(target.token_id.clone());
        }
    }
    Ok((map, missing))
}

fn extract_books_array(value: &Value) -> Option<&Vec<Value>> {
    if let Some(arr) = value.as_array() {
        return Some(arr);
    }
    if let Some(arr) = value.get("data").and_then(|v| v.as_array()) {
        return Some(arr);
    }
    if let Some(arr) = value.get("result").and_then(|v| v.as_array()) {
        return Some(arr);
    }
    value.get("books").and_then(|v| v.as_array())
}

fn build_missing_details(
    missing: &[String],
    lookup: &HashMap<String, PolymarketSnapshotTarget>,
    limit: usize,
) -> Vec<String> {
    let mut out = Vec::new();
    for token_id in missing.iter() {
        if let Some(target) = lookup.get(token_id) {
            out.push(format!(
                "{{token_id={}, market_id={}, token_key={}, side={}, pair_ids={:?}}}",
                target.token_id,
                target.market_id.as_deref().unwrap_or(""),
                target.token_key,
                target.token_side.as_str(),
                target.pair_ids
            ));
        } else {
            out.push(format!("{{token_id={}, target=unknown}}", token_id));
        }
        if out.len() >= limit {
            break;
        }
    }
    out
}

fn extract_book_side<'a>(entry: &'a Value, key: &str) -> Option<&'a Value> {
    entry
        .get(key)
        .or_else(|| entry.get("book").and_then(|v| v.get(key)))
        .or_else(|| entry.get("orderbook").and_then(|v| v.get(key)))
}

fn parse_levels(value: Option<&Value>) -> Option<Vec<BookLevel>> {
    let arr = value?.as_array()?;
    let mut out = Vec::with_capacity(arr.len());
    for level in arr {
        if let Some((price, size)) = parse_level(level) {
            out.push(BookLevel { price, size });
        }
    }
    Some(out)
}

fn parse_level(value: &Value) -> Option<(f64, f64)> {
    if let Some(arr) = value.as_array() {
        if arr.len() >= 2 {
            let price = extract_f64_value(&arr[0])?;
            let size = extract_f64_value(&arr[1])?;
            return Some((price, size));
        }
    }
    if let Some(obj) = value.as_object() {
        let price = obj
            .get("price")
            .or_else(|| obj.get("px"))
            .and_then(extract_f64_value)?;
        let size = obj
            .get("size")
            .or_else(|| obj.get("sz"))
            .or_else(|| obj.get("amount"))
            .and_then(extract_f64_value)?;
        return Some((price, size));
    }
    None
}

fn extract_token_id(entry: &Value) -> Option<String> {
    entry
        .get("token_id")
        .or_else(|| entry.get("tokenId"))
        .or_else(|| entry.get("asset_id"))
        .or_else(|| entry.get("assetId"))
        .and_then(|v| match v {
            Value::String(s) => Some(s.clone()),
            Value::Number(n) => Some(n.to_string()),
            _ => None,
        })
}

fn extract_ts_ms(entry: &Value) -> Option<i64> {
    let ts = entry
        .get("timestamp")
        .or_else(|| entry.get("ts"))
        .or_else(|| entry.get("time"))
        .and_then(extract_i64_value)?;
    Some(normalize_ts_ms(ts))
}

fn extract_f64_value(value: &Value) -> Option<f64> {
    match value {
        Value::Number(num) => num.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn extract_i64_value(value: &Value) -> Option<i64> {
    match value {
        Value::Number(num) => num.as_i64(),
        Value::String(s) => s.parse::<i64>().ok(),
        _ => None,
    }
}

fn normalize_ts_ms(ts_raw: i64) -> i64 {
    if ts_raw < 0 {
        return ts_raw;
    }
    if ts_raw < 100_000_000_000 {
        return ts_raw * 1000;
    }
    if ts_raw < 100_000_000_000_000 {
        return ts_raw;
    }
    if ts_raw < 100_000_000_000_000_000 {
        return ts_raw / 1000;
    }
    ts_raw / 1_000_000
}

fn truncate_text(value: &str, limit: usize) -> String {
    if value.len() <= limit {
        return value.to_string();
    }
    let mut out = value[..limit].to_string();
    out.push_str("...");
    out
}
