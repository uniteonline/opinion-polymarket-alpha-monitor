mod chain;
mod clock;
mod config;
mod alpha_log;
mod db;
mod db_queue;
mod db_writer;
mod discovery;
mod dispatcher;
mod fee;
mod health;
mod mem_stats;
mod migrations;
mod models;
mod pair_aggregator;
mod rest;
mod shard;
mod shock;
mod time_utils;
mod token_registry;
mod trade;
mod ws;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

use crate::chain::{new_shared_chain, run_chain_metrics};
use crate::clock::ClockOffset;
use crate::config::MonitorConfig;
use crate::db::MonitorDb;
use crate::db_queue::{spawn_db_queue, try_send_db, DbMessage};
use crate::discovery::DiscoveryLoader;
use crate::dispatcher::{spawn_dispatcher, DispatcherMessage, DISPATCH_QUEUE_CAPACITY};
use crate::fee::{run_fee_schedule_refresher, FeeScheduleState};
use crate::health::{new_shared_health, WsState};
use crate::pair_aggregator::{pick_pair_shard, PairAggregator, PairMessage};
use crate::models::TokenSide;
use crate::rest::opinion::{run_opinion_snapshotter, OpinionSnapshotTarget, SnapshotCommand};
use crate::rest::polymarket::{
    run_polymarket_snapshotter, PmSnapshotCommand, PolymarketSnapshotTarget,
};
use crate::shard::{init_shock_counter, ShardConfig, ShardMetrics};
use crate::time_utils::now_ts_ms;
use crate::token_registry::{token_key_opinion, token_key_polymarket, TokenRegistry};
use crate::trade::{new_shared_trade_risk, spawn_order_executor, TradeEngine, TradeMessage};
use crate::ws::opinion::run_opinion_ws;
use crate::ws::polymarket::run_polymarket_ws;
use sqlx::Row;
use std::collections::HashMap;
use std::env;
use std::io::Write;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::sync::RwLock;
use tracing_subscriber::EnvFilter;

const PAIR_QUEUE_CAPACITY: usize = 2048;

fn is_missing_token_id(value: &Option<String>) -> bool {
    value
        .as_ref()
        .map(|v| v.trim().is_empty())
        .unwrap_or(true)
}

fn audit_polymarket_token_ids(pairs: &[crate::models::PairRecord]) {
    let mut missing_pairs = Vec::new();
    let mut missing_yes = 0usize;
    let mut missing_no = 0usize;
    for pair in pairs {
        let yes_missing = is_missing_token_id(&pair.polymarket_yes_token_id);
        let no_missing = is_missing_token_id(&pair.polymarket_no_token_id);
        if yes_missing {
            missing_yes += 1;
        }
        if no_missing {
            missing_no += 1;
        }
        if (yes_missing || no_missing) && missing_pairs.len() < 20 {
            missing_pairs.push(format!(
                "pair_id={} market_id={}",
                pair.pair_id,
                pair.polymarket_market_id.as_deref().unwrap_or("n/a")
            ));
        }
    }
    let total_missing = missing_yes + missing_no;
    if total_missing > 0 {
        tracing::warn!(
            "polymarket token ids missing yes_missing={} no_missing={} sample_pairs=[{}]",
            missing_yes,
            missing_no,
            missing_pairs.join(", ")
        );
    } else {
        tracing::info!("polymarket token ids audit ok (no missing token ids)");
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    filter = filter.add_directive("monitor=info".parse()?);
    let _ = tracing_subscriber::fmt().with_env_filter(filter).try_init();

    let run_id = now_ts_ms();
    let pid = std::process::id();
    let rust_log = std::env::var("RUST_LOG").unwrap_or_else(|_| "default".to_string());
    tracing::info!(
        "monitor started run_id={} pid={} rust_log={}",
        run_id,
        pid,
        rust_log
    );

    let config = MonitorConfig::load()?;
    if config.opinion_api_key.is_empty() {
        tracing::info!(
            "opinion_api_key missing source={}",
            config.opinion_api_key_source
        );
    } else {
        tracing::info!(
            "opinion_api_key loaded source={} key={}",
            config.opinion_api_key_source,
            mask_secret(&config.opinion_api_key)
        );
    }
    tracing::info!("opinion_rest_base={}", config.opinion_rest_base);
    tracing::info!(
        "opinion_rest_max_rps={} opinion_rest_bootstrap_concurrency={}",
        config.opinion_rest_max_rps,
        config.opinion_rest_bootstrap_concurrency
    );
    tracing::info!(
        "opinion_rest_fast_interval_sec={} opinion_rest_inactive_sec={} opinion_active_window_sec={}",
        config.opinion_rest_fast_interval_sec,
        config.opinion_rest_inactive_sec,
        config.opinion_active_window_sec
    );
    tracing::info!(
        "alpha_capture_enabled={} pm_staleness_ms={} other_staleness_ms={} hard_clear_ms={}",
        config.alpha_capture_enabled,
        config.pm_staleness_ms,
        config.other_staleness_ms,
        config.hard_clear_ms
    );
    if !config.alpha_capture_enabled {
        tracing::warn!("alpha_capture_enabled=false pending_responses will be skipped");
    }
    tracing::info!(
        "analysis_staleness_ms={} analysis_gap_allow={} analysis_move_eps={}",
        config.analysis_staleness_ms,
        config.analysis_gap_allow,
        config.analysis_move_eps
    );
    if config.monitoring_db_url.trim().is_empty() {
        return Err(anyhow::anyhow!(
            "monitoring_db_url is empty; set MONITOR_DB_URL or config.yaml"
        ));
    }
    let subcommand = env::args().nth(1);
    if subcommand.as_deref() == Some("healthcheck") {
        run_healthcheck(&config).await?;
        return Ok(());
    }
    mem_stats::spawn_if_enabled(Duration::from_secs(5));

    let monitor_db = MonitorDb::connect(&config.monitoring_db_url).await?;
    match sqlx::query("SELECT COALESCE(MAX(shock_id), 0) FROM poly_shock_events")
        .fetch_one(&monitor_db.pool)
        .await
    {
        Ok(row) => {
            let max_id: i64 = row.try_get(0).unwrap_or(0);
            let next_id = max_id.saturating_add(1).max(1);
            init_shock_counter(next_id);
            tracing::info!(
                "shock_counter initialized next_id={} max_id={}",
                next_id,
                max_id
            );
        }
        Err(err) => {
            tracing::warn!("shock_counter init failed: {}", err);
        }
    }
    let discovery = DiscoveryLoader::connect(&config.discovery_db_path).await?;

    let (watchlist_version, mut pairs) = discovery.load_latest_pairs().await?;
    tracing::info!(
        "pair_loader completed watchlist_version={} pairs_len={}",
        watchlist_version,
        pairs.len()
    );
    match monitor_db.load_pair_overrides().await {
        Ok(overrides) => {
            if !overrides.is_empty() {
                let mut applied = 0usize;
                for pair in &mut pairs {
                    if let Some((yes, no)) = overrides.get(&pair.pair_id) {
                        pair.opinion_yes_token_id = yes.clone();
                        pair.opinion_no_token_id = no.clone();
                        applied += 1;
                    }
                }
                tracing::info!(
                    "pair_overrides applied overrides_count={} pairs_len={}",
                    applied,
                    pairs.len()
                );
            }
        }
        Err(err) => {
            tracing::warn!("pair_overrides load failed: {}", err);
        }
    }
    audit_polymarket_token_ids(&pairs);
    mem_stats::snapshot("after_pair_load");
    monitor_db.upsert_pairs(watchlist_version, &pairs).await?;

    let registry = TokenRegistry::build(&pairs);
    tracing::info!(
        "token_registry built tokens_len={} token_pairs_len={} token_sides_len={} opi_market_pairs_len={} opi_market_side_key_len={} opi_expected_token_id_len={} pm_asset_key_len={} pm_market_pairs_len={} opi_market_ids_len={} pm_asset_ids_len={}",
        registry.tokens.len(),
        registry.state.token_pairs.len(),
        registry.state.token_sides.len(),
        registry.state.opi_market_pairs.len(),
        registry.state.opi_market_side_key.len(),
        registry.state.opi_expected_token_id.len(),
        registry.state.pm_asset_key.len(),
        registry.state.pm_market_pairs.len(),
        registry.state.opi_market_ids.len(),
        registry.state.pm_asset_ids.len()
    );
    mem_stats::snapshot("after_token_registry");
    monitor_db.upsert_tokens(&registry.tokens).await?;
    let token_pairs = registry.state.token_pairs.clone();

    let mut opi_token_map: HashMap<(i64, TokenSide), String> = HashMap::new();
    let mut pm_token_map: HashMap<(i64, TokenSide), String> = HashMap::new();
    for pair in &pairs {
        let op_yes = token_key_opinion(
            pair.opinion_yes_token_id.as_deref(),
            pair.opinion_market_id.as_deref(),
            1,
        );
        let op_no = token_key_opinion(
            pair.opinion_no_token_id.as_deref(),
            pair.opinion_market_id.as_deref(),
            2,
        );
        opi_token_map.insert((pair.pair_id, TokenSide::Yes), op_yes);
        opi_token_map.insert((pair.pair_id, TokenSide::No), op_no);

        let pm_yes = token_key_polymarket(
            pair.polymarket_yes_token_id.as_deref(),
            pair.polymarket_market_id.as_deref(),
            TokenSide::Yes,
        );
        let pm_no = token_key_polymarket(
            pair.polymarket_no_token_id.as_deref(),
            pair.polymarket_market_id.as_deref(),
            TokenSide::No,
        );
        pm_token_map.insert((pair.pair_id, TokenSide::Yes), pm_yes);
        pm_token_map.insert((pair.pair_id, TokenSide::No), pm_no);
    }

    let registry_state = Arc::new(RwLock::new(registry.state));

    let (db_tx, db_stats) = spawn_db_queue(
        monitor_db.pool.clone(),
        config.alpha_capture_enabled,
        &config.db_queue_path,
    )
    .await?;

    let fee_schedule = Arc::new(RwLock::new(FeeScheduleState::new(
        config.fee_bps_polymarket,
        config.fee_bps_opinion,
    )));
    tokio::spawn(run_fee_schedule_refresher(
        monitor_db.pool.clone(),
        fee_schedule.clone(),
        60,
    ));

    let chain_history_len = if config.chain_metrics_interval_sec > 0 {
        ((24 * 60 * 60) / config.chain_metrics_interval_sec) as usize
    } else {
        17_280
    };
    let chain_state = new_shared_chain(chain_history_len);
    if config.chain_metrics_interval_sec > 0 {
        tokio::spawn(run_chain_metrics(
            db_tx.clone(),
            chain_state.clone(),
            config.chain_metrics_interval_sec,
            config.chain_metrics_base_fee,
            config.chain_metrics_priority_fee,
            config.chain_metrics_gas_used_est,
        ));
    }

    let (opi_snapshot_trigger_tx, mut opi_snapshot_trigger_rx) = if config.opinion_api_key.is_empty()
    {
        (None, None)
    } else {
        let (tx, rx) = mpsc::channel::<SnapshotCommand>(1024);
        (Some(tx), Some(rx))
    };

    let trade_sender = if config.trade.enabled {
        let trade_queue_capacity = config.trade.queue_capacity.max(1024);
        let trade_shard_count = config.trade.engine_shards.max(1);
        let (trade_tx, mut trade_rx) = mpsc::channel::<TradeMessage>(trade_queue_capacity);
        let order_targets = Arc::new(crate::trade::build_opinion_order_map(&pairs));
        let opinion_client = if config.trade.dry_run {
            None
        } else {
            match crate::trade::build_opinion_client(&config.trade) {
                Ok(client) => Some(Arc::new(client)),
                Err(err) => {
                    tracing::warn!("trade opinion client init failed: {}", err);
                    None
                }
            }
        };
        if let Some(client) = opinion_client.as_ref() {
            let mut market_ids = std::collections::HashSet::new();
            for target in order_targets.values() {
                market_ids.insert(target.market_id);
            }
            if !market_ids.is_empty() {
                let prefetch_base = if config.trade.opinion_prefetch_base.trim().is_empty() {
                    config.opinion_rest_base.clone()
                } else {
                    config.trade.opinion_prefetch_base.clone()
                };
                let interval_ms = config.trade.opinion_prefetch_interval_ms;
                if prefetch_base.trim().is_empty() {
                    tracing::warn!("opinion_openapi prefetch skipped (base_url empty)");
                } else {
                    tracing::info!(
                        "opinion_openapi prefetch market cache start base={} markets={} interval_ms={}",
                        prefetch_base,
                        market_ids.len(),
                        interval_ms
                    );
                    let mut ids: Vec<i64> = market_ids.into_iter().collect();
                    ids.sort_unstable();
                    match client.prefetch_markets(&prefetch_base, &ids, interval_ms).await {
                        Ok((ok, failed)) => {
                            tracing::info!(
                                "opinion_openapi prefetch market cache done ok={} failed={}",
                                ok,
                                failed
                            );
                        }
                        Err(err) => {
                            tracing::warn!("opinion_openapi prefetch market cache failed: {}", err);
                        }
                    }
                }
            }
            if config.trade.opinion_enable_trading_on_startup {
                match client.enable_trading().await {
                    Ok(()) => {
                        tracing::info!("opinion_enable_trading startup ok");
                    }
                    Err(err) => {
                        tracing::warn!("opinion_enable_trading startup failed: {}", err);
                    }
                }
            }
        }
        tracing::info!(
            "trade_engine starting enabled={} opinion_enable_trading_on_startup={} shards={} queue_capacity={} order_concurrency={}",
            config.trade.enabled,
            config.trade.opinion_enable_trading_on_startup,
            trade_shard_count,
            trade_queue_capacity,
            config.trade.order_executor_concurrency
        );
        let order_exec = spawn_order_executor(
            opinion_client.clone(),
            config.trade.order_executor_concurrency,
        );
        let risk_state = new_shared_trade_risk();

        let mut trade_shard_senders: Vec<mpsc::Sender<TradeMessage>> =
            Vec::with_capacity(trade_shard_count);
        let mut trade_shard_receivers: Vec<mpsc::Receiver<TradeMessage>> =
            Vec::with_capacity(trade_shard_count);
        for _ in 0..trade_shard_count {
            let (tx, rx) = mpsc::channel::<TradeMessage>(trade_queue_capacity);
            trade_shard_senders.push(tx);
            trade_shard_receivers.push(rx);
        }
        let trade_shard_senders_clone = trade_shard_senders.clone();
        tokio::spawn(async move {
            while let Some(msg) = trade_rx.recv().await {
                let shard_id = match &msg {
                    TradeMessage::PairBar(bar) => pick_pair_shard(bar.pair_id, trade_shard_count),
                    TradeMessage::Shock(shock) => pick_pair_shard(shock.pair_id, trade_shard_count),
                    TradeMessage::OpinionOrderUpdate(update) => {
                        pick_pair_shard(update.pair_id, trade_shard_count)
                    }
                    TradeMessage::OpinionTradeRecord(record) => {
                        pick_pair_shard(record.pair_id, trade_shard_count)
                    }
                };
                if let Err(err) = trade_shard_senders_clone[shard_id].send(msg).await {
                    tracing::warn!("trade_router send failed shard_id={} err={}", shard_id, err);
                }
            }
        });

        for trade_rx in trade_shard_receivers {
            let (order_resp_tx, order_resp_rx) = mpsc::unbounded_channel();
            let trade_engine = TradeEngine::new(
                trade_rx,
                config.trade.clone(),
                fee_schedule.clone(),
                chain_state.clone(),
                db_tx.clone(),
                opinion_client.clone(),
                order_targets.clone(),
                opi_snapshot_trigger_tx.clone(),
                order_exec.clone(),
                order_resp_tx,
                order_resp_rx,
                risk_state.clone(),
            );
            tokio::spawn(trade_engine.run());
        }
        Some(trade_tx)
    } else {
        tracing::info!("trade_engine disabled");
        None
    };

    let opi_bootstrap_done = Arc::new(AtomicBool::new(false));
    let pair_shard_count = 64usize;
    tracing::info!("pair_aggregator shards={} mode=fixed", pair_shard_count);
    let mut pair_senders: Vec<mpsc::Sender<PairMessage>> = Vec::with_capacity(pair_shard_count);
    let mut pair_receivers: Vec<mpsc::Receiver<PairMessage>> = Vec::with_capacity(pair_shard_count);
    for _ in 0..pair_shard_count {
        let (tx, rx) = mpsc::channel::<PairMessage>(PAIR_QUEUE_CAPACITY);
        pair_senders.push(tx);
        pair_receivers.push(rx);
    }
    let mut opi_token_maps: Vec<HashMap<(i64, TokenSide), String>> =
        vec![HashMap::new(); pair_shard_count];
    for (key, value) in opi_token_map {
        let shard_id = pick_pair_shard(key.0, pair_shard_count);
        opi_token_maps[shard_id].insert(key, value);
    }
    let mut pm_token_maps: Vec<HashMap<(i64, TokenSide), String>> =
        vec![HashMap::new(); pair_shard_count];
    for (key, value) in pm_token_map {
        let shard_id = pick_pair_shard(key.0, pair_shard_count);
        pm_token_maps[shard_id].insert(key, value);
    }
    for (shard_id, pair_rx) in pair_receivers.into_iter().enumerate() {
        let pair_agg = PairAggregator::new(
            pair_rx,
            db_tx.clone(),
            trade_sender.clone(),
            fee_schedule.clone(),
            chain_state.clone(),
            opi_bootstrap_done.clone(),
            config.analysis_staleness_ms,
            config.analysis_gap_allow,
            config.analysis_move_eps,
            config.pair_flush_late_arrival_s,
            std::mem::take(&mut opi_token_maps[shard_id]),
            std::mem::take(&mut pm_token_maps[shard_id]),
            config.alpha_capture_enabled,
        );
        tokio::spawn(pair_agg.run());
    }

    let health = new_shared_health();
    let shard_metrics = Arc::new(ShardMetrics::new(config.shard_count));
    let shard_config = ShardConfig {
        pm_staleness_ms: config.pm_staleness_ms,
        other_staleness_ms: config.other_staleness_ms,
        hard_clear_ms: config.hard_clear_ms,
        alpha_capture_enabled: config.alpha_capture_enabled,
    };
    let dispatcher_handle = spawn_dispatcher(
        config.shard_count,
        registry_state.clone(),
        db_tx.clone(),
        Arc::new(pair_senders.clone()),
        health.clone(),
        shard_metrics.clone(),
        shard_config,
    )
    .await;

    let metrics_db_stats = db_stats.clone();
    let metrics_dispatch_tx = dispatcher_handle.sender.clone();
    let metrics_shards = shard_metrics.clone();
    let metrics_run_id = run_id;
    let metrics_pid = pid;
    let metrics_file = std::env::var("MONITOR_METRICS_FILE").ok();
    std::thread::spawn(move || {
        let start = Instant::now();
        let mut last_tick = start;
        let mut seq: u64 = 0;
        let mut file = metrics_file.and_then(|path| {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .ok()
        });
        loop {
            seq += 1;
            let now = Instant::now();
            let uptime_ms = start.elapsed().as_millis() as i64;
            let tick_lag_ms = now.duration_since(last_tick).as_millis() as i64;
            last_tick = now;
            let db_queue_len = metrics_db_stats.pending_total();
            let dispatcher_queue_len =
                DISPATCH_QUEUE_CAPACITY.saturating_sub(metrics_dispatch_tx.capacity());
            let snapshot = metrics_shards.totals();
            let line = format!(
                "runtime_metrics db_queue_len={} dispatcher_queue_len={} token_states={} orderbook_levels_total={} pending_shocks={} book_count_pm={} book_count_opi={} book_nonzero_levels_total={} max_nonzero_levels={} max_nonzero_levels_token_key={} bootstrap_progress={} run_id={} pid={} seq={} uptime_ms={} tick_lag_ms={}",
                db_queue_len,
                dispatcher_queue_len,
                snapshot.token_states,
                snapshot.orderbook_levels_total,
                snapshot.pending_shocks,
                snapshot.book_count_pm,
                snapshot.book_count_opi,
                snapshot.orderbook_levels_total,
                snapshot.max_nonzero_levels,
                snapshot.max_nonzero_levels_token_key,
                snapshot.bootstrap_progress,
                metrics_run_id,
                metrics_pid,
                seq,
                uptime_ms,
                tick_lag_ms
            );
            tracing::info!("{line}");
            if let Some(out) = file.as_mut() {
                let _ = writeln!(out, "{line}");
                let _ = out.flush();
            }
            std::thread::sleep(Duration::from_secs(5));
        }
    });

    let clock_offset = Arc::new(ClockOffset::new(
        config.clock_offset_alpha,
        config.clock_offset_max_step_ms,
    ));

    let mut polymarket_targets = Vec::new();
    for token in &registry.tokens {
        if token.venue == crate::models::Venue::Polymarket {
            if let Some(token_id) = token.external_token_id.clone() {
                let pair_ids = token_pairs
                    .get(&token.token_key)
                    .cloned()
                    .unwrap_or_default();
                polymarket_targets.push(PolymarketSnapshotTarget {
                    token_key: token.token_key.clone(),
                    token_id,
                    token_side: token.token_side,
                    market_id: token.market_id.clone(),
                    pair_ids,
                });
            }
        }
    }

    let mut opinion_targets = Vec::new();
    for token in &registry.tokens {
        if token.venue == crate::models::Venue::Opinion {
            if let Some(token_id) = token.external_token_id.clone() {
                opinion_targets.push(OpinionSnapshotTarget {
                    token_key: token.token_key.clone(),
                    token_id,
                });
            }
        }
    }
    let opinion_targets_count = opinion_targets.len();


    let ws_reconnect_stale_ms = (config.ws_reconnect_stale_sec as i64) * 1000;
    let ws_error_reconnect_window_ms = (config.ws_error_reconnect_window_sec as i64) * 1000;
    let ws_error_reconnect_threshold = config.ws_error_reconnect_threshold;
    if let (Some(opi_snapshot_trigger_tx), Some(opi_snapshot_trigger_rx)) = (
        opi_snapshot_trigger_tx.clone(),
        opi_snapshot_trigger_rx.take(),
    ) {
        let dispatcher_clone = dispatcher_handle.sender.clone();
        let db_sender = db_tx.clone();
        let clock = clock_offset.clone();
        tokio::spawn(run_opinion_snapshotter(
            config.opinion_rest_base.clone(),
            config.opinion_api_key.clone(),
            opinion_targets,
            clock,
            dispatcher_clone,
            db_sender,
            Some(opi_bootstrap_done.clone()),
            config.snapshot_interval_min,
            config.opinion_rest_bootstrap_concurrency,
            config.opinion_rest_max_rps,
            config.opinion_rest_bootstrap_batch_size,
            config.opinion_rest_bootstrap_batch_delay_ms,
            config.opinion_rest_bootstrap_max_rps,
            config.opinion_rest_trigger_batch_size,
            config.opinion_rest_trigger_batch_delay_ms,
            config.opinion_rest_trigger_max_rps,
            config.opinion_rest_ws_connect_grace_ms,
            config.opinion_rest_fast_interval_sec,
            config.opinion_rest_inactive_sec,
            config.opinion_rest_latest_price_interval_sec,
            config.opinion_rest_latest_price_all,
            config.opinion_rest_latest_price_ws_stale_sec,
            config.opinion_rest_latest_price_book_stale_sec,
            config.opinion_rest_orderbook_ws_stale_sec,
            config.opinion_rest_orderbook_staleness_ms,
            config.opinion_rest_active_only,
            config.opinion_rest_ws_primary,
            config.opinion_rest_stale_cooldown_threshold_ms,
            config.opinion_rest_stale_cooldown_duration_sec,
            config.opinion_active_window_sec,
            config.opinion_ts_max_skew_ms,
            opi_snapshot_trigger_rx,
        ));

        let opi_dispatcher = dispatcher_handle.sender.clone();
        let registry_for_opi = registry_state.clone();
        let db_sender = db_tx.clone();
        let health_opi = health.clone();
        let opi_trigger = opi_snapshot_trigger_tx.clone();
        let trade_sender_for_opi = trade_sender.clone();
        let opi_markets: Vec<String> = {
            let state = registry_for_opi.read().await;
            state.opi_market_ids.iter().cloned().collect()
        };
        let opi_markets_count = opi_markets.len();
        let opi_targets_count = opinion_targets_count;
        let expected_targets = opi_markets_count.saturating_mul(2);
        if expected_targets == 0 {
            tracing::warn!(
                "opinion coverage check targets_len={} markets_count={}",
                opi_targets_count,
                opi_markets_count
            );
        } else if opi_targets_count != expected_targets {
            tracing::warn!(
                "opinion coverage mismatch targets_len={} markets_count={} expected_targets={}",
                opi_targets_count,
                opi_markets_count,
                expected_targets
            );
        } else {
            tracing::info!(
                "opinion coverage ok targets_len={} markets_count={} expected_targets={}",
                opi_targets_count,
                opi_markets_count,
                expected_targets
            );
        }
        tokio::spawn(run_opinion_ws(
            config.opinion_ws_url.clone(),
            config.opinion_api_key.clone(),
            opi_markets,
            registry_for_opi,
            clock_offset.clone(),
            opi_dispatcher,
            trade_sender_for_opi,
            db_sender,
            health_opi,
            opi_trigger,
            config.heartbeat_interval_sec,
            ws_reconnect_stale_ms,
            config.hard_clear_ms,
            ws_error_reconnect_window_ms,
            ws_error_reconnect_threshold,
            config.opinion_ts_max_skew_ms,
        ));
    } else {
        tracing::warn!("opinion ws disabled: opinion_api_key is empty");
    }

    let (pm_snapshot_trigger_tx, pm_snapshot_trigger_rx) =
        mpsc::channel::<PmSnapshotCommand>(1024);
    let pm_dispatcher = dispatcher_handle.sender.clone();
    tokio::spawn(run_polymarket_snapshotter(
        config.polymarket_rest_base.clone(),
        polymarket_targets,
        pm_dispatcher,
        config.polymarket_rest_refresh_interval_sec,
        config.polymarket_rest_stale_threshold_sec,
        config.polymarket_rest_batch_size,
        pm_snapshot_trigger_rx,
    ));

    let pm_dispatcher = dispatcher_handle.sender.clone();
    let registry_for_pm = registry_state.clone();
    let health_pm = health.clone();
    let pm_assets = {
        let state = registry_for_pm.read().await;
        state.pm_asset_ids.iter().cloned().collect()
    };
    tokio::spawn(run_polymarket_ws(
        config.polymarket_ws_url.clone(),
        pm_assets,
        registry_for_pm,
        pm_dispatcher,
        health_pm,
        ws_reconnect_stale_ms,
        Some(pm_snapshot_trigger_tx),
    ));

    let tick_sender = dispatcher_handle.sender.clone();
    let pair_tick_senders = pair_senders.clone();
    let health_tick = health.clone();
    let db_health = db_tx.clone();
    let tick_interval_ms = config.tick_interval_ms;
    let ws_stale_alert_ms = (config.ws_stale_alert_sec as i64) * 1000;
    tokio::spawn(async move {
        let mut interval =
            tokio::time::interval(std::time::Duration::from_millis(tick_interval_ms));
        let start_ts_ms = now_ts_ms();
        let mut last_warn_pm_ms: i64 = 0;
        let mut last_warn_opi_ms: i64 = 0;
        loop {
            interval.tick().await;
            let now_ms = now_ts_ms();
            let bar_second = now_ms / 1000;
            let _ = tick_sender.send(DispatcherMessage::Tick(bar_second)).await;
            for sender in &pair_tick_senders {
                let _ = sender.send(PairMessage::Tick(bar_second)).await;
            }
            let state = health_tick.lock().await;
            let pm_row = state.snapshot(crate::models::Venue::Polymarket, bar_second);
            let opi_row = state.snapshot(crate::models::Venue::Opinion, bar_second);
            if ws_stale_alert_ms > 0 {
                maybe_warn_ws_stale(
                    crate::models::Venue::Polymarket,
                    &pm_row,
                    &mut last_warn_pm_ms,
                    now_ms,
                    start_ts_ms,
                    ws_stale_alert_ms,
                );
                maybe_warn_ws_stale(
                    crate::models::Venue::Opinion,
                    &opi_row,
                    &mut last_warn_opi_ms,
                    now_ms,
                    start_ts_ms,
                    ws_stale_alert_ms,
                );
            }
            try_send_db(&db_health, DbMessage::ConnectionHealth(pm_row), "conn_health_pm");
            try_send_db(&db_health, DbMessage::ConnectionHealth(opi_row), "conn_health_opi");
        }
    });

    tokio::signal::ctrl_c().await?;
    Ok(())
}

async fn run_healthcheck(config: &MonitorConfig) -> anyhow::Result<()> {
    let monitor_db = MonitorDb::connect(&config.monitoring_db_url).await?;
    sqlx::query("SELECT 1")
        .execute(&monitor_db.pool)
        .await?;

    ensure_column(&monitor_db.pool, "bars_1s_token", "event_count_1s").await?;
    ensure_column(&monitor_db.pool, "bars_1s_token", "ts_missing_count_1s").await?;
    ensure_column(&monitor_db.pool, "bars_1s_token", "ts_anomaly_count_1s").await?;
    ensure_column(&monitor_db.pool, "bars_1s_token", "book_resync_flag").await?;
    ensure_column(&monitor_db.pool, "bars_1s_token", "last_price").await?;
    ensure_column(&monitor_db.pool, "bars_1s_token", "best_bid_px_state").await?;
    ensure_column(&monitor_db.pool, "bars_1s_token", "best_ask_px_state").await?;
    ensure_column(&monitor_db.pool, "bars_1s_token", "mid_px_state").await?;
    ensure_column(&monitor_db.pool, "bars_1s_pair", "opi_last_price").await?;
    ensure_column(&monitor_db.pool, "bars_1s_pair", "poly_best_bid_state").await?;
    ensure_column(&monitor_db.pool, "bars_1s_pair", "poly_best_ask_state").await?;
    ensure_column(&monitor_db.pool, "bars_1s_pair", "poly_mid_state").await?;
    ensure_column(&monitor_db.pool, "bars_1s_pair", "opi_best_bid_state").await?;
    ensure_column(&monitor_db.pool, "bars_1s_pair", "opi_best_ask_state").await?;
    ensure_column(&monitor_db.pool, "bars_1s_pair", "opi_mid_state").await?;
    ensure_column(&monitor_db.pool, "raw_events", "channel").await?;
    ensure_column(&monitor_db.pool, "poly_shock_events", "status").await?;
    ensure_column(&monitor_db.pool, "poly_shock_events", "peak_finalized").await?;

    let discovery = DiscoveryLoader::connect(&config.discovery_db_path).await?;
    let (watchlist_version, pairs) = discovery.load_latest_pairs().await?;
    tracing::info!(
        "healthcheck ok: watchlist_version={} active_pairs={}",
        watchlist_version,
        pairs.len()
    );
    Ok(())
}

async fn ensure_column(pool: &sqlx::PgPool, table: &str, column: &str) -> anyhow::Result<()> {
    let row = sqlx::query(
        "SELECT 1 FROM information_schema.columns \
         WHERE table_schema = 'public' AND table_name = $1 AND column_name = $2 \
         LIMIT 1",
    )
    .bind(table)
    .bind(column)
    .fetch_optional(pool)
    .await?;
    if row.is_some() {
        Ok(())
    } else {
        Err(anyhow::anyhow!("missing column {}.{}", table, column))
    }
}

fn maybe_warn_ws_stale(
    venue: crate::models::Venue,
    row: &crate::models::ConnectionHealthRow,
    last_warn_ms: &mut i64,
    now_ms: i64,
    start_ts_ms: i64,
    threshold_ms: i64,
) {
    if row.ws_state != WsState::Up as i64 {
        return;
    }
    let age_ms = row
        .last_msg_age_ms
        .unwrap_or_else(|| (now_ms - start_ts_ms).max(0));
    if age_ms < threshold_ms {
        return;
    }
    if now_ms - *last_warn_ms < threshold_ms {
        return;
    }
    tracing::warn!(
        "ws stale venue={} last_msg_age_ms={} threshold_ms={} ws_state={} reconnect_count={}",
        venue.as_str(),
        age_ms,
        threshold_ms,
        row.ws_state,
        row.reconnect_count
    );
    *last_warn_ms = now_ms;
}

fn mask_secret(value: &str) -> String {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return "<empty>".to_string();
    }
    let chars: Vec<char> = trimmed.chars().collect();
    let len = chars.len();
    if len <= 8 {
        return format!("len={} value=***", len);
    }
    let prefix: String = chars.iter().take(4).collect();
    let suffix: String = chars
        .iter()
        .rev()
        .take(4)
        .cloned()
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    format!("len={} {}...{}", len, prefix, suffix)
}
