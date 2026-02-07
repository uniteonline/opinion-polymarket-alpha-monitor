use crate::db_writer::{run_db_writer, DbAck, DbEnvelope};
pub use crate::db_writer::DbMessage;
use crate::time_utils::now_ts_ms;
use anyhow::Context;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{QueryBuilder, Row, Sqlite, SqlitePool};
use std::collections::VecDeque;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio::time::Duration;
use tracing::warn;

const DB_WRITER_CHANNEL_CAPACITY: usize = 4096;
const DB_QUEUE_INSERT_BATCH: usize = 400;
const DB_QUEUE_FETCH_BATCH: usize = 1000;
const DB_QUEUE_ACK_BATCH: usize = 500;
const DB_QUEUE_INSERT_INTERVAL_MS: u64 = 50;
const DB_QUEUE_SEND_INTERVAL_MS: u64 = 20;
const DB_QUEUE_ACK_INTERVAL_MS: u64 = 200;

#[derive(Default)]
pub struct DbQueueStats {
    in_mem: AtomicUsize,
    disk_pending: AtomicUsize,
    in_flight: AtomicUsize,
}

impl DbQueueStats {
    pub fn pending_total(&self) -> usize {
        self.in_mem.load(Ordering::Relaxed) + self.disk_pending.load(Ordering::Relaxed)
    }

    pub fn in_mem(&self) -> usize {
        self.in_mem.load(Ordering::Relaxed)
    }

    pub fn disk_pending(&self) -> usize {
        self.disk_pending.load(Ordering::Relaxed)
    }

    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct DbSender {
    tx: mpsc::UnboundedSender<DbMessage>,
}

static DB_SEND_FAIL_COUNTER: AtomicUsize = AtomicUsize::new(0);

pub fn try_send_db(sender: &DbSender, msg: DbMessage, label: &'static str) {
    if sender.tx.send(msg).is_err() {
        let count = DB_SEND_FAIL_COUNTER.fetch_add(1, Ordering::Relaxed) + 1;
        if count == 1 || count % 1000 == 0 {
            warn!("db_queue_send_fail count={} label={} err=closed", count, label);
        }
    }
}

pub async fn spawn_db_queue(
    pool: sqlx::PgPool,
    alpha_capture_enabled: bool,
    queue_path: &str,
) -> anyhow::Result<(DbSender, Arc<DbQueueStats>)> {
    let sqlite_pool = init_queue_db(queue_path).await?;
    let stats = Arc::new(DbQueueStats::default());
    let (count,): (i64,) = sqlx::query_as("SELECT COUNT(1) FROM db_queue")
        .fetch_one(&sqlite_pool)
        .await
        .context("db_queue count")?;
    stats
        .disk_pending
        .store(count.max(0) as usize, Ordering::Relaxed);

    let (ingest_tx, ingest_rx) = mpsc::unbounded_channel();
    let (writer_tx, writer_rx) = mpsc::channel(DB_WRITER_CHANNEL_CAPACITY);
    let (ack_tx, ack_rx) = mpsc::unbounded_channel();

    tokio::spawn(run_db_writer(
        writer_rx,
        ack_tx,
        pool,
        alpha_capture_enabled,
    ));

    tokio::spawn(run_db_spooler(
        ingest_rx,
        writer_tx,
        ack_rx,
        sqlite_pool,
        stats.clone(),
    ));

    Ok((DbSender { tx: ingest_tx }, stats))
}

async fn init_queue_db(path: &str) -> anyhow::Result<SqlitePool> {
    if let Some(parent) = Path::new(path).parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!("db_queue create dir failed path={}", parent.display())
        })?;
    }
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(true);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .with_context(|| format!("db_queue connect failed path={}", path))?;
    sqlx::query("PRAGMA journal_mode = WAL;")
        .execute(&pool)
        .await
        .ok();
    sqlx::query("PRAGMA synchronous = NORMAL;")
        .execute(&pool)
        .await
        .ok();
    sqlx::query("PRAGMA busy_timeout = 5000;")
        .execute(&pool)
        .await
        .ok();
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS db_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          payload BLOB NOT NULL,
          created_ms INTEGER NOT NULL
        );
        "#,
    )
    .execute(&pool)
    .await
    .context("db_queue create table")?;
    Ok(pool)
}

async fn run_db_spooler(
    mut ingest_rx: mpsc::UnboundedReceiver<DbMessage>,
    db_writer_tx: mpsc::Sender<DbEnvelope>,
    mut ack_rx: mpsc::UnboundedReceiver<DbAck>,
    sqlite_pool: SqlitePool,
    stats: Arc<DbQueueStats>,
) {
    let mut insert_buf: Vec<DbMessage> = Vec::new();
    let mut pending_send: VecDeque<DbEnvelope> = VecDeque::new();
    let mut ack_buf: Vec<i64> = Vec::new();
    let mut last_sent_id: i64 = 0;

    let mut insert_tick = tokio::time::interval(Duration::from_millis(DB_QUEUE_INSERT_INTERVAL_MS));
    let mut send_tick = tokio::time::interval(Duration::from_millis(DB_QUEUE_SEND_INTERVAL_MS));
    let mut ack_tick = tokio::time::interval(Duration::from_millis(DB_QUEUE_ACK_INTERVAL_MS));

    loop {
        tokio::select! {
            Some(msg) = ingest_rx.recv() => {
                insert_buf.push(msg);
                stats.in_mem.fetch_add(1, Ordering::Relaxed);
                if insert_buf.len() >= DB_QUEUE_INSERT_BATCH {
                    flush_insert(&sqlite_pool, &mut insert_buf, &stats).await;
                }
            }
            Some(ack_id) = ack_rx.recv() => {
                ack_buf.push(ack_id);
                stats.in_flight.fetch_sub(1, Ordering::Relaxed);
                if ack_buf.len() >= DB_QUEUE_ACK_BATCH {
                    flush_ack(&sqlite_pool, &mut ack_buf, &stats).await;
                }
            }
            _ = insert_tick.tick() => {
                flush_insert(&sqlite_pool, &mut insert_buf, &stats).await;
            }
            _ = send_tick.tick() => {
                pump_send(&sqlite_pool, &db_writer_tx, &mut pending_send, &mut last_sent_id, &stats).await;
            }
            _ = ack_tick.tick() => {
                flush_ack(&sqlite_pool, &mut ack_buf, &stats).await;
            }
        }
    }
}

async fn flush_insert(
    sqlite_pool: &SqlitePool,
    insert_buf: &mut Vec<DbMessage>,
    stats: &DbQueueStats,
) {
    if insert_buf.is_empty() {
        return;
    }

    let mut batch = std::mem::take(insert_buf);
    let now_ms = now_ts_ms();
    let mut serializable: Vec<(DbMessage, Vec<u8>)> = Vec::with_capacity(batch.len());
    for msg in batch.drain(..) {
        match serde_json::to_vec(&msg) {
            Ok(payload) => serializable.push((msg, payload)),
            Err(err) => {
                warn!("db_queue serialize err={}", err);
                stats.in_mem.fetch_sub(1, Ordering::Relaxed);
            }
        }
    }

    if serializable.is_empty() {
        return;
    }

    let mut qb: QueryBuilder<Sqlite> =
        QueryBuilder::new("INSERT INTO db_queue (payload, created_ms) ");
    qb.push_values(serializable.iter(), |mut b, (_, payload)| {
        b.push_bind(payload).push_bind(now_ms);
    });

    match qb.build().execute(sqlite_pool).await {
        Ok(_) => {
            let inserted = serializable.len();
            stats.in_mem.fetch_sub(inserted, Ordering::Relaxed);
            stats.disk_pending.fetch_add(inserted, Ordering::Relaxed);
        }
        Err(err) => {
            warn!("db_queue insert err={}", err);
            insert_buf.extend(serializable.into_iter().map(|(msg, _)| msg));
        }
    }
}

async fn pump_send(
    sqlite_pool: &SqlitePool,
    db_writer_tx: &mpsc::Sender<DbEnvelope>,
    pending_send: &mut VecDeque<DbEnvelope>,
    last_sent_id: &mut i64,
    stats: &DbQueueStats,
) {
    while let Some(env) = pending_send.pop_front() {
        match db_writer_tx.try_send(env) {
            Ok(()) => {
                stats.in_flight.fetch_add(1, Ordering::Relaxed);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(env)) => {
                pending_send.push_front(env);
                return;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                warn!("db_queue db_writer channel closed");
                return;
            }
        }
    }

    let capacity = db_writer_tx.capacity();
    if capacity == 0 {
        return;
    }
    if pending_send.len() >= DB_QUEUE_FETCH_BATCH {
        return;
    }
    let mut fetch_limit = DB_QUEUE_FETCH_BATCH.saturating_sub(pending_send.len());
    fetch_limit = fetch_limit.min(capacity);
    if fetch_limit == 0 {
        return;
    }

    let rows = match sqlx::query("SELECT id, payload FROM db_queue WHERE id > ? ORDER BY id LIMIT ?")
        .bind(*last_sent_id)
        .bind(fetch_limit as i64)
        .fetch_all(sqlite_pool)
        .await
    {
        Ok(rows) => rows,
        Err(err) => {
            warn!("db_queue fetch err={}", err);
            return;
        }
    };

    if rows.is_empty() {
        return;
    }

    let mut bad_ids: Vec<i64> = Vec::new();
    for row in rows {
        let id: i64 = row.get("id");
        let payload: Vec<u8> = row.get("payload");
        match serde_json::from_slice::<DbMessage>(&payload) {
            Ok(msg) => {
                pending_send.push_back(DbEnvelope { id, msg });
                *last_sent_id = id;
            }
            Err(err) => {
                warn!("db_queue decode err id={} err={}", id, err);
                bad_ids.push(id);
                *last_sent_id = id;
            }
        }
    }

    if !bad_ids.is_empty() {
        let deleted = delete_ids(sqlite_pool, &bad_ids).await;
        if deleted > 0 {
            stats.disk_pending.fetch_sub(deleted, Ordering::Relaxed);
        }
    }

    while let Some(env) = pending_send.pop_front() {
        match db_writer_tx.try_send(env) {
            Ok(()) => {
                stats.in_flight.fetch_add(1, Ordering::Relaxed);
            }
            Err(tokio::sync::mpsc::error::TrySendError::Full(env)) => {
                pending_send.push_front(env);
                break;
            }
            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                warn!("db_queue db_writer channel closed");
                break;
            }
        }
    }
}

async fn flush_ack(
    sqlite_pool: &SqlitePool,
    ack_buf: &mut Vec<i64>,
    stats: &DbQueueStats,
) {
    if ack_buf.is_empty() {
        return;
    }

    let mut ids = std::mem::take(ack_buf);
    ids.sort_unstable();
    ids.dedup();

    let mut remaining: Vec<i64> = Vec::new();
    for chunk in ids.chunks(DB_QUEUE_ACK_BATCH) {
        let deleted = delete_ids(sqlite_pool, chunk).await;
        if deleted == 0 {
            remaining.extend_from_slice(chunk);
        } else {
            stats.disk_pending.fetch_sub(deleted, Ordering::Relaxed);
        }
    }

    if !remaining.is_empty() {
        ack_buf.extend(remaining);
    }
}

async fn delete_ids(sqlite_pool: &SqlitePool, ids: &[i64]) -> usize {
    if ids.is_empty() {
        return 0;
    }
    let mut qb: QueryBuilder<Sqlite> = QueryBuilder::new("DELETE FROM db_queue WHERE id IN (");
    let mut separated = qb.separated(", ");
    for id in ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");
    match qb.build().execute(sqlite_pool).await {
        Ok(result) => result.rows_affected() as usize,
        Err(err) => {
            warn!("db_queue delete err={}", err);
            0
        }
    }
}
