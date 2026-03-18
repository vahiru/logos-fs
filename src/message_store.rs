use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::Mutex;

use crate::service::VfsError;

const RANGE_FETCH_DEFAULT_LIMIT: i32 = 50;
const RANGE_FETCH_MAX_LIMIT: i32 = 200;
const FTS_DEFAULT_LIMIT: i32 = 10;
const FTS_MAX_LIMIT: i32 = 50;

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub msg_id: i64,
    pub external_id: String,
    pub ts: String,
    pub chat_id: String,
    pub speaker: String,
    pub reply_to: Option<i64>,
    pub text: String,
    pub mentions: Vec<String>,
    pub session_id: String,
}

pub struct MessageStore {
    db_root: PathBuf,
    connections: Mutex<HashMap<String, Arc<std::sync::Mutex<Connection>>>>,
}

impl MessageStore {
    pub fn new(db_root: PathBuf) -> std::io::Result<Self> {
        std::fs::create_dir_all(&db_root)?;
        Ok(Self {
            db_root,
            connections: Mutex::new(HashMap::new()),
        })
    }

    pub async fn insert_messages(
        &self,
        chat_id: &str,
        session_id: &str,
        messages: &[InsertMessage],
    ) -> Result<(), VfsError> {
        if messages.is_empty() {
            return Ok(());
        }
        let conn = self.get_connection(chat_id).await?;
        let session_id = session_id.to_string();
        let messages: Vec<InsertMessage> = messages.to_vec();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                VfsError::Sqlite(format!("failed to acquire sqlite lock: {e}"))
            })?;
            let tx = conn.unchecked_transaction().map_err(|e| {
                VfsError::Sqlite(format!("begin transaction failed: {e}"))
            })?;

            {
                let mut insert_stmt = tx
                    .prepare_cached(
                        "INSERT INTO messages (external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id)
                         VALUES (?1, ?2, ?3, ?4, NULL, ?5, ?6, ?7)",
                    )
                    .map_err(|e| VfsError::Sqlite(format!("prepare insert failed: {e}")))?;

                for msg in &messages {
                    let mentions_json = serde_json::to_string(&msg.mentions)
                        .unwrap_or_else(|_| "[]".to_string());
                    insert_stmt.execute(rusqlite::params![
                        msg.external_id,
                        msg.ts,
                        msg.chat_id,
                        msg.speaker,
                        msg.text,
                        mentions_json,
                        session_id,
                    ])
                    .map_err(|e| VfsError::Sqlite(format!("insert message failed: {e}")))?;
                }
            }

            // Second pass: resolve reply_to external_id -> internal msg_id (RFC 003 S2.1)
            {
                let mut resolve_stmt = tx
                    .prepare_cached(
                        "SELECT msg_id FROM messages WHERE chat_id = ?1 AND external_id = ?2 LIMIT 1",
                    )
                    .map_err(|e| VfsError::Sqlite(format!("prepare resolve failed: {e}")))?;
                let mut update_stmt = tx
                    .prepare_cached(
                        "UPDATE messages SET reply_to = ?1 WHERE chat_id = ?2 AND external_id = ?3",
                    )
                    .map_err(|e| VfsError::Sqlite(format!("prepare update failed: {e}")))?;

                for msg in &messages {
                    if msg.reply_to.is_empty() {
                        continue;
                    }
                    let resolved: Option<i64> = resolve_stmt
                        .query_row(
                            rusqlite::params![msg.chat_id, msg.reply_to],
                            |row| row.get(0),
                        )
                        .ok();
                    if let Some(ref_msg_id) = resolved {
                        update_stmt
                            .execute(rusqlite::params![ref_msg_id, msg.chat_id, msg.external_id])
                            .map_err(|e| VfsError::Sqlite(format!("update reply_to failed: {e}")))?;
                    }
                }
            }

            sync_fts_for_session(&tx, &session_id)?;

            tx.commit()
                .map_err(|e| VfsError::Sqlite(format!("commit failed: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_message_by_id(
        &self,
        chat_id: &str,
        msg_id: i64,
    ) -> Result<Option<StoredMessage>, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                VfsError::Sqlite(format!("failed to acquire sqlite lock: {e}"))
            })?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT msg_id, external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id
                     FROM messages WHERE msg_id = ?1",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let mut rows = stmt
                .query_map(rusqlite::params![msg_id], row_to_stored_message)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            match rows.next() {
                Some(Ok(m)) => Ok(Some(m)),
                Some(Err(e)) => Err(VfsError::Sqlite(format!("read row failed: {e}"))),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_messages_by_session(
        &self,
        chat_id: &str,
        session_id: &str,
    ) -> Result<Vec<StoredMessage>, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        let session_id = session_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                VfsError::Sqlite(format!("failed to acquire sqlite lock: {e}"))
            })?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT msg_id, external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id
                     FROM messages WHERE session_id = ?1 ORDER BY msg_id ASC",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare query failed: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![session_id], row_to_stored_message)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            collect_rows(rows)
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn range_fetch(
        &self,
        chat_id: &str,
        ranges: &[(i64, i64)],
        limit: i32,
        offset: i32,
    ) -> Result<Vec<StoredMessage>, VfsError> {
        if chat_id.trim().is_empty() {
            return Err(VfsError::InvalidRequest(
                "chat_id cannot be empty".to_string(),
            ));
        }
        if ranges.is_empty() {
            return Ok(Vec::new());
        }

        let limit = clamp(limit, 1, RANGE_FETCH_DEFAULT_LIMIT, RANGE_FETCH_MAX_LIMIT);
        let offset = offset.max(0);

        let conn = self.get_connection(chat_id).await?;
        let ranges = ranges.to_vec();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                VfsError::Sqlite(format!("failed to acquire sqlite lock: {e}"))
            })?;

            let mut conditions = Vec::with_capacity(ranges.len());
            let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();
            for (start, end) in &ranges {
                let base = params.len();
                conditions.push(format!("(msg_id >= ?{} AND msg_id <= ?{})", base + 1, base + 2));
                params.push(Box::new(*start));
                params.push(Box::new(*end));
            }
            let where_clause = conditions.join(" OR ");
            let sql = format!(
                "SELECT msg_id, external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id
                 FROM messages WHERE ({}) ORDER BY msg_id ASC LIMIT ?{} OFFSET ?{}",
                where_clause,
                params.len() + 1,
                params.len() + 2,
            );
            params.push(Box::new(limit));
            params.push(Box::new(offset));

            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| VfsError::Sqlite(format!("prepare range_fetch failed: {e}")))?;
            let rows = stmt
                .query_map(param_refs.as_slice(), row_to_stored_message)
                .map_err(|e| VfsError::Sqlite(format!("range_fetch query failed: {e}")))?;
            collect_rows(rows)
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn search_fts(
        &self,
        chat_id: &str,
        query: &str,
        limit: i32,
    ) -> Result<Vec<StoredMessage>, VfsError> {
        if chat_id.trim().is_empty() {
            return Err(VfsError::InvalidRequest(
                "chat_id cannot be empty for FTS search".to_string(),
            ));
        }
        if query.trim().is_empty() {
            return Err(VfsError::InvalidRequest(
                "query cannot be empty for FTS search".to_string(),
            ));
        }

        let limit = clamp(limit, 1, FTS_DEFAULT_LIMIT, FTS_MAX_LIMIT);
        let conn = self.get_connection(chat_id).await?;
        let query = query.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn.lock().map_err(|e| {
                VfsError::Sqlite(format!("failed to acquire sqlite lock: {e}"))
            })?;

            let mut stmt = conn
                .prepare_cached(
                    "SELECT m.msg_id, m.external_id, m.ts, m.chat_id, m.speaker, m.reply_to, m.text, m.mentions, m.session_id
                     FROM messages_fts fts
                     JOIN messages m ON fts.rowid = m.msg_id
                     WHERE messages_fts MATCH ?1
                     ORDER BY rank
                     LIMIT ?2",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare FTS query failed: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![query, limit], row_to_stored_message)
                .map_err(|e| VfsError::Sqlite(format!("FTS query failed: {e}")))?;
            collect_rows(rows)
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    async fn get_connection(
        &self,
        chat_id: &str,
    ) -> Result<Arc<std::sync::Mutex<Connection>>, VfsError> {
        let mut map = self.connections.lock().await;
        if let Some(conn) = map.get(chat_id) {
            return Ok(Arc::clone(conn));
        }

        let db_path = self.db_root.join(format!("{chat_id}.db"));
        let conn = Connection::open(&db_path).map_err(|e| {
            VfsError::Sqlite(format!(
                "open sqlite {}: {e}",
                db_path.display()
            ))
        })?;
        init_schema(&conn)?;
        let conn = Arc::new(std::sync::Mutex::new(conn));
        map.insert(chat_id.to_string(), Arc::clone(&conn));
        Ok(conn)
    }
}

#[derive(Debug, Clone)]
pub struct InsertMessage {
    pub external_id: String,
    pub ts: String,
    pub chat_id: String,
    pub speaker: String,
    pub reply_to: String,
    pub text: String,
    pub mentions: Vec<String>,
}

fn init_schema(conn: &Connection) -> Result<(), VfsError> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;

         CREATE TABLE IF NOT EXISTS messages (
           msg_id       INTEGER PRIMARY KEY AUTOINCREMENT,
           external_id  TEXT NOT NULL,
           ts           TEXT NOT NULL,
           chat_id      TEXT NOT NULL,
           speaker      TEXT NOT NULL,
           reply_to     INTEGER REFERENCES messages(msg_id),
           text         TEXT NOT NULL,
           mentions     TEXT,
           session_id   TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages(chat_id, ts);
         CREATE INDEX IF NOT EXISTS idx_messages_reply_to ON messages(reply_to);
         CREATE INDEX IF NOT EXISTS idx_messages_session ON messages(session_id);
         CREATE INDEX IF NOT EXISTS idx_messages_external ON messages(chat_id, external_id);

         CREATE TABLE IF NOT EXISTS summaries (
           id            INTEGER PRIMARY KEY AUTOINCREMENT,
           chat_id       TEXT NOT NULL,
           layer         TEXT NOT NULL,
           period_start  TEXT NOT NULL,
           period_end    TEXT NOT NULL,
           source_refs   TEXT NOT NULL,
           content       TEXT NOT NULL,
           generated_at  TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_summaries_chat_layer ON summaries(chat_id, layer, period_start);

         CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(text, content=messages, content_rowid=msg_id);",
    )
    .map_err(|e| VfsError::Sqlite(format!("init schema failed: {e}")))
}

fn sync_fts_for_session(
    tx: &rusqlite::Transaction<'_>,
    session_id: &str,
) -> Result<(), VfsError> {
    tx.execute(
        "INSERT INTO messages_fts(messages_fts) VALUES('rebuild')",
        [],
    )
    .map_err(|e| {
        VfsError::Sqlite(format!(
            "FTS rebuild after session {session_id} failed: {e}"
        ))
    })?;
    Ok(())
}

fn row_to_stored_message(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredMessage> {
    let mentions_raw: String = row.get(7)?;
    let mentions: Vec<String> = serde_json::from_str(&mentions_raw).unwrap_or_default();
    Ok(StoredMessage {
        msg_id: row.get(0)?,
        external_id: row.get(1)?,
        ts: row.get(2)?,
        chat_id: row.get(3)?,
        speaker: row.get(4)?,
        reply_to: row.get::<_, Option<i64>>(5)?,
        text: row.get(6)?,
        mentions,
        session_id: row.get(8)?,
    })
}

fn collect_rows(
    rows: rusqlite::MappedRows<'_, impl FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<StoredMessage>>,
) -> Result<Vec<StoredMessage>, VfsError> {
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
}

// --- Summary types and methods ---

#[derive(Debug, Clone)]
pub struct Summary {
    pub id: i64,
    pub chat_id: String,
    pub layer: String,
    pub period_start: String,
    pub period_end: String,
    pub source_refs: String,
    pub content: String,
    pub generated_at: String,
}

pub struct InsertSummary {
    pub layer: String,
    pub period_start: String,
    pub period_end: String,
    pub source_refs: String,
    pub content: String,
}

impl MessageStore {
    pub async fn insert_summary(
        &self,
        chat_id: &str,
        summary: &InsertSummary,
    ) -> Result<i64, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        let chat_id = chat_id.to_string();
        let layer = summary.layer.clone();
        let period_start = summary.period_start.clone();
        let period_end = summary.period_end.clone();
        let source_refs = summary.source_refs.clone();
        let content = summary.content.clone();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let now = now_iso8601();
            conn.execute(
                "INSERT INTO summaries (chat_id, layer, period_start, period_end, source_refs, content, generated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                rusqlite::params![chat_id, layer, period_start, period_end, source_refs, content, now],
            )
            .map_err(|e| VfsError::Sqlite(format!("insert summary failed: {e}")))?;
            Ok(conn.last_insert_rowid())
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_summary(
        &self,
        chat_id: &str,
        layer: &str,
        period_start: &str,
    ) -> Result<Option<Summary>, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        let layer = layer.to_string();
        let period_start = period_start.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
                     FROM summaries WHERE layer = ?1 AND period_start = ?2
                     ORDER BY id DESC LIMIT 1",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let mut rows = stmt
                .query_map(rusqlite::params![layer, period_start], row_to_summary)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            match rows.next() {
                Some(Ok(s)) => Ok(Some(s)),
                Some(Err(e)) => Err(VfsError::Sqlite(format!("read row failed: {e}"))),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_latest_summary(
        &self,
        chat_id: &str,
        layer: &str,
    ) -> Result<Option<Summary>, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        let layer = layer.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
                     FROM summaries WHERE layer = ?1
                     ORDER BY period_start DESC LIMIT 1",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let mut rows = stmt
                .query_map(rusqlite::params![layer], row_to_summary)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            match rows.next() {
                Some(Ok(s)) => Ok(Some(s)),
                Some(Err(e)) => Err(VfsError::Sqlite(format!("read row failed: {e}"))),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_summaries_by_period_prefix(
        &self,
        chat_id: &str,
        layer: &str,
        prefix: &str,
    ) -> Result<Vec<Summary>, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        let layer = layer.to_string();
        let pattern = format!("{prefix}%");

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
                     FROM summaries WHERE layer = ?1 AND period_start LIKE ?2
                     ORDER BY period_start DESC",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![layer, pattern], row_to_summary)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn list_summaries(
        &self,
        chat_id: &str,
        layer: &str,
        start: Option<&str>,
        end: Option<&str>,
        limit: i32,
    ) -> Result<Vec<Summary>, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        let layer = layer.to_string();
        let start = start.map(|s| s.to_string());
        let end = end.map(|s| s.to_string());
        let limit = clamp(limit, 1, 50, 200);

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;

            let (sql, params) = build_list_summaries_query(&layer, &start, &end, limit);
            let mut stmt = conn
                .prepare(&sql)
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                params.iter().map(|p| p.as_ref()).collect();
            let rows = stmt
                .query_map(param_refs.as_slice(), row_to_summary)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn delete_summaries_before(
        &self,
        chat_id: &str,
        layer: &str,
        before: &str,
    ) -> Result<usize, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        let layer = layer.to_string();
        let before = before.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            conn.execute(
                "DELETE FROM summaries WHERE layer = ?1 AND period_start < ?2",
                rusqlite::params![layer, before],
            )
            .map_err(|e| VfsError::Sqlite(format!("delete summaries failed: {e}")))
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_messages_by_ts_range(
        &self,
        chat_id: &str,
        ts_start: &str,
        ts_end: &str,
    ) -> Result<Vec<StoredMessage>, VfsError> {
        let conn = self.get_connection(chat_id).await?;
        let ts_start = ts_start.to_string();
        let ts_end = ts_end.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT msg_id, external_id, ts, chat_id, speaker, reply_to, text, mentions, session_id
                     FROM messages WHERE ts >= ?1 AND ts < ?2 ORDER BY msg_id ASC",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![ts_start, ts_end], row_to_stored_message)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            collect_rows(rows)
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub fn list_chat_ids(&self) -> Result<Vec<String>, VfsError> {
        let mut ids = Vec::new();
        let entries = std::fs::read_dir(&self.db_root)
            .map_err(|e| VfsError::Io(format!("read db_root failed: {e}")))?;
        for entry in entries {
            let entry = entry.map_err(|e| VfsError::Io(format!("read entry failed: {e}")))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(chat_id) = name.strip_suffix(".db") {
                if !chat_id.is_empty() {
                    ids.push(chat_id.to_string());
                }
            }
        }
        Ok(ids)
    }
}

fn row_to_summary(row: &rusqlite::Row<'_>) -> rusqlite::Result<Summary> {
    Ok(Summary {
        id: row.get(0)?,
        chat_id: row.get(1)?,
        layer: row.get(2)?,
        period_start: row.get(3)?,
        period_end: row.get(4)?,
        source_refs: row.get(5)?,
        content: row.get(6)?,
        generated_at: row.get(7)?,
    })
}

fn build_list_summaries_query(
    layer: &str,
    start: &Option<String>,
    end: &Option<String>,
    limit: i32,
) -> (String, Vec<Box<dyn rusqlite::types::ToSql>>) {
    let mut conditions = vec!["layer = ?1".to_string()];
    let mut params: Vec<Box<dyn rusqlite::types::ToSql>> = vec![Box::new(layer.to_string())];

    if let Some(s) = start {
        params.push(Box::new(s.clone()));
        conditions.push(format!("period_start >= ?{}", params.len()));
    }
    if let Some(e) = end {
        params.push(Box::new(e.clone()));
        conditions.push(format!("period_start <= ?{}", params.len()));
    }
    params.push(Box::new(limit));
    let sql = format!(
        "SELECT id, chat_id, layer, period_start, period_end, source_refs, content, generated_at
         FROM summaries WHERE {} ORDER BY period_start DESC LIMIT ?{}",
        conditions.join(" AND "),
        params.len()
    );
    (sql, params)
}

pub fn now_iso8601_pub() -> String {
    now_iso8601()
}

fn now_iso8601() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = secs / 86400;
    let tod = secs % 86400;
    let (y, mo, d) = civil_from_days_u(days as i64);
    format!(
        "{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}Z",
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60,
    )
}

pub fn civil_from_days_u(days: i64) -> (i32, u32, u32) {
    let z = days + 719468;
    let era = (if z >= 0 { z } else { z - 146096 }) / 146097;
    let doe = (z - era * 146097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y as i32, m, d)
}

fn clamp(value: i32, min: i32, default: i32, max: i32) -> i32 {
    if value <= 0 {
        return default;
    }
    value.max(min).min(max)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn make_test_store(dir: &Path) -> MessageStore {
        MessageStore::new(dir.to_path_buf()).unwrap()
    }

    fn make_messages(n: usize, chat_id: &str) -> Vec<InsertMessage> {
        (0..n)
            .map(|i| InsertMessage {
                external_id: format!("ext-{i}"),
                ts: format!("2026-03-18T{:02}:00:00Z", i % 24),
                chat_id: chat_id.to_string(),
                speaker: format!("user-{}", i % 3),
                reply_to: String::new(),
                text: format!("message number {i} about rust and databases"),
                mentions: vec![],
            })
            .collect()
    }

    #[tokio::test]
    async fn insert_and_get_by_session() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(5, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .get_messages_by_session("chat-1", "session-a")
            .await
            .unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].external_id, "ext-0");
        assert_eq!(result[4].external_id, "ext-4");
        assert_eq!(result[0].session_id, "session-a");
    }

    #[tokio::test]
    async fn range_fetch_basic() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(10, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .range_fetch("chat-1", &[(1, 5)], 50, 0)
            .await
            .unwrap();
        assert_eq!(result.len(), 5);
        assert_eq!(result[0].msg_id, 1);
        assert_eq!(result[4].msg_id, 5);
    }

    #[tokio::test]
    async fn range_fetch_multiple_ranges() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(10, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .range_fetch("chat-1", &[(1, 3), (7, 9)], 50, 0)
            .await
            .unwrap();
        assert_eq!(result.len(), 6);
    }

    #[tokio::test]
    async fn range_fetch_with_offset() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(10, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .range_fetch("chat-1", &[(1, 10)], 3, 2)
            .await
            .unwrap();
        assert_eq!(result.len(), 3);
        assert_eq!(result[0].msg_id, 3);
    }

    #[tokio::test]
    async fn fts_search() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let messages = make_messages(5, "chat-1");

        store
            .insert_messages("chat-1", "session-a", &messages)
            .await
            .unwrap();

        let result = store
            .search_fts("chat-1", "rust databases", 10)
            .await
            .unwrap();
        assert!(!result.is_empty());
    }

    #[tokio::test]
    async fn per_group_isolation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store
            .insert_messages("chat-1", "s1", &make_messages(3, "chat-1"))
            .await
            .unwrap();
        store
            .insert_messages("chat-2", "s2", &make_messages(5, "chat-2"))
            .await
            .unwrap();

        let r1 = store
            .get_messages_by_session("chat-1", "s1")
            .await
            .unwrap();
        let r2 = store
            .get_messages_by_session("chat-2", "s2")
            .await
            .unwrap();
        assert_eq!(r1.len(), 3);
        assert_eq!(r2.len(), 5);

        let r_cross = store
            .get_messages_by_session("chat-1", "s2")
            .await
            .unwrap();
        assert_eq!(r_cross.len(), 0);
    }

    #[tokio::test]
    async fn empty_ranges_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        let result = store.range_fetch("chat-1", &[], 50, 0).await.unwrap();
        assert!(result.is_empty());
    }

    #[tokio::test]
    async fn fts_empty_query_rejected() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        let err = store.search_fts("chat-1", "  ", 10).await;
        assert!(err.is_err());
    }

    #[tokio::test]
    async fn summary_crud() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        let s = InsertSummary {
            layer: "short".to_string(),
            period_start: "2026-03-18T14".to_string(),
            period_end: "2026-03-18T15".to_string(),
            source_refs: "[[1,50]]".to_string(),
            content: "Summary of hour 14.".to_string(),
        };
        let id = store.insert_summary("chat-1", &s).await.unwrap();
        assert!(id > 0);

        let fetched = store
            .get_summary("chat-1", "short", "2026-03-18T14")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(fetched.content, "Summary of hour 14.");
        assert_eq!(fetched.id, id);
    }

    #[tokio::test]
    async fn summary_get_latest() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        for hour in 10..15 {
            store
                .insert_summary(
                    "chat-1",
                    &InsertSummary {
                        layer: "short".to_string(),
                        period_start: format!("2026-03-18T{hour:02}"),
                        period_end: format!("2026-03-18T{:02}", hour + 1),
                        source_refs: "[]".to_string(),
                        content: format!("Hour {hour}"),
                    },
                )
                .await
                .unwrap();
        }

        let latest = store
            .get_latest_summary("chat-1", "short")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(latest.period_start, "2026-03-18T14");
    }

    #[tokio::test]
    async fn summary_list_with_range() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        for hour in 10..20 {
            store
                .insert_summary(
                    "chat-1",
                    &InsertSummary {
                        layer: "short".to_string(),
                        period_start: format!("2026-03-18T{hour:02}"),
                        period_end: format!("2026-03-18T{:02}", hour + 1),
                        source_refs: "[]".to_string(),
                        content: format!("Hour {hour}"),
                    },
                )
                .await
                .unwrap();
        }

        let results = store
            .list_summaries("chat-1", "short", Some("2026-03-18T12"), Some("2026-03-18T16"), 50)
            .await
            .unwrap();
        assert_eq!(results.len(), 5);
    }

    #[tokio::test]
    async fn summary_delete_before() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        for hour in 10..15 {
            store
                .insert_summary(
                    "chat-1",
                    &InsertSummary {
                        layer: "short".to_string(),
                        period_start: format!("2026-03-18T{hour:02}"),
                        period_end: format!("2026-03-18T{:02}", hour + 1),
                        source_refs: "[]".to_string(),
                        content: format!("Hour {hour}"),
                    },
                )
                .await
                .unwrap();
        }

        let deleted = store
            .delete_summaries_before("chat-1", "short", "2026-03-18T13")
            .await
            .unwrap();
        assert_eq!(deleted, 3);

        let remaining = store
            .list_summaries("chat-1", "short", None, None, 50)
            .await
            .unwrap();
        assert_eq!(remaining.len(), 2);
    }

    #[tokio::test]
    async fn list_chat_ids_returns_db_names() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store
            .insert_messages("group-a", "s1", &make_messages(1, "group-a"))
            .await
            .unwrap();
        store
            .insert_messages("group-b", "s1", &make_messages(1, "group-b"))
            .await
            .unwrap();

        let mut ids = store.list_chat_ids().unwrap();
        ids.sort();
        assert_eq!(ids, vec!["group-a", "group-b"]);
    }
}
