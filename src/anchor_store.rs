use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;

use crate::service::VfsError;

pub struct AnchorStore {
    conn: Arc<std::sync::Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct Anchor {
    pub id: String,
    pub task_id: String,
    pub summary: String,
    pub facts: String,
    pub created_at: String,
}

#[derive(Debug, Clone)]
pub struct AnchorFact {
    pub id: String,
    pub task_id: String,
    pub summary: String,
    pub facts: String,
    pub created_at: String,
}

impl AnchorStore {
    pub fn new(db_path: PathBuf) -> Result<Self, VfsError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| VfsError::Io(format!("create system dir failed: {e}")))?;
        }
        let conn = Connection::open(&db_path)
            .map_err(|e| VfsError::Sqlite(format!("open system db failed: {e}")))?;
        init_anchor_schema(&conn)?;
        Ok(Self {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        })
    }

    pub async fn create_anchor(
        &self,
        task_id: &str,
        summary: &str,
        facts: &str,
    ) -> Result<String, VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();
        let summary = summary.to_string();
        let facts = facts.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let anchor_id = generate_anchor_id();
            let now = crate::message_store::now_iso8601_pub();
            conn.execute(
                "INSERT INTO anchors (id, task_id, summary, facts, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![anchor_id, task_id, summary, facts, now],
            )
            .map_err(|e| VfsError::Sqlite(format!("insert anchor failed: {e}")))?;

            // Sync FTS
            conn.execute(
                "INSERT INTO anchors_fts(rowid, facts)
                 SELECT rowid, facts FROM anchors WHERE id = ?1 AND task_id = ?2",
                rusqlite::params![anchor_id, task_id],
            )
            .map_err(|e| VfsError::Sqlite(format!("sync anchor FTS failed: {e}")))?;

            Ok(anchor_id)
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_anchor(
        &self,
        task_id: &str,
        anchor_id: &str,
    ) -> Result<Option<Anchor>, VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();
        let anchor_id = anchor_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, task_id, summary, facts, created_at
                     FROM anchors WHERE task_id = ?1 AND id = ?2",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let mut rows = stmt
                .query_map(rusqlite::params![task_id, anchor_id], row_to_anchor)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            match rows.next() {
                Some(Ok(a)) => Ok(Some(a)),
                Some(Err(e)) => Err(VfsError::Sqlite(format!("read row failed: {e}"))),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn list_anchors(
        &self,
        task_id: &str,
    ) -> Result<Vec<Anchor>, VfsError> {
        let conn = Arc::clone(&self.conn);
        let task_id = task_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, task_id, summary, facts, created_at
                     FROM anchors WHERE task_id = ?1 ORDER BY created_at ASC",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![task_id], row_to_anchor)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn search_facts_bm25(
        &self,
        query: &str,
        limit: i32,
    ) -> Result<Vec<AnchorFact>, VfsError> {
        if query.trim().is_empty() {
            return Ok(Vec::new());
        }
        let conn = Arc::clone(&self.conn);
        let query = query.to_string();
        let limit = limit.max(1).min(50);

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT a.id, a.task_id, a.summary, a.facts, a.created_at
                     FROM anchors_fts fts
                     JOIN anchors a ON fts.rowid = a.rowid
                     WHERE anchors_fts MATCH ?1
                     ORDER BY rank
                     LIMIT ?2",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare FTS query failed: {e}")))?;
            let rows = stmt
                .query_map(rusqlite::params![query, limit], |row| {
                    Ok(AnchorFact {
                        id: row.get(0)?,
                        task_id: row.get(1)?,
                        summary: row.get(2)?,
                        facts: row.get(3)?,
                        created_at: row.get(4)?,
                    })
                })
                .map_err(|e| VfsError::Sqlite(format!("FTS query failed: {e}")))?;
            rows.collect::<Result<Vec<_>, _>>()
                .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }
}

fn init_anchor_schema(conn: &Connection) -> Result<(), VfsError> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;

         CREATE TABLE IF NOT EXISTS anchors (
           id         TEXT NOT NULL,
           task_id    TEXT NOT NULL,
           summary    TEXT NOT NULL,
           facts      TEXT NOT NULL,
           created_at TEXT NOT NULL,
           PRIMARY KEY (task_id, id)
         );
         CREATE INDEX IF NOT EXISTS idx_anchors_task ON anchors(task_id, created_at);

         CREATE VIRTUAL TABLE IF NOT EXISTS anchors_fts USING fts5(facts, content=anchors, content_rowid=rowid);",
    )
    .map_err(|e| VfsError::Sqlite(format!("init anchor schema failed: {e}")))
}

fn row_to_anchor(row: &rusqlite::Row<'_>) -> rusqlite::Result<Anchor> {
    Ok(Anchor {
        id: row.get(0)?,
        task_id: row.get(1)?,
        summary: row.get(2)?,
        facts: row.get(3)?,
        created_at: row.get(4)?,
    })
}

fn generate_anchor_id() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("anc-{nanos:x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_store(dir: &std::path::Path) -> AnchorStore {
        AnchorStore::new(dir.join("system.db")).unwrap()
    }

    #[tokio::test]
    async fn create_and_get_anchor() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        let anchor_id = store
            .create_anchor("task-1", "completed auth", "[\"fact1\", \"fact2\"]")
            .await
            .unwrap();
        assert!(!anchor_id.is_empty());

        let anchor = store.get_anchor("task-1", &anchor_id).await.unwrap().unwrap();
        assert_eq!(anchor.summary, "completed auth");
    }

    #[tokio::test]
    async fn list_anchors_for_task() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.create_anchor("task-1", "s1", "facts1").await.unwrap();
        store.create_anchor("task-1", "s2", "facts2").await.unwrap();
        store.create_anchor("task-2", "s3", "facts3").await.unwrap();

        let anchors = store.list_anchors("task-1").await.unwrap();
        assert_eq!(anchors.len(), 2);
    }

    #[tokio::test]
    async fn search_facts_bm25_works() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store
            .create_anchor("task-1", "auth done", "implemented JWT authentication with refresh tokens")
            .await
            .unwrap();
        store
            .create_anchor("task-2", "db done", "migrated database schema to PostgreSQL")
            .await
            .unwrap();

        let results = store.search_facts_bm25("JWT authentication", 10).await.unwrap();
        assert!(!results.is_empty());
        assert_eq!(results[0].task_id, "task-1");
    }

    #[tokio::test]
    async fn search_empty_query_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());
        let results = store.search_facts_bm25("", 10).await.unwrap();
        assert!(results.is_empty());
    }
}
