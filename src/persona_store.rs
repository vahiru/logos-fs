use std::path::PathBuf;
use std::sync::Arc;

use rusqlite::Connection;

use crate::service::VfsError;

pub struct PersonaStore {
    conn: Arc<std::sync::Mutex<Connection>>,
}

#[derive(Debug, Clone)]
pub struct PersonaEntry {
    pub id: i64,
    pub user_id: String,
    pub layer: String,
    pub period: String,
    pub content: String,
    pub created_at: String,
}

impl PersonaStore {
    pub fn new(db_path: PathBuf) -> Result<Self, VfsError> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| VfsError::Io(format!("create persona dir failed: {e}")))?;
        }
        let conn = Connection::open(&db_path)
            .map_err(|e| VfsError::Sqlite(format!("open persona db failed: {e}")))?;
        init_persona_schema(&conn)?;
        Ok(Self {
            conn: Arc::new(std::sync::Mutex::new(conn)),
        })
    }

    pub async fn append_short(
        &self,
        user_id: &str,
        period: &str,
        content: &str,
    ) -> Result<(), VfsError> {
        let conn = Arc::clone(&self.conn);
        let user_id = user_id.to_string();
        let period = period.to_string();
        let content = content.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let now = crate::message_store::now_iso8601_pub();
            conn.execute(
                "INSERT INTO persona (user_id, layer, period, content, created_at)
                 VALUES (?1, 'short', ?2, ?3, ?4)",
                rusqlite::params![user_id, period, content, now],
            )
            .map_err(|e| VfsError::Sqlite(format!("insert persona short failed: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn write_mid(
        &self,
        user_id: &str,
        period: &str,
        content: &str,
    ) -> Result<(), VfsError> {
        let conn = Arc::clone(&self.conn);
        let user_id = user_id.to_string();
        let period = period.to_string();
        let content = content.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let now = crate::message_store::now_iso8601_pub();
            conn.execute(
                "DELETE FROM persona WHERE user_id = ?1 AND layer = 'mid' AND period = ?2",
                rusqlite::params![user_id, period],
            )
            .map_err(|e| VfsError::Sqlite(format!("delete old mid failed: {e}")))?;
            conn.execute(
                "INSERT INTO persona (user_id, layer, period, content, created_at)
                 VALUES (?1, 'mid', ?2, ?3, ?4)",
                rusqlite::params![user_id, period, content, now],
            )
            .map_err(|e| VfsError::Sqlite(format!("insert persona mid failed: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn write_long(
        &self,
        user_id: &str,
        content: &str,
    ) -> Result<(), VfsError> {
        let conn = Arc::clone(&self.conn);
        let user_id = user_id.to_string();
        let content = content.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let now = crate::message_store::now_iso8601_pub();
            conn.execute(
                "DELETE FROM persona WHERE user_id = ?1 AND layer = 'long'",
                rusqlite::params![user_id],
            )
            .map_err(|e| VfsError::Sqlite(format!("delete old long failed: {e}")))?;
            conn.execute(
                "INSERT INTO persona (user_id, layer, period, content, created_at)
                 VALUES (?1, 'long', '', ?2, ?3)",
                rusqlite::params![user_id, content, now],
            )
            .map_err(|e| VfsError::Sqlite(format!("insert persona long failed: {e}")))?;
            Ok(())
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_latest_mid(
        &self,
        user_id: &str,
    ) -> Result<Option<PersonaEntry>, VfsError> {
        let conn = Arc::clone(&self.conn);
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, user_id, layer, period, content, created_at
                     FROM persona WHERE user_id = ?1 AND layer = 'mid'
                     ORDER BY period DESC LIMIT 1",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let mut rows = stmt
                .query_map(rusqlite::params![user_id], row_to_persona)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            match rows.next() {
                Some(Ok(e)) => Ok(Some(e)),
                Some(Err(e)) => Err(VfsError::Sqlite(format!("read row failed: {e}"))),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn get_long(
        &self,
        user_id: &str,
    ) -> Result<Option<PersonaEntry>, VfsError> {
        let conn = Arc::clone(&self.conn);
        let user_id = user_id.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;
            let mut stmt = conn
                .prepare_cached(
                    "SELECT id, user_id, layer, period, content, created_at
                     FROM persona WHERE user_id = ?1 AND layer = 'long'
                     ORDER BY id DESC LIMIT 1",
                )
                .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
            let mut rows = stmt
                .query_map(rusqlite::params![user_id], row_to_persona)
                .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
            match rows.next() {
                Some(Ok(e)) => Ok(Some(e)),
                Some(Err(e)) => Err(VfsError::Sqlite(format!("read row failed: {e}"))),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }

    pub async fn list_short(
        &self,
        user_id: &str,
        since: &str,
    ) -> Result<Vec<PersonaEntry>, VfsError> {
        let conn = Arc::clone(&self.conn);
        let user_id = user_id.to_string();
        let since = since.to_string();

        tokio::task::spawn_blocking(move || {
            let conn = conn
                .lock()
                .map_err(|e| VfsError::Sqlite(format!("lock failed: {e}")))?;

            if since.is_empty() {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT id, user_id, layer, period, content, created_at
                         FROM persona WHERE user_id = ?1 AND layer = 'short'
                         ORDER BY period DESC",
                    )
                    .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
                let rows = stmt
                    .query_map(rusqlite::params![user_id], row_to_persona)
                    .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
            } else {
                let mut stmt = conn
                    .prepare_cached(
                        "SELECT id, user_id, layer, period, content, created_at
                         FROM persona WHERE user_id = ?1 AND layer = 'short' AND period >= ?2
                         ORDER BY period DESC",
                    )
                    .map_err(|e| VfsError::Sqlite(format!("prepare failed: {e}")))?;
                let rows = stmt
                    .query_map(rusqlite::params![user_id, since], row_to_persona)
                    .map_err(|e| VfsError::Sqlite(format!("query failed: {e}")))?;
                rows.collect::<Result<Vec<_>, _>>()
                    .map_err(|e| VfsError::Sqlite(format!("read rows failed: {e}")))
            }
        })
        .await
        .map_err(|e| VfsError::Io(format!("spawn_blocking join failed: {e}")))?
    }
}

fn init_persona_schema(conn: &Connection) -> Result<(), VfsError> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;

         CREATE TABLE IF NOT EXISTS persona (
           id         INTEGER PRIMARY KEY AUTOINCREMENT,
           user_id    TEXT NOT NULL,
           layer      TEXT NOT NULL,
           period     TEXT NOT NULL,
           content    TEXT NOT NULL,
           created_at TEXT NOT NULL
         );
         CREATE INDEX IF NOT EXISTS idx_persona_user_layer ON persona(user_id, layer, period);",
    )
    .map_err(|e| VfsError::Sqlite(format!("init persona schema failed: {e}")))
}

fn row_to_persona(row: &rusqlite::Row<'_>) -> rusqlite::Result<PersonaEntry> {
    Ok(PersonaEntry {
        id: row.get(0)?,
        user_id: row.get(1)?,
        layer: row.get(2)?,
        period: row.get(3)?,
        content: row.get(4)?,
        created_at: row.get(5)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_store(dir: &std::path::Path) -> PersonaStore {
        PersonaStore::new(dir.join("personas.db")).unwrap()
    }

    #[tokio::test]
    async fn short_append_and_list() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.append_short("user-1", "2026-03-18T14", "likes rust").await.unwrap();
        store.append_short("user-1", "2026-03-18T15", "uses vim").await.unwrap();

        let entries = store.list_short("user-1", "").await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].period, "2026-03-18T15");
    }

    #[tokio::test]
    async fn mid_write_replaces() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.write_mid("user-1", "2026-03-18", "v1").await.unwrap();
        store.write_mid("user-1", "2026-03-18", "v2").await.unwrap();

        let entry = store.get_latest_mid("user-1").await.unwrap().unwrap();
        assert_eq!(entry.content, "v2");
    }

    #[tokio::test]
    async fn long_write_replaces() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.write_long("user-1", "v1").await.unwrap();
        store.write_long("user-1", "v2").await.unwrap();

        let entry = store.get_long("user-1").await.unwrap().unwrap();
        assert_eq!(entry.content, "v2");
    }

    #[tokio::test]
    async fn user_isolation() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.append_short("user-1", "2026-03-18T14", "data1").await.unwrap();
        store.append_short("user-2", "2026-03-18T14", "data2").await.unwrap();

        let u1 = store.list_short("user-1", "").await.unwrap();
        let u2 = store.list_short("user-2", "").await.unwrap();
        assert_eq!(u1.len(), 1);
        assert_eq!(u2.len(), 1);
        assert_eq!(u1[0].content, "data1");
        assert_eq!(u2[0].content, "data2");
    }

    #[tokio::test]
    async fn list_short_with_since_filter() {
        let tmp = tempfile::tempdir().unwrap();
        let store = make_test_store(tmp.path());

        store.append_short("user-1", "2026-03-18T10", "old").await.unwrap();
        store.append_short("user-1", "2026-03-18T14", "new").await.unwrap();

        let entries = store.list_short("user-1", "2026-03-18T12").await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].content, "new");
    }
}
