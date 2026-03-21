mod messages;
mod summaries;

use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};

use messages::MessageDb;
use summaries::SummaryDb;

/// The Memory Module — handles `logos://memory/`.
///
/// URI routing:
///   logos://memory/groups/{gid}/messages/{msg_id}         → read single message
///   logos://memory/groups/{gid}/messages                  → write (insert) message
///   logos://memory/groups/{gid}/messages/search/{query}[/{limit}]  → FTS search
///   logos://memory/groups/{gid}/messages/range            → read (range_fetch, JSON body via write)
///   logos://memory/groups/{gid}/summary/{layer}/latest    → read latest summary
///   logos://memory/groups/{gid}/summary/{layer}/{period}  → read/write summary
pub struct MemoryModule {
    messages: MessageDb,
    // Summaries use the same per-group SQLite databases as messages.
    // We access them via MessageDb's connection pool.
    db_root: PathBuf,
}

impl MemoryModule {
    pub fn init(db_root: PathBuf) -> Result<Self, VfsError> {
        let messages = MessageDb::new(db_root.clone())?;
        Ok(Self { messages, db_root })
    }

    /// Get or create a SummaryDb for a given chat_id.
    /// Summaries live in the same per-group database as messages.
    async fn summary_db(&self, chat_id: &str) -> Result<SummaryDb, VfsError> {
        // We reuse the connection from MessageDb by creating a new connection
        // to the same database file. This is safe because SQLite handles
        // concurrent access via its own locking.
        let db_path = self.db_root.join(format!("{chat_id}.db"));
        let conn = rusqlite::Connection::open(&db_path)
            .map_err(|e| VfsError::Sqlite(format!("open summary db: {e}")))?;
        SummaryDb::new(Arc::new(std::sync::Mutex::new(conn)))
    }
}

#[async_trait]
impl Namespace for MemoryModule {
    fn name(&self) -> &str {
        "memory"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        // path: ["groups", gid, resource, ...]
        if path.len() < 3 || path[0] != "groups" {
            return Err(VfsError::InvalidPath(
                "expected logos://memory/groups/{gid}/...".to_string(),
            ));
        }
        let gid = path[1];
        let resource = path[2];

        match resource {
            "messages" => {
                if let Some(sub) = path.get(3) {
                    if *sub == "search" {
                        let query = path.get(4).unwrap_or(&"");
                        let limit: i64 = path.get(5).and_then(|s| s.parse().ok()).unwrap_or(10);
                        return self.messages.search_fts(gid, query, limit).await;
                    }
                    if *sub == "range" {
                        // logos://memory/groups/{gid}/messages/range/{ranges}[/{limit}[/{offset}]]
                        // ranges format: "89-142,201-234"
                        let range_str = path.get(4).unwrap_or(&"");
                        let limit: i64 = path.get(5).and_then(|s| s.parse().ok()).unwrap_or(50);
                        let offset: i64 = path.get(6).and_then(|s| s.parse().ok()).unwrap_or(0);
                        let ranges: Vec<Vec<i64>> = range_str
                            .split(',')
                            .filter_map(|r| {
                                let parts: Vec<&str> = r.split('-').collect();
                                if parts.len() == 2 {
                                    Some(vec![parts[0].parse().ok()?, parts[1].parse().ok()?])
                                } else {
                                    None
                                }
                            })
                            .collect();
                        let params = serde_json::json!({
                            "ranges": ranges, "limit": limit, "offset": offset
                        });
                        return self.messages.range_fetch(gid, &params.to_string()).await;
                    }
                    // Assume numeric msg_id
                    let msg_id: i64 = sub
                        .parse()
                        .map_err(|_| VfsError::InvalidPath(format!("invalid msg_id: {sub}")))?;
                    return self
                        .messages
                        .get_by_id(gid, msg_id)
                        .await
                        .map(|opt| opt.unwrap_or_else(|| "null".to_string()));
                }
                Err(VfsError::InvalidPath("missing msg_id or sub-resource".to_string()))
            }
            "summary" => {
                let layer = path.get(3).ok_or_else(|| {
                    VfsError::InvalidPath("missing summary layer".to_string())
                })?;
                let period = path.get(4).copied().unwrap_or("latest");
                let db = self.summary_db(gid).await?;
                db.read(gid, layer, period)
                    .await
                    .map(|opt| opt.unwrap_or_else(|| "null".to_string()))
            }
            _ => Err(VfsError::InvalidPath(format!(
                "unknown memory resource: {resource}"
            ))),
        }
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        if path.len() < 3 || path[0] != "groups" {
            return Err(VfsError::InvalidPath(
                "expected logos://memory/groups/{gid}/...".to_string(),
            ));
        }
        let gid = path[1];
        let resource = path[2];

        match resource {
            "messages" => {
                if path.get(3).map(|s| *s) == Some("range") {
                    // range_fetch: write params, read results (overloaded for simplicity)
                    return Err(VfsError::InvalidPath(
                        "use read for range_fetch".to_string(),
                    ));
                }
                self.messages.insert(gid, content).await
            }
            "summary" => {
                if path.len() < 5 {
                    return Err(VfsError::InvalidPath(
                        "expected logos://memory/groups/{gid}/summary/{layer}/{period}".to_string(),
                    ));
                }
                let db = self.summary_db(gid).await?;
                db.write(gid, content).await
            }
            _ => Err(VfsError::InvalidPath(format!(
                "unknown memory resource for write: {resource}"
            ))),
        }
    }

    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), VfsError> {
        // For now, patch is the same as write for memory namespace
        self.write(path, partial).await
    }
}
