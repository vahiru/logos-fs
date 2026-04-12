//! Session Clustering Module — RFC 003 §5
//!
//! Implements social-topology-first session clustering with a three-layer
//! LRU memory hierarchy (L0 active → L1 inactive → L2 archived to LanceDB).
//!
//! Core insight (RFC 003 §5.1): reply chains are hard bindings;
//! semantic similarity is only a fallback when topology is absent.
//!
//! L2 backend: LanceDB with vector embeddings via Ollama for semantic fallback.

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use arrow_array::types::Float32Type;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, RecordBatchIterator, StringArray,
};
use arrow_schema::{DataType, Field, Schema};
use futures::TryStreamExt;
use lancedb::query::{ExecutableQuery, QueryBase};
use tokio::sync::Mutex;

/// Embedding dimension — qwen3-embedding:0.6b produces 1024-dim vectors.
const DEFAULT_EMBED_DIM: i32 = 1024;
const LANCEDB_OPT_ENABLE_V2_MANIFEST_PATHS: &str = "new_table_enable_v2_manifest_paths";

/// Similarity threshold for semantic fallback.
/// Sessions with L2 distance below this are considered a match.
const SEMANTIC_THRESHOLD: f32 = 0.5;

/// A reference to a message within a session.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MsgRef {
    pub msg_id: i64,
    pub chat_id: String,
    pub reply_to: Option<i64>,
    pub text: String,
    pub speaker: String,
    pub ts: String,
}

/// A session — a cluster of topologically linked messages.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Session {
    pub session_id: String,
    pub chat_id: String,
    pub messages: Vec<MsgRef>,
    pub last_active: chrono::DateTime<chrono::Utc>,
}

impl Session {
    fn new(chat_id: &str, first_msg: MsgRef) -> Self {
        let session_id = format!("s-{}-{}", chat_id, first_msg.msg_id);
        Self {
            session_id,
            chat_id: chat_id.to_string(),
            messages: vec![first_msg],
            last_active: chrono::Utc::now(),
        }
    }

    fn touch(&mut self) {
        self.last_active = chrono::Utc::now();
    }

    fn add_message(&mut self, msg: MsgRef) {
        self.messages.push(msg);
        self.touch();
    }

    pub fn to_json(&self) -> serde_json::Value {
        serde_json::to_value(self).unwrap_or(serde_json::json!(null))
    }

    /// Concatenate all message texts for embedding.
    fn text_for_embed(&self) -> String {
        self.messages
            .iter()
            .map(|m| m.text.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

struct Inner {
    msg_index: HashMap<i64, String>, // msg_id -> session_id
    session_msg_ids: HashMap<String, HashSet<i64>>, // session_id -> msg_ids for O(k) cleanup
    l0: LruLayer,
    l1: LruLayer,
    l0_capacity: usize,
    l1_capacity: usize,
    lru_ticket: u64,
}

impl Inner {
    fn next_ticket(&mut self) -> u64 {
        self.lru_ticket = self.lru_ticket.wrapping_add(1);
        self.lru_ticket
    }

    fn index_session_messages(&mut self, session: &Session) {
        let sid = session.session_id.clone();
        let entry = self.session_msg_ids.entry(sid.clone()).or_default();
        for m in &session.messages {
            self.msg_index.insert(m.msg_id, sid.clone());
            entry.insert(m.msg_id);
        }
    }

    fn index_message(&mut self, session_id: &str, msg_id: i64) {
        self.msg_index.insert(msg_id, session_id.to_string());
        self.session_msg_ids
            .entry(session_id.to_string())
            .or_default()
            .insert(msg_id);
    }

    fn remove_session_from_index(&mut self, session_id: &str) {
        if let Some(msg_ids) = self.session_msg_ids.remove(session_id) {
            for msg_id in msg_ids {
                self.msg_index.remove(&msg_id);
            }
        }
    }

    fn insert_l0(&mut self, session: Session) {
        let sid = session.session_id.clone();
        let ticket = self.next_ticket();
        self.l0.insert(sid, session, ticket);
    }

    fn append_message_to_session(&mut self, sid: &str, msg: MsgRef) -> bool {
        let ticket = self.next_ticket();
        if self
            .l0
            .with_session_mut(sid, |session| session.add_message(msg.clone()), ticket)
        {
            return true;
        }
        if let Some(mut session) = self.l1.remove(sid) {
            session.add_message(msg);
            self.l0.insert(sid.to_string(), session, ticket);
            return true;
        }
        false
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct LruEntry {
    last_active: chrono::DateTime<chrono::Utc>,
    ticket: u64,
    session_id: String,
}

impl Ord for LruEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.last_active
            .cmp(&other.last_active)
            .then_with(|| self.ticket.cmp(&other.ticket))
            .then_with(|| self.session_id.cmp(&other.session_id))
    }
}

impl PartialOrd for LruEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct LruSlot {
    session: Session,
    ticket: u64,
}

struct LruLayer {
    sessions: HashMap<String, LruSlot>,
    order: BinaryHeap<Reverse<LruEntry>>,
}

impl LruLayer {
    fn new() -> Self {
        Self {
            sessions: HashMap::new(),
            order: BinaryHeap::new(),
        }
    }

    fn len(&self) -> usize {
        self.sessions.len()
    }

    fn insert(&mut self, sid: String, session: Session, ticket: u64) {
        let last_active = session.last_active;
        self.sessions
            .insert(sid.clone(), LruSlot { session, ticket });
        self.order.push(Reverse(LruEntry {
            last_active,
            ticket,
            session_id: sid,
        }));
    }

    fn remove(&mut self, sid: &str) -> Option<Session> {
        self.sessions.remove(sid).map(|slot| slot.session)
    }

    fn get(&self, sid: &str) -> Option<&Session> {
        self.sessions.get(sid).map(|slot| &slot.session)
    }

    fn values(&self) -> impl Iterator<Item = &Session> {
        self.sessions.values().map(|slot| &slot.session)
    }

    fn with_session_mut<F>(&mut self, sid: &str, mutator: F, ticket: u64) -> bool
    where
        F: FnOnce(&mut Session),
    {
        let Some(slot) = self.sessions.get_mut(sid) else {
            return false;
        };
        mutator(&mut slot.session);
        slot.ticket = ticket;
        self.order.push(Reverse(LruEntry {
            last_active: slot.session.last_active,
            ticket,
            session_id: sid.to_string(),
        }));
        true
    }

    fn pop_oldest(&mut self) -> Option<(String, Session)> {
        while let Some(Reverse(entry)) = self.order.pop() {
            let Some(slot) = self.sessions.get(&entry.session_id) else {
                continue;
            };
            if slot.ticket != entry.ticket || slot.session.last_active != entry.last_active {
                continue;
            }
            let sid = entry.session_id;
            if let Some(slot) = self.sessions.remove(&sid) {
                return Some((sid, slot.session));
            }
        }
        None
    }
}
// =============================================================================
// L2 LanceDB Store
// =============================================================================

/// LanceDB-backed L2 persistent store with Ollama embeddings.
struct L2Store {
    db: lancedb::Connection,
    ollama_url: String,
    model: String,
    embed_dim: i32,
    http: reqwest::Client,
}

impl L2Store {
    async fn new(
        db_path: &str,
        ollama_url: &str,
        model: &str,
        embed_dim: i32,
    ) -> Result<Self, logos_vfs::VfsError> {
        let db = lancedb::connect(db_path)
            .storage_option(LANCEDB_OPT_ENABLE_V2_MANIFEST_PATHS, "true")
            .execute()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("lancedb connect: {e}")))?;

        let store = Self {
            db,
            ollama_url: ollama_url.to_string(),
            model: model.to_string(),
            embed_dim,
            http: reqwest::Client::new(),
        };

        store.ensure_tables().await?;
        Ok(store)
    }

    fn sessions_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("session_id", DataType::Utf8, false),
            Field::new("chat_id", DataType::Utf8, false),
            Field::new("data", DataType::Utf8, false),
            Field::new("last_active", DataType::Utf8, false),
            Field::new("text_for_embed", DataType::Utf8, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    self.embed_dim,
                ),
                true,
            ),
        ]))
    }

    fn msg_index_schema(&self) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("msg_id", DataType::Utf8, false),
            Field::new("session_id", DataType::Utf8, false),
        ]))
    }

    async fn ensure_tables(&self) -> Result<(), logos_vfs::VfsError> {
        let table_names = self
            .db
            .table_names()
            .execute()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("list tables: {e}")))?;

        if !table_names.contains(&"sessions".to_string()) {
            self.db
                .create_empty_table("sessions", self.sessions_schema())
                .execute()
                .await
                .map_err(|e| logos_vfs::VfsError::Io(format!("create sessions table: {e}")))?;
        }

        if !table_names.contains(&"msg_index".to_string()) {
            self.db
                .create_empty_table("msg_index", self.msg_index_schema())
                .execute()
                .await
                .map_err(|e| logos_vfs::VfsError::Io(format!("create msg_index table: {e}")))?;
        }

        Ok(())
    }

    /// Call Ollama embedding API.
    async fn embed(&self, text: &str) -> Result<Vec<f32>, logos_vfs::VfsError> {
        let url = format!("{}/api/embed", self.ollama_url);
        let body = serde_json::json!({
            "model": self.model,
            "input": text,
        });

        let resp = self
            .http
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("ollama request: {e}")))?;

        let json: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("ollama response: {e}")))?;

        let vec = json["embeddings"][0]
            .as_array()
            .ok_or_else(|| logos_vfs::VfsError::Io("ollama: no embeddings in response".into()))?
            .iter()
            .map(|v| v.as_f64().unwrap_or(0.0) as f32)
            .collect();

        Ok(vec)
    }

    async fn persist(&self, session: &Session) -> Result<(), logos_vfs::VfsError> {
        let text = session.text_for_embed();
        let vector = self.embed(&text).await?;
        let data = serde_json::to_string(session)
            .map_err(|e| logos_vfs::VfsError::Io(format!("serialize: {e}")))?;
        let last_active = session.last_active.to_rfc3339();

        let table = self.open_sessions().await?;

        let dim = self.embed_dim as usize;
        if vector.len() != dim {
            return Err(logos_vfs::VfsError::Io(format!(
                "embedding dim mismatch: expected {dim}, got {}",
                vector.len()
            )));
        }

        let vector_array = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
            std::iter::once(Some(vector.into_iter().map(Some).collect::<Vec<_>>())),
            self.embed_dim,
        );

        let batch = RecordBatch::try_new(
            self.sessions_schema(),
            vec![
                Arc::new(StringArray::from(vec![session.session_id.as_str()])),
                Arc::new(StringArray::from(vec![session.chat_id.as_str()])),
                Arc::new(StringArray::from(vec![data.as_str()])),
                Arc::new(StringArray::from(vec![last_active.as_str()])),
                Arc::new(StringArray::from(vec![text.as_str()])),
                Arc::new(vector_array),
            ],
        )
        .map_err(|e| logos_vfs::VfsError::Io(format!("record batch: {e}")))?;

        let source = RecordBatchIterator::new(vec![Ok(batch)].into_iter(), self.sessions_schema());
        let mut merge = table.merge_insert(&["session_id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        merge
            .execute(Box::new(source))
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("lancedb upsert: {e}")))?;

        // Update msg_index
        self.update_msg_index(session).await?;

        Ok(())
    }

    async fn update_msg_index(&self, session: &Session) -> Result<(), logos_vfs::VfsError> {
        let idx_table = self.open_msg_index().await?;
        let msg_ids: Vec<i64> = session.messages.iter().map(|m| m.msg_id).collect();
        if msg_ids.is_empty() {
            return Ok(());
        }

        let msg_id_strings: Vec<String> = msg_ids.iter().map(|m| m.to_string()).collect();
        let msg_id_refs: Vec<&str> = msg_id_strings.iter().map(|s| s.as_str()).collect();
        let sid_repeated: Vec<&str> = msg_ids
            .iter()
            .map(|_| session.session_id.as_str())
            .collect();

        let idx_batch = RecordBatch::try_new(
            self.msg_index_schema(),
            vec![
                Arc::new(StringArray::from(msg_id_refs)),
                Arc::new(StringArray::from(sid_repeated)),
            ],
        )
        .map_err(|e| logos_vfs::VfsError::Io(format!("msg_index batch: {e}")))?;

        let source =
            RecordBatchIterator::new(vec![Ok(idx_batch)].into_iter(), self.msg_index_schema());
        let mut merge = idx_table.merge_insert(&["msg_id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        merge
            .execute(Box::new(source))
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("msg_index upsert: {e}")))?;

        Ok(())
    }

    async fn delete(&self, session_id: &str) -> Result<(), logos_vfs::VfsError> {
        let table = self.open_sessions().await?;
        table
            .delete(&format!("session_id = '{}'", session_id))
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("lancedb delete: {e}")))?;

        let idx_table = self.open_msg_index().await?;
        idx_table
            .delete(&format!("session_id = '{}'", session_id))
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("msg_index delete: {e}")))?;

        Ok(())
    }

    async fn load_by_id(&self, session_id: &str) -> Result<Option<Session>, logos_vfs::VfsError> {
        let table = self.open_sessions().await?;
        let batches: Vec<RecordBatch> = table
            .query()
            .only_if(format!("session_id = '{}'", session_id))
            .execute()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("query: {e}")))?
            .try_collect()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("collect: {e}")))?;

        Self::extract_session(&batches)
    }

    async fn load_by_msg(&self, msg_id: i64) -> Result<Option<Session>, logos_vfs::VfsError> {
        let idx_table = self.open_msg_index().await?;
        let batches: Vec<RecordBatch> = idx_table
            .query()
            .only_if(format!("msg_id = '{}'", msg_id))
            .execute()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("idx query: {e}")))?
            .try_collect()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("idx collect: {e}")))?;

        if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                let sid_col = batch
                    .column_by_name("session_id")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>());
                if let Some(arr) = sid_col {
                    let sid = arr.value(0);
                    let loaded = self.load_by_id(sid).await?;
                    if let Some(ref session) = loaded {
                        if session.messages.iter().any(|msg| msg.msg_id == msg_id) {
                            return Ok(loaded);
                        }
                    }
                    return Ok(None);
                }
            }
        }
        Ok(None)
    }

    /// Semantic fallback: find the most similar session by vector search.
    async fn semantic_search(
        &self,
        text: &str,
        chat_id: &str,
    ) -> Result<Option<Session>, logos_vfs::VfsError> {
        let vector = self.embed(text).await?;

        let table = self.open_sessions().await?;
        let batches: Vec<RecordBatch> = table
            .query()
            .limit(1)
            .only_if(format!("chat_id = '{}'", chat_id))
            .nearest_to(vector.as_slice())
            .map_err(|e| logos_vfs::VfsError::Io(format!("vector search: {e}")))?
            .execute()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("search exec: {e}")))?
            .try_collect()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("search collect: {e}")))?;

        if let Some(batch) = batches.first() {
            if batch.num_rows() == 0 {
                return Ok(None);
            }
            // Check distance threshold
            if let Some(dist_col) = batch
                .column_by_name("_distance")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
            {
                let dist = dist_col.value(0);
                if dist > SEMANTIC_THRESHOLD {
                    return Ok(None);
                }
            }
            return Self::extract_session(&batches);
        }

        Ok(None)
    }

    fn extract_session(batches: &[RecordBatch]) -> Result<Option<Session>, logos_vfs::VfsError> {
        if let Some(batch) = batches.first() {
            if batch.num_rows() > 0 {
                let data_col = batch
                    .column_by_name("data")
                    .and_then(|c| c.as_any().downcast_ref::<StringArray>());
                if let Some(arr) = data_col {
                    let json_str = arr.value(0);
                    let session: Session = serde_json::from_str(json_str)
                        .map_err(|e| logos_vfs::VfsError::Io(format!("deserialize: {e}")))?;
                    return Ok(Some(session));
                }
            }
        }
        Ok(None)
    }

    async fn open_sessions(&self) -> Result<lancedb::Table, logos_vfs::VfsError> {
        self.db
            .open_table("sessions")
            .execute()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("open sessions: {e}")))
    }

    async fn open_msg_index(&self) -> Result<lancedb::Table, logos_vfs::VfsError> {
        self.db
            .open_table("msg_index")
            .execute()
            .await
            .map_err(|e| logos_vfs::VfsError::Io(format!("open msg_index: {e}")))
    }
}

// =============================================================================
// SessionStore — public API
// =============================================================================

/// Session store with three-layer LRU: L0 (active) → L1 (inactive) → L2 (LanceDB).
pub struct SessionStore {
    inner: Mutex<Inner>,
    l2: Option<L2Store>,
}

impl SessionStore {
    /// Create an in-memory-only session store (no L2 persistence).
    pub fn new(l0_capacity: usize, l1_capacity: usize) -> Self {
        Self {
            inner: Mutex::new(Inner {
                msg_index: HashMap::new(),
                session_msg_ids: HashMap::new(),
                l0: LruLayer::new(),
                l1: LruLayer::new(),
                l0_capacity,
                l1_capacity,
                lru_ticket: 0,
            }),
            l2: None,
        }
    }

    /// Create a session store with LanceDB L2 persistence and Ollama embeddings.
    pub async fn with_lancedb(
        db_path: impl AsRef<std::path::Path>,
        ollama_url: &str,
        model: &str,
        embed_dim: i32,
        l0_capacity: usize,
        l1_capacity: usize,
    ) -> Result<Self, logos_vfs::VfsError> {
        if let Some(parent) = db_path.as_ref().parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| logos_vfs::VfsError::Io(format!("create dir: {e}")))?;
        }

        let l2 = L2Store::new(
            db_path.as_ref().to_str().unwrap_or("sessions.lance"),
            ollama_url,
            model,
            embed_dim,
        )
        .await?;

        Ok(Self {
            inner: Mutex::new(Inner {
                msg_index: HashMap::new(),
                session_msg_ids: HashMap::new(),
                l0: LruLayer::new(),
                l1: LruLayer::new(),
                l0_capacity,
                l1_capacity,
                lru_ticket: 0,
            }),
            l2: Some(l2),
        })
    }

    /// Observe a new message and update session topology.
    pub async fn observe(&self, msg: MsgRef) {
        let msg_id = msg.msg_id;
        let reply_to = msg.reply_to;

        // Page fault: load from L2 outside the lock
        let page_fault_session = if let Some(reply_to) = reply_to {
            let need_fault = {
                let inner = self.inner.lock().await;
                inner.msg_index.get(&reply_to).is_none()
            };
            if need_fault {
                if let Some(ref l2) = self.l2 {
                    match l2.load_by_msg(reply_to).await {
                        Ok(s) => s,
                        Err(e) => {
                            eprintln!("[logos-session] L2 load_by_msg failed: {e}");
                            None
                        }
                    }
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        // Delete from L2 outside the lock (if we loaded a session)
        if let Some(ref session) = page_fault_session {
            if let Some(ref l2) = self.l2 {
                if let Err(e) = l2.delete(&session.session_id).await {
                    eprintln!("[logos-session] L2 delete (page fault) failed: {e}");
                }
            }
        }

        // Semantic fallback: when no reply_to and no page fault, try vector search
        let semantic_match = if reply_to.is_none() && page_fault_session.is_none() {
            if let Some(ref l2) = self.l2 {
                match l2.semantic_search(&msg.text, &msg.chat_id).await {
                    Ok(s) => s,
                    Err(e) => {
                        eprintln!("[logos-session] L2 semantic_search failed: {e}");
                        None
                    }
                }
            } else {
                None
            }
        } else {
            None
        };

        // If semantic match found, delete from L2 (we'll promote to L0)
        if let Some(ref session) = semantic_match {
            if let Some(ref l2) = self.l2 {
                if let Err(e) = l2.delete(&session.session_id).await {
                    eprintln!("[logos-session] L2 delete (semantic) failed: {e}");
                }
            }
        }

        // Now take the lock for in-memory operations only
        let evicted = {
            let mut inner = self.inner.lock().await;

            if let Some(mut session) = page_fault_session {
                // Page fault from L2: restore to L0
                inner.index_session_messages(&session);
                session.add_message(msg.clone());
                inner.index_message(&session.session_id, msg_id);
                inner.insert_l0(session);
                Self::evict_collect(&mut inner)
            } else if let Some(reply_to) = reply_to {
                // Reply chain: join existing in-memory session
                if let Some(sid) = inner.msg_index.get(&reply_to).cloned() {
                    if inner.append_message_to_session(&sid, msg.clone()) {
                        inner.index_message(&sid, msg_id);
                        Self::evict_collect(&mut inner)
                    } else {
                        let chat_id = msg.chat_id.clone();
                        let session = Session::new(&chat_id, msg);
                        let sid = session.session_id.clone();
                        inner.index_message(&sid, msg_id);
                        inner.insert_l0(session);
                        Self::evict_collect(&mut inner)
                    }
                } else {
                    let chat_id = msg.chat_id.clone();
                    let session = Session::new(&chat_id, msg);
                    let sid = session.session_id.clone();
                    inner.index_message(&sid, msg_id);
                    inner.insert_l0(session);
                    Self::evict_collect(&mut inner)
                }
            } else if let Some(mut session) = semantic_match {
                // Semantic fallback: matched an L2 session by vector similarity
                inner.index_session_messages(&session);
                session.add_message(msg.clone());
                inner.index_message(&session.session_id, msg_id);
                inner.insert_l0(session);
                Self::evict_collect(&mut inner)
            } else {
                // No reply, no semantic match: create new session
                let chat_id = msg.chat_id.clone();
                let session = Session::new(&chat_id, msg);
                let sid = session.session_id.clone();
                inner.index_message(&sid, msg_id);
                inner.insert_l0(session);
                Self::evict_collect(&mut inner)
            }
        };

        // Persist evicted sessions to L2 outside the lock
        if let Some(ref l2) = self.l2 {
            for session in evicted {
                if let Err(e) = l2.persist(&session).await {
                    eprintln!(
                        "[logos-session] L2 persist failed for {}: {e}",
                        session.session_id
                    );
                }
            }
        }
    }

    fn evict_collect(inner: &mut Inner) -> Vec<Session> {
        while inner.l0.len() > inner.l0_capacity {
            if let Some((sid, session)) = inner.l0.pop_oldest() {
                let ticket = inner.next_ticket();
                inner.l1.insert(sid, session, ticket);
            } else {
                break;
            }
        }
        let mut evicted = Vec::new();
        while inner.l1.len() > inner.l1_capacity {
            if let Some((sid, session)) = inner.l1.pop_oldest() {
                inner.remove_session_from_index(&sid);
                evicted.push(session);
            } else {
                break;
            }
        }
        evicted
    }

    pub async fn get_session_for_msg(&self, msg_id: i64) -> Option<Session> {
        let inner = self.inner.lock().await;
        if let Some(sid) = inner.msg_index.get(&msg_id) {
            if let Some(s) = inner.l0.get(sid) {
                return Some(s.clone());
            }
            if let Some(s) = inner.l1.get(sid) {
                return Some(s.clone());
            }
        }
        drop(inner);
        // Try L2
        if let Some(ref l2) = self.l2 {
            match l2.load_by_msg(msg_id).await {
                Ok(s) => return s,
                Err(e) => eprintln!("[logos-session] L2 load_by_msg failed: {e}"),
            }
        }
        None
    }

    pub async fn get_session(&self, session_id: &str) -> Option<Session> {
        let inner = self.inner.lock().await;
        if let Some(s) = inner.l0.get(session_id) {
            return Some(s.clone());
        }
        if let Some(s) = inner.l1.get(session_id) {
            return Some(s.clone());
        }
        drop(inner);
        if let Some(ref l2) = self.l2 {
            match l2.load_by_id(session_id).await {
                Ok(s) => return s,
                Err(e) => eprintln!("[logos-session] L2 load_by_id failed: {e}"),
            }
        }
        None
    }

    pub async fn get_active_session(&self, chat_id: &str) -> Option<Session> {
        let inner = self.inner.lock().await;
        inner
            .l0
            .values()
            .filter(|s| s.chat_id == chat_id)
            .max_by_key(|s| s.last_active)
            .cloned()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn make_msg(msg_id: i64, chat_id: &str, reply_to: Option<i64>) -> MsgRef {
        MsgRef {
            msg_id,
            chat_id: chat_id.to_string(),
            reply_to,
            text: format!("msg-{msg_id}"),
            speaker: "test".to_string(),
            ts: "2026-03-20T10:00:00Z".to_string(),
        }
    }

    // --- Pure in-memory tests (no L2, no Ollama) ---

    #[tokio::test]
    async fn new_message_creates_session() {
        let store = SessionStore::new(64, 256);
        store.observe(make_msg(1, "c1", None)).await;
        let s = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(s.messages.len(), 1);
    }

    #[tokio::test]
    async fn reply_joins_existing_session() {
        let store = SessionStore::new(64, 256);
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", Some(1))).await;
        let s = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(s.messages.len(), 2);
        assert_eq!(
            s.session_id,
            store.get_session_for_msg(2).await.unwrap().session_id
        );
    }

    #[tokio::test]
    async fn no_reply_creates_separate_session() {
        let store = SessionStore::new(64, 256);
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", None)).await;
        let s1 = store.get_session_for_msg(1).await.unwrap();
        let s2 = store.get_session_for_msg(2).await.unwrap();
        assert_ne!(s1.session_id, s2.session_id);
    }

    #[tokio::test]
    async fn deep_reply_chain() {
        let store = SessionStore::new(64, 256);
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", Some(1))).await;
        store.observe(make_msg(3, "c1", Some(2))).await;
        let s = store.get_session_for_msg(3).await.unwrap();
        assert_eq!(s.messages.len(), 3);
    }

    #[tokio::test]
    async fn lru_eviction_l0_to_l1() {
        let store = SessionStore::new(2, 256);
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", None)).await;
        store.observe(make_msg(3, "c1", None)).await;
        assert!(store.get_session_for_msg(1).await.is_some());
        assert!(store.get_session_for_msg(2).await.is_some());
        assert!(store.get_session_for_msg(3).await.is_some());
    }

    #[tokio::test]
    async fn reply_promotes_from_l1() {
        let store = SessionStore::new(2, 256);
        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", None)).await;
        store.observe(make_msg(3, "c1", None)).await;
        store.observe(make_msg(4, "c1", Some(1))).await;
        let s = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(s.messages.len(), 2);
    }

    // --- L2 LanceDB tests (require Ollama) ---

    #[tokio::test]
    #[ignore] // requires Ollama running with qwen3-embedding:0.6b
    async fn l2_persistence_and_page_fault() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("sessions.lance");
        let store = SessionStore::with_lancedb(
            &db_path,
            "http://localhost:11434",
            "qwen3-embedding:0.6b",
            DEFAULT_EMBED_DIM,
            2,
            2,
        )
        .await
        .unwrap();

        store.observe(make_msg(1, "c1", None)).await;
        store.observe(make_msg(2, "c1", None)).await;
        store.observe(make_msg(3, "c1", None)).await;
        store.observe(make_msg(4, "c1", None)).await;
        store.observe(make_msg(5, "c1", None)).await;

        // Reply to msg1 (should page fault from L2)
        store.observe(make_msg(6, "c1", Some(1))).await;
        let s = store.get_session_for_msg(1).await.unwrap();
        assert_eq!(s.messages.len(), 2);
    }

    #[tokio::test]
    #[ignore] // requires Ollama
    async fn l2_semantic_fallback() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("sessions.lance");
        let store = SessionStore::with_lancedb(
            &db_path,
            "http://localhost:11434",
            "qwen3-embedding:0.6b",
            DEFAULT_EMBED_DIM,
            2,
            2,
        )
        .await
        .unwrap();

        let mut msg1 = make_msg(1, "c1", None);
        msg1.text = "我在学习Rust编程语言，所有权系统很有趣".to_string();
        store.observe(msg1).await;

        // Force to L2
        store.observe(make_msg(10, "c1", None)).await;
        store.observe(make_msg(11, "c1", None)).await;
        store.observe(make_msg(12, "c1", None)).await;
        store.observe(make_msg(13, "c1", None)).await;

        // Semantically similar, no reply_to
        let mut msg2 = make_msg(20, "c1", None);
        msg2.text = "Rust的借用检查器太严格了".to_string();
        store.observe(msg2).await;

        let s = store.get_session_for_msg(20).await.unwrap();
        println!(
            "Semantic fallback: session has {} messages",
            s.messages.len()
        );
    }
}
