use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

use tonic::{Request, Response, Status};

use crate::embedding::EmbeddingProvider;
use crate::message_store::MessageStore;
use crate::pb::{
    memory_vfs_server::MemoryVfs, ArchiveRequest, ArchiveResponse, PatchRequest, PatchResponse,
    RangeFetchRequest, RangeFetchResponse, ReadRequest, ReadResponse, ReadSummaryRequest,
    ReadSummaryResponse, SearchMode, SearchRequest, SearchResponse, WriteRequest, WriteResponse,
    WriteSummaryRequest, WriteSummaryResponse, ListSummariesRequest, ListSummariesResponse,
    ReadPersonaRequest, ReadPersonaResponse, WritePersonaRequest, WritePersonaResponse,
    CreateAnchorRequest, CreateAnchorResponse, SearchTasksRequest, SearchTasksResponse,
};
use crate::sessions_store::SessionsStore;
use crate::users_store::UsersStore;

pub struct MemoryVfsService {
    users: UsersStore,
    sessions: SessionsStore,
    messages: Arc<MessageStore>,
    persona_store: Option<Arc<crate::persona_store::PersonaStore>>,
    anchor_store: Option<Arc<crate::anchor_store::AnchorStore>>,
    task_store: Option<Arc<crate::task_store::TaskStore>>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum VfsError {
    #[error("invalid path: {0}")]
    InvalidPath(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("invalid json: {0}")]
    InvalidJson(String),
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("http error: {0}")]
    Http(String),
    #[error("lancedb error: {0}")]
    Lance(String),
    #[error("sqlite error: {0}")]
    Sqlite(String),
}

impl From<VfsError> for Status {
    fn from(e: VfsError) -> Status {
        match &e {
            VfsError::InvalidPath(_) | VfsError::InvalidRequest(_) | VfsError::InvalidJson(_) => {
                Status::invalid_argument(e.to_string())
            }
            VfsError::NotFound(_) => Status::not_found(e.to_string()),
            _ => Status::internal(e.to_string()),
        }
    }
}

impl MemoryVfsService {
    pub fn new(
        users_root: PathBuf,
        embedder: Arc<dyn EmbeddingProvider>,
        messages: Arc<MessageStore>,
    ) -> std::io::Result<Self> {
        let users = UsersStore::new(users_root)?;
        let state_root = users
            .users_root()
            .parent()
            .map(|p| p.to_path_buf())
            .unwrap_or_else(|| PathBuf::from("."));
        let lancedb_dir = state_root.join("lancedb");
        std::fs::create_dir_all(&lancedb_dir)?;

        Ok(Self {
            users,
            sessions: SessionsStore::new(
                lancedb_dir.to_string_lossy().to_string(),
                embedder,
                Arc::clone(&messages),
            ),
            messages,
            persona_store: None,
            anchor_store: None,
            task_store: None,
        })
    }

    async fn route_read(&self, uri: &crate::router::LogosUri) -> Result<String, VfsError> {
        match uri.namespace.as_str() {
            "users" => {
                let user_path = format!("mem://users/{}", uri.path.join("/"));
                // Check if this is a persona read
                if uri.path.len() >= 2 && uri.path[1] == "persona" {
                    if let Some(ref ps) = self.persona_store {
                        let user_id = &uri.path[0];
                        let layer = uri.path.get(2).map(|s| s.as_str()).unwrap_or("long");
                        let entries = match layer {
                            "long" => ps.get_long(user_id).await?.into_iter().collect(),
                            "mid" => ps.get_latest_mid(user_id).await?.into_iter().collect(),
                            "short" => ps.list_short(user_id, "").await?,
                            _ => return Err(VfsError::InvalidPath(format!("unknown persona layer: {layer}"))),
                        };
                        return Ok(serde_json::to_string(&entries.iter().map(|e| &e.content).collect::<Vec<_>>())
                            .unwrap_or_else(|_| "[]".to_string()));
                    }
                    return Err(VfsError::InvalidRequest("persona store not initialized".to_string()));
                }
                self.users.read(&user_path).await
            }
            "memory" => {
                // logos://memory/groups/{gid}/summary/{layer}/{period}
                if uri.path.len() >= 4 && uri.path[0] == "groups" && uri.path[2] == "summary" {
                    let chat_id = &uri.path[1];
                    let layer = &uri.path[3];
                    let period = uri.path.get(4).map(|s| s.as_str()).unwrap_or("");
                    let summary = if period.is_empty() {
                        self.messages.get_latest_summary(chat_id, layer).await?
                    } else {
                        self.messages.get_summary(chat_id, layer, period).await?
                    };
                    return Ok(summary
                        .map(|s| serde_json::to_string(&serde_json::json!({
                            "id": s.id, "layer": s.layer,
                            "period_start": s.period_start, "period_end": s.period_end,
                            "content": s.content, "generated_at": s.generated_at,
                        })).unwrap_or_default())
                        .unwrap_or_else(|| "null".to_string()));
                }
                Err(VfsError::InvalidPath(format!("unrecognized memory path: {}", uri.path.join("/"))))
            }
            "system" => {
                if uri.path.first().map(|s| s.as_str()) == Some("tasks") {
                    if let Some(ref ts) = self.task_store {
                        if let Some(task_id) = uri.path.get(1) {
                            let task = ts.get_task(task_id).await?;
                            return Ok(task
                                .map(|t| serde_json::to_string(&serde_json::json!({
                                    "task_id": t.task_id, "description": t.description,
                                    "status": t.status, "chat_id": t.chat_id,
                                })).unwrap_or_default())
                                .unwrap_or_else(|| "null".to_string()));
                        }
                        let tasks = ts.list_tasks(None, None).await?;
                        return Ok(serde_json::to_string(&tasks.iter().map(|t| &t.task_id).collect::<Vec<_>>())
                            .unwrap_or_else(|_| "[]".to_string()));
                    }
                    return Err(VfsError::InvalidRequest("task store not initialized".to_string()));
                }
                if uri.path.first().map(|s| s.as_str()) == Some("anchors") {
                    if let Some(ref a) = self.anchor_store {
                        if let Some(task_id) = uri.path.get(1) {
                            if let Some(anchor_id) = uri.path.get(2) {
                                let anchor = a.get_anchor(task_id, anchor_id).await?;
                                return Ok(anchor
                                    .map(|a| serde_json::to_string(&serde_json::json!({
                                        "id": a.id, "task_id": a.task_id,
                                        "summary": a.summary, "facts": a.facts,
                                    })).unwrap_or_default())
                                    .unwrap_or_else(|| "null".to_string()));
                            }
                            let anchors = a.list_anchors(task_id).await?;
                            return Ok(serde_json::to_string(&anchors.iter().map(|a| &a.id).collect::<Vec<_>>())
                                .unwrap_or_else(|_| "[]".to_string()));
                        }
                    }
                    return Err(VfsError::InvalidRequest("anchor store not initialized".to_string()));
                }
                Err(VfsError::InvalidPath(format!("unrecognized system path: {}", uri.path.join("/"))))
            }
            _ => Err(VfsError::InvalidPath(format!("unknown namespace: {}", uri.namespace))),
        }
    }

    async fn route_write(&self, uri: &crate::router::LogosUri, content: &str) -> Result<(), VfsError> {
        match uri.namespace.as_str() {
            "users" => {
                if uri.path.len() >= 2 && uri.path[1] == "persona" {
                    if let Some(ref ps) = self.persona_store {
                        let user_id = &uri.path[0];
                        let layer = uri.path.get(2).map(|s| s.as_str()).unwrap_or("short");
                        let period = uri.path.get(3).map(|s| s.as_str()).unwrap_or("");
                        return match layer {
                            "short" => ps.append_short(user_id, period, content).await,
                            "mid" => ps.write_mid(user_id, period, content).await,
                            "long" => ps.write_long(user_id, content).await,
                            _ => Err(VfsError::InvalidPath(format!("unknown persona layer: {layer}"))),
                        };
                    }
                    return Err(VfsError::InvalidRequest("persona store not initialized".to_string()));
                }
                let user_path = format!("mem://users/{}", uri.path.join("/"));
                self.users.write(&user_path, content).await
            }
            "system" => {
                if uri.path.first().map(|s| s.as_str()) == Some("tasks") {
                    if let Some(ref ts) = self.task_store {
                        let task: serde_json::Value = serde_json::from_str(content)
                            .map_err(|e| VfsError::InvalidJson(format!("invalid task JSON: {e}")))?;
                        let new_task = crate::task_store::NewTask {
                            task_id: task["task_id"].as_str().unwrap_or_default().to_string(),
                            description: task["description"].as_str().unwrap_or_default().to_string(),
                            workspace: task["workspace"].as_str().unwrap_or_default().to_string(),
                            resource: task["resource"].as_str().unwrap_or_default().to_string(),
                            chat_id: task["chat_id"].as_str().unwrap_or_default().to_string(),
                            trigger: task["trigger"].as_str().unwrap_or("user").to_string(),
                        };
                        return ts.create_task(&new_task).await;
                    }
                    return Err(VfsError::InvalidRequest("task store not initialized".to_string()));
                }
                Err(VfsError::InvalidPath(format!("unrecognized system write path: {}", uri.path.join("/"))))
            }
            _ => Err(VfsError::InvalidPath(format!("namespace {} does not support write", uri.namespace))),
        }
    }

    pub fn set_persona_store(&mut self, ps: Arc<crate::persona_store::PersonaStore>) {
        self.persona_store = Some(ps);
    }

    pub fn set_anchor_store(&mut self, a: Arc<crate::anchor_store::AnchorStore>) {
        self.anchor_store = Some(a);
    }

    pub fn set_task_store(&mut self, ts: Arc<crate::task_store::TaskStore>) {
        self.task_store = Some(ts);
    }
}

fn log_ok(op: &str, detail: &str, started_at: Instant) {
    println!(
        "[vfs] op={} status=ok elapsed_ms={} {}",
        op,
        started_at.elapsed().as_millis(),
        detail
    );
}

fn log_err(op: &str, detail: &str, err: &VfsError, started_at: Instant) {
    eprintln!(
        "[vfs] op={} status=error elapsed_ms={} {} err=\"{}\"",
        op,
        started_at.elapsed().as_millis(),
        detail,
        err
    );
}

fn resolve_search_mode(raw: i32) -> SearchMode {
    SearchMode::try_from(raw).unwrap_or(SearchMode::Unspecified)
}

#[tonic::async_trait]
impl MemoryVfs for MemoryVfsService {
    async fn read(&self, request: Request<ReadRequest>) -> Result<Response<ReadResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("path={}", req.path);

        let result = match crate::router::parse_uri(&req.path) {
            Ok(uri) => self.route_read(&uri).await,
            Err(_) => self.users.read(&req.path).await,
        };

        match result {
            Ok(content) => {
                log_ok("read", &detail, started_at);
                Ok(Response::new(ReadResponse { content }))
            }
            Err(err) => {
                log_err("read", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn write(
        &self,
        request: Request<WriteRequest>,
    ) -> Result<Response<WriteResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("path={} content_len={}", req.path, req.content.len());

        let result = match crate::router::parse_uri(&req.path) {
            Ok(uri) => self.route_write(&uri, &req.content).await,
            Err(_) => self.users.write(&req.path, &req.content).await,
        };

        match result {
            Ok(_) => {
                log_ok("write", &detail, started_at);
                Ok(Response::new(WriteResponse {}))
            }
            Err(err) => {
                log_err("write", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn patch(
        &self,
        request: Request<PatchRequest>,
    ) -> Result<Response<PatchResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!(
            "path={} partial_content_len={}",
            req.path,
            req.partial_content.len()
        );

        let result = match crate::router::parse_uri(&req.path) {
            Ok(uri) => self.route_write(&uri, &req.partial_content).await,
            Err(_) => self.users.patch(&req.path, &req.partial_content).await,
        };

        match result {
            Ok(_) => {
                log_ok("patch", &detail, started_at);
                Ok(Response::new(PatchResponse {}))
            }
            Err(err) => {
                log_err("patch", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn search(
        &self,
        request: Request<SearchRequest>,
    ) -> Result<Response<SearchResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!(
            "scope={} limit={} query_len={} mode={:?}",
            req.scope,
            req.limit,
            req.query.len(),
            resolve_search_mode(req.mode),
        );

        match resolve_search_mode(req.mode) {
            SearchMode::Fts => {
                match self
                    .messages
                    .search_fts(&req.scope, &req.query, req.limit)
                    .await
                {
                    Ok(stored_messages) => {
                        log_ok(
                            "search",
                            &format!("{} result_count={}", detail, stored_messages.len()),
                            started_at,
                        );
                        Ok(Response::new(SearchResponse {
                            results: vec![crate::pb::SearchResult {
                                session_id: String::new(),
                                center_vector: Vec::new(),
                                abstract_summary: String::new(),
                                messages: stored_messages
                                    .into_iter()
                                    .map(|m| crate::pb::ChatMessage {
                                        user_id: m.speaker.clone(),
                                        message_id: m.external_id,
                                        chat_id: m.chat_id,
                                        conversation_type: String::new(),
                                        context: m.text,
                                        timestamp: m.msg_id,
                                        metadata: None,
                                        vector: Vec::new(),
                                    })
                                    .collect(),
                                score: 0.0,
                            }],
                        }))
                    }
                    Err(err) => {
                        log_err("search", &detail, &err, started_at);
                        Err(err.into())
                    }
                }
            }
            _ => match self.sessions.search(req).await {
                Ok(results) => {
                    log_ok(
                        "search",
                        &format!("{} result_count={}", detail, results.len()),
                        started_at,
                    );
                    Ok(Response::new(SearchResponse { results }))
                }
                Err(err) => {
                    log_err("search", &detail, &err, started_at);
                    Err(err.into())
                }
            },
        }
    }

    async fn archive(
        &self,
        request: Request<ArchiveRequest>,
    ) -> Result<Response<ArchiveResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!(
            "session_id={} chat_id={} messages_count={}",
            req.session_id,
            req.chat_id,
            req.messages.len()
        );
        match self.sessions.archive(req).await {
            Ok(_) => {
                log_ok("archive", &detail, started_at);
                Ok(Response::new(ArchiveResponse {}))
            }
            Err(err) => {
                log_err("archive", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn range_fetch(
        &self,
        request: Request<RangeFetchRequest>,
    ) -> Result<Response<RangeFetchResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!(
            "chat_id={} ranges_count={} limit={} offset={}",
            req.chat_id,
            req.ranges.len(),
            req.limit,
            req.offset,
        );

        let ranges: Vec<(i64, i64)> = req.ranges.iter().map(|r| (r.start, r.end)).collect();
        match self
            .messages
            .range_fetch(&req.chat_id, &ranges, req.limit, req.offset)
            .await
        {
            Ok(messages) => {
                log_ok(
                    "range_fetch",
                    &format!("{} result_count={}", detail, messages.len()),
                    started_at,
                );
                Ok(Response::new(RangeFetchResponse {
                    messages: messages
                        .into_iter()
                        .map(stored_message_to_proto)
                        .collect(),
                }))
            }
            Err(err) => {
                log_err("range_fetch", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn write_summary(
        &self,
        request: Request<WriteSummaryRequest>,
    ) -> Result<Response<WriteSummaryResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("chat_id={} layer={} period={}", req.chat_id, req.layer, req.period_start);

        let insert = crate::message_store::InsertSummary {
            layer: req.layer,
            period_start: req.period_start,
            period_end: req.period_end,
            source_refs: req.source_refs,
            content: req.content,
        };
        match self.messages.insert_summary(&req.chat_id, &insert).await {
            Ok(id) => {
                log_ok("write_summary", &detail, started_at);
                Ok(Response::new(WriteSummaryResponse { id }))
            }
            Err(err) => {
                log_err("write_summary", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn read_summary(
        &self,
        request: Request<ReadSummaryRequest>,
    ) -> Result<Response<ReadSummaryResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("chat_id={} layer={} period={}", req.chat_id, req.layer, req.period_start);

        let result = if req.period_start.is_empty() {
            self.messages.get_latest_summary(&req.chat_id, &req.layer).await
        } else {
            self.messages.get_summary(&req.chat_id, &req.layer, &req.period_start).await
        };

        match result {
            Ok(opt) => {
                log_ok("read_summary", &detail, started_at);
                Ok(Response::new(ReadSummaryResponse {
                    summary: opt.map(summary_to_proto),
                }))
            }
            Err(err) => {
                log_err("read_summary", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn list_summaries(
        &self,
        request: Request<ListSummariesRequest>,
    ) -> Result<Response<ListSummariesResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("chat_id={} layer={}", req.chat_id, req.layer);

        let start = if req.start.is_empty() { None } else { Some(req.start.as_str()) };
        let end = if req.end.is_empty() { None } else { Some(req.end.as_str()) };

        match self.messages.list_summaries(&req.chat_id, &req.layer, start, end, req.limit).await {
            Ok(summaries) => {
                log_ok("list_summaries", &format!("{detail} count={}", summaries.len()), started_at);
                Ok(Response::new(ListSummariesResponse {
                    summaries: summaries.into_iter().map(summary_to_proto).collect(),
                }))
            }
            Err(err) => {
                log_err("list_summaries", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn read_persona(
        &self,
        request: Request<ReadPersonaRequest>,
    ) -> Result<Response<ReadPersonaResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("user_id={} layer={}", req.user_id, req.layer);

        let result = if let Some(ref ps) = self.persona_store {
            match req.layer.as_str() {
                "long" => ps.get_long(&req.user_id).await.map(|o| o.into_iter().collect()),
                "mid" => ps.get_latest_mid(&req.user_id).await.map(|o| o.into_iter().collect()),
                "short" => {
                    let since = if req.period.is_empty() { "" } else { &req.period };
                    ps.list_short(&req.user_id, since).await
                }
                _ => Err(VfsError::InvalidRequest(format!("unknown persona layer: {}", req.layer))),
            }
        } else {
            Err(VfsError::InvalidRequest("persona store not initialized".to_string()))
        };

        match result {
            Ok(entries) => {
                log_ok("read_persona", &detail, started_at);
                Ok(Response::new(ReadPersonaResponse {
                    entries: entries.into_iter().map(persona_to_proto).collect(),
                }))
            }
            Err(err) => {
                log_err("read_persona", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn write_persona(
        &self,
        request: Request<WritePersonaRequest>,
    ) -> Result<Response<WritePersonaResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("user_id={} layer={}", req.user_id, req.layer);

        let result = if let Some(ref ps) = self.persona_store {
            match req.layer.as_str() {
                "short" => ps.append_short(&req.user_id, &req.period, &req.content).await,
                "mid" => ps.write_mid(&req.user_id, &req.period, &req.content).await,
                "long" => ps.write_long(&req.user_id, &req.content).await,
                _ => Err(VfsError::InvalidRequest(format!("unknown persona layer: {}", req.layer))),
            }
        } else {
            Err(VfsError::InvalidRequest("persona store not initialized".to_string()))
        };

        match result {
            Ok(()) => {
                log_ok("write_persona", &detail, started_at);
                Ok(Response::new(WritePersonaResponse {}))
            }
            Err(err) => {
                log_err("write_persona", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn create_anchor(
        &self,
        request: Request<CreateAnchorRequest>,
    ) -> Result<Response<CreateAnchorResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("task_id={}", req.task_id);

        let result = if let Some(ref a) = self.anchor_store {
            a.create_anchor(&req.task_id, &req.summary, &req.facts).await
        } else {
            Err(VfsError::InvalidRequest("anchor store not initialized".to_string()))
        };

        match result {
            Ok(anchor_id) => {
                log_ok("create_anchor", &detail, started_at);
                Ok(Response::new(CreateAnchorResponse { anchor_id }))
            }
            Err(err) => {
                log_err("create_anchor", &detail, &err, started_at);
                Err(err.into())
            }
        }
    }

    async fn search_tasks(
        &self,
        request: Request<SearchTasksRequest>,
    ) -> Result<Response<SearchTasksResponse>, Status> {
        let started_at = Instant::now();
        let req = request.into_inner();
        let detail = format!("query={} chat_id={} status={}", req.query, req.chat_id, req.status);

        let tasks = if let Some(ref ts) = self.task_store {
            let chat_id = if req.chat_id.is_empty() { None } else { Some(req.chat_id.as_str()) };
            let status = if req.status.is_empty() { None } else { Some(req.status.as_str()) };
            ts.list_tasks(chat_id, status).await?
        } else {
            Vec::new()
        };

        let anchors = if !req.query.is_empty() {
            if let Some(ref a) = self.anchor_store {
                a.search_facts_bm25(&req.query, req.limit).await?
            } else {
                Vec::new()
            }
        } else {
            Vec::new()
        };

        log_ok("search_tasks", &format!("{detail} tasks={} anchors={}", tasks.len(), anchors.len()), started_at);
        Ok(Response::new(SearchTasksResponse {
            tasks: tasks.into_iter().map(task_to_proto).collect(),
            anchors: anchors.into_iter().map(anchor_fact_to_proto).collect(),
        }))
    }
}

fn stored_message_to_proto(m: crate::message_store::StoredMessage) -> crate::pb::StoredMessage {
    crate::pb::StoredMessage {
        msg_id: m.msg_id,
        external_id: m.external_id,
        ts: m.ts,
        chat_id: m.chat_id,
        speaker: m.speaker,
        reply_to: m.reply_to.unwrap_or(0),
        text: m.text,
        mentions: m.mentions,
        session_id: m.session_id,
    }
}

fn summary_to_proto(s: crate::message_store::Summary) -> crate::pb::SummaryRecord {
    crate::pb::SummaryRecord {
        id: s.id,
        chat_id: s.chat_id,
        layer: s.layer,
        period_start: s.period_start,
        period_end: s.period_end,
        source_refs: s.source_refs,
        content: s.content,
        generated_at: s.generated_at,
    }
}

fn persona_to_proto(e: crate::persona_store::PersonaEntry) -> crate::pb::PersonaEntry {
    crate::pb::PersonaEntry {
        id: e.id,
        user_id: e.user_id,
        layer: e.layer,
        period: e.period,
        content: e.content,
        created_at: e.created_at,
    }
}

fn task_to_proto(t: crate::task_store::Task) -> crate::pb::TaskRecord {
    crate::pb::TaskRecord {
        task_id: t.task_id,
        description: t.description,
        workspace: t.workspace,
        resource: t.resource,
        status: t.status,
        chat_id: t.chat_id,
        trigger: t.trigger,
        created_at: t.created_at,
        updated_at: t.updated_at,
    }
}

fn anchor_fact_to_proto(a: crate::anchor_store::AnchorFact) -> crate::pb::AnchorRecord {
    crate::pb::AnchorRecord {
        id: a.id,
        task_id: a.task_id,
        summary: a.summary,
        facts: a.facts,
        created_at: a.created_at,
    }
}
