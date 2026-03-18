mod anchor_store;
mod consolidator;
mod embedding;
mod llm_client;
mod message_store;
mod persona_store;
mod router;
mod service;
mod sessions_store;
mod task_store;
mod users_store;

use std::path::PathBuf;
use std::sync::Arc;
use std::{io, net::SocketAddr};

use anchor_store::AnchorStore;
use consolidator::Consolidator;
use embedding::OllamaEmbedder;
use llm_client::{LlmClient, LlmConfig};
use message_store::MessageStore;
use persona_store::PersonaStore;
use service::MemoryVfsService;
use task_store::TaskStore;
use tokio::net::UnixListener;
use tokio_stream::wrappers::UnixListenerStream;
use tonic::transport::Server;

pub mod pb {
    tonic::include_proto!("kairos.vfs.v1");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    load_repo_env_file();
    let listen = resolve_listen_target();
    let users_root = resolve_users_root();

    let state_root = users_root
        .parent()
        .map(|p| p.to_path_buf())
        .unwrap_or_else(|| PathBuf::from("."));

    // Embedding provider
    let embedder: Arc<dyn embedding::EmbeddingProvider> = Arc::new(OllamaEmbedder::new(
        resolve_env("OLLAMA_BASE_URL", "http://127.0.0.1:11434"),
        resolve_env("OLLAMA_EMBED_MODEL", "qwen3-embedding:0.6b"),
        resolve_env("OLLAMA_EMBED_DIM", "1024")
            .parse::<usize>()
            .unwrap_or(1024),
    ));

    // Stores
    let memory_db_root = state_root.join("memory");
    let message_store = Arc::new(MessageStore::new(memory_db_root)?);

    let persona_store = Arc::new(PersonaStore::new(state_root.join("personas.db"))?);

    let system_db_path = state_root.join("system.db");
    let anchor_store = Arc::new(AnchorStore::new(system_db_path.clone())?);
    let task_store = Arc::new(TaskStore::new(system_db_path)?);

    // Service
    let mut service = MemoryVfsService::new(
        users_root.clone(),
        Arc::clone(&embedder),
        Arc::clone(&message_store),
    )?;
    service.set_persona_store(Arc::clone(&persona_store));
    service.set_anchor_store(Arc::clone(&anchor_store));
    service.set_task_store(Arc::clone(&task_store));

    // Consolidator (LLM-powered cron jobs)
    let llm_config = LlmConfig {
        base_url: resolve_env("OLLAMA_BASE_URL", "http://127.0.0.1:11434"),
        model: resolve_env("OLLAMA_LLM_MODEL", "qwen3:8b"),
    };
    let llm_client = Arc::new(LlmClient::new(llm_config));
    let consolidator = Arc::new(Consolidator::new(
        Arc::clone(&message_store),
        Arc::clone(&persona_store),
        llm_client,
    ));
    let _consolidator_handles = consolidator.start();

    // gRPC server
    let grpc_service = pb::memory_vfs_server::MemoryVfsServer::new(service);

    if let Some(socket_path) = parse_uds_path(&listen) {
        prepare_unix_socket(&socket_path)?;
        let listener = UnixListener::bind(&socket_path)?;
        println!(
            "memory-vfs listening on unix://{}, users root: {}",
            socket_path.display(),
            users_root.display()
        );
        Server::builder()
            .add_service(grpc_service)
            .serve_with_incoming(UnixListenerStream::new(listener))
            .await?;
    } else {
        let addr: SocketAddr = listen.parse()?;
        println!(
            "memory-vfs listening on {addr}, users root: {}",
            users_root.display()
        );
        Server::builder()
            .add_service(grpc_service)
            .serve(addr)
            .await?;
    }

    Ok(())
}

fn resolve_env(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

fn resolve_listen_target() -> String {
    std::env::var("VFS_LISTEN")
        .or_else(|_| std::env::var("KAIROS_VFS_SOCKET"))
        .unwrap_or_else(|_| "unix:///run/kairos-runtime/sockets/kairos-runtime-vfs.sock".to_string())
}

fn parse_uds_path(listen: &str) -> Option<PathBuf> {
    if let Some(path) = listen.strip_prefix("unix://") {
        if path.is_empty() {
            return None;
        }
        return Some(PathBuf::from(path));
    }
    if let Some(path) = listen.strip_prefix("unix:") {
        if path.is_empty() {
            return None;
        }
        return Some(PathBuf::from(path));
    }
    None
}

fn prepare_unix_socket(socket_path: &PathBuf) -> io::Result<()> {
    if let Some(parent) = socket_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    match std::fs::remove_file(socket_path) {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    Ok(())
}

fn resolve_users_root() -> PathBuf {
    if let Ok(path) = std::env::var("VFS_USERS_ROOT") {
        return PathBuf::from(path);
    }
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../data/state/entities")
}

fn load_repo_env_file() {
    let env_path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..").join(".env");
    let _ = dotenvy::from_path(env_path);
}
