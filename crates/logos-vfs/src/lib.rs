pub mod namespace;
pub mod table;
pub mod uri;

pub use namespace::Namespace;
pub use table::RoutingTable;

#[derive(Debug, thiserror::Error)]
pub enum VfsError {
    #[error("invalid URI: {0}")]
    InvalidUri(String),

    #[error("namespace not mounted: {0}")]
    NamespaceNotMounted(String),

    #[error("kernel not ready — boot sequence incomplete")]
    NotReady,

    #[error("not found: {0}")]
    NotFound(String),

    #[error("invalid path: {0}")]
    InvalidPath(String),

    #[error("invalid json: {0}")]
    InvalidJson(String),

    #[error("io error: {0}")]
    Io(String),

    #[error("sqlite error: {0}")]
    Sqlite(String),
}
