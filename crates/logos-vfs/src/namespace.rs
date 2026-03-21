use async_trait::async_trait;

use crate::VfsError;

/// A mountable namespace handler.
///
/// Every Logos subsystem (memory, system, users, tmp, ...) implements this
/// trait and registers itself into the VFS routing table at boot.
///
/// The routing table calls `read`/`write`/`patch` with the path segments
/// *after* the namespace prefix has been stripped. For example, a request to
/// `logos://memory/groups/abc/messages/1` arrives at the `memory` namespace's
/// `read` method with `path = ["groups", "abc", "messages", "1"]`.
#[async_trait]
pub trait Namespace: Send + Sync {
    /// The namespace name used for URI routing (e.g. "memory", "system").
    fn name(&self) -> &str;

    async fn read(&self, path: &[&str]) -> Result<String, VfsError>;

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError>;

    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), VfsError>;
}
