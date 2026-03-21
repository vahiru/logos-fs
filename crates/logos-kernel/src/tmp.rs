use std::collections::HashMap;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};
use tokio::sync::RwLock;

/// Tmp namespace — `logos://tmp/`.
///
/// In-memory key-value store. Ephemeral — lost on restart.
pub struct TmpNs {
    store: RwLock<HashMap<String, String>>,
}

impl TmpNs {
    pub fn new() -> Self {
        Self {
            store: RwLock::new(HashMap::new()),
        }
    }
}

#[async_trait]
impl Namespace for TmpNs {
    fn name(&self) -> &str {
        "tmp"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        let key = path.join("/");
        if key.is_empty() {
            // List all keys
            let store = self.store.read().await;
            let keys: Vec<&str> = store.keys().map(|s| s.as_str()).collect();
            return Ok(serde_json::to_string(&keys).unwrap_or_else(|_| "[]".to_string()));
        }
        let store = self.store.read().await;
        store
            .get(&key)
            .cloned()
            .ok_or_else(|| VfsError::NotFound(format!("logos://tmp/{key}")))
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        let key = path.join("/");
        if key.is_empty() {
            return Err(VfsError::InvalidPath("empty tmp path".to_string()));
        }
        let mut store = self.store.write().await;
        store.insert(key, content.to_string());
        Ok(())
    }

    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), VfsError> {
        self.write(path, partial).await
    }
}
