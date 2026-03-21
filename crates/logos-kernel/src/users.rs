use std::path::PathBuf;

use async_trait::async_trait;
use logos_vfs::{Namespace, VfsError};

/// Users namespace — `logos://users/`.
///
/// Backed by the filesystem. Each user gets a directory under `root/{uid}/`.
/// Files are read/written as-is. Directories are listed as JSON arrays.
pub struct UsersNs {
    root: PathBuf,
}

impl UsersNs {
    pub fn init(root: PathBuf) -> Result<Self, VfsError> {
        std::fs::create_dir_all(&root)
            .map_err(|e| VfsError::Io(format!("create users dir: {e}")))?;
        Ok(Self { root })
    }
}

#[async_trait]
impl Namespace for UsersNs {
    fn name(&self) -> &str {
        "users"
    }

    async fn read(&self, path: &[&str]) -> Result<String, VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("empty users path".to_string()));
        }
        let file_path = self.root.join(path.join("/"));

        if file_path.is_dir() {
            // List directory contents
            let mut entries = Vec::new();
            let dir = std::fs::read_dir(&file_path)
                .map_err(|e| VfsError::Io(format!("read dir {}: {e}", file_path.display())))?;
            for entry in dir.flatten() {
                if let Some(name) = entry.file_name().to_str() {
                    entries.push(name.to_string());
                }
            }
            return Ok(serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string()));
        }

        tokio::fs::read_to_string(&file_path)
            .await
            .map_err(|e| match e.kind() {
                std::io::ErrorKind::NotFound => {
                    VfsError::NotFound(format!("logos://users/{}", path.join("/")))
                }
                _ => VfsError::Io(format!("read {}: {e}", file_path.display())),
            })
    }

    async fn write(&self, path: &[&str], content: &str) -> Result<(), VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("empty users path".to_string()));
        }
        let file_path = self.root.join(path.join("/"));

        if let Some(parent) = file_path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| VfsError::Io(format!("mkdir {}: {e}", parent.display())))?;
        }

        tokio::fs::write(&file_path, content)
            .await
            .map_err(|e| VfsError::Io(format!("write {}: {e}", file_path.display())))
    }

    async fn patch(&self, path: &[&str], partial: &str) -> Result<(), VfsError> {
        if path.is_empty() {
            return Err(VfsError::InvalidPath("empty users path".to_string()));
        }
        let file_path = self.root.join(path.join("/"));

        // Read existing content, deep-merge JSON, write back
        let existing = tokio::fs::read_to_string(&file_path).await.unwrap_or_default();

        if let (Ok(mut base), Ok(patch)) = (
            serde_json::from_str::<serde_json::Value>(&existing),
            serde_json::from_str::<serde_json::Value>(partial),
        ) {
            json_merge(&mut base, &patch);
            let merged = serde_json::to_string_pretty(&base).unwrap_or_else(|_| partial.to_string());
            return self.write(path, &merged).await;
        }

        // Fallback: overwrite
        self.write(path, partial).await
    }
}

fn json_merge(base: &mut serde_json::Value, patch: &serde_json::Value) {
    if let (Some(base_obj), Some(patch_obj)) = (base.as_object_mut(), patch.as_object()) {
        for (key, value) in patch_obj {
            let entry = base_obj.entry(key.clone()).or_insert(serde_json::Value::Null);
            json_merge(entry, value);
        }
    } else {
        *base = patch.clone();
    }
}
