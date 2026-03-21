use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::RwLock;

/// One-time token registry for task binding.
///
/// Flow (RFC 002 §11.2):
///   1. Runtime calls RegisterToken(token, task_id)
///   2. Runtime launches agent with token in env
///   3. Agent connects and calls Handshake(token)
///   4. Kernel consumes token → binds connection to task_id
///   5. All subsequent calls on this connection are scoped to that task
pub struct TokenRegistry {
    /// Pending tokens: token → task_id. Consumed on handshake.
    pending: RwLock<HashMap<String, String>>,
    /// Active sessions: session_key → task_id. Set after handshake.
    sessions: RwLock<HashMap<String, String>>,
    /// Counter for generating session keys.
    counter: std::sync::atomic::AtomicU64,
}

impl TokenRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            pending: RwLock::new(HashMap::new()),
            sessions: RwLock::new(HashMap::new()),
            counter: std::sync::atomic::AtomicU64::new(1),
        })
    }

    /// Register a one-time token → task_id mapping. Called by the runtime.
    pub async fn register(&self, token: String, task_id: String) {
        self.pending.write().await.insert(token, task_id);
    }

    /// Consume a token and return (session_key, task_id).
    /// The token is invalidated after this call.
    pub async fn consume(&self, token: &str) -> Option<(String, String)> {
        let task_id = self.pending.write().await.remove(token)?;
        let session_key = format!(
            "s-{}",
            self.counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        self.sessions
            .write()
            .await
            .insert(session_key.clone(), task_id.clone());
        Some((session_key, task_id))
    }

    /// Look up the task_id for a session key.
    pub async fn resolve(&self, session_key: &str) -> Option<String> {
        self.sessions.read().await.get(session_key).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn register_and_consume() {
        let reg = TokenRegistry::new();
        reg.register("tok-1".to_string(), "task-001".to_string())
            .await;
        let (key, task_id) = reg.consume("tok-1").await.unwrap();
        assert_eq!(task_id, "task-001");
        assert!(key.starts_with("s-"));
    }

    #[tokio::test]
    async fn consume_is_one_time() {
        let reg = TokenRegistry::new();
        reg.register("tok-2".to_string(), "task-002".to_string())
            .await;
        assert!(reg.consume("tok-2").await.is_some());
        assert!(reg.consume("tok-2").await.is_none()); // second consume fails
    }

    #[tokio::test]
    async fn unknown_token_returns_none() {
        let reg = TokenRegistry::new();
        assert!(reg.consume("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn resolve_after_handshake() {
        let reg = TokenRegistry::new();
        reg.register("tok-3".to_string(), "task-003".to_string())
            .await;
        let (key, _) = reg.consume("tok-3").await.unwrap();
        assert_eq!(reg.resolve(&key).await, Some("task-003".to_string()));
    }
}
