use async_trait::async_trait;
use reqwest::Client;
use serde::Deserialize;

use crate::service::VfsError;

#[async_trait]
pub trait EmbeddingProvider: Send + Sync {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, VfsError>;
    fn dimension(&self) -> usize;
}

pub struct OllamaEmbedder {
    base_url: String,
    model: String,
    dim: usize,
    http_client: Client,
}

#[derive(Deserialize)]
struct OllamaEmbedResponse {
    embedding: Option<Vec<f32>>,
}

impl OllamaEmbedder {
    pub fn new(base_url: String, model: String, dim: usize) -> Self {
        Self {
            base_url,
            model,
            dim,
            http_client: Client::new(),
        }
    }
}

#[async_trait]
impl EmbeddingProvider for OllamaEmbedder {
    async fn embed(&self, text: &str) -> Result<Vec<f32>, VfsError> {
        let base_url = self.base_url.trim_end_matches('/');
        let input = if text.trim().is_empty() {
            "(empty)"
        } else {
            text.trim()
        };

        let response = self
            .http_client
            .post(format!("{base_url}/api/embeddings"))
            .json(&serde_json::json!({
                "model": self.model,
                "prompt": input,
            }))
            .send()
            .await
            .map_err(|e| VfsError::Http(format!("ollama embed request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "failed to read body".to_string());
            return Err(VfsError::Http(format!(
                "ollama embed failed ({status}): {body}"
            )));
        }

        let payload: OllamaEmbedResponse = response
            .json()
            .await
            .map_err(|e| VfsError::Http(format!("invalid ollama embed response: {e}")))?;

        payload
            .embedding
            .ok_or_else(|| VfsError::Http("ollama embedding field missing".to_string()))
    }

    fn dimension(&self) -> usize {
        self.dim
    }
}
