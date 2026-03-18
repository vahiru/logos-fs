use reqwest::Client;
use serde::Deserialize;

use crate::service::VfsError;

#[derive(Clone, Debug)]
pub struct LlmConfig {
    pub base_url: String,
    pub model: String,
}

pub struct LlmClient {
    config: LlmConfig,
    http_client: Client,
}

#[derive(Deserialize)]
struct OllamaGenerateResponse {
    response: Option<String>,
}

impl LlmClient {
    pub fn new(config: LlmConfig) -> Self {
        Self {
            config,
            http_client: Client::new(),
        }
    }

    pub async fn complete(
        &self,
        system_prompt: &str,
        user_prompt: &str,
    ) -> Result<String, VfsError> {
        let base_url = self.config.base_url.trim_end_matches('/');
        let body = serde_json::json!({
            "model": self.config.model,
            "system": system_prompt,
            "prompt": user_prompt,
            "stream": false,
        });

        let response = self
            .http_client
            .post(format!("{base_url}/api/generate"))
            .json(&body)
            .send()
            .await
            .map_err(|e| VfsError::Http(format!("ollama generate request failed: {e}")))?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response
                .text()
                .await
                .unwrap_or_else(|_| "failed to read body".to_string());
            return Err(VfsError::Http(format!(
                "ollama generate failed ({status}): {body}"
            )));
        }

        let payload: OllamaGenerateResponse = response
            .json()
            .await
            .map_err(|e| VfsError::Http(format!("invalid ollama generate response: {e}")))?;

        payload
            .response
            .ok_or_else(|| VfsError::Http("ollama response field missing".to_string()))
    }
}
