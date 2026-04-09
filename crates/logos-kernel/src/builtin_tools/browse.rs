use async_trait::async_trait;
use logos_vfs::VfsError;
use reqwest::Client;

use crate::proc::ProcTool;

/// browse — browser control via PinchTab HTTP API
pub struct BrowseTool {
    http: Client,
    base_url: String,
}

impl BrowseTool {
    pub fn new(base_url: String) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_default();
        Self { http, base_url }
    }
}

#[async_trait]
impl ProcTool for BrowseTool {
    fn name(&self) -> &str {
        "browse"
    }

    fn schema(&self) -> serde_json::Value {
        serde_json::json!({
            "name": "browse",
            "description": "Control a browser via PinchTab. Navigate to URLs, read page content as Accessibility Tree, click elements, or type text.",
            "parameters": {
                "type": "object",
                "properties": {
                    "url": { "type": "string", "description": "URL to navigate to (triggers navigation + snapshot)" },
                    "action": { "type": "string", "enum": ["snap", "click", "type"], "default": "snap", "description": "Action: snap (default, get page tree), click, type" },
                    "ref": { "type": "string", "description": "Element ref (e.g. 'e6') for click/type actions" },
                    "text": { "type": "string", "description": "Text to type (for type action)" }
                }
            }
        })
    }

    async fn call(&self, params: &str) -> Result<String, VfsError> {
        let val: serde_json::Value =
            serde_json::from_str(params).map_err(|e| VfsError::InvalidJson(e.to_string()))?;

        let action = val["action"].as_str().unwrap_or("snap");

        // If url is provided, navigate first
        if let Some(url) = val["url"].as_str() {
            self.navigate(url).await?;
            // Brief wait for page load
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
        }

        match action {
            "snap" => self.snapshot().await,
            "click" => {
                let r = val["ref"].as_str()
                    .ok_or_else(|| VfsError::InvalidJson("'ref' required for click".into()))?;
                self.click(r).await
            }
            "type" => {
                let r = val["ref"].as_str()
                    .ok_or_else(|| VfsError::InvalidJson("'ref' required for type".into()))?;
                let text = val["text"].as_str()
                    .ok_or_else(|| VfsError::InvalidJson("'text' required for type".into()))?;
                self.fill(r, text).await
            }
            _ => Err(VfsError::InvalidJson(format!("unknown action: {action}"))),
        }
    }
}

impl BrowseTool {
    async fn navigate(&self, url: &str) -> Result<String, VfsError> {
        let resp = self.http
            .post(format!("{}/navigate", self.base_url))
            .json(&serde_json::json!({ "url": url }))
            .send()
            .await
            .map_err(|e| VfsError::Io(format!("pinchtab navigate: {e}")))?;
        resp.text().await
            .map_err(|e| VfsError::Io(format!("pinchtab navigate body: {e}")))
    }

    async fn snapshot(&self) -> Result<String, VfsError> {
        let resp = self.http
            .get(format!("{}/snapshot", self.base_url))
            .send()
            .await
            .map_err(|e| VfsError::Io(format!("pinchtab snapshot: {e}")))?;
        resp.text().await
            .map_err(|e| VfsError::Io(format!("pinchtab snapshot body: {e}")))
    }

    async fn click(&self, element_ref: &str) -> Result<String, VfsError> {
        let resp = self.http
            .post(format!("{}/click", self.base_url))
            .json(&serde_json::json!({ "ref": element_ref }))
            .send()
            .await
            .map_err(|e| VfsError::Io(format!("pinchtab click: {e}")))?;
        let result = resp.text().await
            .map_err(|e| VfsError::Io(format!("pinchtab click body: {e}")))?;
        // Return snapshot after click
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        let snap = self.snapshot().await.unwrap_or_default();
        Ok(format!("{result}\n---\n{snap}"))
    }

    async fn fill(&self, element_ref: &str, text: &str) -> Result<String, VfsError> {
        let resp = self.http
            .post(format!("{}/fill", self.base_url))
            .json(&serde_json::json!({ "ref": element_ref, "value": text }))
            .send()
            .await
            .map_err(|e| VfsError::Io(format!("pinchtab fill: {e}")))?;
        resp.text().await
            .map_err(|e| VfsError::Io(format!("pinchtab fill body: {e}")))
    }
}

/// Try to spawn pinchtab server and wait for it to be ready.
/// Returns the Child handle if successful, None if pinchtab is not installed.
pub async fn spawn_pinchtab() -> Option<tokio::process::Child> {
    // Check if pinchtab is installed
    let which = tokio::process::Command::new("which")
        .arg("pinchtab")
        .output()
        .await
        .ok()?;
    if !which.status.success() {
        eprintln!("[logos] pinchtab not found, browse tool disabled. Install: brew install pinchtab/tap/pinchtab");
        return None;
    }

    // Spawn server
    let child = tokio::process::Command::new("pinchtab")
        .arg("server")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .ok()?;

    println!("[logos] spawned pinchtab server, waiting for ready...");

    // Poll for readiness
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap_or_default();

    for _ in 0..10 {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        if client.get("http://127.0.0.1:9867/health").send().await.is_ok() {
            println!("[logos] pinchtab ready");
            return Some(child);
        }
    }

    eprintln!("[logos] WARNING: pinchtab failed to start within 10s, browse tool may not work");
    Some(child)
}
