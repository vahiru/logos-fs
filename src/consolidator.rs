use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time;

use crate::llm_client::LlmClient;
use crate::message_store::{InsertSummary, MessageStore};
use crate::persona_store::PersonaStore;

const SHORT_SUMMARY_INTERVAL: Duration = Duration::from_secs(3600);
const MID_SUMMARY_INTERVAL: Duration = Duration::from_secs(86400);
const SHORT_PERSONA_INTERVAL: Duration = Duration::from_secs(3600);
const MID_PERSONA_INTERVAL: Duration = Duration::from_secs(86400);

const SHORT_SUMMARY_SYSTEM_PROMPT: &str = "\
You are a conversation summarizer. Given a batch of chat messages from the last hour, \
produce a concise summary capturing the key topics, decisions, and action items. \
Keep it under 300 words. Output only the summary text.";

const MID_SUMMARY_SYSTEM_PROMPT: &str = "\
You are a conversation summarizer. Given a set of hourly summaries from today, \
produce a concise daily summary capturing the key themes, decisions, and outcomes. \
Keep it under 500 words. Output only the summary text.";

const SHORT_PERSONA_SYSTEM_PROMPT: &str = "\
You are a user behavior analyst. Given chat messages from the last hour, \
extract new observations about each user's personality, preferences, communication style, \
and expertise. Output one paragraph per user, prefixed with their username. \
Only include genuinely new observations.";

const MID_PERSONA_SYSTEM_PROMPT: &str = "\
You are a user behavior analyst. Given recent short-term persona observations and the current \
mid-term persona summary, produce an updated mid-term persona summary for this user. \
Merge new observations with existing knowledge, resolve contradictions in favor of newer data, \
and keep the result under 500 words.";

pub struct Consolidator {
    message_store: Arc<MessageStore>,
    persona_store: Arc<PersonaStore>,
    llm: Arc<LlmClient>,
}

impl Consolidator {
    pub fn new(
        message_store: Arc<MessageStore>,
        persona_store: Arc<PersonaStore>,
        llm: Arc<LlmClient>,
    ) -> Self {
        Self {
            message_store,
            persona_store,
            llm,
        }
    }

    pub fn start(self: Arc<Self>) -> Vec<JoinHandle<()>> {
        vec![
            tokio::spawn(self.clone().run_short_summary()),
            tokio::spawn(self.clone().run_mid_summary()),
            tokio::spawn(self.clone().run_short_persona()),
            tokio::spawn(self.run_mid_persona()),
        ]
    }

    async fn run_short_summary(self: Arc<Self>) {
        let mut interval = time::interval(SHORT_SUMMARY_INTERVAL);
        interval.tick().await; // skip immediate first tick
        loop {
            interval.tick().await;
            if let Err(e) = self.consolidate_short_summary().await {
                eprintln!("[consolidator] short_summary error: {e}");
            }
        }
    }

    async fn run_mid_summary(self: Arc<Self>) {
        let mut interval = time::interval(MID_SUMMARY_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(e) = self.consolidate_mid_summary().await {
                eprintln!("[consolidator] mid_summary error: {e}");
            }
        }
    }

    async fn run_short_persona(self: Arc<Self>) {
        let mut interval = time::interval(SHORT_PERSONA_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(e) = self.consolidate_short_persona().await {
                eprintln!("[consolidator] short_persona error: {e}");
            }
        }
    }

    async fn run_mid_persona(self: Arc<Self>) {
        let mut interval = time::interval(MID_PERSONA_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            if let Err(e) = self.consolidate_mid_persona().await {
                eprintln!("[consolidator] mid_persona error: {e}");
            }
        }
    }

    async fn consolidate_short_summary(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let chat_ids = self.message_store.list_chat_ids()?;
        let (ts_start, ts_end, period_start) = last_hour_range();

        for chat_id in &chat_ids {
            let messages = self
                .message_store
                .get_messages_by_ts_range(chat_id, &ts_start, &ts_end)
                .await?;
            if messages.is_empty() {
                continue;
            }

            let user_prompt = format_messages_for_summary(&messages);
            let summary_text = self.llm.complete(SHORT_SUMMARY_SYSTEM_PROMPT, &user_prompt).await?;

            let first_id = messages.first().map(|m| m.msg_id).unwrap_or(0);
            let last_id = messages.last().map(|m| m.msg_id).unwrap_or(0);
            let source_refs = format!("[[{first_id},{last_id}]]");

            self.message_store
                .insert_summary(
                    chat_id,
                    &InsertSummary {
                        layer: "short".to_string(),
                        period_start: period_start.clone(),
                        period_end: ts_end.clone(),
                        source_refs,
                        content: summary_text,
                    },
                )
                .await?;

            println!("[consolidator] short_summary chat_id={chat_id} period={period_start} msgs={}", messages.len());
        }
        Ok(())
    }

    async fn consolidate_mid_summary(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let chat_ids = self.message_store.list_chat_ids()?;
        let (day_start, day_end, period_start) = last_day_range();

        for chat_id in &chat_ids {
            let short_summaries = self
                .message_store
                .list_summaries(chat_id, "short", Some(&day_start), Some(&day_end), 100)
                .await?;
            if short_summaries.is_empty() {
                continue;
            }

            let user_prompt = short_summaries
                .iter()
                .map(|s| format!("[{}] {}", s.period_start, s.content))
                .collect::<Vec<_>>()
                .join("\n\n");

            let summary_text = self.llm.complete(MID_SUMMARY_SYSTEM_PROMPT, &user_prompt).await?;

            let source_refs: Vec<String> = short_summaries
                .iter()
                .map(|s| format!("\"{}\"", s.period_start))
                .collect();
            let source_refs = format!("[{}]", source_refs.join(","));

            self.message_store
                .insert_summary(
                    chat_id,
                    &InsertSummary {
                        layer: "mid".to_string(),
                        period_start: period_start.clone(),
                        period_end: day_end.clone(),
                        source_refs,
                        content: summary_text,
                    },
                )
                .await?;

            println!("[consolidator] mid_summary chat_id={chat_id} period={period_start} shorts={}", short_summaries.len());
        }
        Ok(())
    }

    async fn consolidate_short_persona(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let chat_ids = self.message_store.list_chat_ids()?;
        let (ts_start, ts_end, period) = last_hour_range();

        for chat_id in &chat_ids {
            let messages = self
                .message_store
                .get_messages_by_ts_range(chat_id, &ts_start, &ts_end)
                .await?;
            if messages.is_empty() {
                continue;
            }

            // Group messages by speaker
            let mut by_speaker: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            for msg in &messages {
                by_speaker
                    .entry(msg.speaker.clone())
                    .or_default()
                    .push(msg.text.clone());
            }

            let user_prompt = format_messages_for_summary(&messages);
            let persona_text = self.llm.complete(SHORT_PERSONA_SYSTEM_PROMPT, &user_prompt).await?;

            // Store as a single entry with all speakers' observations
            for speaker in by_speaker.keys() {
                self.persona_store
                    .append_short(speaker, &period, &persona_text)
                    .await?;
            }

            println!("[consolidator] short_persona chat_id={chat_id} period={period} speakers={}", by_speaker.len());
        }
        Ok(())
    }

    async fn consolidate_mid_persona(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let chat_ids = self.message_store.list_chat_ids()?;
        let (day_start, _day_end, period) = last_day_range();

        // Collect all unique speakers across all chats
        let mut all_speakers = std::collections::HashSet::new();
        for chat_id in &chat_ids {
            let messages = self
                .message_store
                .get_messages_by_ts_range(chat_id, &day_start, "9999-12-31T23:59:59Z")
                .await?;
            for msg in &messages {
                all_speakers.insert(msg.speaker.clone());
            }
        }

        for speaker in &all_speakers {
            let short_entries = self.persona_store.list_short(speaker, &day_start).await?;
            if short_entries.is_empty() {
                continue;
            }

            let existing_mid = self.persona_store.get_latest_mid(speaker).await?;
            let existing_mid_text = existing_mid
                .map(|e| e.content)
                .unwrap_or_else(|| "(no existing mid-term persona)".to_string());

            let user_prompt = format!(
                "Current mid-term persona:\n{existing_mid_text}\n\n\
                 Recent short-term observations:\n{}",
                short_entries
                    .iter()
                    .map(|e| format!("[{}] {}", e.period, e.content))
                    .collect::<Vec<_>>()
                    .join("\n")
            );

            let mid_text = self.llm.complete(MID_PERSONA_SYSTEM_PROMPT, &user_prompt).await?;
            self.persona_store.write_mid(speaker, &period, &mid_text).await?;

            println!("[consolidator] mid_persona speaker={speaker} period={period}");
        }
        Ok(())
    }
}

fn format_messages_for_summary(messages: &[crate::message_store::StoredMessage]) -> String {
    messages
        .iter()
        .map(|m| format!("[{} {}] {}", m.ts, m.speaker, m.text))
        .collect::<Vec<_>>()
        .join("\n")
}

fn last_hour_range() -> (String, String, String) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let hour_secs = 3600u64;
    let start_secs = now_secs - hour_secs;

    let start = format_ts(start_secs);
    let end = format_ts(now_secs);
    let period = format_period_hour(start_secs);
    (start, end, period)
}

fn last_day_range() -> (String, String, String) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let day_secs = 86400u64;
    let start_secs = now_secs - day_secs;

    let start = format_ts(start_secs);
    let end = format_ts(now_secs);
    let period = format_period_day(start_secs);
    (start, end, period)
}

fn format_ts(secs: u64) -> String {
    let days = secs / 86400;
    let tod = secs % 86400;
    let (y, mo, d) = crate::message_store::civil_from_days_u(days as i64);
    format!(
        "{y:04}-{mo:02}-{d:02}T{:02}:{:02}:{:02}Z",
        tod / 3600,
        (tod % 3600) / 60,
        tod % 60
    )
}

fn format_period_hour(secs: u64) -> String {
    let days = secs / 86400;
    let tod = secs % 86400;
    let (y, mo, d) = crate::message_store::civil_from_days_u(days as i64);
    format!("{y:04}-{mo:02}-{d:02}T{:02}", tod / 3600)
}

fn format_period_day(secs: u64) -> String {
    let days = secs / 86400;
    let (y, mo, d) = crate::message_store::civil_from_days_u(days as i64);
    format!("{y:04}-{mo:02}-{d:02}")
}
