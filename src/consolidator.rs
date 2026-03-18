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

const LONG_SUMMARY_SYSTEM_PROMPT: &str = "\
You are a conversation summarizer. Given a set of daily summaries from this month, \
produce a concise monthly summary capturing the major themes, completed milestones, \
recurring topics, and significant decisions. Keep it under 800 words. Output only the summary text.";

// RFC 003 Section 8.1: persona input is raw messages, not summaries
const SHORT_PERSONA_SYSTEM_PROMPT: &str = "\
You are a user behavior analyst. Given chat messages from the last hour for a SPECIFIC user, \
extract new observations about this user's personality, preferences, communication style, \
expertise, and any expressed needs or interests. Be specific and factual. \
Only include genuinely new observations. Output a concise list of observations.";

const MID_PERSONA_SYSTEM_PROMPT: &str = "\
You are a user behavior analyst. Given recent short-term persona observations and the current \
mid-term persona summary, produce an updated mid-term persona summary for this user. \
Merge new observations with existing knowledge, resolve contradictions in favor of newer data, \
and keep the result under 500 words.";

const LONG_PERSONA_SYSTEM_PROMPT: &str = "\
You are a user behavior analyst. Given the current mid-term persona and the existing long-term \
persona for this user, produce an updated long-term persona containing only the most stable, \
enduring traits. Remove transient patterns. Keep it under 300 words.";

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
        interval.tick().await;
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
            if let Err(e) = self.consolidate_long_summary().await {
                eprintln!("[consolidator] long_summary error: {e}");
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
            if let Err(e) = self.consolidate_long_persona().await {
                eprintln!("[consolidator] long_persona error: {e}");
            }
        }
    }

    // RFC 003 Section 4: short summary hourly, source_refs = [[start_msg_id, end_msg_id]]
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

            // RFC 003 Section 2.2: short source_refs = [[start,end]] (msg_id range pairs)
            let first_id = messages.first().map(|m| m.msg_id).unwrap_or(0);
            let last_id = messages.last().map(|m| m.msg_id).unwrap_or(0);
            let source_refs = serde_json::to_string(&vec![[first_id, last_id]])?;

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

            println!(
                "[consolidator] short_summary chat_id={chat_id} period={period_start} msgs={}",
                messages.len()
            );
        }
        Ok(())
    }

    // RFC 003 Section 4: mid summary daily, source_refs = ["dateThour", ...]
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

            // RFC 003 Section 2.2: mid source_refs = ["2026-03-10T8", "2026-03-10T14"]
            let source_refs: Vec<&str> = short_summaries
                .iter()
                .map(|s| s.period_start.as_str())
                .collect();
            let source_refs = serde_json::to_string(&source_refs)?;

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

            println!(
                "[consolidator] mid_summary chat_id={chat_id} period={period_start} shorts={}",
                short_summaries.len()
            );
        }
        Ok(())
    }

    // RFC 003 Section 4.1: long summary monthly, source_refs = ["2026-03-08", "2026-03-10"]
    async fn consolidate_long_summary(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let chat_ids = self.message_store.list_chat_ids()?;
        let (month_start, month_end, period_start) = last_month_range();

        for chat_id in &chat_ids {
            let mid_summaries = self
                .message_store
                .list_summaries(chat_id, "mid", Some(&month_start), Some(&month_end), 100)
                .await?;
            if mid_summaries.is_empty() {
                continue;
            }

            // Only generate if we don't already have a long summary for this month
            let existing = self
                .message_store
                .get_summaries_by_period_prefix(chat_id, "long", &period_start)
                .await?;
            if !existing.is_empty() {
                continue;
            }

            let user_prompt = mid_summaries
                .iter()
                .map(|s| format!("[{}] {}", s.period_start, s.content))
                .collect::<Vec<_>>()
                .join("\n\n");

            let summary_text = self.llm.complete(LONG_SUMMARY_SYSTEM_PROMPT, &user_prompt).await?;

            // RFC 003 Section 2.2: long source_refs = ["2026-03-08", "2026-03-10", ...]
            let source_refs: Vec<&str> = mid_summaries
                .iter()
                .map(|s| s.period_start.as_str())
                .collect();
            let source_refs = serde_json::to_string(&source_refs)?;

            self.message_store
                .insert_summary(
                    chat_id,
                    &InsertSummary {
                        layer: "long".to_string(),
                        period_start: period_start.clone(),
                        period_end: month_end.clone(),
                        source_refs,
                        content: summary_text,
                    },
                )
                .await?;

            println!(
                "[consolidator] long_summary chat_id={chat_id} period={period_start} mids={}",
                mid_summaries.len()
            );
        }
        Ok(())
    }

    // RFC 003 Section 8.1: short persona hourly, per-user from raw messages
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

            // RFC 003 Section 8.1: per-user observations from raw messages
            let mut by_speaker: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::new();
            for msg in &messages {
                by_speaker
                    .entry(msg.speaker.clone())
                    .or_default()
                    .push(format!("[{}] {}", msg.ts, msg.text));
            }

            for (speaker, texts) in &by_speaker {
                let user_prompt = format!(
                    "User: {speaker}\nMessages:\n{}",
                    texts.join("\n")
                );
                let persona_text = self
                    .llm
                    .complete(SHORT_PERSONA_SYSTEM_PROMPT, &user_prompt)
                    .await?;
                self.persona_store
                    .append_short(speaker, &period, &persona_text)
                    .await?;
            }

            println!(
                "[consolidator] short_persona chat_id={chat_id} period={period} speakers={}",
                by_speaker.len()
            );
        }
        Ok(())
    }

    // RFC 003 Section 8.1: mid persona daily rewrite from short observations
    async fn consolidate_mid_persona(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let chat_ids = self.message_store.list_chat_ids()?;
        let (day_start, _day_end, period) = last_day_range();

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

    // RFC 003 Section 8.1: long persona (weekly/monthly) from mid
    async fn consolidate_long_persona(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let chat_ids = self.message_store.list_chat_ids()?;

        let mut all_speakers = std::collections::HashSet::new();
        for chat_id in &chat_ids {
            let messages = self
                .message_store
                .get_messages_by_ts_range(chat_id, "0000-01-01", "9999-12-31")
                .await?;
            for msg in &messages {
                all_speakers.insert(msg.speaker.clone());
            }
        }

        for speaker in &all_speakers {
            let existing_mid = self.persona_store.get_latest_mid(speaker).await?;
            let mid_text = match existing_mid {
                Some(e) => e.content,
                None => continue,
            };

            let existing_long = self.persona_store.get_long(speaker).await?;
            let existing_long_text = existing_long
                .map(|e| e.content)
                .unwrap_or_else(|| "(no existing long-term persona)".to_string());

            let user_prompt = format!(
                "Current long-term persona:\n{existing_long_text}\n\n\
                 Current mid-term persona:\n{mid_text}"
            );

            let long_text = self
                .llm
                .complete(LONG_PERSONA_SYSTEM_PROMPT, &user_prompt)
                .await?;
            self.persona_store.write_long(speaker, &long_text).await?;

            println!("[consolidator] long_persona speaker={speaker}");
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
    let start_secs = now_secs - 3600;

    let start = format_ts(start_secs);
    let end = format_ts(now_secs);
    // RFC 003 Section 3.1: {date}T{hour} format (0-23)
    let period = format_period_hour(start_secs);
    (start, end, period)
}

fn last_day_range() -> (String, String, String) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let start_secs = now_secs - 86400;

    let start = format_ts(start_secs);
    let end = format_ts(now_secs);
    let period = format_period_day(start_secs);
    (start, end, period)
}

fn last_month_range() -> (String, String, String) {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let days = (now_secs / 86400) as i64;
    let (y, mo, _d) = crate::message_store::civil_from_days_u(days);

    let period = format!("{y:04}-{mo:02}");
    let month_start = format!("{y:04}-{mo:02}-01");
    let month_end = format_ts(now_secs);
    (month_start, month_end, period)
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
    format!("{y:04}-{mo:02}-{d:02}T{}", tod / 3600)
}

fn format_period_day(secs: u64) -> String {
    let days = secs / 86400;
    let (y, mo, d) = crate::message_store::civil_from_days_u(days as i64);
    format!("{y:04}-{mo:02}-{d:02}")
}
