use crate::service::VfsError;

#[derive(Debug, Clone, PartialEq)]
pub struct LogosUri {
    pub namespace: String,
    pub path: Vec<String>,
}

/// Parse a `logos://` or legacy `mem://` URI into namespace + path segments.
///
/// Examples:
///   - `logos://memory/groups/abc/messages/1` -> namespace="memory", path=["groups","abc","messages","1"]
///   - `logos://users/uid/persona/short`      -> namespace="users", path=["uid","persona","short"]
///   - `logos://system/tasks`                 -> namespace="system", path=["tasks"]
///   - `mem://users/uid/preferences.json`     -> namespace="users", path=["uid","preferences.json"]
pub fn parse_uri(raw: &str) -> Result<LogosUri, VfsError> {
    let stripped = raw
        .strip_prefix("logos://")
        .or_else(|| raw.strip_prefix("mem://"))
        .ok_or_else(|| {
            VfsError::InvalidPath(format!(
                "URI must start with logos:// or mem://, got: {raw}"
            ))
        })?;

    let segments: Vec<&str> = stripped
        .split('/')
        .filter(|s| !s.is_empty())
        .collect();

    if segments.is_empty() {
        return Err(VfsError::InvalidPath(
            "URI has no namespace segment".to_string(),
        ));
    }

    Ok(LogosUri {
        namespace: segments[0].to_string(),
        path: segments[1..].iter().map(|s| s.to_string()).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_message_uri() {
        let uri = parse_uri("logos://memory/groups/abc/messages/42").unwrap();
        assert_eq!(uri.namespace, "memory");
        assert_eq!(uri.path, vec!["groups", "abc", "messages", "42"]);
    }

    #[test]
    fn parse_summary_short_latest() {
        let uri = parse_uri("logos://memory/groups/gid/summary/short/latest").unwrap();
        assert_eq!(uri.namespace, "memory");
        assert_eq!(uri.path, vec!["groups", "gid", "summary", "short", "latest"]);
    }

    #[test]
    fn parse_summary_short_date_hour() {
        let uri = parse_uri("logos://memory/groups/gid/summary/short/2026-03-12T14").unwrap();
        assert_eq!(uri.path[4], "2026-03-12T14");
    }

    #[test]
    fn parse_summary_mid_date() {
        let uri = parse_uri("logos://memory/groups/gid/summary/mid/2026-03-12").unwrap();
        assert_eq!(uri.path[3], "mid");
        assert_eq!(uri.path[4], "2026-03-12");
    }

    #[test]
    fn parse_summary_long_year_month() {
        let uri = parse_uri("logos://memory/groups/gid/summary/long/2026-03").unwrap();
        assert_eq!(uri.path[3], "long");
        assert_eq!(uri.path[4], "2026-03");
    }

    #[test]
    fn parse_persona_long_md() {
        let uri = parse_uri("logos://users/1234/persona/long.md").unwrap();
        assert_eq!(uri.namespace, "users");
        assert_eq!(uri.path, vec!["1234", "persona", "long.md"]);
    }

    #[test]
    fn parse_persona_mid() {
        let uri = parse_uri("logos://users/1234/persona/mid/").unwrap();
        assert_eq!(uri.namespace, "users");
        assert_eq!(uri.path, vec!["1234", "persona", "mid"]);
    }

    #[test]
    fn parse_persona_short() {
        let uri = parse_uri("logos://users/1234/persona/short/").unwrap();
        assert_eq!(uri.path, vec!["1234", "persona", "short"]);
    }

    #[test]
    fn parse_system_anchors() {
        let uri = parse_uri("logos://system/anchors/task-001/2026-03-12T14:32:00").unwrap();
        assert_eq!(uri.namespace, "system");
        assert_eq!(uri.path, vec!["anchors", "task-001", "2026-03-12T14:32:00"]);
    }

    #[test]
    fn parse_system_anchors_list() {
        let uri = parse_uri("logos://system/anchors/task-001/").unwrap();
        assert_eq!(uri.path, vec!["anchors", "task-001"]);
    }

    #[test]
    fn parse_system_tasks() {
        let uri = parse_uri("logos://system/tasks").unwrap();
        assert_eq!(uri.namespace, "system");
        assert_eq!(uri.path, vec!["tasks"]);
    }

    #[test]
    fn parse_system_task_description_write() {
        let uri = parse_uri("logos://system/tasks/task-001/description").unwrap();
        assert_eq!(uri.path, vec!["tasks", "task-001", "description"]);
    }

    #[test]
    fn parse_legacy_mem_uri() {
        let uri = parse_uri("mem://users/uid/preferences.json").unwrap();
        assert_eq!(uri.namespace, "users");
        assert_eq!(uri.path, vec!["uid", "preferences.json"]);
    }

    #[test]
    fn reject_unknown_scheme() {
        assert!(parse_uri("http://example.com").is_err());
    }

    #[test]
    fn reject_empty_path() {
        assert!(parse_uri("logos://").is_err());
    }
}
