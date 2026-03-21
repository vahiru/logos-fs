use crate::VfsError;

#[derive(Debug, Clone, PartialEq)]
pub struct LogosUri<'a> {
    pub namespace: &'a str,
    pub path: Vec<&'a str>,
}

/// Parse a `logos://` or legacy `mem://` URI into namespace + path segments.
///
/// ```text
/// logos://memory/groups/abc/messages/1  → namespace="memory", path=["groups","abc","messages","1"]
/// logos://users/uid/persona/short       → namespace="users",  path=["uid","persona","short"]
/// logos://system/tasks                  → namespace="system", path=["tasks"]
/// mem://users/uid/prefs.json            → namespace="users",  path=["uid","prefs.json"]
/// ```
pub fn parse(raw: &str) -> Result<LogosUri<'_>, VfsError> {
    let stripped = raw
        .strip_prefix("logos://")
        .or_else(|| raw.strip_prefix("mem://"))
        .ok_or_else(|| VfsError::InvalidUri(raw.to_string()))?;

    let segments: Vec<&str> = stripped.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() {
        return Err(VfsError::InvalidUri("URI has no namespace".to_string()));
    }

    Ok(LogosUri {
        namespace: segments[0],
        path: segments[1..].to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn memory_message() {
        let uri = parse("logos://memory/groups/abc/messages/42").unwrap();
        assert_eq!(uri.namespace, "memory");
        assert_eq!(uri.path, ["groups", "abc", "messages", "42"]);
    }

    #[test]
    fn summary_short_latest() {
        let uri = parse("logos://memory/groups/gid/summary/short/latest").unwrap();
        assert_eq!(uri.namespace, "memory");
        assert_eq!(uri.path, ["groups", "gid", "summary", "short", "latest"]);
    }

    #[test]
    fn summary_short_date_hour() {
        let uri = parse("logos://memory/groups/gid/summary/short/2026-03-12T14").unwrap();
        assert_eq!(uri.path[4], "2026-03-12T14");
    }

    #[test]
    fn summary_mid_date() {
        let uri = parse("logos://memory/groups/gid/summary/mid/2026-03-12").unwrap();
        assert_eq!(uri.path[3], "mid");
        assert_eq!(uri.path[4], "2026-03-12");
    }

    #[test]
    fn summary_long_year_month() {
        let uri = parse("logos://memory/groups/gid/summary/long/2026-03").unwrap();
        assert_eq!(uri.path[3], "long");
        assert_eq!(uri.path[4], "2026-03");
    }

    #[test]
    fn persona_long_md() {
        let uri = parse("logos://users/1234/persona/long.md").unwrap();
        assert_eq!(uri.namespace, "users");
        assert_eq!(uri.path, ["1234", "persona", "long.md"]);
    }

    #[test]
    fn system_tasks() {
        let uri = parse("logos://system/tasks").unwrap();
        assert_eq!(uri.namespace, "system");
        assert_eq!(uri.path, ["tasks"]);
    }

    #[test]
    fn system_anchors() {
        let uri = parse("logos://system/anchors/task-001/2026-03-12T14:32:00").unwrap();
        assert_eq!(uri.namespace, "system");
        assert_eq!(uri.path, ["anchors", "task-001", "2026-03-12T14:32:00"]);
    }

    #[test]
    fn legacy_mem_scheme() {
        let uri = parse("mem://users/uid/prefs.json").unwrap();
        assert_eq!(uri.namespace, "users");
        assert_eq!(uri.path, ["uid", "prefs.json"]);
    }

    #[test]
    fn reject_unknown_scheme() {
        assert!(parse("http://example.com").is_err());
    }

    #[test]
    fn reject_empty_path() {
        assert!(parse("logos://").is_err());
    }

    #[test]
    fn trailing_slash_ignored() {
        let uri = parse("logos://users/uid/persona/mid/").unwrap();
        assert_eq!(uri.path, ["uid", "persona", "mid"]);
    }
}
