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
    fn parse_logos_memory_uri() {
        let uri = parse_uri("logos://memory/groups/abc/messages/1").unwrap();
        assert_eq!(uri.namespace, "memory");
        assert_eq!(uri.path, vec!["groups", "abc", "messages", "1"]);
    }

    #[test]
    fn parse_logos_users_uri() {
        let uri = parse_uri("logos://users/uid123/persona/short").unwrap();
        assert_eq!(uri.namespace, "users");
        assert_eq!(uri.path, vec!["uid123", "persona", "short"]);
    }

    #[test]
    fn parse_logos_system_uri() {
        let uri = parse_uri("logos://system/tasks").unwrap();
        assert_eq!(uri.namespace, "system");
        assert_eq!(uri.path, vec!["tasks"]);
    }

    #[test]
    fn parse_legacy_mem_uri() {
        let uri = parse_uri("mem://users/uid/preferences.json").unwrap();
        assert_eq!(uri.namespace, "users");
        assert_eq!(uri.path, vec!["uid", "preferences.json"]);
    }

    #[test]
    fn parse_trailing_slashes() {
        let uri = parse_uri("logos://memory/groups/abc/").unwrap();
        assert_eq!(uri.namespace, "memory");
        assert_eq!(uri.path, vec!["groups", "abc"]);
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
