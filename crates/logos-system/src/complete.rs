use std::sync::Arc;

use logos_vfs::VfsError;

use crate::anchors::AnchorDb;
use crate::tasks::TaskDb;

/// Parameters for logos_complete — the turn terminator.
pub struct CompleteParams {
    pub task_id: String,
    pub summary: String,
    pub reply: String,
    pub anchor: bool,
    pub anchor_facts: String,
    pub task_log: String,
    pub sleep_reason: String,
    pub sleep_retry: bool,
    pub resume_task_id: String,
}

/// Execute logos_complete atomically:
///   1. Update task status (finished / sleep)
///   2. Append task_log (if provided)
///   3. Create anchor (if anchor: true)
///   4. Handle resume (discard current, activate target)
pub async fn execute(
    tasks: &TaskDb,
    anchors: &AnchorDb,
    params: CompleteParams,
) -> Result<(), VfsError> {
    let conn = Arc::clone(&tasks.conn);
    let anchor_conn = Arc::clone(&anchors.conn);

    tokio::task::spawn_blocking(move || {
        // Tasks and anchors share the same SQLite database, so this is one connection.
        let conn = conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;

        // Determine target status
        let new_status = if !params.resume_task_id.is_empty() {
            // Resume: discard current empty task, activate target
            // The current task is just abandoned (no explicit cancelled state)
            TaskDb::transition_status_sync(&conn, &params.resume_task_id, "active")?;
            return Ok(());
        } else if !params.sleep_reason.is_empty() {
            "sleep"
        } else {
            "finished"
        };

        // 1. Transition task status
        if !params.task_id.is_empty() {
            TaskDb::transition_status_sync(&conn, &params.task_id, new_status)?;
        }

        // 2. Append task_log (stored as a separate write to sandbox — for now, no-op
        //    since sandbox/ is not mounted. The runtime can write logs via logos_write
        //    to logos://sandbox/{task_id}/log directly.)

        // 3. Create anchor if requested
        if params.anchor && !params.task_id.is_empty() {
            // anchors share the same db connection
            let _anchor_conn = anchor_conn.lock().map_err(|e| VfsError::Sqlite(e.to_string()))?;
            AnchorDb::create_sync(&conn, &params.task_id, &params.summary, &params.anchor_facts)?;
        }

        Ok(())
    })
    .await
    .map_err(|e| VfsError::Io(e.to_string()))?
}
