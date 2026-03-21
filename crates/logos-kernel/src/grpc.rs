use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::pb::logos_server::Logos;
use crate::pb::*;
use logos_vfs::{RoutingTable, VfsError};

pub struct LogosService {
    table: Arc<RoutingTable>,
    system: Arc<logos_system::SystemModule>,
}

impl LogosService {
    pub fn new(table: Arc<RoutingTable>, system: Arc<logos_system::SystemModule>) -> Self {
        Self { table, system }
    }
}

fn vfs_to_status(e: VfsError) -> Status {
    match &e {
        VfsError::InvalidUri(_) | VfsError::InvalidPath(_) | VfsError::InvalidJson(_) => {
            Status::invalid_argument(e.to_string())
        }
        VfsError::NotFound(_) | VfsError::NamespaceNotMounted(_) => {
            Status::not_found(e.to_string())
        }
        VfsError::NotReady => Status::unavailable(e.to_string()),
        _ => Status::internal(e.to_string()),
    }
}

#[tonic::async_trait]
impl Logos for LogosService {
    async fn read(&self, request: Request<ReadReq>) -> Result<Response<ReadRes>, Status> {
        let uri = request.into_inner().uri;
        let content = self.table.read(&uri).await.map_err(vfs_to_status)?;
        Ok(Response::new(ReadRes { content }))
    }

    async fn write(&self, request: Request<WriteReq>) -> Result<Response<WriteRes>, Status> {
        let req = request.into_inner();
        self.table
            .write(&req.uri, &req.content)
            .await
            .map_err(vfs_to_status)?;
        Ok(Response::new(WriteRes {}))
    }

    async fn patch(&self, request: Request<PatchReq>) -> Result<Response<PatchRes>, Status> {
        let req = request.into_inner();
        self.table
            .patch(&req.uri, &req.partial)
            .await
            .map_err(vfs_to_status)?;
        Ok(Response::new(PatchRes {}))
    }

    async fn exec(&self, _request: Request<ExecReq>) -> Result<Response<ExecRes>, Status> {
        Err(Status::unimplemented("sandbox not mounted"))
    }

    async fn call(&self, _request: Request<CallReq>) -> Result<Response<CallRes>, Status> {
        Err(Status::unimplemented("proc not mounted"))
    }

    async fn complete(
        &self,
        request: Request<CompleteReq>,
    ) -> Result<Response<CompleteRes>, Status> {
        let req = request.into_inner();
        let params = logos_system::complete::CompleteParams {
            task_id: String::new(), // TODO: derive from connection-bound task
            summary: req.summary,
            reply: req.reply,
            anchor: req.anchor,
            task_log: req.task_log,
            sleep_reason: req.sleep_reason,
            sleep_retry: req.sleep_retry,
            resume_task_id: req.resume_task_id,
        };
        self.system.complete(params).await.map_err(vfs_to_status)?;
        Ok(Response::new(CompleteRes {}))
    }
}
