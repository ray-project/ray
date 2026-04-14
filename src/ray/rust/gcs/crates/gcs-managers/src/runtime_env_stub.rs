//! Stub implementation of RuntimeEnvGcsService.
//!
//! Returns OK/empty responses for all RPCs.

use tonic::{Request, Response, Status};

use gcs_proto::ray::rpc::runtime_env_gcs_service_server::RuntimeEnvGcsService;
use gcs_proto::ray::rpc::*;

pub struct RuntimeEnvServiceStub;

#[tonic::async_trait]
impl RuntimeEnvGcsService for RuntimeEnvServiceStub {
    async fn pin_runtime_env_uri(
        &self,
        _req: Request<PinRuntimeEnvUriRequest>,
    ) -> Result<Response<PinRuntimeEnvUriReply>, Status> {
        Ok(Response::new(PinRuntimeEnvUriReply::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_pin_runtime_env_uri() {
        let svc = RuntimeEnvServiceStub;
        let reply = svc
            .pin_runtime_env_uri(Request::new(PinRuntimeEnvUriRequest::default()))
            .await;
        assert!(reply.is_ok());
    }
}
