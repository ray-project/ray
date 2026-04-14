//! Stub implementation of EventAggregatorService.
//!
//! Returns OK/empty responses for all RPCs.

use tonic::{Request, Response, Status};

use gcs_proto::ray::rpc::events::event_aggregator_service_server::EventAggregatorService;
use gcs_proto::ray::rpc::events::*;

pub struct EventExportServiceStub;

#[tonic::async_trait]
impl EventAggregatorService for EventExportServiceStub {
    async fn add_events(
        &self,
        _req: Request<AddEventsRequest>,
    ) -> Result<Response<AddEventsReply>, Status> {
        Ok(Response::new(AddEventsReply::default()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_events() {
        let svc = EventExportServiceStub;
        let reply = svc
            .add_events(Request::new(AddEventsRequest::default()))
            .await;
        assert!(reply.is_ok());
    }
}
