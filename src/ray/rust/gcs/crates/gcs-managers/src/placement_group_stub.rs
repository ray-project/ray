//! Full implementation of PlacementGroupInfoGcsService.
//!
//! Stores placement group data and supports create/get/remove operations.
//! Does not perform actual scheduling -- placement groups are immediately
//! set to CREATED state when created. Actual bundle scheduling is handled
//! by raylet in the real system.
//!
//! Maps C++ `GcsPlacementGroupManager` from
//! `src/ray/gcs/gcs_placement_group_manager.h/cc`.

use std::sync::Arc;

use dashmap::DashMap;
use tonic::{Request, Response, Status};
use tracing::info;

use gcs_proto::ray::rpc::placement_group_info_gcs_service_server::PlacementGroupInfoGcsService;
use gcs_proto::ray::rpc::placement_group_table_data::PlacementGroupState;
use gcs_proto::ray::rpc::*;
use gcs_table_storage::GcsTableStorage;

fn ok_status() -> GcsStatus {
    GcsStatus {
        code: 0,
        message: String::new(),
    }
}

fn not_found_status(msg: &str) -> GcsStatus {
    GcsStatus {
        code: 17, // Ray StatusCode::NotFound
        message: msg.to_string(),
    }
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// GCS Placement Group Manager.
pub struct GcsPlacementGroupManager {
    /// placement_group_id -> PlacementGroupTableData
    placement_groups: DashMap<Vec<u8>, PlacementGroupTableData>,
    /// (name, namespace) -> placement_group_id for named placement groups
    named_pgs: DashMap<(String, String), Vec<u8>>,
    table_storage: Arc<GcsTableStorage>,
}

impl GcsPlacementGroupManager {
    pub fn new(table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            placement_groups: DashMap::new(),
            named_pgs: DashMap::new(),
            table_storage,
        }
    }
}

#[tonic::async_trait]
impl PlacementGroupInfoGcsService for GcsPlacementGroupManager {
    async fn create_placement_group(
        &self,
        req: Request<CreatePlacementGroupRequest>,
    ) -> Result<Response<CreatePlacementGroupReply>, Status> {
        let inner = req.into_inner();
        if let Some(spec) = inner.placement_group_spec {
            let pg_id = spec.placement_group_id.clone();

            // Build the table data from the spec.
            let pg_data = PlacementGroupTableData {
                placement_group_id: pg_id.clone(),
                name: spec.name.clone(),
                bundles: spec.bundles.clone(),
                strategy: spec.strategy,
                state: PlacementGroupState::Created as i32, // Immediately mark as created.
                creator_job_id: spec.creator_job_id.clone(),
                creator_actor_id: spec.creator_actor_id.clone(),
                creator_job_dead: spec.creator_job_dead,
                creator_actor_dead: spec.creator_actor_dead,
                is_detached: spec.is_detached,
                ray_namespace: String::new(), // PlacementGroupSpec doesn't carry namespace
                ..Default::default()
            };

            // Index by name if it has one.
            if !pg_data.name.is_empty() {
                self.named_pgs.insert(
                    (pg_data.name.clone(), pg_data.ray_namespace.clone()),
                    pg_id.clone(),
                );
            }

            // Persist.
            self.table_storage
                .placement_group_table()
                .put(&hex(&pg_id), &pg_data)
                .await;
            self.placement_groups.insert(pg_id.clone(), pg_data);

            info!(pg_id = hex(&pg_id), "Placement group created");
        }

        Ok(Response::new(CreatePlacementGroupReply {
            status: Some(ok_status()),
        }))
    }

    async fn remove_placement_group(
        &self,
        req: Request<RemovePlacementGroupRequest>,
    ) -> Result<Response<RemovePlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let pg_id = inner.placement_group_id;

        if let Some((_, pg_data)) = self.placement_groups.remove(&pg_id) {
            // Remove from named index.
            if !pg_data.name.is_empty() {
                self.named_pgs
                    .remove(&(pg_data.name, pg_data.ray_namespace));
            }
            // Remove from storage.
            self.table_storage
                .placement_group_table()
                .delete(&hex(&pg_id))
                .await;
            info!(pg_id = hex(&pg_id), "Placement group removed");
        }

        Ok(Response::new(RemovePlacementGroupReply {
            status: Some(ok_status()),
        }))
    }

    async fn get_placement_group(
        &self,
        req: Request<GetPlacementGroupRequest>,
    ) -> Result<Response<GetPlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let pg = self
            .placement_groups
            .get(&inner.placement_group_id)
            .map(|r| r.clone());

        Ok(Response::new(GetPlacementGroupReply {
            status: Some(if pg.is_some() {
                ok_status()
            } else {
                not_found_status("Placement group not found")
            }),
            placement_group_table_data: pg,
        }))
    }

    async fn get_named_placement_group(
        &self,
        req: Request<GetNamedPlacementGroupRequest>,
    ) -> Result<Response<GetNamedPlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let key = (inner.name, inner.ray_namespace);

        let pg = self
            .named_pgs
            .get(&key)
            .and_then(|pg_id| {
                self.placement_groups
                    .get(pg_id.value())
                    .map(|r| r.clone())
            });

        Ok(Response::new(GetNamedPlacementGroupReply {
            status: Some(if pg.is_some() {
                ok_status()
            } else {
                not_found_status("Named placement group not found")
            }),
            placement_group_table_data: pg,
        }))
    }

    async fn get_all_placement_group(
        &self,
        req: Request<GetAllPlacementGroupRequest>,
    ) -> Result<Response<GetAllPlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let limit = inner.limit.filter(|&l| l > 0).map(|l| l as usize);

        let mut pgs: Vec<PlacementGroupTableData> = Vec::new();
        let total = self.placement_groups.len() as i64;

        for entry in self.placement_groups.iter() {
            if let Some(lim) = limit {
                if pgs.len() >= lim {
                    break;
                }
            }
            pgs.push(entry.value().clone());
        }

        Ok(Response::new(GetAllPlacementGroupReply {
            status: Some(ok_status()),
            placement_group_table_data: pgs,
            total,
        }))
    }

    async fn wait_placement_group_until_ready(
        &self,
        _req: Request<WaitPlacementGroupUntilReadyRequest>,
    ) -> Result<Response<WaitPlacementGroupUntilReadyReply>, Status> {
        // Since we immediately mark PGs as Created, just return OK.
        Ok(Response::new(WaitPlacementGroupUntilReadyReply {
            status: Some(ok_status()),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;

    fn make_manager() -> GcsPlacementGroupManager {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        GcsPlacementGroupManager::new(table_storage)
    }

    fn make_pg_spec(id: &[u8], name: &str) -> PlacementGroupSpec {
        PlacementGroupSpec {
            placement_group_id: id.to_vec(),
            name: name.to_string(),
            creator_job_id: b"job1".to_vec(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_create_and_get_pg() {
        let mgr = make_manager();

        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"pg1", "my_pg")),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_placement_group(Request::new(GetPlacementGroupRequest {
                placement_group_id: b"pg1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.placement_group_table_data.is_some());
        let pg = reply.placement_group_table_data.unwrap();
        assert_eq!(pg.state, PlacementGroupState::Created as i32);
    }

    #[tokio::test]
    async fn test_remove_pg() {
        let mgr = make_manager();

        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"pg1", "")),
        }))
        .await
        .unwrap();

        mgr.remove_placement_group(Request::new(RemovePlacementGroupRequest {
            placement_group_id: b"pg1".to_vec(),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_placement_group(Request::new(GetPlacementGroupRequest {
                placement_group_id: b"pg1".to_vec(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.placement_group_table_data.is_none());
    }

    #[tokio::test]
    async fn test_get_all_pgs() {
        let mgr = make_manager();

        for i in 0..3 {
            mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
                placement_group_spec: Some(make_pg_spec(
                    format!("pg{i}").as_bytes(),
                    &format!("pg_{i}"),
                )),
            }))
            .await
            .unwrap();
        }

        let reply = mgr
            .get_all_placement_group(Request::new(GetAllPlacementGroupRequest::default()))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.placement_group_table_data.len(), 3);
        assert_eq!(reply.total, 3);
    }

    #[tokio::test]
    async fn test_get_named_pg() {
        let mgr = make_manager();

        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"pg1", "named_pg")),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_named_placement_group(Request::new(GetNamedPlacementGroupRequest {
                name: "named_pg".to_string(),
                ray_namespace: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.placement_group_table_data.is_some());
    }

    #[tokio::test]
    async fn test_wait_placement_group_until_ready() {
        let mgr = make_manager();

        // Create a placement group first.
        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"pg_wait", "wait_pg")),
        }))
        .await
        .unwrap();

        // Call wait_until_ready -- since PGs are immediately Created, this should return OK.
        let reply = mgr
            .wait_placement_group_until_ready(Request::new(
                WaitPlacementGroupUntilReadyRequest {
                    placement_group_id: b"pg_wait".to_vec(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }
}
