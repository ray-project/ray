//! Full implementation of PlacementGroupInfoGcsService.
//!
//! Manages placement group registration, lifecycle, state transitions,
//! and name uniqueness enforcement.
//!
//! Maps C++ `GcsPlacementGroupManager` from
//! `src/ray/gcs/gcs_placement_group_manager.h/cc`.
//!
//! ## Placement Group Lifecycle (matching C++ semantics)
//!
//! States: PENDING → CREATED → REMOVED
//!                  → RESCHEDULING → CREATED
//!
//! Since we don't have a bundle scheduler, PGs transition directly to CREATED
//! on creation. The PENDING and RESCHEDULING states will be used when
//! scheduler integration is added.
//!
//! Key C++ behaviors preserved:
//! - Named PG uniqueness per namespace
//! - Idempotent creation (duplicate PG ID returns OK)
//! - REMOVED state persisted to storage (not deleted)
//! - `wait_placement_group_until_ready` returns meaningful status

use std::sync::Arc;

use dashmap::DashMap;
use tonic::{Request, Response, Status};
use tracing::{info, warn};

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

fn error_status(code: i32, msg: &str) -> GcsStatus {
    GcsStatus {
        code,
        message: msg.to_string(),
    }
}

fn not_found_status(msg: &str) -> GcsStatus {
    error_status(17, msg)
}

fn invalid_status(msg: &str) -> GcsStatus {
    error_status(5, msg)
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// GCS Placement Group Manager.
pub struct GcsPlacementGroupManager {
    /// placement_group_id -> PlacementGroupTableData
    /// Contains all registered (non-removed) placement groups.
    /// Removed PGs are deleted from this map but kept in storage.
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

    /// Initialize from persisted data (on restart recovery).
    pub fn initialize(
        &self,
        pgs: &std::collections::HashMap<String, PlacementGroupTableData>,
    ) {
        for (_key, pg) in pgs {
            // Skip REMOVED PGs — they stay in storage but not in the in-memory map.
            if pg.state == PlacementGroupState::Removed as i32 {
                continue;
            }
            let pg_id = pg.placement_group_id.clone();
            if !pg.name.is_empty() {
                self.named_pgs.insert(
                    (pg.name.clone(), pg.ray_namespace.clone()),
                    pg_id.clone(),
                );
            }
            self.placement_groups.insert(pg_id, pg.clone());
        }
        info!(
            placement_groups = self.placement_groups.len(),
            named = self.named_pgs.len(),
            "Placement group manager initialized"
        );
    }

    /// Check if a named PG slot is occupied by a non-REMOVED PG.
    fn is_named_pg_active(&self, name: &str, namespace: &str) -> bool {
        let key = (name.to_string(), namespace.to_string());
        if let Some(existing_id) = self.named_pgs.get(&key) {
            if let Some(existing_pg) = self.placement_groups.get(existing_id.value()) {
                return existing_pg.state != PlacementGroupState::Removed as i32;
            }
        }
        false
    }
}

#[tonic::async_trait]
impl PlacementGroupInfoGcsService for GcsPlacementGroupManager {
    /// Create a placement group.
    ///
    /// Matches C++ RegisterPlacementGroup (gcs_placement_group_manager.cc:107-190):
    /// - Idempotent: if PG ID already exists, returns OK
    /// - Named PG uniqueness: returns Invalid if name taken
    /// - Since no scheduler exists, PGs go directly to CREATED state
    async fn create_placement_group(
        &self,
        req: Request<CreatePlacementGroupRequest>,
    ) -> Result<Response<CreatePlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let Some(spec) = inner.placement_group_spec else {
            return Ok(Response::new(CreatePlacementGroupReply {
                status: Some(ok_status()),
            }));
        };

        let pg_id = spec.placement_group_id.clone();

        // Idempotency: if PG already exists, return OK (network retry).
        if self.placement_groups.contains_key(&pg_id) {
            return Ok(Response::new(CreatePlacementGroupReply {
                status: Some(ok_status()),
            }));
        }

        let pg_data = PlacementGroupTableData {
            placement_group_id: pg_id.clone(),
            name: spec.name.clone(),
            bundles: spec.bundles.clone(),
            strategy: spec.strategy,
            state: PlacementGroupState::Created as i32,
            creator_job_id: spec.creator_job_id.clone(),
            creator_actor_id: spec.creator_actor_id.clone(),
            creator_job_dead: spec.creator_job_dead,
            creator_actor_dead: spec.creator_actor_dead,
            is_detached: spec.is_detached,
            ray_namespace: String::new(),
            ..Default::default()
        };

        // Named PG uniqueness check (matching C++).
        if !pg_data.name.is_empty()
            && self.is_named_pg_active(&pg_data.name, &pg_data.ray_namespace)
        {
            return Ok(Response::new(CreatePlacementGroupReply {
                status: Some(invalid_status(&format!(
                    "Placement group with name '{}' already exists in namespace '{}'",
                    pg_data.name, pg_data.ray_namespace
                ))),
            }));
        }

        // Register named PG.
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

        Ok(Response::new(CreatePlacementGroupReply {
            status: Some(ok_status()),
        }))
    }

    /// Remove a placement group.
    ///
    /// Matches C++ RemovePlacementGroup (gcs_placement_group_manager.cc:400-476):
    /// - Transitions state to REMOVED (not deleted from storage)
    /// - Removes from named index
    /// - Removes from in-memory map (but keeps in storage for wait queries)
    async fn remove_placement_group(
        &self,
        req: Request<RemovePlacementGroupRequest>,
    ) -> Result<Response<RemovePlacementGroupReply>, Status> {
        let inner = req.into_inner();
        let pg_id = inner.placement_group_id;

        if let Some((_, mut pg_data)) = self.placement_groups.remove(&pg_id) {
            // Remove from named index.
            if !pg_data.name.is_empty() {
                self.named_pgs
                    .remove(&(pg_data.name.clone(), pg_data.ray_namespace.clone()));
            }

            // Transition to REMOVED and persist (matching C++: state stays in storage).
            pg_data.state = PlacementGroupState::Removed as i32;
            self.table_storage
                .placement_group_table()
                .put(&hex(&pg_id), &pg_data)
                .await;

            info!(pg_id = hex(&pg_id), "Placement group removed");
        }

        Ok(Response::new(RemovePlacementGroupReply {
            status: Some(ok_status()),
        }))
    }

    /// Get placement group by ID.
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

    /// Get named placement group by (name, namespace).
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

    /// Get all placement groups, with optional limit.
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

    /// Wait for a placement group to be ready (CREATED state).
    ///
    /// Matches C++ WaitPlacementGroupUntilReady (gcs_placement_group_manager.cc:574-641):
    /// - If PG in memory AND CREATED: return OK
    /// - If PG not in memory: check storage (may be REMOVED)
    /// - If REMOVED or not found: return NotFound
    async fn wait_placement_group_until_ready(
        &self,
        req: Request<WaitPlacementGroupUntilReadyRequest>,
    ) -> Result<Response<WaitPlacementGroupUntilReadyReply>, Status> {
        let inner = req.into_inner();
        let pg_id = inner.placement_group_id;

        // Check in-memory map first.
        if let Some(pg) = self.placement_groups.get(&pg_id) {
            if pg.state == PlacementGroupState::Created as i32 {
                return Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                    status: Some(ok_status()),
                }));
            }
            if pg.state == PlacementGroupState::Removed as i32 {
                return Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                    status: Some(not_found_status(
                        "Placement group is removed",
                    )),
                }));
            }
            // PENDING/RESCHEDULING: in practice won't happen without a scheduler,
            // but if it does, we return OK since we don't have a callback mechanism.
            return Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                status: Some(ok_status()),
            }));
        }

        // Not in memory — check storage (may be a REMOVED PG).
        let stored = self
            .table_storage
            .placement_group_table()
            .get(&hex(&pg_id))
            .await;

        match stored {
            Some(pg) if pg.state == PlacementGroupState::Removed as i32 => {
                Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                    status: Some(not_found_status(
                        "Placement group is removed",
                    )),
                }))
            }
            Some(_) => {
                // Found in storage but not in memory — shouldn't happen normally.
                warn!(
                    pg_id = hex(&pg_id),
                    "PG found in storage but not in memory"
                );
                Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                    status: Some(ok_status()),
                }))
            }
            None => Ok(Response::new(WaitPlacementGroupUntilReadyReply {
                status: Some(not_found_status("Placement group not found")),
            })),
        }
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

        // PG should be removed from in-memory map.
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

        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"pg_wait", "wait_pg")),
        }))
        .await
        .unwrap();

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

    // --- New tests for C++ semantic parity ---

    #[tokio::test]
    async fn test_named_pg_uniqueness() {
        let mgr = make_manager();

        // First creation succeeds.
        let reply = mgr
            .create_placement_group(Request::new(CreatePlacementGroupRequest {
                placement_group_spec: Some(make_pg_spec(b"pg1", "SharedName")),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);

        // Second creation with same name fails.
        let reply = mgr
            .create_placement_group(Request::new(CreatePlacementGroupRequest {
                placement_group_spec: Some(make_pg_spec(b"pg2", "SharedName")),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 5); // Invalid
    }

    #[tokio::test]
    async fn test_create_idempotent() {
        let mgr = make_manager();
        let spec = make_pg_spec(b"pg1", "my_pg");

        // First creation.
        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();

        // Second creation with same PG ID succeeds (idempotent).
        let reply = mgr
            .create_placement_group(Request::new(CreatePlacementGroupRequest {
                placement_group_spec: Some(spec),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);

        // Only one PG in the map.
        assert_eq!(mgr.placement_groups.len(), 1);
    }

    #[tokio::test]
    async fn test_remove_sets_removed_state() {
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

        // PG should be gone from in-memory map.
        assert!(!mgr.placement_groups.contains_key(b"pg1".as_slice()));

        // But should still exist in storage with REMOVED state.
        let stored = mgr
            .table_storage
            .placement_group_table()
            .get(&hex(b"pg1"))
            .await;
        assert!(stored.is_some());
        assert_eq!(
            stored.unwrap().state,
            PlacementGroupState::Removed as i32
        );
    }

    #[tokio::test]
    async fn test_wait_for_created_pg() {
        let mgr = make_manager();

        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"pg1", "")),
        }))
        .await
        .unwrap();

        let reply = mgr
            .wait_placement_group_until_ready(Request::new(
                WaitPlacementGroupUntilReadyRequest {
                    placement_group_id: b"pg1".to_vec(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }

    #[tokio::test]
    async fn test_wait_for_removed_pg() {
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

        // Wait should return NotFound since PG is removed.
        let reply = mgr
            .wait_placement_group_until_ready(Request::new(
                WaitPlacementGroupUntilReadyRequest {
                    placement_group_id: b"pg1".to_vec(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        let status = reply.status.unwrap();
        assert_eq!(status.code, 17); // NotFound
        assert!(status.message.contains("removed"));
    }

    #[tokio::test]
    async fn test_wait_for_nonexistent_pg() {
        let mgr = make_manager();

        let reply = mgr
            .wait_placement_group_until_ready(Request::new(
                WaitPlacementGroupUntilReadyRequest {
                    placement_group_id: b"no_such_pg".to_vec(),
                },
            ))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 17); // NotFound
    }

    #[tokio::test]
    async fn test_remove_frees_name() {
        let mgr = make_manager();

        mgr.create_placement_group(Request::new(CreatePlacementGroupRequest {
            placement_group_spec: Some(make_pg_spec(b"pg1", "SharedName")),
        }))
        .await
        .unwrap();

        // Remove the named PG.
        mgr.remove_placement_group(Request::new(RemovePlacementGroupRequest {
            placement_group_id: b"pg1".to_vec(),
        }))
        .await
        .unwrap();

        // Name should be freed — a new PG can reuse it.
        let reply = mgr
            .create_placement_group(Request::new(CreatePlacementGroupRequest {
                placement_group_spec: Some(make_pg_spec(b"pg2", "SharedName")),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(reply.status.unwrap().code, 0);
    }
}
