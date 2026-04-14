//! Full implementation of ActorInfoGcsService.
//!
//! Manages actor registration, creation, state transitions, and lookup.
//! Publishes state changes via PubSubManager so that subscribers
//! (raylet, dashboard, etc.) get notified.
//!
//! Maps C++ `GcsActorManager` from `src/ray/gcs/gcs_actor_manager.h/cc`.

use std::sync::Arc;

use dashmap::DashMap;
use tonic::{Request, Response, Status};
use tracing::{debug, info, warn};

use gcs_proto::ray::rpc::actor_info_gcs_service_server::ActorInfoGcsService;
use gcs_proto::ray::rpc::actor_table_data::ActorState;
use gcs_proto::ray::rpc::pub_message::InnerMessage;
use gcs_proto::ray::rpc::*;
use gcs_pubsub::PubSubManager;
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

/// GCS Actor Manager.
pub struct GcsActorManager {
    /// actor_id -> ActorTableData
    actors: DashMap<Vec<u8>, ActorTableData>,
    /// (name, namespace) -> actor_id for named actors
    named_actors: DashMap<(String, String), Vec<u8>>,
    /// actor_id -> TaskSpec (the creation task spec)
    actor_task_specs: DashMap<Vec<u8>, TaskSpec>,
    publisher: Arc<PubSubManager>,
    table_storage: Arc<GcsTableStorage>,
}

impl GcsActorManager {
    pub fn new(publisher: Arc<PubSubManager>, table_storage: Arc<GcsTableStorage>) -> Self {
        Self {
            actors: DashMap::new(),
            named_actors: DashMap::new(),
            actor_task_specs: DashMap::new(),
            publisher,
            table_storage,
        }
    }

    /// Initialize from persisted data (on restart recovery).
    pub fn initialize(
        &self,
        actors: &std::collections::HashMap<String, ActorTableData>,
        task_specs: &std::collections::HashMap<String, TaskSpec>,
    ) {
        for (_key, actor) in actors {
            let actor_id = actor.actor_id.clone();
            // Rebuild the named_actors index.
            if !actor.name.is_empty() {
                self.named_actors.insert(
                    (actor.name.clone(), actor.ray_namespace.clone()),
                    actor_id.clone(),
                );
            }
            self.actors.insert(actor_id, actor.clone());
        }
        for (_key, spec) in task_specs {
            if let Some(actor_id) = Self::actor_id_from_task_spec(spec) {
                self.actor_task_specs.insert(actor_id, spec.clone());
            }
        }
        info!(
            actors = self.actors.len(),
            named = self.named_actors.len(),
            task_specs = self.actor_task_specs.len(),
            "Actor manager initialized"
        );
    }

    /// Publish an actor state change on the GCS_ACTOR_CHANNEL (channel_type = 3).
    fn publish_actor_update(&self, actor: &ActorTableData) {
        let msg = PubMessage {
            channel_type: ChannelType::GcsActorChannel as i32,
            key_id: actor.actor_id.clone(),
            sequence_id: 0, // PubSubManager assigns the real sequence_id.
            inner_message: Some(InnerMessage::ActorMessage(actor.clone())),
        };
        self.publisher.publish(msg);
    }

    /// Extract the actor_id from a TaskSpec's actor_creation_task_spec.
    fn actor_id_from_task_spec(task_spec: &TaskSpec) -> Option<Vec<u8>> {
        task_spec
            .actor_creation_task_spec
            .as_ref()
            .map(|s| s.actor_id.clone())
    }

    /// Build initial ActorTableData from a TaskSpec.
    fn build_actor_table_data(task_spec: &TaskSpec, state: ActorState) -> ActorTableData {
        let creation = task_spec.actor_creation_task_spec.as_ref();
        let actor_id = creation
            .map(|c| c.actor_id.clone())
            .unwrap_or_default();
        let name = creation
            .map(|c| c.name.clone())
            .unwrap_or_default();
        let ray_namespace = creation
            .map(|c| c.ray_namespace.clone())
            .unwrap_or_default();
        let is_detached = creation.map(|c| c.is_detached).unwrap_or(false);
        let max_restarts = creation.map(|c| c.max_actor_restarts).unwrap_or(0);

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as f64;

        ActorTableData {
            actor_id,
            parent_id: task_spec.caller_id.clone(),
            job_id: task_spec.job_id.clone(),
            state: state as i32,
            max_restarts,
            num_restarts: 0,
            address: task_spec.caller_address.clone(),
            owner_address: task_spec.caller_address.clone(),
            is_detached,
            name,
            timestamp: now_ms,
            function_descriptor: task_spec.function_descriptor.clone(),
            ray_namespace,
            required_resources: task_spec.required_resources.clone(),
            serialized_runtime_env: task_spec
                .runtime_env_info
                .as_ref()
                .map(|r| r.serialized_runtime_env.clone())
                .unwrap_or_default(),
            class_name: task_spec.name.clone(),
            ..Default::default()
        }
    }
}

#[tonic::async_trait]
impl ActorInfoGcsService for GcsActorManager {
    /// Register an actor -- store its task spec and create initial ActorTableData
    /// in DEPENDENCIES_UNREADY state.
    async fn register_actor(
        &self,
        req: Request<RegisterActorRequest>,
    ) -> Result<Response<RegisterActorReply>, Status> {
        let inner = req.into_inner();
        if let Some(task_spec) = inner.task_spec {
            if let Some(actor_id) = Self::actor_id_from_task_spec(&task_spec) {
                let actor_data = Self::build_actor_table_data(
                    &task_spec,
                    ActorState::DependenciesUnready,
                );

                // Store in named_actors index if it has a name.
                if !actor_data.name.is_empty() {
                    self.named_actors.insert(
                        (actor_data.name.clone(), actor_data.ray_namespace.clone()),
                        actor_id.clone(),
                    );
                }

                // Persist task spec.
                self.table_storage
                    .actor_task_spec_table()
                    .put(&hex(&actor_id), &task_spec)
                    .await;
                self.actor_task_specs
                    .insert(actor_id.clone(), task_spec);

                // Persist actor data.
                self.table_storage
                    .actor_table()
                    .put(&hex(&actor_id), &actor_data)
                    .await;
                self.actors.insert(actor_id.clone(), actor_data.clone());

                self.publish_actor_update(&actor_data);

                info!(actor_id = hex(&actor_id), "Actor registered");
            }
        }
        Ok(Response::new(RegisterActorReply {
            status: Some(ok_status()),
        }))
    }

    /// Create an actor -- transition to ALIVE state.
    /// In the C++ code this involves scheduling onto a node, but for now we
    /// set the actor to ALIVE immediately (the caller/raylet handles actual placement).
    async fn create_actor(
        &self,
        req: Request<CreateActorRequest>,
    ) -> Result<Response<CreateActorReply>, Status> {
        let inner = req.into_inner();
        if let Some(task_spec) = inner.task_spec {
            if let Some(actor_id) = Self::actor_id_from_task_spec(&task_spec) {
                // Update or create the actor data.
                let mut actor_data = self
                    .actors
                    .get(&actor_id)
                    .map(|r| r.clone())
                    .unwrap_or_else(|| {
                        Self::build_actor_table_data(&task_spec, ActorState::Alive)
                    });

                // Transition to ALIVE.
                actor_data.state = ActorState::Alive as i32;
                actor_data.timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as f64;

                // Update address from task spec if available.
                if task_spec.caller_address.is_some() {
                    actor_data.address = task_spec.caller_address.clone();
                }

                // Store in named_actors index if it has a name.
                if !actor_data.name.is_empty() {
                    self.named_actors.insert(
                        (actor_data.name.clone(), actor_data.ray_namespace.clone()),
                        actor_id.clone(),
                    );
                }

                // Persist.
                self.table_storage
                    .actor_table()
                    .put(&hex(&actor_id), &actor_data)
                    .await;
                self.actors.insert(actor_id.clone(), actor_data.clone());

                // Store task spec if not already stored.
                if !self.actor_task_specs.contains_key(&actor_id) {
                    self.table_storage
                        .actor_task_spec_table()
                        .put(&hex(&actor_id), &task_spec)
                        .await;
                    self.actor_task_specs.insert(actor_id.clone(), task_spec);
                }

                self.publish_actor_update(&actor_data);

                info!(actor_id = hex(&actor_id), "Actor created (ALIVE)");

                return Ok(Response::new(CreateActorReply {
                    status: Some(ok_status()),
                    actor_address: actor_data.address.clone(),
                    ..Default::default()
                }));
            }
        }
        Ok(Response::new(CreateActorReply {
            status: Some(ok_status()),
            ..Default::default()
        }))
    }

    /// Get actor info by actor_id.
    async fn get_actor_info(
        &self,
        req: Request<GetActorInfoRequest>,
    ) -> Result<Response<GetActorInfoReply>, Status> {
        let inner = req.into_inner();
        let actor = self
            .actors
            .get(&inner.actor_id)
            .map(|r| r.clone());

        match actor {
            Some(data) => Ok(Response::new(GetActorInfoReply {
                status: Some(ok_status()),
                actor_table_data: Some(data),
            })),
            None => Ok(Response::new(GetActorInfoReply {
                status: Some(not_found_status("Actor not found")),
                actor_table_data: None,
            })),
        }
    }

    /// Get named actor info by (name, namespace).
    async fn get_named_actor_info(
        &self,
        req: Request<GetNamedActorInfoRequest>,
    ) -> Result<Response<GetNamedActorInfoReply>, Status> {
        let inner = req.into_inner();
        let key = (inner.name, inner.ray_namespace);
        let actor_id = self.named_actors.get(&key).map(|r| r.clone());

        match actor_id {
            Some(aid) => {
                let actor = self.actors.get(&aid).map(|r| r.clone());
                let task_spec = self.actor_task_specs.get(&aid).map(|r| r.clone());
                Ok(Response::new(GetNamedActorInfoReply {
                    status: Some(ok_status()),
                    actor_table_data: actor,
                    task_spec,
                }))
            }
            None => Ok(Response::new(GetNamedActorInfoReply {
                status: Some(not_found_status("Named actor not found")),
                actor_table_data: None,
                task_spec: None,
            })),
        }
    }

    /// List all named actors, optionally filtered by namespace.
    async fn list_named_actors(
        &self,
        req: Request<ListNamedActorsRequest>,
    ) -> Result<Response<ListNamedActorsReply>, Status> {
        let inner = req.into_inner();
        let mut result = Vec::new();

        for entry in self.named_actors.iter() {
            let (name, ns) = entry.key();
            if inner.all_namespaces || *ns == inner.ray_namespace {
                // Only include actors that are not DEAD.
                let actor_id = entry.value();
                let is_alive = self
                    .actors
                    .get(actor_id)
                    .map(|a| a.state != ActorState::Dead as i32)
                    .unwrap_or(false);
                if is_alive {
                    result.push(NamedActorInfo {
                        ray_namespace: ns.clone(),
                        name: name.clone(),
                    });
                }
            }
        }

        Ok(Response::new(ListNamedActorsReply {
            status: Some(ok_status()),
            named_actors_list: result,
        }))
    }

    /// Get all actors, with optional filters and limit.
    async fn get_all_actor_info(
        &self,
        req: Request<GetAllActorInfoRequest>,
    ) -> Result<Response<GetAllActorInfoReply>, Status> {
        let inner = req.into_inner();
        let limit = inner.limit.filter(|&l| l > 0).map(|l| l as usize);

        let mut actors: Vec<ActorTableData> = Vec::new();
        let mut num_filtered: i64 = 0;
        let total = self.actors.len() as i64;

        for entry in self.actors.iter() {
            let actor = entry.value();

            // Apply filters if present.
            if let Some(ref filters) = inner.filters {
                if let Some(ref filter_actor_id) = filters.actor_id {
                    if !filter_actor_id.is_empty() && actor.actor_id != *filter_actor_id {
                        num_filtered += 1;
                        continue;
                    }
                }
                if let Some(ref filter_job_id) = filters.job_id {
                    if !filter_job_id.is_empty() && actor.job_id != *filter_job_id {
                        num_filtered += 1;
                        continue;
                    }
                }
                if let Some(filter_state) = filters.state {
                    if actor.state != filter_state {
                        num_filtered += 1;
                        continue;
                    }
                }
            }

            if let Some(lim) = limit {
                if actors.len() >= lim {
                    continue;
                }
            }

            actors.push(actor.clone());
        }

        Ok(Response::new(GetAllActorInfoReply {
            status: Some(ok_status()),
            actor_table_data: actors,
            total,
            num_filtered,
        }))
    }

    /// Kill an actor via GCS -- mark it DEAD and publish the update.
    async fn kill_actor_via_gcs(
        &self,
        req: Request<KillActorViaGcsRequest>,
    ) -> Result<Response<KillActorViaGcsReply>, Status> {
        let inner = req.into_inner();
        let actor_id = inner.actor_id;

        if let Some(mut entry) = self.actors.get_mut(&actor_id) {
            entry.state = ActorState::Dead as i32;
            entry.timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as f64;
            entry.end_time = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            let actor_data = entry.clone();
            drop(entry);

            // Persist.
            self.table_storage
                .actor_table()
                .put(&hex(&actor_id), &actor_data)
                .await;

            self.publish_actor_update(&actor_data);
            info!(actor_id = hex(&actor_id), "Actor killed via GCS");
        } else {
            warn!(actor_id = hex(&actor_id), "Kill request for unknown actor");
        }

        Ok(Response::new(KillActorViaGcsReply {
            status: Some(ok_status()),
        }))
    }

    /// Handle actor going out of scope.
    async fn report_actor_out_of_scope(
        &self,
        req: Request<ReportActorOutOfScopeRequest>,
    ) -> Result<Response<ReportActorOutOfScopeReply>, Status> {
        let inner = req.into_inner();
        let actor_id = inner.actor_id;

        if let Some(mut entry) = self.actors.get_mut(&actor_id) {
            // If not detached, mark as dead.
            if !entry.is_detached {
                entry.state = ActorState::Dead as i32;
                entry.timestamp = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as f64;
                entry.end_time = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;

                let actor_data = entry.clone();
                drop(entry);

                self.table_storage
                    .actor_table()
                    .put(&hex(&actor_id), &actor_data)
                    .await;
                self.publish_actor_update(&actor_data);
                debug!(
                    actor_id = hex(&actor_id),
                    "Actor out of scope, marked DEAD"
                );
            }
        }

        Ok(Response::new(ReportActorOutOfScopeReply {
            status: Some(ok_status()),
        }))
    }

    /// Restart actor for lineage reconstruction.
    async fn restart_actor_for_lineage_reconstruction(
        &self,
        req: Request<RestartActorForLineageReconstructionRequest>,
    ) -> Result<Response<RestartActorForLineageReconstructionReply>, Status> {
        let inner = req.into_inner();
        let actor_id = inner.actor_id;

        if let Some(mut entry) = self.actors.get_mut(&actor_id) {
            // Mark as restarting.
            entry.state = ActorState::Restarting as i32;
            entry.num_restarts += 1;
            entry.num_restarts_due_to_lineage_reconstruction =
                inner.num_restarts_due_to_lineage_reconstruction;
            entry.timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as f64;

            let actor_data = entry.clone();
            drop(entry);

            self.table_storage
                .actor_table()
                .put(&hex(&actor_id), &actor_data)
                .await;
            self.publish_actor_update(&actor_data);
            info!(
                actor_id = hex(&actor_id),
                "Actor restarting for lineage reconstruction"
            );
        }

        Ok(Response::new(
            RestartActorForLineageReconstructionReply {
                status: Some(ok_status()),
            },
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcs_store::InMemoryStoreClient;

    fn make_manager() -> GcsActorManager {
        let store = Arc::new(InMemoryStoreClient::new());
        let table_storage = Arc::new(GcsTableStorage::new(store));
        let publisher = Arc::new(PubSubManager::new(b"test_pub".to_vec()));
        GcsActorManager::new(publisher, table_storage)
    }

    fn make_task_spec(actor_id: &[u8], name: &str) -> TaskSpec {
        TaskSpec {
            r#type: 1, // ACTOR_CREATION_TASK
            name: name.to_string(),
            job_id: b"job1".to_vec(),
            task_id: b"task1".to_vec(),
            caller_id: b"caller1".to_vec(),
            caller_address: Some(Address {
                node_id: b"node1".to_vec(),
                ip_address: "127.0.0.1".to_string(),
                port: 10001,
                worker_id: b"worker1".to_vec(),
            }),
            actor_creation_task_spec: Some(ActorCreationTaskSpec {
                actor_id: actor_id.to_vec(),
                name: name.to_string(),
                ray_namespace: "default".to_string(),
                is_detached: false,
                max_actor_restarts: 0,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn test_register_and_get_actor() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "MyActor");

        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_actor_info(Request::new(GetActorInfoRequest {
                actor_id: b"actor1".to_vec(),
                name: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.actor_table_data.is_some());
        let actor = reply.actor_table_data.unwrap();
        assert_eq!(actor.state, ActorState::DependenciesUnready as i32);
    }

    #[tokio::test]
    async fn test_create_actor_alive() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "MyActor");

        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();

        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_actor_info(Request::new(GetActorInfoRequest {
                actor_id: b"actor1".to_vec(),
                name: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        let actor = reply.actor_table_data.unwrap();
        assert_eq!(actor.state, ActorState::Alive as i32);
    }

    #[tokio::test]
    async fn test_get_named_actor() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "MyNamedActor");

        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_named_actor_info(Request::new(GetNamedActorInfoRequest {
                name: "MyNamedActor".to_string(),
                ray_namespace: "default".to_string(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert!(reply.actor_table_data.is_some());
    }

    #[tokio::test]
    async fn test_kill_actor() {
        let mgr = make_manager();
        let spec = make_task_spec(b"actor1", "");

        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        mgr.kill_actor_via_gcs(Request::new(KillActorViaGcsRequest {
            actor_id: b"actor1".to_vec(),
            force_kill: true,
            no_restart: true,
        }))
        .await
        .unwrap();

        let reply = mgr
            .get_actor_info(Request::new(GetActorInfoRequest {
                actor_id: b"actor1".to_vec(),
                name: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        let actor = reply.actor_table_data.unwrap();
        assert_eq!(actor.state, ActorState::Dead as i32);
    }

    #[tokio::test]
    async fn test_get_all_actors() {
        let mgr = make_manager();

        for i in 0..3 {
            let spec = make_task_spec(
                format!("actor{i}").as_bytes(),
                &format!("Actor{i}"),
            );
            mgr.create_actor(Request::new(CreateActorRequest {
                task_spec: Some(spec),
            }))
            .await
            .unwrap();
        }

        let reply = mgr
            .get_all_actor_info(Request::new(GetAllActorInfoRequest::default()))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.actor_table_data.len(), 3);
        assert_eq!(reply.total, 3);
    }

    #[tokio::test]
    async fn test_list_named_actors() {
        let mgr = make_manager();

        let spec1 = make_task_spec(b"a1", "NamedActor1");
        let spec2 = make_task_spec(b"a2", "NamedActor2");

        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec1),
        }))
        .await
        .unwrap();

        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec2),
        }))
        .await
        .unwrap();

        let reply = mgr
            .list_named_actors(Request::new(ListNamedActorsRequest {
                all_namespaces: true,
                ray_namespace: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(reply.named_actors_list.len(), 2);
    }

    #[tokio::test]
    async fn test_report_actor_out_of_scope() {
        let mgr = make_manager();
        // Register a non-detached actor.
        let spec = make_task_spec(b"actor_oos", "");
        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();

        // Create it (transition to ALIVE).
        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        // Report out of scope.
        mgr.report_actor_out_of_scope(Request::new(ReportActorOutOfScopeRequest {
            actor_id: b"actor_oos".to_vec(),
            num_restarts_due_to_lineage_reconstruction: 0,
        }))
        .await
        .unwrap();

        // Verify state is DEAD (non-detached actor should be killed).
        let reply = mgr
            .get_actor_info(Request::new(GetActorInfoRequest {
                actor_id: b"actor_oos".to_vec(),
                name: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        let actor = reply.actor_table_data.unwrap();
        assert_eq!(actor.state, ActorState::Dead as i32);
    }

    #[tokio::test]
    async fn test_restart_actor_for_lineage_reconstruction() {
        let mgr = make_manager();
        // Register + create an actor.
        let spec = make_task_spec(b"actor_restart", "");
        mgr.register_actor(Request::new(RegisterActorRequest {
            task_spec: Some(spec.clone()),
        }))
        .await
        .unwrap();
        mgr.create_actor(Request::new(CreateActorRequest {
            task_spec: Some(spec),
        }))
        .await
        .unwrap();

        // Verify it is ALIVE.
        let reply = mgr
            .get_actor_info(Request::new(GetActorInfoRequest {
                actor_id: b"actor_restart".to_vec(),
                name: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(
            reply.actor_table_data.unwrap().state,
            ActorState::Alive as i32
        );

        // Restart for lineage reconstruction.
        mgr.restart_actor_for_lineage_reconstruction(Request::new(
            RestartActorForLineageReconstructionRequest {
                actor_id: b"actor_restart".to_vec(),
                num_restarts_due_to_lineage_reconstruction: 1,
            },
        ))
        .await
        .unwrap();

        // Verify state is RESTARTING.
        let reply = mgr
            .get_actor_info(Request::new(GetActorInfoRequest {
                actor_id: b"actor_restart".to_vec(),
                name: String::new(),
            }))
            .await
            .unwrap()
            .into_inner();
        let actor = reply.actor_table_data.unwrap();
        assert_eq!(actor.state, ActorState::Restarting as i32);
        assert_eq!(actor.num_restarts, 1);
    }
}
