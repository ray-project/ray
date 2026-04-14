//! GCS initialization data -- loads persisted state on startup.
//!
//! Maps C++ `GcsInitData` from `src/ray/gcs/gcs_init_data.h/cc`.
//! On GCS server restart, this loads all 5 persistent tables from storage
//! in parallel so that managers can be re-initialized with recovered state.

use std::collections::HashMap;

use gcs_proto::ray::rpc::*;
use gcs_table_storage::GcsTableStorage;
use tracing::info;

/// All persisted GCS state, loaded from storage on startup.
///
/// Contains the 5 tables that survive GCS restarts: jobs, nodes,
/// actors, actor task specs, and placement groups.
pub struct GcsInitData {
    pub jobs: HashMap<String, JobTableData>,
    pub nodes: HashMap<String, GcsNodeInfo>,
    pub actors: HashMap<String, ActorTableData>,
    pub actor_task_specs: HashMap<String, TaskSpec>,
    pub placement_groups: HashMap<String, PlacementGroupTableData>,
}

impl GcsInitData {
    /// Load all persistent tables from storage in parallel.
    ///
    /// Maps C++ `GcsInitData::AsyncLoad` which uses a countdown latch
    /// to wait for all 5 tables. Here we use `tokio::join!`.
    pub async fn load(storage: &GcsTableStorage) -> Self {
        let (jobs, nodes, actors, actor_task_specs, placement_groups) = tokio::join!(
            storage.job_table().get_all(),
            storage.node_table().get_all(),
            storage.actor_table().get_all(),
            storage.actor_task_spec_table().get_all(),
            storage.placement_group_table().get_all(),
        );

        info!(
            jobs = jobs.len(),
            nodes = nodes.len(),
            actors = actors.len(),
            actor_task_specs = actor_task_specs.len(),
            placement_groups = placement_groups.len(),
            "Loaded GCS init data from storage"
        );

        Self {
            jobs,
            nodes,
            actors,
            actor_task_specs,
            placement_groups,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use gcs_store::InMemoryStoreClient;

    #[tokio::test]
    async fn test_load_empty_tables() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);

        let init_data = GcsInitData::load(&storage).await;
        assert!(init_data.jobs.is_empty());
        assert!(init_data.nodes.is_empty());
        assert!(init_data.actors.is_empty());
        assert!(init_data.actor_task_specs.is_empty());
        assert!(init_data.placement_groups.is_empty());
    }

    #[tokio::test]
    async fn test_load_populated_tables() {
        let store = Arc::new(InMemoryStoreClient::new());
        let storage = GcsTableStorage::new(store);

        // Populate jobs.
        let mut job = JobTableData::default();
        job.job_id = b"job_1".to_vec();
        job.is_dead = false;
        storage.job_table().put("job_1", &job).await;

        let mut job2 = JobTableData::default();
        job2.job_id = b"job_2".to_vec();
        job2.is_dead = true;
        storage.job_table().put("job_2", &job2).await;

        // Populate nodes.
        let mut node = GcsNodeInfo::default();
        node.node_id = b"node_1".to_vec();
        node.state = 1; // ALIVE
        storage.node_table().put("node_1", &node).await;

        // Populate actors.
        let mut actor = ActorTableData::default();
        actor.actor_id = b"actor_1".to_vec();
        actor.state = ActorState::Alive as i32;
        storage.actor_table().put("actor_1", &actor).await;

        // Populate actor task specs.
        let mut spec = TaskSpec::default();
        spec.task_id = b"task_1".to_vec();
        spec.name = "actor_task".to_string();
        storage.actor_task_spec_table().put("actor_1", &spec).await;

        // Populate placement groups.
        let mut pg = PlacementGroupTableData::default();
        pg.placement_group_id = b"pg_1".to_vec();
        pg.name = "test_pg".to_string();
        storage.placement_group_table().put("pg_1", &pg).await;

        // Load and verify.
        let init_data = GcsInitData::load(&storage).await;

        assert_eq!(init_data.jobs.len(), 2);
        assert_eq!(init_data.jobs["job_1"].job_id, b"job_1");
        assert_eq!(init_data.jobs["job_2"].is_dead, true);

        assert_eq!(init_data.nodes.len(), 1);
        assert_eq!(init_data.nodes["node_1"].node_id, b"node_1");

        assert_eq!(init_data.actors.len(), 1);
        assert_eq!(init_data.actors["actor_1"].actor_id, b"actor_1");

        assert_eq!(init_data.actor_task_specs.len(), 1);
        assert_eq!(init_data.actor_task_specs["actor_1"].name, "actor_task");

        assert_eq!(init_data.placement_groups.len(), 1);
        assert_eq!(init_data.placement_groups["pg_1"].name, "test_pg");
    }

    use gcs_proto::ray::rpc::actor_table_data::ActorState;
}
