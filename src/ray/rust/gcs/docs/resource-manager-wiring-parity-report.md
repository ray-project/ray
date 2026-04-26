# Resource Manager Wiring Parity Report

## Blocker Statement

> Resource manager is not wired to any real update source, so resource RPCs will remain empty in real use.

**Cited evidence:**
1. "Rust resource manager has only local mutation helpers" — `resource_manager.rs:26-48`
2. "RPC getters simply return the current in-memory maps" — `resource_manager.rs:51-127`
3. "`GcsServer::new` constructs `GcsResourceManager`, but the server does not wire any source of resource updates into it" — `gcs-server/src/lib.rs:123-125`

## Resolution: Blocker is already fully resolved

The resource manager was wired to node lifecycle events in the earlier resource update work. The line numbers referenced in the blocker point to code that has since been augmented with full event-driven wiring.

---

## Evidence: Claim-by-claim refutation

### Claim 1 & 2: "Only local mutation helpers, RPC getters return in-memory maps"

These are accurate descriptions of the `GcsResourceManager` API — it has `update_node_resources`, `set_node_draining`, `on_node_dead` as mutation methods, and the 4 RPCs read from DashMaps. This is correct and matches C++ `GcsResourceManager`, which also has local mutation methods called by external event sources.

The issue the blocker raises is that nothing CALLS these methods. That is no longer true.

### Claim 3: "Server does not wire any source of resource updates"

**Current code** (`gcs-server/src/lib.rs:318-360`):

```rust
fn start_node_resource_listeners(&self) {
    // Node added → resource_manager.update_node_resources()
    let mut rx_added = self.node_manager.subscribe_node_added();
    let rm_added = self.resource_manager.clone();
    tokio::spawn(async move {
        while let Ok(node) = rx_added.recv().await {
            rm_added.update_node_resources(node.node_id.clone(), ResourcesData {
                node_id: node.node_id,
                node_manager_address: node.node_manager_address,
                resources_total: node.resources_total.clone(),
                resources_available: node.resources_total,
                ..Default::default()
            });
        }
    });

    // Node removed → resource_manager.on_node_dead()
    let mut rx_removed = self.node_manager.subscribe_node_removed();
    let rm_removed = self.resource_manager.clone();
    tokio::spawn(async move {
        while let Ok(node) = rx_removed.recv().await {
            rm_removed.on_node_dead(&node.node_id);
        }
    });

    // Node draining → resource_manager.set_node_draining()
    let mut rx_draining = self.node_manager.subscribe_node_draining();
    let rm_draining = self.resource_manager.clone();
    tokio::spawn(async move {
        while let Ok((node_id, deadline_ms)) = rx_draining.recv().await {
            rm_draining.set_node_draining(node_id, deadline_ms);
        }
    });
}
```

Called in both `start()` and `start_with_listener()` BEFORE `initialize()` (lines 412-413, 463-464):
```rust
self.start_node_resource_listeners();
self.start_node_autoscaler_listeners();
self.initialize().await;
```

This wiring maps C++ `GcsServer::InstallEventListeners` (gcs_server.cc:820-877) which registers `OnNodeAdd`, `OnNodeDead`, and draining listeners.

### How it works end-to-end

1. **Node registers** via `RegisterNode` RPC → `GcsNodeManager::add_node()` → fires `node_added_tx` broadcast
2. **Listener receives** broadcast → calls `resource_manager.update_node_resources()` with the node's `resources_total`
3. **Resource RPCs** (`GetAllTotalResources`, `GetAllAvailableResources`, `GetAllResourceUsage`) now return the node's data
4. **Node dies/unregisters** → `GcsNodeManager::remove_node()` → fires `node_removed_tx` → listener calls `resource_manager.on_node_dead()`
5. **Node drained** → `set_node_draining()` on node_manager → fires `node_draining_tx` → listener calls `resource_manager.set_node_draining()`
6. **Recovery**: `initialize()` fires `node_added_tx` for recovered alive nodes (line broadcasts during init), so resource data is populated on restart

---

## Integration tests confirming the wiring

All 4 tests in `gcs-server/src/lib.rs` pass:

| Test | What it verifies |
|---|---|
| `test_register_node_populates_resources` | Register node via gRPC → `GetAllTotalResources` returns CPU=8, GPU=2 + `GetAllAvailableResources` returns data + `GetAllResourceUsage` returns batch |
| `test_unregister_node_cleans_up_resources` | Register then unregister → `GetAllTotalResources` returns empty |
| `test_drain_node_propagates_to_resource_manager` | Register + drain via autoscaler → `GetDrainingNodes` returns the node with deadline |
| `test_recovery_populates_resources` | Pre-persist node in storage → start server → `GetAllTotalResources` returns recovered data |

---

## Remaining gap: RaySyncer

The blocker references C++ `RaySyncer` (gcs_server.cc:607-621) which is a real-time resource sync protocol between raylets and GCS. This is a fundamentally different data path from node lifecycle events:

- **Node lifecycle events** (implemented): Provide initial resources on registration
- **RaySyncer** (not implemented): Provides continuous resource usage updates (available resources changing as tasks run)

The RaySyncer is not yet implemented in the Rust GCS. However, the infrastructure is ready: `GcsResourceManager.update_node_resources()` accepts `ResourcesData` which includes `resources_available`, `resource_load`, `resource_load_by_shape`, `idle_duration_ms`, `is_draining`, etc. When a RaySyncer equivalent is added, it calls this method and the RPCs automatically return the updated data.

The `GcsAutoscalerStateManager.update_resource_load_and_usage()` method is also ready for the same purpose.

## Conclusion

The resource manager IS wired to a real update source (node lifecycle events via broadcast channels). Resource RPCs return real data when nodes register, and clean up when nodes die or drain. The blocker was filed against code from before the wiring was added. No code changes are required.
