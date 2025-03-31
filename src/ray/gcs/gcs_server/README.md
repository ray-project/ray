# Ray GCS Server

The Ray GCS (Global Control Store) Server is a critical component of the Ray distributed computing framework that provides centralized coordination and management services for Ray clusters. This implementation supports multiple head nodes, enabling horizontal scaling and improved fault tolerance.

## Components

### 1. Service Discovery
- Handles registration and discovery of services in the Ray cluster
- Maintains service metadata and health status
- Supports service type-based lookup
- Enables dynamic service registration and deregistration

### 2. Load Balancer
- Monitors node load metrics (CPU, memory, network)
- Implements load balancing algorithms
- Handles task migration between nodes
- Provides load statistics and history

### 3. State Synchronization
- Manages distributed state across head nodes
- Handles state changes and updates
- Provides state snapshots and change history
- Ensures consistency across the cluster

### 4. Configuration Management
- Manages cluster configuration
- Validates configuration changes
- Supports configuration versioning
- Provides configuration history

### 5. Monitoring and Metrics
- Collects and stores metrics
- Implements alert rules
- Provides metric history and analysis
- Supports custom metrics and alerts

## Architecture

The GCS Server is designed with a modular architecture:

```
+------------------+
|    GCS Server    |
+------------------+
|  gRPC Services   |
+------------------+
|      Core        |
+------------------+
| Service Discovery|
| Load Balancer    |
| State Sync       |
| Config Manager   |
| Metrics Manager  |
+------------------+
```

## Usage

### Starting the Server

```cpp
// Create GCS server
auto gcs_server = std::make_unique<ray::gcs::GcsServer>(address, port);

// Start GCS server
auto status = gcs_server->Start();
if (!status.ok()) {
    // Handle error
}
```

### Client Usage

```cpp
// Create client
auto client = std::make_unique<ray::rpc::HeadNodeClient>(address, port);

// Service Discovery
auto status = client->RegisterService("service_id", "service_type", address, port, {});

// Load Balancing
auto stats = client->GetLoadStats("node_id");

// State Sync
auto state = client->GetState("key");

// Config Management
auto config = client->GetConfig("key");

// Metrics
auto metric = client->GetMetric("key");
```

## Testing

### Unit Tests
```bash
bazel test //src/ray/gcs/gcs_server/test:gcs_server_test
```

### Integration Tests
```bash
bazel test //src/ray/gcs/gcs_server/test:gcs_server_integration_test
```

## Configuration

The GCS Server can be configured through various parameters:

- `address`: Server address (default: "127.0.0.1")
- `port`: Server port (default: 50051)
- `metrics_export_port`: Metrics export port (default: 50052)
- `log_level`: Logging level (default: INFO)

## Monitoring

The GCS Server exposes metrics through its metrics manager:

- CPU usage
- Memory usage
- Network usage
- Request latencies
- Error rates
- Queue lengths

## Fault Tolerance

The multi-head node support provides:

1. High Availability
   - No single point of failure
   - Automatic failover
   - Leader election

2. Scalability
   - Horizontal scaling
   - Load distribution
   - Resource optimization

3. Consistency
   - State replication
   - Configuration synchronization
   - Metrics aggregation

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests
5. Submit a pull request

## License

Apache License 2.0 