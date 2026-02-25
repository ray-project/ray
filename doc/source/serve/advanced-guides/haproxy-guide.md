(serve-haproxy-guide)=
# HAProxy Load Balancing

By default, Ray Serve uses a Python-based HTTP proxy to route requests to replicas. You can optionally replace this with [HAProxy](https://www.haproxy.org/), a high-performance C-based load balancer, for improved throughput and lower latency at high request rates.

When HAProxy mode is enabled:
- An `HAProxyManager` actor runs on the head node and translates Serve's routing table into HAProxy configuration reloads.
- Each replica opens a direct ingress port, and HAProxy routes traffic directly to replicas — bypassing the Python proxy entirely.
- Live traffic flows through the HAProxy subprocess, not through any Python actor.

## Prerequisites

HAProxy must be installed and available on `$PATH` as `haproxy` on every node that runs a Serve proxy. The official Ray Docker images (2.44+) include HAProxy pre-built.

### Using a Ray Docker image

The `rayproject/ray` Docker images already include HAProxy and its runtime dependencies. No additional installation is needed.

### Installing HAProxy manually

If you are building a custom image or running on bare metal, install HAProxy 2.8+ from source with the required build flags:

```dockerfile
# --- Build stage ---
FROM ubuntu:22.04 AS haproxy-builder

RUN apt-get update -y && apt-get install -y --no-install-recommends \
    build-essential ca-certificates curl libc6-dev \
    liblua5.3-dev libpcre3-dev libssl-dev zlib1g-dev

RUN HAPROXY_VERSION="2.8.12" && \
    BUILD_DIR=$(mktemp -d) && \
    curl -sSfL -o "${BUILD_DIR}/haproxy.tar.gz" \
      "https://www.haproxy.org/download/2.8/src/haproxy-${HAPROXY_VERSION}.tar.gz" && \
    tar -xzf "${BUILD_DIR}/haproxy.tar.gz" -C "${BUILD_DIR}" --strip-components=1 && \
    make -C "${BUILD_DIR}" TARGET=linux-glibc \
      USE_OPENSSL=1 USE_ZLIB=1 USE_PCRE=1 USE_LUA=1 USE_PROMEX=1 -j$(nproc) && \
    make -C "${BUILD_DIR}" install SBINDIR=/usr/local/bin && \
    rm -rf "${BUILD_DIR}"

# --- Runtime stage ---
FROM ubuntu:22.04

# Copy HAProxy binary from build stage
COPY --from=haproxy-builder /usr/local/bin/haproxy /usr/local/bin/haproxy

# Install runtime dependencies
RUN apt-get update -y && apt-get install -y --no-install-recommends \
    socat liblua5.3-0 && \
    mkdir -p /etc/haproxy /run/haproxy /var/log/haproxy && \
    rm -rf /var/lib/apt/lists/*
```

The key build flags are:
- `USE_PROMEX=1` — enables the built-in Prometheus exporter for metrics.
- `USE_LUA=1` — enables Lua scripting (used for health check endpoints).
- `USE_OPENSSL=1` — enables TLS support.

Runtime dependencies:
- `socat` — used for sending commands to the HAProxy admin socket.
- `liblua5.3-0` — Lua runtime library.

## Enabling HAProxy

Set the `RAY_SERVE_ENABLE_HA_PROXY` environment variable to `1` on all nodes **before** starting Ray:

```bash
export RAY_SERVE_ENABLE_HA_PROXY=1
```

This single variable is all that is required. It implicitly enables direct ingress (replicas listen on individual ports that HAProxy routes to).

### KubeRay cluster example

In a KubeRay `RayService` or `RayCluster` spec, set the environment variable on both the head and worker pods:

```yaml
# In the Ray container spec for head and worker groups:
env:
  - name: RAY_SERVE_ENABLE_HA_PROXY
    value: "1"
```

### VM cluster example

When using `ray start`, export the variable before starting the cluster:

```bash
# On every node (head and workers)
export RAY_SERVE_ENABLE_HA_PROXY=1
ray start --head  # or ray start --address=<head-ip>:6379 on workers
```

## Configuration

All HAProxy settings are configured through environment variables. The defaults work well for most deployments; only override them if you have specific requirements.

### Core settings

| Environment variable | Default | Description |
|---|---|---|
| `RAY_SERVE_ENABLE_HA_PROXY` | `0` | Set to `1` to enable HAProxy mode. |
| `RAY_SERVE_HAPROXY_MAXCONN` | `20000` | Maximum concurrent connections. |
| `RAY_SERVE_HAPROXY_NBTHREAD` | `4` | Number of HAProxy threads. |

### Timeout settings

| Environment variable | Default | Description |
|---|---|---|
| `RAY_SERVE_HAPROXY_TIMEOUT_CLIENT_S` | `3600` | Client inactivity timeout in seconds. |
| `RAY_SERVE_HAPROXY_TIMEOUT_SERVER_S` | None (no timeout) | Backend server inactivity timeout in seconds. |
| `RAY_SERVE_HAPROXY_TIMEOUT_CONNECT_S` | None (no timeout) | TCP connection establishment timeout in seconds. |
| `RAY_SERVE_HAPROXY_HARD_STOP_AFTER_S` | `120` | Maximum time in seconds for a graceful HAProxy shutdown. |

### Health check settings

| Environment variable | Default | Description |
|---|---|---|
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_FALL` | `2` | Consecutive failures before marking a backend as DOWN. |
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_RISE` | `2` | Consecutive successes before marking a backend as UP. |
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_INTER` | `5s` | Interval between health checks. |
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_FASTINTER` | `250ms` | Interval during transition states (UP→DOWN or DOWN→UP). |
| `RAY_SERVE_HAPROXY_HEALTH_CHECK_DOWNINTER` | `250ms` | Interval when a backend is DOWN. |

### File path settings

| Environment variable | Default | Description |
|---|---|---|
| `RAY_SERVE_HAPROXY_CONFIG_FILE_LOC` | `/tmp/haproxy-serve/haproxy.cfg` | Path to the generated HAProxy config file. |
| `RAY_SERVE_HAPROXY_SOCKET_PATH` | `/tmp/haproxy-serve/admin.sock` | Path to the HAProxy admin socket. |
| `RAY_SERVE_HAPROXY_SERVER_STATE_BASE` | `/tmp/haproxy-serve` | Base directory for server state files. |
| `RAY_SERVE_HAPROXY_SERVER_STATE_FILE` | `/tmp/haproxy-serve/server-state` | Path to the server state persistence file. |

### Metrics and logging

| Environment variable | Default | Description |
|---|---|---|
| `RAY_SERVE_HAPROXY_METRICS_PORT` | `9101` | Port for the built-in Prometheus metrics exporter. |
| `RAY_SERVE_HAPROXY_SYSLOG_PORT` | `514` | Syslog port for HAProxy logging. |

### Direct ingress port ranges

When HAProxy is enabled, each replica listens on its own port. These variables control the port ranges:

| Environment variable | Default | Description |
|---|---|---|
| `RAY_SERVE_DIRECT_INGRESS_MIN_HTTP_PORT` | `30000` | Minimum HTTP port for replica direct ingress. |
| `RAY_SERVE_DIRECT_INGRESS_MAX_HTTP_PORT` | `31000` | Maximum HTTP port for replica direct ingress. |
| `RAY_SERVE_DIRECT_INGRESS_MIN_GRPC_PORT` | `40000` | Minimum gRPC port for replica direct ingress. |
| `RAY_SERVE_DIRECT_INGRESS_MAX_GRPC_PORT` | `41000` | Maximum gRPC port for replica direct ingress. |

:::{note}
Ensure that the direct ingress port ranges (30000-31000 for HTTP, 40000-41000 for gRPC by default) are open in your firewall and security group rules across all nodes.
:::

## Monitoring

HAProxy exposes Prometheus metrics on the port configured by `RAY_SERVE_HAPROXY_METRICS_PORT` (default: 9101). You can scrape these metrics by pointing your Prometheus instance at `http://<head-node-ip>:9101/metrics`.

You can also inspect HAProxy's state through its admin socket using `socat`:

```bash
# Show backend server status
echo "show stat" | socat stdio /tmp/haproxy-serve/admin.sock

# Show HAProxy general info
echo "show info" | socat stdio /tmp/haproxy-serve/admin.sock
```
