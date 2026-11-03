---
myst:
  html_meta:
    description: "Configure Ray token authentication, on by default for every cluster from Ray 2.61: create and distribute the token, opt out with RAY_AUTH_MODE=disabled, and scrape metrics."
---

(token-auth)=

# Ray token authentication

Ray token authentication secures cluster access and prevents unauthorized use. This guide explains how authentication works and how to set it up for different deployment scenarios.

:::{note}
Token authentication is available in Ray 2.52.0 or later. Ray 2.59 turns it on by default for local clusters, and Ray 2.61 turns it on by default for every cluster, including remote and multi-node clusters.
:::

## How Ray token authentication works

Starting in Ray 2.61, token authentication is on for every Ray cluster unless you set `RAY_AUTH_MODE=disabled`. Ray authenticates all external APIs and internal communications with the token as a shared secret. Every node in a cluster and every client that connects to it needs the same token.

Only the following create a token when none exists:

- `ray.init()` without an `address`, when it starts a new local cluster.
- `ray get-auth-token --generate`.
- `ray up`, which ships the token to the head node. The head node ships it to every worker node it launches.

`ray start --head` never creates a token. Create one first with `ray get-auth-token --generate`, then copy it to every node and client of the cluster. Worker nodes, drivers that connect to an existing cluster, and other clients never create a token either, because a token made there can't match the cluster's.

To run a cluster without authentication, set `RAY_AUTH_MODE=disabled` on every node and every client, as described in [Opt out of token authentication](#opt-out-of-token-authentication).

(what-token-does-ray-use)=

### What token does Ray use?

You can configure authentication tokens using environment variables or the default path. We recommend using the default path when possible to reduce the chances of committing the token to version control.

Ray checks for tokens in the following order, highest priority first:

1. `RAY_AUTH_TOKEN` environment variable.
2. `RAY_AUTH_TOKEN_PATH` environment variable, which provides a path to a token file.
3. The default location, `~/.ray/auth_token`.

When managing multiple tokens, we recommend storing them in local files and using the `RAY_AUTH_TOKEN_PATH` environment variable rather than setting the `RAY_AUTH_TOKEN` value directly to avoid exposing the token to other code that reads environment variables.

## Security considerations

Ray transmits the authentication token as an HTTP header, which is transmitted in plaintext when using insecure `http` connections. We recommend enabling some form of encryption whenever exposing a Ray cluster over the network. Consider the following:

- **Local development**: Traffic doesn't leave your machine, so it needs no encryption. Ray assumes the machine is single-tenant or that you trust every user on it.
- **SSH tunneling**: Use SSH tunneling/port forwarding via the `ray dashboard` command or `kubectl port-forward`.
- **TLS termination**: Deploy a TLS proxy in front of your Ray cluster.
- **VPN/Overlay networks**: Use network-level encryption for all traffic into and within the cluster.

:::{warning}
Don't expose Ray clusters directly to the internet without encryption. Tokens alone don't protect against network eavesdropping.
:::

Tokens have the following properties:

- Ray stores tokens by default in plaintext at `~/.ray/auth_token`.
- Make token files readable only by you, especially on a shared host. For the default location, run `chmod 700 ~/.ray` and `chmod 600 ~/.ray/auth_token`.
- Don't commit tokens to version control.
- Tokens don't expire. Your local token remains valid unless you delete and regenerate the token. Ray clusters use the same token for the lifetime of the cluster.

## Configure token authentication for local development

Local development needs no authentication settings. Everything that runs as the same user on one machine reads the token from `~/.ray/auth_token`.

### Local development with ray.init()

When you run a script that starts a new local Ray cluster with `ray.init()`, meaning `ray.init()` without an `address`, Ray handles the token automatically:

- If no token exists at `~/.ray/auth_token`, Ray generates one, saves it there, and logs a message confirming the new token.
- If a token already exists at `~/.ray/auth_token`, Ray reuses it.

For example:

```bash
python -c "import ray;ray.init()"
```

On the first run, this command and any other script that starts Ray log a line like:

```text
Generated new authentication token and saved to ~/.ray/auth_token
```

Connecting to an existing cluster with `ray.init(address=...)`, or with a `ray.init()` that finds a running local cluster, doesn't generate a token. As with any client, you must have the cluster's token configured, as described in {ref}`What token does Ray use? <what-token-does-ray-use>`.

### Local development with ray start

Before you start a head node with `ray start --head`, create a token once. After that, every `ray start` and `ray.init()` that runs as the same user on the machine reads it:

```bash
ray get-auth-token --generate
ray start --head
ray start --address=127.0.0.1:6379
python -c "import ray; ray.init()"
```

If no token exists, `ray start --head` fails before it starts anything:

```text
Error: Token authentication is enabled but no authentication token was found. Run `ray get-auth-token --generate` to create one, then put that token on every node of the cluster before starting it.
```

## Configure token authentication for remote clusters

When working with remote clusters, all nodes in the cluster need the same token. Any clients that interact with the remote cluster, including your local machine, must also have the token configured. The following sections cover the Ray cluster launcher, self-managed clusters, and clients on other machines.

For instructions on configuring token authentication with KubeRay, see {ref}`Token authentication with KubeRay <kuberay-auth>`.

:::{note}
If you're using a hosted version of Ray, contact your customer support for authentication questions.

Anyscale manages authentication automatically for users.
:::

### Ray clusters on remote virtual machines

`ray up` sets up token authentication with no extra steps. It uses this machine's token, found in the order described in {ref}`What token does Ray use? <what-token-does-ray-use>`, and creates `~/.ray/auth_token` if no token exists. It ships the token to the head node as a file, and the head node ships it to every worker node it launches, including worker nodes the autoscaler adds later. If your cluster YAML already uses `file_mounts` to mount a token at `~/.ray/auth_token`, `ray up` uses that token instead.

Set authentication with the `auth_mode` key of the cluster YAML, not with `RAY_AUTH_MODE` in start commands or Docker `run_options`. For details and for upgrading existing clusters, see {ref}`Token authentication with the cluster launcher <vm-cluster-token-auth>`.

#### Step 1: Launch the Ray cluster

Run the following command to launch a Ray cluster using your cluster configuration YAML:

```bash
ray up cluster.yaml
```

#### Step 2: Configure the Ray dashboard and port forwarding

Connecting to the Ray dashboard configures secure SSH port forwarding between your local machine and the Ray cluster. Complete this step even if you don't plan to use the dashboard for monitoring.

Run the following command to set up port forwarding for the Ray dashboard port (`8265` by default):

```bash
ray dashboard cluster.yaml
```

When you open the dashboard, a prompt asks for your authentication token. To display the token in plaintext, run the following on your local machine:

```bash
ray get-auth-token
```

Paste the token in the prompt and click **Submit**. The token stays in a cookie for up to 30 days. When you open the dashboard for a cluster that uses a different token, a prompt asks for the token for that cluster.

#### Step 3: Submit a Ray job

You can submit a Ray job with token authentication using secure SSH port forwarding:

```bash
ray job submit --address http://127.0.0.1:8265 --working-dir . -- python script.py
```

### Self-managed clusters

If you have a custom deployment where you run `ray start` on multiple nodes, generate a token and distribute it to all nodes in the cluster, as shown in the following steps.

#### Step 1: Generate a token

Generate a token on the head node using the following command:

```bash
ray get-auth-token --generate
```

#### Step 2: Copy the token to all nodes

Copy the same token to each node in your Ray cluster. Don't pass the token on a command line. For example, use `scp` to copy the token file:

```bash
scp ~/.ray/auth_token user@node2:~/.ray/auth_token
scp ~/.ray/auth_token user@node3:~/.ray/auth_token
```

#### Step 3: Start Ray

Start the head node, then start each worker node:

```bash
ray start --head
ray start --address=<head-node-ip>:6379
```

A worker node without the token fails at once with an error that says to run `ray get-auth-token` on the head node and copy the token.

### Clients on other machines

Clients on the head node, or on the machine that ran `ray up`, already have the token. Any other machine that runs a client, such as `ray job submit` or `ray.init(address=...)`, needs the token file copied to it:

```bash
ray job submit --address http://<head-node-ip>:8265 -- python script.py
```

Set `RAY_AUTH_TOKEN_PATH` only when the token file lives somewhere other than `~/.ray/auth_token`.

## Opt out of token authentication

To run a cluster without authentication, set `RAY_AUTH_MODE=disabled` on every node and every client of the cluster:

```bash
RAY_AUTH_MODE=disabled ray start --head
RAY_AUTH_MODE=disabled ray start --address=<head-node-ip>:6379
RAY_AUTH_MODE=disabled ray job submit --address http://<head-node-ip>:8265 -- python script.py
```

The head node logs a warning that authentication is disabled and that anyone who can reach the cluster's ports can run code on it. For clusters that `ray up` launches, set `auth_mode: disabled` in the cluster YAML instead.

A node or client must use the same mode as the cluster. A mismatch never falls back to running without authentication:

| Cluster | Node or client | Result |
|---|---|---|
| Token authentication | Same token | Works |
| Token authentication | Different token | Rejected |
| Token authentication | No token | Fails before connecting |
| Token authentication | `RAY_AUTH_MODE=disabled` | Rejected for a missing token |
| `RAY_AUTH_MODE=disabled` | Token authentication | Rejected with the same error as a missing token |
| `RAY_AUTH_MODE=disabled` | `RAY_AUTH_MODE=disabled` | Works |

With `RAY_AUTH_MODE=disabled`, the dashboard head, the dashboard agent, and the runtime environment agent reject any request that carries an `Authorization` header of any scheme, including `Bearer` and `Basic`, or a non-empty `X-Ray-Authorization` header. A reverse proxy in front of the dashboard that adds its own `Authorization` header must strip it before forwarding the request, or the cluster must turn on token authentication.

Kubernetes token authentication, which `ENABLE_K8S_TOKEN_AUTH` turns on, requires `RAY_AUTH_MODE=token`. Ray refuses to start when you set both `RAY_AUTH_MODE=disabled` and `ENABLE_K8S_TOKEN_AUTH`.

## Scrape metrics with token authentication

With token authentication on, Ray's Prometheus metrics endpoints require the token, so every scraper needs it. For the Prometheus configuration, see {ref}`Scrape metrics with token authentication <metrics-token-auth>`.

To keep the metrics endpoints open while token authentication protects everything else, set `RAY_AUTH_SKIP_METRICS=1` on every node before `ray start`. Scrapers then need no token. The setting has no effect when authentication is disabled.

## Troubleshooting token authentication issues

You might encounter the following problems with token authentication.

### Connection fails with AuthenticationError

A node, driver, or client that the cluster rejects raises `ray.exceptions.AuthenticationError` at once, without retrying. Before Ray 2.61, `ray.init(address=...)` raised `ConnectionError` after retrying. `AuthenticationError` subclasses `ConnectionError`, so existing `except ConnectionError` handlers still catch it.

The error names its fix. Run `ray get-auth-token` on the head node and copy the token to the machine that failed. If the cluster opted out with `RAY_AUTH_MODE=disabled`, set `RAY_AUTH_MODE=disabled` on that machine too.

### Authentication token not found

For local development, run `ray get-auth-token --generate` once to create a token on your machine. For a remote cluster, copy the token from the head node, as described in [Configure token authentication for remote clusters](#configure-token-authentication-for-remote-clusters).

### Invalid authentication token

Any client that tries to interact with a Ray cluster must have the same token as the Ray cluster.

If the token on your local machine doesn't match the token in a Ray cluster, set the `RAY_AUTH_TOKEN_PATH` or `RAY_AUTH_TOKEN` environment variable to configure a token for interacting with that cluster. You must work with the creator of the cluster to get the token.

:::{note}
You can stop a cluster and restart it with a different token. All clients connecting to the cluster must have the updated token to connect successfully.
:::

## Next steps

- See {ref}`overall security guidelines <security>`.
- Read about {ref}`KubeRay authentication <kuberay-auth>` for Kubernetes-specific configuration.
