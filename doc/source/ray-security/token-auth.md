(token-auth)=

# Ray token authentication

Enable token authentication in Ray to secure cluster access and prevent unauthorized use. This guide explains how authentication works and how to set it up for different deployment scenarios.

:::{note}
Token authentication is available in Ray 2.52.0 or later.
:::

## How Ray token authentication works

To enable token authentication, set the environment variable `RAY_AUTH_MODE=token` before starting your Ray cluster. When you start a Ray cluster with authentication enabled, all external Ray APIs and internal communications are authenticated using the token as a shared secret.

The process for generating and configuring authentication tokens differs depending on how you launch your Ray cluster. When you start a local instance of Ray using `ray.init()` with token authentication enabled, Ray automatically generates and uses a token.

Other cluster launching methods require that you generate a token before starting the cluster. You can `ray get-auth-token [--generate]` to retrieve your existing token or generate a new one.

:::{note}
Authentication is disabled by default in Ray 2.52.0. Ray plans to enable token authentication by default in a future release. We recommend enabling token authentication to protect your cluster from unauthorized access.
:::

### What token does Ray use?

You can configure authentication tokens using environment variables or the default path. We recommend using the default path when possible to reduce the chances of committing the token to version control.

Ray checks for tokens in the following order, highest priority first:

1. `RAY_AUTH_TOKEN` environment variable.
2. `RAY_AUTH_TOKEN_PATH` environment variable, which provides a path to a token file.
3. The default location, `~/.ray/auth_token`.

When managing multiple tokens, we recommend storing them in local files and using the `RAY_AUTH_TOKEN_PATH` environment variable rather than setting the `RAY_AUTH_TOKEN` value directly to avoid exposing the token to other code that reads environment variables.

## Security considerations

Ray transmits the authentication token as an HTTP header, which is transmitted in plaintext when using insecure `http` connections. We recommend enabling some form of encryption whenever exposing a Ray cluster over the network. Consider the following:

- **Local development**: Traffic doesn't leave your machine, so no additional security is needed.
- **SSH tunneling**: Use SSH tunneling/port forwarding via the `ray dashboard` command or `kubectl port-forward`.
- **TLS termination**: Deploy a TLS proxy in front of your Ray cluster.
- **VPN/Overlay networks**: Use network-level encryption for all traffic into and within the cluster.

:::{warning}
Don't expose Ray clusters directly to the internet without encryption. Tokens alone don't protect against network eavesdropping.
:::

Tokens have the following properties:

- Ray stores tokens by default in plaintext at `~/.ray/auth_token`.
- Use file permissions to keep token files secure, especially in shared environments.
- Don't commit tokens to version control.
- Tokens don't expire. Your local token remains valid unless you delete and regenerate the token. Ray clusters use the same token for the lifetime of the cluster.

## Configure token authentication for local development

To enable authentication on your local machine for development, set the `RAY_AUTH_MODE=token` environment variable in your shell or IDE. You can persist this configuration in your `.bashrc` file or similar.

### Local development with ray.init()

When you run a script that starts a local Ray instance with `ray.init()` after setting `RAY_AUTH_MODE=token` as an environment variable, Ray handles authentication automatically:

- If a token doesn't already exist at `~/.ray/auth_token`, Ray generates a token and saves it to the file. A log message displays to confirm token creation.
- If a token already exists at `~/.ray/auth_token`, Ray reuses the existing token automatically.

The following example shows what happens on the first run:

```bash
$ export RAY_AUTH_MODE=token
$ python -c "import ray;ray.init()"
```

On the first run, this command (or any other script that initializes Ray) logs a line similar to the following:

```bash
Generated new authentication token and saved to /Users/<username>/.ray/auth_token
```

### Local development with ray start

When you use `ray start --head` to start a local cluster after setting `RAY_AUTH_MODE=token` as an environment variable, you need to generate a token first:

- If no token exists, `ray start` shows an error message with instructions.
- Run `ray get-auth-token --generate` to generate a new token at the path `~/.ray/auth_token`.
- Once generated, Ray uses the token every time you run `ray start`.

The following example demonstrates this flow:

```bash
# Set the environment variable.
$ export RAY_AUTH_MODE=token

# First attempt - an error is raised if no token exists.
$ ray start --head
...
ray.exceptions.AuthenticationError: Token authentication is enabled but no authentication token was found. Ensure that the token for the cluster is available in a local file (e.g., ~/.ray/auth_token or via RAY_AUTH_TOKEN_PATH) or as the `RAY_AUTH_TOKEN` environment variable. To generate a token for local development, use `ray get-auth-token --generate` For remote clusters, ensure that the token is propagated to all nodes of the cluster when token authentication is enabled. For more information, see: https://docs.ray.io/en/latest/ray-security/token-auth.html

# Generate a token.
$ ray get-auth-token --generate
<token is output and written to ~/.ray/auth_token>

# Start local cluster again - works now.
$ ray start --head
...
Ray runtime started.
...
```

## Configure token authentication for remote clusters

When working with remote clusters you must ensure that all nodes in the remote cluster have token authentication enabled and access to the same token. Any clients that interact with the remote cluster, including your local machine, must also have the token configured. The following sections provide an overview of configuring this using the Ray cluster launcher and self-managed clusters.

For instructions on configuring token authentication with KubeRay, see {ref}`Token authentication with KubeRay <kuberay-auth>`.

:::{note}
If you're using a hosted version of Ray, contact your customer support for authentication questions.

Anyscale manages authentication automatically for users.
:::

### Ray clusters on remote virtual machines

This section provides instructions for using `ray up` to launch a remote cluster on virtual machines with token authentication enabled.

#### Step 1: Generate a token

You must generate a token on your local machine. Run the following command to generate a token:

```bash
ray get-auth-token --generate
```

This command generates a token on your local machine at the path `~/.ray/auth_token` and outputs it to the terminal.

#### Step 2: Specify token authentication values in your cluster configuration YAML

To enable and configure token authentication, you add the following setting to your cluster configuration YAML:

- Use `file_mounts` to mount your locally generated token file to all nodes in the cluster.
- Use `initialization_commands` to set the environment variable `RAY_AUTH_MODE=token` for all virtual machines in the cluster.

The following is an example cluster configuration YAML that includes the `file_mounts` and `initialization_commands` settings required to enable token authentication and use the token generated on your local machine at the path `~/.ray/auth_token`:

```yaml
cluster_name: my-cluster-name
provider:
    type: aws
    region: us-west-2
max_workers: 2
available_node_types:
    ray.head.default:
        resources: {}
        node_config:
            InstanceType: m5.large
    ray.worker.default:
        min_workers: 2
        max_workers: 2
        resources: {}
        node_config:
            InstanceType: m5.large

# Mount a locally generated token file to all nodes in the Ray cluster.
file_mounts: {
    "/home/ubuntu/.ray/auth_token": "~/.ray/auth_token",
}

# Set the RAY_AUTH_MODE environment variable for all shell sessions on the cluster.
initialization_commands:
  - echo "export RAY_AUTH_MODE=token" >> ~/.bashrc
```

#### Step 3: Launch the Ray cluster

Run the following command to launch a Ray cluster using your cluster configuration YAML:

```bash
ray up cluster.yaml
```

#### Step 4: Configure the Ray dashboard and port forwarding

Connecting to the Ray dashboard configures secure SSH port forwarding between your local machine and the Ray cluster. Complete this step even if you don't plan to use the dashboard for monitoring.

Run the following command to set up port forwarding for the Ray dashboard port (`8265` by default):

```bash
ray dashboard cluster.yaml
```

Upon opening the dashboard, a prompt displays requesting your authentication token. To display the token in plaintext, you can run the following on your local machine:

```bash
export RAY_AUTH_MODE=token
ray get-auth-token
```

Paste the token in the prompt and click **Submit**. The token gets stored as a cookie for a maximum of 30 days. When you open the dashboard for a cluster that uses a different token, a prompt appears to enter the token for that cluster.

#### Step 5: Submit a Ray job

You can submit a Ray job with token authentication using secure SSH port forwarding:

```bash
export RAY_AUTH_MODE=token
ray job submit --working-dir . -- python script.py
```

### Self-managed clusters

If you have a custom deployment where you run `ray start` on multiple nodes, you can use token authentication by generating a token and distributing it to all nodes in the cluster, as shown in the following steps.

#### Step 1: Generate a token

Generate a token on a single machine using the following command:

```bash
ray get-auth-token --generate
```

:::{note}
Any machine that needs to interact with the cluster must have the token used to configure authentication.
:::

#### Step 2: Copy the token to all nodes

Copy the same token to each node in your Ray cluster. For example, use `scp` to copy the token:

```bash
scp ~/.ray/auth_token user@node1:~/.ray/auth_token;
scp ~/.ray/auth_token user@node2:~/.ray/auth_token;
```

#### Step 3: Start Ray with token authentication

You must set the environment variable `RAY_AUTH_MODE=token` on each node before running `ray start`, as in the following example:

```bash
ssh user@node1 "RAY_AUTH_MODE=token ray start --head";
ssh user@node2 "RAY_AUTH_MODE=token ray start --address=node1:6379";
```

## Troubleshooting token authentication issues

You might encounter the following problems with token authentication.

### Token authentication isn't enabled

Make sure you've set the `RAY_AUTH_MODE=token` environment variable in the environment where you're launching Ray *and* in any shell where you are using a client to connect to Ray.

### Authentication token not found

If running locally, run `ray get-auth-token --generate` to create a token on your local machine.
If running a remote cluster, make sure you've followed instructions to copy your token into the cluster.

### Invalid authentication token

Any client that tries to interact with a Ray cluster must have the same token as the Ray cluster.

If the token on your local machine doesn't match the token in a Ray cluster, you can use the `RAY_AUTH_TOKEN_PATH` or `RAY_AUTH_TOKEN` environment variable to configure a token for interacting with that cluster. You must work with the creator of the cluster to get the token.

:::{note}
It's possible to stop and then restart a cluster using a different token. All clients connecting to the cluster must have the updated token to connect successfully.
:::

## Next steps

- See {ref}`overall security guidelines <security>`.
- Read about {ref}`KubeRay authentication <kuberay-auth>` for Kubernetes-specific configuration.
