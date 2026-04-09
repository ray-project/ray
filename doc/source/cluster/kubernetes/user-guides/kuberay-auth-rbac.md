(kuberay-auth-rbac)=

# Configure Ray clusters to use Kubernetes RBAC authentication

This guide demonstrates how to enable Ray token authentication using Kubernetes RBAC.

Starting in Ray v2.55.0, you can configure Ray to delegate token authentication to Kubernetes RBAC. This allows you to use your existing Kubernetes credentials to authenticate to Ray clusters and use Kubernetes RBAC to manage access control. If your Kubernetes cluster is configured with external identity integrations, you can also use those external credentials to authenticate to Ray clusters (e.g., OIDC, IAM, etc.).

:::{warning}
It is highly recommended to run Ray in secure networks or use TLS when enabling token authentication to prevent leaking Ray tokens. Token authentication does not encrypt traffic, so tokens can be intercepted if transmitted over insecure networks.
:::

## Prerequisites

* A Kubernetes cluster. This guide uses GKE, but the concepts apply to other Kubernetes distributions.
* `kubectl` installed and configured to interact with your cluster.
* `gcloud` CLI installed and configured, if using GKE.
* [Helm](https://helm.sh/) installed.
* Ray 2.55.0 or newer.

## Create or use an existing GKE Cluster

If you don't have a Kubernetes cluster, create one using the following command, or adapt it for your cloud provider:

```bash
gcloud container clusters create kuberay-cluster \
    --num-nodes=2 --zone=us-west1-b --machine-type e2-standard-4
```

## Install the KubeRay Operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

## Deploy a Ray cluster with Kubernetes RBAC enabled

If you are using KubeRay v1.6.0 or newer, you can use the `authOptions` API in RayCluster to enable RBAC authentication:
```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/refs/heads/master/ray-operator/config/samples/ray-cluster.kubernetes.auth.yaml
```

When enabled, the KubeRay operator will:
* Automatically set the `RAY_AUTH_MODE` environment variable to `token` on all Ray containers.
* Automatically set the `RAY_ENABLE_K8S_TOKEN_AUTH` environment variable to `true` on all Ray containers.
* Automatically mount a projected service account token to the Ray containers, for intra-cluster Ray process authentication.

If you are using a KubeRay version older than v1.6.0, you can enable RBAC authentication by setting the `RAY_AUTH_MODE` and `RAY_ENABLE_K8S_TOKEN_AUTH` environment variables and manually mounting the projected service account token to the Ray containers. See the following example:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/refs/heads/master/ray-operator/config/samples/ray-cluster.kubernetes.auth-manual.yaml
```

The examples above also create the following Kubernetes RBAC objects:
* A `ray-cluster-with-k8s-auth` ServiceAccount used by all Ray containers.
* A `ray-authenticator` ClusterRole that grants the `ray-cluster-with-k8s-auth` ServiceAccount access to the Kubernetes `TokenReview` and `SubjectAccessReview` APIs, which are used to delegate authentication to Kubernetes.
* A `ray-authenticator` ClusterRoleBinding that binds the `ray-authenticator` ClusterRole to the `ray-cluster-with-k8s-auth` ServiceAccount.
* A `ray-writer` ClusterRole that grants write access to `RayCluster` resources.
* A `ray-cluster-with-k8s-auth` RoleBinding that binds the `ray-writer` ClusterRole to the `ray-cluster-with-k8s-auth` ServiceAccount.

## Verify initial unauthenticated access

Attempt to submit a Ray job to the cluster to verify that authentication is required. You should receive a `401 Unauthorized` error:

```bash
kubectl port-forward svc/ray-cluster-with-k8s-auth-head-svc 8265:8265 &
ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

You should see an error similar to this:

```bash
ray.exceptions.AuthenticationError: Authentication failed: Forbidden: Invalid authentication token

Token authentication is enabled but the authentication token is invalid or incorrect.. Ensure that the token for the cluster is available in a local file (e.g., ~/.ray/auth_token or via RAY_AUTH_TOKEN_PATH) or as the `RAY_AUTH_TOKEN` environment variable. To generate a token for local development, use `ray get-auth-token --generate` For remote clusters, ensure that the token is propagated to all nodes of the cluster when token authentication is enabled. For more information, see: https://docs.ray.io/en/latest/ray-security/token-auth.html
```

This error confirms that the Ray cluster requires authentication.

## Configure Kubernetes RBAC for Ray

To grant users access to Ray clusters, you must grant them access to the custom verb `ray:write` on the respective `RayCluster` resource. Taking the default namespace as an example, we will create a new Kubernetes ServiceAccount for the user and grant it access to the `ray-cluster-with-k8s-auth` RayCluster resource. In practice, you should create a new Role and RoleBinding for each user and grant them access to the specific `RayCluster` resources they need access to.

First, create a ServiceAccount for the user:

```bash
kubectl create serviceaccount ray-user --namespace=default
```

Then create a Role for the user:

```bash
kubectl create role ray-user --verb=ray:write --resource=rayclusters --resource-name=ray-cluster-with-k8s-auth --namespace=default
```

Then create a RoleBinding to bind the Role to the ServiceAccount:

```bash
kubectl create rolebinding ray-user --role=ray-user --serviceaccount=default:ray-user --namespace=default
```

## Accessing your Ray cluster with Ray CLI

The ServiceAccount can now be used to authenticate to the Ray cluster. To do this, you need to get the token for the ServiceAccount and set the `RAY_AUTH_TOKEN` environment variable.

```bash
export RAY_AUTH_MODE=token
export RAY_AUTH_TOKEN=$(kubectl create token ray-user --namespace=default)

kubectl port-forward svc/ray-cluster-with-k8s-auth-head-svc 8265:8265 &
ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

The job should now succeed and you should see output similar to this:

```bash
Job submission server address: http://localhost:8265

-------------------------------------------------------
Job 'raysubmit_...' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_n2fq2Ui7cbh3p2Js
  Query the status of the job:
    ray job status raysubmit_n2fq2Ui7cbh3p2Js
  Request the job to be stopped:
    ray job stop raysubmit_n2fq2Ui7cbh3p2Js

Tailing logs until the job exits (disable with --no-wait):

...
{'node:10.112.0.52': 1.0, 'memory': ..., 'node:__internal_head__': 1.0, 'object_store_memory': ..., 'CPU': 4.0, 'node:10.112.1.49': 1.0, 'node:10.112.2.36': 1.0}

------------------------------------------
Job 'raysubmit_...' succeeded
------------------------------------------
```

## Accessing your Ray cluster with external IAM (optional)

If your Kubernetes cluster is configured with external identity integrations, you can also use those external credentials to authenticate to Ray clusters (e.g., OIDC, IAM, etc.). For example, on GKE, you can use access tokens from `gcloud` to authenticate to the Ray cluster. 

First, grant your user access to the Ray cluster:

```bash
kubectl create rolebinding ray-user-external --role=ray-user --user=[EMAIL_ADDRESS] --namespace=default
```

Then, you can use the access token from `gcloud` to authenticate to the Ray cluster:
```bash
export RAY_AUTH_MODE=token
export RAY_AUTH_TOKEN=$(gcloud auth print-access-token)

kubectl port-forward svc/ray-cluster-with-k8s-auth-head-svc 8265:8265 &
ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

## Revoke access to a Ray cluster

To revoke access to a Ray cluster, you can delete the RoleBinding:

```bash
kubectl delete rolebinding ray-user --namespace=default
```

Attempting to submit a Ray job to the cluster should now fail with a 401 Unauthorized error:

```bash
export RAY_AUTH_MODE=token
export RAY_AUTH_TOKEN=$(kubectl create token ray-user --namespace=default)

kubectl port-forward svc/ray-cluster-with-k8s-auth-head-svc 8265:8265 &
ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

You should see an error similar to this:

```bash
ray.exceptions.AuthenticationError: Authentication failed: Forbidden: Invalid authentication token

Token authentication is enabled but the authentication token is invalid or incorrect.. Ensure that the token for the cluster is available in a local file (e.g., ~/.ray/auth_token or via RAY_AUTH_TOKEN_PATH) or as the `RAY_AUTH_TOKEN` environment variable. To generate a token for local development, use `ray get-auth-token --generate` For remote clusters, ensure that the token is propagated to all nodes of the cluster when token authentication is enabled. For more information, see: https://docs.ray.io/en/latest/ray-security/token-auth.html
```

> Note: Deleting the RoleBinding may not immediately revoke access to the Ray cluster as Ray caches validated tokens for up to 5 minutes.
