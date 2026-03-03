(kuberay-auth)=

# Configure Ray clusters to use token authentication

This guide demonstrates how to enable Ray token authentication with KubeRay.

## Prerequisites

* A Kubernetes cluster. This guide uses GKE, but the concepts apply to other Kubernetes distributions.
* `kubectl` installed and configured to interact with your cluster.
* `gcloud` CLI installed and configured, if using GKE.
* [Helm](https://helm.sh/) installed.
* Ray 2.52.0 or newer.

## Create or use an existing GKE Cluster

If you don't have a Kubernetes cluster, create one using the following command, or adapt it for your cloud provider:

```bash
gcloud container clusters create kuberay-cluster \
    --num-nodes=2 --zone=us-west1-b --machine-type e2-standard-4
```

## Install the KubeRay Operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

## Deploy a Ray cluster with token authentication

If you are using KubeRay v1.5.1 or newer, you can use the `authOptions` API in RayCluster to enable token authentication:
```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/refs/heads/master/ray-operator/config/samples/ray-cluster.auth.yaml
```

When enabled, the KubeRay operator will:
* Create a Kubernetes Secret containing a randomly generated token.
* Automatically set the `RAY_AUTH_TOKEN` and `RAY_AUTH_MODE` environment variables on all Ray containers.

If you are using a KubeRay version older than v1.5.1, you can enable token authentication by creating a Kubernetes Secret containing
your token and configuring the `RAY_AUTH_MODE` and `RAY_AUTH_TOKEN` environment variables.

```bash
kubectl create secret generic ray-cluster-with-auth --from-literal=auth_token=$(openssl rand -base64 32)
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/refs/heads/master/ray-operator/config/samples/ray-cluster.auth-manual.yaml
```

## Verify initial unauthenticated access

Attempt to submit a Ray job to the cluster to verify that authentication is required. You should receive a `401 Unauthorized` error:

```bash
kubectl port-forward svc/ray-cluster-with-auth-head-svc 8265:8265 &
ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

You should see an error similar to this:

```bash
RuntimeError: Authentication required: Unauthorized: Missing authentication token

The Ray cluster requires authentication, but no token was provided.

Please provide an authentication token using one of these methods:
  1. Set the `RAY_AUTH_TOKEN` environment variable.
  2. Set the `RAY_AUTH_TOKEN_PATH` environment variable (pointing to a file containing the token).
  3. Create a token file at the default location: `~/.ray/auth_token`.
```

This error confirms that the Ray cluster requires authentication.

## Accessing your Ray cluster with the Ray CLI

To access your Ray cluster using the Ray CLI, you need to configure the following environment variables:
* `RAY_AUTH_MODE`: this configures the Ray CLI to set the necessary authorization headers for token authentication
* `RAY_AUTH_TOKEN`: this contains the token that will be used for authentication. 
* `RAY_AUTH_TOKEN_PATH`: if `RAY_AUTH_TOKEN` is not set, the Ray CLI will instead read the token from this path (defaults to `~/.ray/auth_token`).

Submit a job with an authenticated Ray CLI:

```bash
export RAY_AUTH_MODE=token
export RAY_AUTH_TOKEN=$(kubectl get secrets ray-cluster-with-auth --template={{.data.auth_token}} | base64 -d)
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

## Viewing the Ray dashboard (optional)
To view the Ray dashboard from your browser, first port forward to from your local machine to the cluster:

```bash
kubectl port-forward svc/ray-cluster-with-auth-head-svc 8265:8265 &
```

Then open `localhost:8265` in your browser. You will be prompted to provide the auth token for the cluster, which can be retrieved with:

```bash
kubectl get secrets ray-cluster-with-auth --template={{.data.auth_token}} | base64 -d
```
