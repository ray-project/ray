(kuberay-auth)=

# Configure Ray clusters with authentication and access control using KubeRay

This guide demonstrates how to secure Ray clusters deployed with KubeRay by enabling authentication and access control using Kubernetes Role-Based Access Control (RBAC).

> **Note:** This guide is only supported for the RayCluster custom resource.

## Prerequisites

* A Kubernetes cluster. This guide uses GKE, but the concepts apply to other Kubernetes distributions.
* `kubectl` installed and configured to interact with your cluster.
* `gcloud` CLI installed and configured, if using GKE.
* [Helm](https://helm.sh/) installed.
* Ray installed locally.

## Create or use an existing GKE Cluster

If you don't have a Kubernetes cluster, create one using the following command, or adapt it for your cloud provider:

```bash
gcloud container clusters create kuberay-cluster \
    --num-nodes=2 --zone=us-west1-b --machine-type e2-standard-4
```

## Install the KubeRay Operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

## Deploy a Ray cluster with authentication enabled

Deploy a RayCluster configured with `kube-rbac-proxy` for authentication and authorization:

```bash
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/refs/heads/master/ray-operator/config/samples/ray-cluster.auth.yaml
```

This command deploys:
* A `RayCluster` resource with a `kube-rbac-proxy` sidecar container on the Head Pod. This proxy handles authentication and authorization.
* A `ConfigMap` for kube-rbac-proxy, containing resource attributes required for authorization.
* A `ServiceAccount`, `ClusterRole`, and `ClusterRoleBinding` that allow the `kube-rbac-proxy` to access the Kubernetes TokenReview and SubjectAccessReview APIs.

## Verify initial unauthorized access

Attempt to submit a Ray job to the cluster to verify that authentication is required. You should receive a `401 Unauthorized` error:

```bash
kubectl port-forward svc/ray-cluster-with-auth-head-svc 8265:8265 &
ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

You may see an error similar to this:

```
...
requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: http://localhost:8265/api/version
```

This error confirms that the Ray cluster requires authentication.

## Configure Kubernetes RBAC for access control

To access the RayCluster, you need:
*  **Authentication:** Provide a valid authentication token (e.g., a Kubernetes service account token or a cloud IAM token) in the request headers.
*  **Authorization:** Your authenticated user or service account must have the necessary Kubernetes RBAC permissions to access the `RayCluster` resource.

This guide demonstrates granting access using a Kubernetes service account, but the same principles apply to individual Kubernetes users or cloud IAM users.

### Create a Kubernetes service account

Create a service account that represents your Ray job submitter:

```bash
kubectl create serviceaccount ray-user
```

Confirm that the service account currently can't access the `RayCluster` resource:

```bash
kubectl auth can-i get rayclusters.ray.io/ray-cluster-with-auth --as=system:serviceaccount:default:ray-user
```

The output should be `no`.

### Grant access using Kubernetes RBAC

Create a `Role` and `RoleBinding` to grant the necessary permissions to the `ray-user` service account:

```yaml
# ray-cluster-rbac.yaml
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ray-user
  namespace: default
rules:
- apiGroups: ["ray.io"]
  resources:
  - 'rayclusters'
  verbs: ["*"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ray-user
  namespace: default
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: Role
  name: ray-user
subjects:
- kind: ServiceAccount
  name: ray-user
  namespace: default
```

Apply the RBAC configuration:

```bash
kubectl apply -f ray-cluster-rbac.yaml
```

### Verify access

Confirm that the service account now has access to the `RayCluster` resource:

```bash
kubectl auth can-i get rayclusters.ray.io/ray-cluster-with-auth --as=system:serviceaccount:default:ray-user
```

The output should be `yes`.

## Submit a Ray job with authentication

Now you can submit a Ray job using the service account's authentication token.

Get a token for the `ray-user` service account and store it in the `RAY_JOB_HEADERS` environment variable:

```bash
export RAY_JOB_HEADERS="{\"Authorization\": \"Bearer $(kubectl create token ray-user --duration=1h)\"}"
```

> **Note:** `kubectl create token` command is only available on Kubernetes v1.24+

Submit the Ray job:

```bash
ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

The job should now succeed, and you should see output similar to this:

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

## Verify access using cloud IAM (Optional)

Most cloud providers allow you to authenticate to the Kubernetes cluster as your cloud IAM user. This method is a convenient way to interact with the cluster without managing separate Kubernetes credentials.

**Example using Google Cloud (GKE):**

Get an access token for your Google Cloud user:

```bash
export RAY_JOB_HEADERS="{\"Authorization\": \"Bearer $(gcloud auth print-access-token)\"}"
```

Submit a Ray job using the IAM token:

```bash
ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
```

The job should succeed if your cloud user has the necessary Kubernetes RBAC permissions. You may need to configure additional RBAC rules for your cloud user.

## View the Ray dashboard (optional)

To view the Ray dashboard from your browser, first configure port-forwarding:

```bash
kubectl port-forward svc/ray-cluster-with-auth-head-svc 8265:8265 &
```

Use a Chrome extension like [Requestly](https://requestly.com/) to automatically add authorization headers to requests for the dashboard endpoint `http://localhost:8265`. The authorization header format is: `Authorization: Bearer <token>`.
