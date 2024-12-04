(kuberay-auth)=

# Configure Ray cluster authentication and access control using Kubernetes RBAC

This guide demonstrates how to configure authentication and access control for Ray clusters using KubeRay and Kubernetes RBAC.

## Create a GKE cluster

Create a GKE cluster
```
gcloud container clusters create kuberay-cluster \
    --num-nodes=2 --zone=us-west1-b --machine-type e2-standard-4
```

## Install the KubeRay operator

Follow [Deploy a KubeRay operator](kuberay-operator-deploy) to install the latest stable KubeRay operator from the Helm repository.

## Deploy a RayCluster with kube-rbac-proxy

Create a RayCluster resource running a kube-rbac-proxy sidecar container:
```
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/refs/heads/master/ray-operator/config/samples/ray-cluster.auth.yaml
``` 

This step deploys the following:
1. A RayCluster resource running a kube-rbac-proxy sidecar container for authentication/authorization on the Head Pod.
2. A kube-rbac-proxy ConfigMap containing resource attributes required for authorization
3. A ServiceAccount, ClusterRole and ClusterRoleBinding used by kube-rbac-proxy to access the TokenReview and SubjectAcceessReview APIs

## Verify 401 unauthorized error when accessing Ray cluster

Submit a Ray job and verify that 401 unauthorized error is returned
```
$ kubectl port-forward svc/ray-cluster-with-auth-head-svc 8265:8265 &
$ ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
...
...
requests.exceptions.HTTPError: 401 Client Error: Unauthorized for url: http://localhost:8265/api/version
```

## Configure Kubernetes RBAC for access control

The following is required in order to access the RayCluster:
* **Authentication**: you must provide an authentication token (via headers) to authenticate your Kubernetes user  
* **Authorization**: your user must be authorized (via Kubernetes RBAC) to access the RayCluster resource

In most Kubernetes offerings, you can authenticate to your Kubernetes cluster using your cloud user and authorization is granted by an admin
who can modify Kubernetes RBAC rules. This guide will use a Kubernetes service account to demonstrate how to grant access to an unauthorized
subject. The same steps can be applied to grant access to Kubernetes users.  

Create a Kubernetes service account:
```
$ kubectl create serviceaccount ray-user
serviceaccount/ray-user created
```

Verify that the service account cannot access RayCluster resources:
```bash
$ kubectl auth can-i get rayclusters.ray.io/ray-cluster-with-auth --as=system:serviceaccount:default:ray-user
no
```

Grant the ServiceAccount access to the RayCluster resource used in this guide:
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

Verify that the service account can access RayCluster resources
```bash
$ kubectl auth can-i get rayclusters.ray.io/ray-cluster-with-auth --as=system:serviceaccount:default:ray-user
yes
```

## Submit a job

Set the `RAY_JOB_HEADERS` environment variable containing a token from the Kubernetes service account:
```bash
$ export RAY_JOB_HEADERS="{\"Authorization\": \"Bearer $(kubectl create token ray-user)\"}"
```

Submit a Ray job:
```bash
$ ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
Job submission server address: http://localhost:8265

-------------------------------------------------------
Job 'raysubmit_n2fq2Ui7cbh3p2Js' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_n2fq2Ui7cbh3p2Js
  Query the status of the job:
    ray job status raysubmit_n2fq2Ui7cbh3p2Js
  Request the job to be stopped:
    ray job stop raysubmit_n2fq2Ui7cbh3p2Js

Tailing logs until the job exits (disable with --no-wait):

2024-12-04 12:21:02,613	INFO job_manager.py:530 -- Runtime env is setting up.
2024-12-04 12:21:04,182	INFO worker.py:1494 -- Using address 10.112.0.52:6379 set in the environment variable RAY_ADDRESS
2024-12-04 12:21:04,183	INFO worker.py:1634 -- Connecting to existing Ray cluster at address: 10.112.0.52:6379...
2024-12-04 12:21:04,198	INFO worker.py:1810 -- Connected to Ray cluster. View the dashboard at 127.0.0.1:8443
{'node:10.112.0.52': 1.0, 'memory': 12884901888.0, 'node:__internal_head__': 1.0, 'object_store_memory': 3708144844.0, 'CPU': 4.0, 'node:10.112.1.49': 1.0, 'node:10.112.2.36': 1.0}

------------------------------------------
Job 'raysubmit_n2fq2Ui7cbh3p2Js' succeeded
------------------------------------------
```

## Verify access using cloud IAM (optional)

Most cloud providers support authenticating to your Kubernetes cluster as your cloud IAM user. For example, on GKE you can authenticate as your Google Cloud user using `gcloud auth print-access-token`:
```bash
$ export RAY_JOB_HEADERS="{\"Authorization\": \"Bearer $(gcloud auth print-access-token)\"}"
```

Submit a Ray job:
```bash
$ ray job submit --address http://localhost:8265  -- python -c "import ray; ray.init(); print(ray.cluster_resources())"
Job submission server address: http://localhost:8265

-------------------------------------------------------
Job 'raysubmit_L8D1bBiTP9CYsA5P' submitted successfully
-------------------------------------------------------

Next steps
  Query the logs of the job:
    ray job logs raysubmit_L8D1bBiTP9CYsA5P
  Query the status of the job:
    ray job status raysubmit_L8D1bBiTP9CYsA5P
  Request the job to be stopped:
    ray job stop raysubmit_L8D1bBiTP9CYsA5P

Tailing logs until the job exits (disable with --no-wait):
2024-12-04 12:26:39,949	INFO job_manager.py:530 -- Runtime env is setting up.
2024-12-04 12:26:41,119	INFO worker.py:1494 -- Using address 10.112.0.52:6379 set in the environment variable RAY_ADDRESS
2024-12-04 12:26:41,120	INFO worker.py:1634 -- Connecting to existing Ray cluster at address: 10.112.0.52:6379...
2024-12-04 12:26:41,130	INFO worker.py:1810 -- Connected to Ray cluster. View the dashboard at 127.0.0.1:8443
{'memory': 12884901888.0, 'node:__internal_head__': 1.0, 'CPU': 4.0, 'object_store_memory': 3708144844.0, 'node:10.112.0.52': 1.0, 'node:10.112.1.49': 1.0, 'node:10.112.2.36': 1.0}

------------------------------------------
Job 'raysubmit_L8D1bBiTP9CYsA5P' succeeded
------------------------------------------
```
