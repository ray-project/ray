(kuberay-rayservice-no-ray-serve-replica)=

# RayService Worker No Ray Serve Replica

This guide explores a specific scenario in KubeRay with Ray where a Ray worker Pod remains in an unready state due to the lack of a Ray Serve replica.  

## What's a Ray Serve Replica?

A Ray Serve Replica is an instance of a deployment in Ray Serve that processes incoming requests. Each replica runs as a Ray actor and can handle HTTP or Python function calls. Users can scale the number of replicas based on workload requirements, distributing traffic across the available replicas for load balancing and high availability.  

## Example: Serve two simple Ray Serve applications using RayService

## Step 1: Create a Kubernetes cluster with Kind

```sh
kind create cluster --image=kindest/node:v1.26.0
```

## Step 2: Install the KubeRay operator

Follow [this document](kuberay-operator-deploy) to install the latest stable KubeRay operator using Helm repository.

## Step 3: Install a RayService

::::{tab-set}

:::{tab-item} x86-64 (Intel/Linux)
```sh
curl -O https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.no-ray-serve-replica.yaml
kubectl apply -f ray-service.no-ray-serve-replica.yaml
```
:::

:::{tab-item} ARM64 (Apple silicon)
```sh
curl -s https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-service.no-ray-serve-replica.yaml | sed 's/2.41.0/2.41.0-aarch64/g' > ray-service.no-ray-serve-replica.yaml
kubectl apply -f ray-service.no-ray-serve-replica.yaml
```
:::

::::

Look at the Ray Serve configuration `serveConfigV2` embedded in the RayService YAML. Notice the only deployment in `deployments` of the application named `simple_app`:
  * `num_replicas`: Controls the number of replicas to run that handle requests to this deployment. Initialize to 1 to ensure the overall number of the ray serve replicas is 1.
  * `max_replicas_per_node`: Controls the maximum number of replicas on one single pod.  

See [Ray Serve Documentation](https://docs.ray.io/en/master/serve/configure-serve-deployment.html) for more details.
```yaml
serveConfigV2: |
  applications:
    - name: simple_app
      import_path: ray-operator.config.samples.ray-serve.single_deployment_dag:DagNode
      route_prefix: /basic
      runtime_env:
        working_dir: "https://github.com/ray-project/kuberay/archive/master.zip"
      deployments:
        - name: BaseService
          num_replicas: 1
          max_replicas_per_node: 1
          ray_actor_options:
            num_cpus: 0.1
```

Look at the head Pod configuration `rayClusterConfig:headGroupSpec` embedded in the RayService YAML.  
The configuration sets the CPU resources for the head Pod to 0 by passing the option `num-cpus: "0"` to `rayStartParams`. This setup avoids Ray Serve replicas running on the head Pod
See [rayStartParams](https://github.com/ray-project/kuberay/blob/master/docs/guidance/rayStartParams.md) for more details.  
```sh
headGroupSpec:
  rayStartParams:
    num-cpus: "0"
  template: ...
```

## Step 4: Verify the Kubernetes cluster status

```sh
# Step 4.1: List all RayService custom resources in the `default` namespace.
kubectl get rayservice

# [Example output]
# NAME                              SERVICE STATUS   NUM SERVE ENDPOINTS
# rayservice-no-ray-serve-replica   Running          2

# Step 4.2: List all RayCluster custom resources in the `default` namespace.
kubectl get raycluster

# [Example output]
# NAME                                               DESIRED WORKERS   AVAILABLE WORKERS   CPUS   MEMORY   GPUS   STATUS   AGE
# rayservice-no-ray-serve-replica-raycluster-dnm28   2                 2                   1      6Gi      0               87s

# Step 4.3: List services in the `default` namespace.
kubectl get services

# [Example output]
# NAME                                                 TYPE        CLUSTER-IP    EXTERNAL-IP   PORT(S)                                         AGE
# ...
# ice-no-ray-serve-replica-raycluster-dnm28-head-svc   ClusterIP   None          <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   3m22s
# rayservice-no-ray-serve-replica-head-svc             ClusterIP   None          <none>        10001/TCP,8265/TCP,6379/TCP,8080/TCP,8000/TCP   3m1s
# rayservice-no-ray-serve-replica-serve-svc            ClusterIP   10.96.8.250   <none>        8000/TCP                                        3m1s
```

## Step 5: Why 1 worker Pod isn't ready?

```sh
# Step 5.1: List all Ray Pods in the `default` namespace.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                                                              READY   STATUS    RESTARTS   AGE
# rayservice-no-ray-serve-replica-raycluster-dnm28-head-9h2qt       1/1     Running   0          2m21s
# rayservice-no-ray-serve-replica-raycluster-dnm28-s-worker-46t7l   1/1     Running   0          2m21s
# rayservice-no-ray-serve-replica-raycluster-dnm28-s-worker-77rzk   0/1     Running   0          2m20s

# Step 5.2: Check unready worker pod events
kubectl describe pods {YOUR_UNREADY_WORKER_POD_NAME}

# [Example output]
# Events:
#   Type     Reason     Age                   From               Message
#   ----     ------     ----                  ----               -------
#   Normal   Scheduled  3m4s                  default-scheduler  Successfully assigned default/rayservice-no-ray-serve-replica-raycluster-dnm28-s-worker-77rzk to kind-control-plane
#   Normal   Pulled     3m3s                  kubelet            Container image "rayproject/ray:2.41.0" already present on machine
#   Normal   Created    3m3s                  kubelet            Created container wait-gcs-ready
#   Normal   Started    3m3s                  kubelet            Started container wait-gcs-ready
#   Normal   Pulled     2m57s                 kubelet            Container image "rayproject/ray:2.41.0" already present on machine
#   Normal   Created    2m57s                 kubelet            Created container ray-worker
#   Normal   Started    2m57s                 kubelet            Started container ray-worker
#   Warning  Unhealthy  78s (x19 over 2m43s)  kubelet            Readiness probe failed: success
```

KubeRay creates a RayCluster based on `spec.rayClusterConfig` defined in the RayService YAML for a RayService custom resource.  
Next, once the head Pod is running and ready, KubeRay submits a request to the head's dashboard port to create the Ray Serve applications defined in `spec.serveConfigV2`.  
KubeRay deploys these Ray Serve applications as Ray Serve replicas in worker Pods.  

Look at the output of Step 4.3. One worker Pod is running and ready, while the other is running but not ready.  
Starting from Ray 2.8, a Ray worker Pod that doesn't have any Ray Serve replica won't have a Proxy actor.  
Starting from KubeRay v1.1.0, KubeRay adds a readiness probe to every worker Pod's Ray container to check if the worker Pod has a Proxy actor or not.  
If the worker Pod lacks a Proxy actor, the readiness probe fails, rendering the worker Pod unready, and thus, it doesn't receive any traffic.  

With `spec.serveConfigV2`, KubeRay only creates one Ray Serve replica and schedules it to one of the worker Pods.
The worker Pod with a Ray Serve replica is setup with a Proxy actor and marked as ready.
KubeRay marks the other worker Pod, which doesn't have any Ray Serve replica and a Proxy actor, as unready.

## Step 6: Verify the status of the Serve applications

```sh
kubectl port-forward svc/rayservice-no-ray-serve-replica-head-svc 8265:8265
```

See [rayservice-troubleshooting.md](kuberay-raysvc-troubleshoot) for more details on RayService observability.  

Below is a screenshot example of the Serve page in the Ray dashboard.  
There's a `ray::ServeReplica::simple_app::BaseService` and a `ray::ProxyActor` running on one of the worker pod, while no Ray Serve replica and Proxy actor running on the another. KubeRay marks the former as ready and the later as unready.
  ![Ray Serve Dashboard](../images/rayservice-no-ray-serve-replica-dashboard.png)

## Step 7: Send requests to the Serve applications by the Kubernetes serve service

```sh
# Step 7.1: Run a curl Pod.
# If you already have a curl Pod, you can use `kubectl exec -it <curl-pod> -- sh` to access the Pod.
kubectl run curl --image=radial/busyboxplus:curl -i --tty

# Step 7.2: Send a request to the simple_app.
curl -X POST -H 'Content-Type: application/json' rayservice-no-ray-serve-replica-serve-svc:8000/basic
# [Expected output]: hello world
```

* `rayservice-no-ray-serve-serve-svc` does traffic routing among all the workers which have Ray Serve replicas.
Although one worker Pod is unready, Ray Serve can still route the traffic to the ready worker Pod with Ray Serve replica running.

## Step 8: In-place update for Ray Serve applications

Update the `num_replicas` of application from `1` to `2` in `ray-service.no-ray-serve-replica.yaml`. This change reconfigures the existing RayCluster.

```sh
# Step 8.1: Update the num_replicas of application from 1 to 2.
# [ray-service.no-ray-serve-replica.yaml]
# deployments:
#   - name: BaseService
#     num_replicas: 2
#     max_replicas_per_node: 1
#     ray_actor_options:
#       num_cpus: 0.1

# Step 8.2: Apply the updated RayService config.
kubectl apply -f ray-service.no-ray-serve-replica.yaml

# Step 8.3: List all Ray Pods in the `default` namespace.
kubectl get pods -l=ray.io/is-ray-node=yes

# [Example output]
# NAME                                                              READY   STATUS    RESTARTS   AGE
# rayservice-no-ray-serve-replica-raycluster-dnm28-head-9h2qt       1/1     Running   0          46m
# rayservice-no-ray-serve-replica-raycluster-dnm28-s-worker-46t7l   1/1     Running   0          46m
# rayservice-no-ray-serve-replica-raycluster-dnm28-s-worker-77rzk   1/1     Running   0          46m
```

After reconfiguration, KubeRay requests the head Pod to create an additional Ray Serve replica to match the `num_replicas` configuration. Because the `max_replicas_per_node` is `1`, the new Ray Serve replica is running on the worker Pod without any replicas. After that, KubeRay marks the worker Pod as ready.

## Step 9: Clean up the Kubernetes cluster

```sh
# Delete the RayService.
kubectl delete -f ray-service.no-ray-serve-replica.yaml

# Uninstall the KubeRay operator.
helm uninstall kuberay-operator

# Delete the curl Pod.
kubectl delete pod curl
```

## Next steps

* See [RayService troubleshooting guide](kuberay-raysvc-troubleshoot) if you encounter any issues.
* See [Examples](kuberay-examples) for more RayService examples.
