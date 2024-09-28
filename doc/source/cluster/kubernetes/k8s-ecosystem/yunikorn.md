(kuberay-yunikorn)=
# KubeRay integration with Apache YuniKorn

[Apache YuniKorn](https://yunikorn.apache.org/) is a light-weight, universal resource scheduler for container orchestrator systems. It is created to achieve fine-grained resource sharing for various workloads efficiently on a large scale, multi-tenant, and cloud-native environment. YuniKorn brings a unified, cross-platform, scheduling experience for mixed workloads that consist of stateless batch workloads and stateful services.

KubeRay's Apache YuniKorn integration enables more efficient scheduling of Ray pods in multi-tenant Kubernetes environments.

## Setup

### Step 1: Create a Kubernetes cluster with KinD
Run the following command in a terminal:

```shell
kind create cluster
```

### Step 2: Install Apache YuniKorn

You need to successfully install Apache YuniKorn on your Kubernetes cluster before enabling Apache YuniKorn integration with KubeRay.
See [Get Started](https://yunikorn.apache.org/docs/) for Apache YuniKorn installation instructions.

### Step 3: Install the KubeRay Operator with Apache YuniKorn support

Deploy the KubeRay Operator with the `--enable-batch-scheduler` flag to enable Volcano batch scheduling support.

When installing KubeRay Operator using Helm, you should use one of these two options:

* Set `batchScheduler.name` to `yunikorn` in your
[`values.yaml`](https://github.com/ray-project/kuberay/blob/df5577fe1b3f537f36dbda3870b4743edd2f11bb/helm-chart/kuberay-operator/values.yaml#L83)
file:
```shell
# values.yaml file
batchScheduler:
    name: yunikorn
```

* Pass the `--set batchScheduler.name=yunikorn` flag when running on the command line:
```shell
# Install the Helm chart with --enable-batch-scheduler flag set to true
helm install kuberay-operator kuberay/kuberay-operator --namespace ray-system --version 1.2.2 --create-namespace --set batchScheduler.name=yunikorn
```

### Step 4: Use Apache YuniKorn for batch scheduling

## Example

### Gang scheduling

This example walks through how gang scheduling works with Apache YuniKorn and KubeRay.

First, create a queue with a capacity of 4 CPUs and 6Gi of RAM by editing the ConfigMap:

Run `kubectl edit configmap -n yunikorn yunikorn-defaults`

Add a `queues.yaml` config to under the `data` key, the final ConfigMap should look like this:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  # Metadata for the ConfigMap, skip for brevity
data:
  queues.yaml: |
    partitions:
      - name: default
        queues:
          - name: root
            queues:
              - name: test
                submitacl: "*"
                parent: false
                resources:
                  guaranteed:
                    memory: 6G
                    vcore: 4
                  max:
                    memory: 6G
                    vcore: 4
```

Save the changes and exit the editor. This configuration creates a queue named `root.default` with a capacity of 4 CPUs and 6Gi of RAM.

Next, create a RayCluster with a head node (1 CPU + 2Gi of RAM) and two workers (1 CPU + 1Gi of RAM each), for a total of 3 CPU and 4Gi of RAM:


```shell
# Path: kuberay/ray-operator/config/samples
# Includes the necessary labels for the Apache YuniKorn scheduler gang scheduling:
# - `ray.io/gang-scheduling-enabled`
# - `yunikorn.apache.org/app-id`
# - `yunikorn.apache.org/queue`
wget https://raw.githubusercontent.com/ray-project/kuberay/v1.2.2/ray-operator/config/samples/ray-cluster.yunikorn-scheduler.yaml
kubectl apply -f ray-cluster.yunikorn-scheduler.yaml
```

Because the queue has a capacity of 4 CPU and 6Gi of RAM, this resource should schedule successfully without any issues.

```shell
$ kubectl get pods

NAME                                  READY   STATUS    RESTARTS   AGE
test-yunikorn-0-head-98fmp            1/1     Running   0          67s
test-yunikorn-0-worker-worker-42tgg   1/1     Running   0          67s
test-yunikorn-0-worker-worker-467mn   1/1     Running   0          67s
```

Next, add an additional RayCluster with the same configuration of head and worker nodes, but with a different name:

```shell
# Path: kuberay/ray-operator/config/samples
# Includes the `ray.io/scheduler-name: volcano` and `volcano.sh/queue-name: kuberay-test-queue` labels in the metadata.labels
# Replaces the name to test-yunikorn-1
sed 's/test-yunikorn-0/test-yunikorn-1/' ray-cluster.yunikorn-scheduler.yaml | kubectl apply -f-
```

Now all the pods for `test-yunikorn-1` are in the `Pending` state:

```shell
$ kubectl get pods

NAME                                      READY   STATUS    RESTARTS   AGE
test-yunikorn-0-head-98fmp                1/1     Running   0          4m22s
test-yunikorn-0-worker-worker-42tgg       1/1     Running   0          4m22s
test-yunikorn-0-worker-worker-467mn       1/1     Running   0          4m22s
test-yunikorn-1-head-xl2r5                0/1     Pending   0          71s
test-yunikorn-1-worker-worker-l6ttz       0/1     Pending   0          71s
test-yunikorn-1-worker-worker-vjsts       0/1     Pending   0          71s
tg-test-yunikorn-1-headgroup-vgzvoot0dh   0/1     Pending   0          69s
tg-test-yunikorn-1-worker-eyti2bn2jv      1/1     Running   0          69s
tg-test-yunikorn-1-worker-k8it0x6s73      0/1     Pending   0          69s
```

Because the new cluster requires more CPU and RAM than our queue allows, even though one of the pods would fit in the remaining 1 CPU and 2Gi of RAM, none of the cluster's pods are placed until there is enough room for all the pods. Without using Volcano for gang scheduling in this way, one of the pods would ordinarily be placed, leading to the cluster being partially allocated, and some jobs (like [Horovod](https://github.com/horovod/horovod) training) being stuck waiting for resources to become available.

Delete the first RayCluster to make space in the queue:

```shell
kubectl delete raycluster test-yunikorn-0
```

Now all the pods for the second cluster changed to the `Running` state, because enough resources are now available to schedule the entire set of pods:

Check the pods again to see that the second cluster is now up and running:

```shell
$ kubectl get pods

NAME                                  READY   STATUS    RESTARTS   AGE
test-yunikorn-1-head-xl2r5            1/1     Running   0          3m34s
test-yunikorn-1-worker-worker-l6ttz   1/1     Running   0          3m34s
test-yunikorn-1-worker-worker-vjsts   1/1     Running   0          3m34s
```

Clean up the resources:

```shell
kubectl delete raycluster test-yunikorn-1
```
