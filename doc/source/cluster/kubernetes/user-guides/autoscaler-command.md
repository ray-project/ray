(kuberay-autoscaler-command)=

# Specify container commands for the autoscaler

KubeRay generates a start command for the autoscaler container.
Sometimes, you may want to execute certain commands either before or instead of the generated autoscaler start command.
This document shows you how to do that using the `command` and `args` fields in `autoscalerOptions`.

## Specify a custom autoscaler command

Starting with KubeRay v1.7.0, you can set `command` and `args` in `autoscalerOptions` to override the generated autoscaler container command.
This works similarly to how `KUBERAY_GEN_RAY_START_CMD` works for Ray head/worker Pods (see {ref}`kuberay-pod-command`).

When `command` or `args` is set in `autoscalerOptions`, KubeRay uses those values directly instead of generating the autoscaler start command.
KubeRay also injects the environment variable `KUBERAY_GEN_AUTOSCALER_START_CMD` into the autoscaler container so you can reference the generated command in your custom `args`.

```yaml
apiVersion: ray.io/v1
kind: RayCluster
metadata:
  name: raycluster-autoscaler-overwrite-cmd
spec:
  rayVersion: "2.52.0"
  enableInTreeAutoscaling: true
  autoscalerOptions:
    version: v2
    # KubeRay uses `command` and `args` as-is when they are present in autoscalerOptions,
    # instead of generating the autoscaler start command.
    command: ["/bin/bash", "-lc", "--"]
    # `KUBERAY_GEN_AUTOSCALER_START_CMD` is injected by KubeRay and holds the
    # generated autoscaler start command, so you can wrap it with extra logic.
    args: ["echo 'Starting autoscaler...'; $KUBERAY_GEN_AUTOSCALER_START_CMD"]
  headGroupSpec:
    rayStartParams:
      num-cpus: "0"
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.52.0
          ...
```
The preceding example YAML is a part of [ray-cluster.autoscaler-v2-overwrite-cmd.yaml](https://github.com/ray-project/kuberay/blob/master/ray-operator/config/samples/ray-cluster.autoscaler-v2-overwrite-cmd.yaml)

* `autoscalerOptions.command`: Overrides the autoscaler container's entrypoint. When specified, KubeRay uses this value directly instead of the generated command.

* `autoscalerOptions.args`: Overrides the autoscaler container's arguments. When specified, KubeRay uses this value directly.

* `$KUBERAY_GEN_AUTOSCALER_START_CMD`: KubeRay injects this environment variable into the autoscaler container. It holds the generated autoscaler start command, so you can reference it in your custom `args` to run setup logic before starting the autoscaler.
  ```sh
  # Example of KUBERAY_GEN_AUTOSCALER_START_CMD in the autoscaler container.
  ray kuberay-autoscaler --cluster-name $(RAY_CLUSTER_NAME) --cluster-namespace $(RAY_CLUSTER_NAMESPACE)
  ```

  Note that `$(RAY_CLUSTER_NAME)` and `$(RAY_CLUSTER_NAMESPACE)` are the environment variable references resolved by the system before the container starts. When bash later expands `$KUBERAY_GEN_AUTOSCALER_START_CMD`, the command already contains the resolved cluster name and namespace.

## Example

```sh
# Prerequisite: A KubeRay operator is running in the Kubernetes cluster.

# Create a RayCluster with autoscaler command override
kubectl apply -f https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.autoscaler-v2-overwrite-cmd.yaml

# Check the autoscaler Pod
kubectl get pod -l ray.io/node-type=head

# Check the autoscaler container's command and args inside the head Pod.
export RAYCLUSTER_HEAD_POD=$(kubectl get pods --selector=ray.io/node-type=head -o custom-columns=POD:metadata.name --no-headers)
kubectl get pods ${RAYCLUSTER_HEAD_POD} -o jsonpath='{range .spec.containers[?(@.name=="autoscaler")]}{"Command: "}{.command}{"\n"}{"Args: "}{.args}{"\n"}{end}'

# The autoscaler container's args will look like:
# Command: ["/bin/bash","-lc","--"]
# Args: ["echo 'Starting autoscaler...'; $KUBERAY_GEN_AUTOSCALER_START_CMD"]
```
