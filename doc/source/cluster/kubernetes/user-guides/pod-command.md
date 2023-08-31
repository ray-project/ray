(kuberay-pod-command)=

# Specify container commands for Ray head/worker Pods
You can execute commands on the head/worker pods at two timings:

* (1) **Before `ray start`**: As an example, you can set up some environment variables that will be used by `ray start`.

* (2) **After `ray start` (RayCluster is ready)**: As an example, you can launch a Ray serve deployment when the RayCluster is ready.

## Current KubeRay operator behavior for container commands
* The current behavior for container commands is not finalized, and **may be updated in the future**.
* See [code](https://github.com/ray-project/kuberay/blob/47148921c7d14813aea26a7974abda7cf22bbc52/ray-operator/controllers/ray/common/pod.go#L301-L326) for more details.

## Timing 1: Before `ray start`
Currently, for timing (1), we can set the container's `Command` and `Args` in RayCluster specification to reach the goal.

```yaml
# https://github.com/ray-project/kuberay/ray-operator/config/samples/ray-cluster.head-command.yaml
    rayStartParams:
        ...
    #pod template
    template:
      spec:
        containers:
        - name: ray-head
          image: rayproject/ray:2.5.0
          resources:
            ...
          ports:
            ...
          # `command` and `args` will become a part of `spec.containers.0.args` in the head Pod.
          command: ["echo 123"]
          args: ["456"]
```

* Ray head Pod
    * `spec.containers.0.command` is hardcoded with `["/bin/bash", "-lc", "--"]`.
    * `spec.containers.0.args` contains two parts:
        * (Part 1) **user-specified command**: A string concatenates `headGroupSpec.template.spec.containers.0.command` from RayCluster and `headGroupSpec.template.spec.containers.0.args` from RayCluster together.
        * (Part 2) **ray start command**: The command is created based on `rayStartParams` specified in RayCluster. The command will look like `ulimit -n 65536; ray start ...`.
        * To summarize, `spec.containers.0.args` will be `$(user-specified command) && $(ray start command)`.

* Example
    ```sh
    # Prerequisite: There is a KubeRay operator in the Kubernetes cluster.

    # Download `ray-cluster.head-command.yaml`
    curl -LO https://raw.githubusercontent.com/ray-project/kuberay/master/ray-operator/config/samples/ray-cluster.head-command.yaml

    # Create a RayCluster
    kubectl apply -f ray-cluster.head-command.yaml

    # Check ${RAYCLUSTER_HEAD_POD}
    kubectl get pod -l ray.io/node-type=head

    # Check `spec.containers.0.command` and `spec.containers.0.args`.
    kubectl describe pod ${RAYCLUSTER_HEAD_POD}

    # Command:
    #   /bin/bash
    #   -lc
    #   --
    # Args:
    #    echo 123  456  && ulimit -n 65536; ray start --head  --dashboard-host=0.0.0.0  --num-cpus=1  --block  --metrics-export-port=8080  --memory=2147483648
    ```


## Timing 2: After `ray start` (RayCluster is ready)
We have two solutions to execute commands after the RayCluster is ready. The main difference between these two solutions is users can check the logs via `kubectl logs` with Solution 1.

### Solution 1: Container command (Recommended)
As we mentioned in the section "Timing 1: Before `ray start`", user-specified command will be executed before the `ray start` command. Hence, we can execute the `ray_cluster_resources.sh` in background by updating `headGroupSpec.template.spec.containers.0.command` in `ray-cluster.head-command.yaml`.

```yaml
# https://github.com/ray-project/kuberay/ray-operator/config/samples/ray-cluster.head-command.yaml
# Parentheses for the command is required.
command: ["(/home/ray/samples/ray_cluster_resources.sh&)"]

# ray_cluster_resources.sh
apiVersion: v1
kind: ConfigMap
metadata:
  name: ray-example
data:
  ray_cluster_resources.sh: |
    #!/bin/bash

    # wait for ray cluster to finish initialization
    while true; do
        ray health-check 2>/dev/null
        if [ "$?" = "0" ]; then
            break
        else
            echo "INFO: waiting for ray head to start"
            sleep 1
        fi
    done

    # Print the resources in the ray cluster after the cluster is ready.
    python -c "import ray; ray.init(); print(ray.cluster_resources())"

    echo "INFO: Print Ray cluster resources"
```

* Example
    ```sh
    # (1) Update `command` to ["(/home/ray/samples/ray_cluster_resources.sh&)"]
    # (2) Comment out `postStart` and `args`.
    kubectl apply -f ray-cluster.head-command.yaml

    # Check ${RAYCLUSTER_HEAD_POD}
    kubectl get pod -l ray.io/node-type=head

    # Check the logs
    kubectl logs ${RAYCLUSTER_HEAD_POD}

    # INFO: waiting for ray head to start
    # .
    # . => Cluster initialization
    # .
    # 2023-02-16 18:44:43,724 INFO worker.py:1231 -- Using address 127.0.0.1:6379 set in the environment variable RAY_ADDRESS
    # 2023-02-16 18:44:43,724 INFO worker.py:1352 -- Connecting to existing Ray cluster at address: 10.244.0.26:6379...
    # 2023-02-16 18:44:43,735 INFO worker.py:1535 -- Connected to Ray cluster. View the dashboard at http://10.244.0.26:8265
    # {'object_store_memory': 539679129.0, 'node:10.244.0.26': 1.0, 'CPU': 1.0, 'memory': 2147483648.0}
    # INFO: Print Ray cluster resources
    ```

### Solution 2: postStart hook
```yaml
# https://github.com/ray-project/kuberay/ray-operator/config/samples/ray-cluster.head-command.yaml
lifecycle:
  postStart:
    exec:
      command: ["/bin/sh","-c","/home/ray/samples/ray_cluster_resources.sh"]
```

* We execute the script `ray_cluster_resources.sh` via the postStart hook. Based on [this document](https://kubernetes.io/docs/concepts/containers/container-lifecycle-hooks/#container-hooks), there is no guarantee that the hook will execute before the container ENTRYPOINT. Hence, we need to wait for RayCluster to finish initialization in `ray_cluster_resources.sh`.

* Example
    ```sh
    kubectl apply -f ray-cluster.head-command.yaml

    # Check ${RAYCLUSTER_HEAD_POD}
    kubectl get pod -l ray.io/node-type=head

    # Forward the port of Dashboard
    kubectl port-forward --address 0.0.0.0 ${RAYCLUSTER_HEAD_POD} 8265:8265

    # Open the browser and check the Dashboard (${YOUR_IP}:8265/#/job).
    # You shold see a SUCCEEDED job with the following Entrypoint:
    #
    # `python -c "import ray; ray.init(); print(ray.cluster_resources())"`
    ```
