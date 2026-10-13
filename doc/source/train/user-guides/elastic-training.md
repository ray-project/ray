---
myst:
  html_meta:
    description: "Elastic training that adapts to changing resource availability, continuing through node preemption and scaling up as new nodes join."
---

(train-elastic-training)=

# Elastic training

Ray Train supports elastic training, where a job adapts to changes in resource availability. Training keeps running through hardware failures and node preemptions instead of sitting idle. As more nodes become available, the cluster scales up to speed up training with more worker processes.

To enable elastic training, set {attr}`~ray.train.ScalingConfig.num_workers` to a `(min_workers, max_workers)` tuple instead of a fixed worker group size. Also set {attr}`~ray.train.FailureConfig.max_failures` so that training can recover from worker failures instead of exiting immediately.

The following examples show how to configure elastic training with GPUs and TPUs:

::::{tab-set}
:::{tab-item} GPU
:sync: GPU
The following example configures elastic training with one to eight GPU workers:

```python
from ray.train import FailureConfig, RunConfig, ScalingConfig
from ray.train.torch import TorchTrainer

def train_func():
    # Your training code here.
    ...

scaling_config = ScalingConfig(
    num_workers=(1, 8),
    use_gpu=True,
)

# Allow retries so training survives worker failures.
run_config = RunConfig(failure_config=FailureConfig(max_failures=3))

trainer = TorchTrainer(
    train_func,
    scaling_config=scaling_config,
    run_config=run_config,
)
trainer.fit()
```
:::

:::{tab-item} TPU
:sync: TPU
The following example configures elastic training with one or two `v6e` TPU slices. Each `num_workers` value maps to the total number of TPU VM hosts across all slices. This example uses a `4x4` TPU topology, where one slice has four hosts, so both `min_workers` and `max_workers` are multiples of four.

```python
from ray.train import FailureConfig, RunConfig, ScalingConfig
from ray.train.v2.jax import JaxTrainer

def train_func():
    # Your JAX training code here.
    ...

scaling_config = ScalingConfig(
    num_workers=(4, 8),
    use_tpu=True,
    topology="4x4",
    accelerator_type="TPU-V6E",
)

# Allow retries so training survives worker failures.
run_config = RunConfig(failure_config=FailureConfig(max_failures=3))

trainer = JaxTrainer(
    train_func,
    scaling_config=scaling_config,
    run_config=run_config,
)
trainer.fit()
```
:::
::::

For TPU elastic training, set `min_workers` and `max_workers` to multiples of the number of hosts in one TPU slice. Ray Train resizes TPU jobs by complete slices so that workers are placed on intact TPU topologies. For details, see {ref}`train_scaling_config`.

## How does elastic training work?

Ray Train adjusts the worker group when training starts, when a failure happens, and when more nodes become available.

(starting-with-available-workers)=

### How does training start?

Ray Train always requests `max_workers` workers. If it can't get all of them, it starts once `min_workers` workers are available, so training begins without waiting for the full set of resources.

(when-failures-happen)=

### What happens when a failure occurs?

When a failure happens, such as a worker crash or a node preemption, Ray Train restarts with fewer workers. It then tries again to bring the worker group back up to `max_workers`. Without a retry limit, the run exits on the first such failure. To retry the run when worker failures occur, configure {attr}`~ray.train.RunConfig.failure_config` with {attr}`~ray.train.FailureConfig.max_failures`:

```{code-block} python
:emphasize-lines: 4

from ray.train import RunConfig, FailureConfig

# Retry up to 3 times on worker failures (e.g. preemption, node loss)
run_config = RunConfig(failure_config=FailureConfig(max_failures=3))

trainer = TorchTrainer(
    train_func,
    scaling_config=scaling_config,
    run_config=run_config,
)
```

(when-more-nodes-become-available)=

### What happens when more nodes become available?

If the cluster later gains more nodes, Ray Train can resize the worker group and restart with the new workers added, so training uses the extra capacity. By default, the controller considers resizing every 60 seconds while the worker group is healthy. To change how often the controller makes resize decisions, set {attr}`~ray.train.ScalingConfig.elastic_resize_monitor_interval_s` in your scaling configuration:

```python
# Consider resizing the worker group every 30 seconds (default is 60)
scaling_config = ScalingConfig(
    num_workers=(1, 8),
    use_gpu=True,
    elastic_resize_monitor_interval_s=30.0,
)
```

## Configure cluster autoscaling

For elastic training to scale up when more resources become available, configure the cluster autoscaler to match your elastic training settings. The cluster needs to be able to provision up to `max_workers` nodes and scale down to `min_workers` nodes.

:::::{tab-set}
::::{tab-item} KubeRay
Set the `minReplicas` and `maxReplicas` fields on your worker group to match the elastic training range. The following example configures a worker group that can scale between one and eight nodes:

```{code-block} yaml
:emphasize-lines: 3,4

workerGroupSpecs:
  - groupName: gpu-workers
    minReplicas: 1
    maxReplicas: 8
    replicas: 1
    template:
      spec:
        containers:
          - name: ray-worker
            image: rayproject/ray:2.56.1
```

:::{note}
If the Kubernetes cluster doesn't have enough physical nodes, also configure a Kubernetes-level autoscaler, such as the Cluster Autoscaler or Karpenter, to provision new Kubernetes nodes for the Ray worker Pods. For details, see {ref}`kuberay-autoscaling-config`.
:::
::::

:::{tab-item} VMs
Set the `min_workers` and `max_workers` fields in your cluster configuration to match the elastic training range:

```{code-block} yaml
:emphasize-lines: 5,6

max_workers: 8

available_node_types:
  gpu_worker:
    min_workers: 1
    max_workers: 8
```

For details, see {ref}`vms-autoscaling`.
:::
:::::
