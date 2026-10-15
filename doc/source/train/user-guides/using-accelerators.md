---
myst:
  html_meta:
    description: "Configure Ray Train scale and accelerators: worker count, GPUs per worker, accelerator type, communication backend, and per-worker resources."
---

(train_scaling_config)=

# Configure scale and accelerators
Scale a Ray Train training run with a few lines of code. The main interface is the {class}`~ray.train.ScalingConfig`, which configures the number of workers and the resources each worker uses.

In this guide, a *worker* means a Ray Train distributed training worker, which is a {ref}`Ray actor <actor-key-concept>` that runs your training function.

(increasing-the-number-of-workers)=

## Increase the number of workers
To control parallelism in your training code, set the number of workers. Pass the `num_workers` attribute to the {class}`~ray.train.ScalingConfig`:

```{testcode}
from ray.train import ScalingConfig

scaling_config = ScalingConfig(
    num_workers=8
)
```

## Use accelerators

::::{tab-set}
:::{tab-item} GPU
:sync: GPU
To use GPUs, pass `use_gpu=True` to the {class}`~ray.train.ScalingConfig`. This requests one GPU per training worker. In the following example, training runs on 8 GPUs, with 8 workers that each use one GPU.

```{testcode}
from ray.train import ScalingConfig

scaling_config = ScalingConfig(
    num_workers=8,
    use_gpu=True
)
```
:::

:::{tab-item} TPU
:sync: TPU
To use TPUs, pass `use_tpu=True` to the {class}`~ray.train.ScalingConfig`. Also specify `topology` and `accelerator_type`.

Ray Train supports two worker granularities on TPUs:

- **One worker per TPU host (default, JAX):** By default, each Ray Train worker maps to one TPU host and manages all TPU chips on that host (for example, with {class}`~ray.train.v2.jax.JaxTrainer`). The total `num_workers` must be a multiple of the number of hosts in a single slice. For example, a [TPU v6e](https://cloud.google.com/tpu/docs/v6e) slice with a `4x4` topology has four hosts, so valid values include `num_workers=4` for one slice or `num_workers=8` for two slices.
- **One worker per TPU device (TorchTPU):** For PyTorch workloads using TorchTPU and {class}`~ray.train.torch.TorchTrainer`, set `resources_per_worker={"TPU": 1}` to launch one worker process per logical TPU device. Set `num_workers` to the total number of logical TPU devices across your slices (such as `num_workers=16` for one 16-device TPU v6e `4x4` or [TPU7x](https://cloud.google.com/kubernetes-engine/docs/concepts/tpu-ironwood) `2x2x2` slice).

For details on how TPU topologies map to hosts and devices, see [Plan TPUs in GKE](https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus).

```{testcode}
:skipif: True

from ray.train import ScalingConfig

# Single slice (1 worker per host / JaxTrainer): 4 v6e VMs in a 4x4 topology (4 chips per VM)
scaling_config = ScalingConfig(
    num_workers=4,
    use_tpu=True,
    topology="4x4",
    accelerator_type="TPU-V6E",
)

# Single slice (1 worker per device / TorchTrainer with TorchTPU): 16 workers, 1 logical TPU device per worker
scaling_config = ScalingConfig(
    num_workers=16,
    use_tpu=True,
    topology="4x4",
    accelerator_type="TPU-V6E",
    resources_per_worker={"TPU": 1},
)

# Multi-slice (1 worker per host / JaxTrainer): 2 v6e slices, 8 VMs total
scaling_config = ScalingConfig(
    num_workers=8,
    use_tpu=True,
    topology="4x4",
    accelerator_type="TPU-V6E",
)
```
:::
::::


(using-accelerators-in-the-training-function)=

### Use accelerators in the training function

::::{tab-set}
:::{tab-item} GPU
:sync: GPU
When you set `use_gpu=True`, Ray Train automatically sets up environment variables, such as `CUDA_VISIBLE_DEVICES`, in your training function so that your code can detect and use the GPUs.

Get the associated device with {meth}`ray.train.torch.get_device`.

```{testcode}
import torch
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer, get_device


def train_func():
    assert torch.cuda.is_available()

    device = get_device()
    assert device == torch.device("cuda:0")

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=1,
        use_gpu=True
    )
)
trainer.fit()
```
:::

:::{tab-item} TPU
:sync: TPU
When you set `use_tpu=True`, Ray Train configures the distributed environment for TPU execution on each worker using a topology-aware {class}`~ray.util.tpu.SlicePlacementGroup`.

**JAX (`JaxTrainer`):**

With {class}`~ray.train.v2.jax.JaxTrainer`, Ray Train runs one worker per TPU host and initializes the JAX distributed runtime across the slice:

```{testcode}
:skipif: True

import ray.train
from ray.train import ScalingConfig
from ray.train.v2.jax import JaxTrainer


def train_func():
    import jax
    devices = jax.devices()
    ray.train.report({"num_devices": len(devices)})

trainer = JaxTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=4,
        use_tpu=True,
        topology="4x4",
        accelerator_type="TPU-V6E",
    )
)
trainer.fit()
```

**PyTorch with TorchTPU (`TorchTrainer`):**

With {class}`~ray.train.torch.TorchTrainer` and `use_tpu=True`, set `resources_per_worker={"TPU": 1}` to launch one worker per logical TPU device. Ray Train initializes the `tpu_dist` backend (TorchTPU), sets standard `torch.distributed` and multi-slice `MEGASCALE_*` environment variables, and binds each worker to `torch.device("tpu")` via {meth}`ray.train.torch.get_device`. Standard PyTorch APIs such as `DistributedDataParallel`, FSDP2 (`fully_shard`), and `DTensor` work without code changes:

```{testcode}
:skipif: True

import torch
import torch.distributed as dist
import ray.train
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer, get_device


def train_func():
    device = get_device()
    assert device == torch.device("tpu")
    x = torch.ones(1, device=device)
    dist.all_reduce(x, op=dist.ReduceOp.SUM)
    ray.train.report({"all_reduce_sum": x.item()})

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=16,
        use_tpu=True,
        topology="4x4",
        accelerator_type="TPU-V6E",
        resources_per_worker={"TPU": 1},
    ),
)
trainer.fit()
```
:::
::::


(assigning-multiple-accelerators-to-a-worker)=

### Assign multiple accelerators to a worker

::::{tab-set}
:::{tab-item} GPU
:sync: GPU
To allocate multiple GPUs to each worker, set `resources_per_worker` in the `ScalingConfig`. For example, `resources_per_worker={"GPU": 2}` assigns 2 GPUs to each worker.

Get a list of the associated devices with {meth}`ray.train.torch.get_devices`.

```{testcode}
import torch
from ray.train import ScalingConfig
from ray.train.torch import TorchTrainer, get_device, get_devices


def train_func():
    assert torch.cuda.is_available()

    device = get_device()
    devices = get_devices()
    assert device == torch.device("cuda:0")
    assert devices == [torch.device("cuda:0"), torch.device("cuda:1")]

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=1,
        use_gpu=True,
        resources_per_worker={"GPU": 2}
    )
)
trainer.fit()
```
:::

:::{tab-item} TPU
:sync: TPU
Each TPU host has multiple TPU chips (for example, four chips on a TPU v6e `4x4` or TPU7x `2x2x2` host). By default, when you specify `topology` and `accelerator_type`, Ray Train auto-detects the full-host `resources_per_worker` (`{"TPU": 4}`) so one worker process manages the entire host.

To launch one worker process per logical TPU device (such as with TorchTPU and {class}`~ray.train.torch.TorchTrainer`), set `resources_per_worker={"TPU": 1}`. On TPU7x, where each dual-chiplet physical chip exposes two logical TPU devices (`8` devices per four-chip host), also set `RAY_TPU_RESOURCE_PER_CHIP=2` and advertise `{"TPU": 8}` on the cluster nodes:

```{testcode}
:skipif: True

from ray.train import ScalingConfig

# 1 logical TPU device per worker (16 workers across a 16-device TPU v6e 4x4 or TPU7x 2x2x2 slice)
scaling_config = ScalingConfig(
    num_workers=16,
    use_tpu=True,
    topology="4x4",
    accelerator_type="TPU-V6E",
    resources_per_worker={"TPU": 1},
)
```
:::
::::


(setting-the-accelerator-type)=

### Set the accelerator type
Specify the accelerator type for each worker when you want to train on a specific accelerator. In a heterogeneous Ray cluster, your training workers then must run on the specified accelerator type rather than on any arbitrary accelerator node. For the supported `accelerator_type` values, see {ref}`the available accelerator types <accelerator_types>`.

:::::{tab-set}
::::{tab-item} GPU
:sync: GPU
The following example specifies `accelerator_type="A100"` to assign each worker an NVIDIA A100 GPU.

:::{tip}
Make sure that your cluster has instances with the specified accelerator type or can autoscale to fulfill the request.
:::

```{testcode}
ScalingConfig(
    num_workers=1,
    use_gpu=True,
    accelerator_type="A100"
)
```
::::

:::{tab-item} TPU
:sync: TPU
For TPUs, `accelerator_type` specifies the TPU generation, such as `TPU-V6E` (Trillium) or `TPU-V7X` (Ironwood). For the full list of supported values, see {ref}`the available accelerator types <accelerator_types>`.

```{testcode}
:skipif: True

ScalingConfig(
    num_workers=4,
    use_tpu=True,
    topology="4x4",
    accelerator_type="TPU-V6E",
)
```
:::
:::::


(pytorch-setting-the-communication-backend)=

### PyTorch: Set the communication backend

PyTorch Distributed supports multiple [backends](https://docs.pytorch.org/docs/stable/distributed.html#backends) for communicating tensors across workers. By default, Ray Train uses NCCL when `use_gpu=True`, `tpu_dist` (TorchTPU) when `use_tpu=True`, and Gloo otherwise.

To override the default, configure a {class}`~ray.train.torch.TorchConfig` and pass it to the {class}`~ray.train.torch.TorchTrainer`.

```{testcode}
:hide:

num_training_workers = 1
```

```{testcode}
from ray.train.torch import TorchConfig, TorchTrainer

trainer = TorchTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=num_training_workers,
        use_gpu=True, # Defaults to NCCL
    ),
    torch_config=TorchConfig(backend="gloo"),
)
```

(nccl-setting-the-communication-network-interface)=

### NCCL: Set the communication network interface

When you use NCCL for distributed training, you can configure which network interface cards the GPUs use to communicate by setting the [`NCCL_SOCKET_IFNAME`](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/env.html#nccl-socket-ifname) environment variable.

To set the environment variable on all training workers, pass it in a {ref}`Ray runtime environment <runtime-environments>`:

```{testcode}
:skipif: True

import ray

runtime_env = {"env_vars": {"NCCL_SOCKET_IFNAME": "ens5"}}
ray.init(runtime_env=runtime_env)

trainer = TorchTrainer(...)
```

(setting-the-resources-per-worker)=

## Set the resources per worker
To allocate more than one CPU or accelerator per training worker, or to use {ref}`custom cluster resources <cluster-resources>` that you defined, set the `resources_per_worker` attribute:

```{testcode}
from ray.train import ScalingConfig

scaling_config = ScalingConfig(
    num_workers=8,
    resources_per_worker={
        "CPU": 4,
        "GPU": 2,
    },
    use_gpu=True,
)
```


:::{note}
If you specify GPUs in `resources_per_worker`, you also need to set `use_gpu=True`.
:::

You can also assign fractional GPUs to each worker. In that case, multiple workers share the same CUDA device.

```{testcode}
from ray.train import ScalingConfig

scaling_config = ScalingConfig(
    num_workers=8,
    resources_per_worker={
        "CPU": 4,
        "GPU": 0.5,
    },
    use_gpu=True,
)
```


(deprecated-trainer-resources)=

## Deprecated: Set trainer resources

:::{important}
This API is deprecated. For details, see the [migration guide](https://github.com/ray-project/ray/issues/49454).
:::


The preceding sections configure resources for each training worker. Each training worker is a {ref}`Ray actor <actor-guide>`. Ray Train also schedules an actor for the trainer object when you call `trainer.fit()`.

This object often manages only lightweight communication between the training workers. By default, a trainer uses 1 CPU. On a cluster with 8 CPUs, you can't start 4 training workers at 2 CPUs each, because the run requires 4 * 2 + 1 = 9 CPUs. In that case, set the trainer resources to 0 CPUs:

```{testcode}
from ray.train import ScalingConfig

scaling_config = ScalingConfig(
    num_workers=4,
    resources_per_worker={
        "CPU": 2,
    },
    trainer_resources={
        "CPU": 0,
    }
)
```
