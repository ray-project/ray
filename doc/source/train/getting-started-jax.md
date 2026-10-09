---
myst:
  html_meta:
    description: "Distribute JAX training across GPUs and TPUs with JaxTrainer: SPMD execution, ScalingConfig topology for TPU slices, and CUDA setup."
---

(train-jax)=

# Get started with distributed training using JAX

This guide introduces the {class}`~ray.train.v2.jax.JaxTrainer` in Ray Train.

(what-is-jax)=

## What's JAX?

[JAX](https://github.com/jax-ml/jax) is a Python library for accelerator-oriented array computation and program transformation, designed for high-performance numerical computing and large-scale machine learning.

JAX provides an extensible system of transformations for numerical functions, such as `jax.grad`, `jax.jit`, and `jax.vmap`. It uses the Accelerated Linear Algebra (XLA) compiler to create optimized code that scales efficiently on accelerators such as GPUs and TPUs. You can combine these transformations to build complex, high-performance numerical programs for distributed execution.

JAX and the {class}`~ray.train.v2.jax.JaxTrainer` support accelerators such as GPUs and TPUs. For details, see [Supported platforms](https://docs.jax.dev/en/latest/installation.html#supported-platforms) in the JAX documentation.


## What are TPUs?

Tensor Processing Units (TPUs) are custom accelerators that Google designed for machine learning workloads. Unlike general-purpose CPUs or parallel-processing GPUs, TPUs specialize in the large matrix and tensor computations of deep learning, which makes them efficient for that work.

The primary advantage of TPUs is performance at scale. Google designed them to connect into large, multi-host configurations called "Pod slices" through a high-speed inter-chip interconnect (ICI). This design makes TPUs well suited to training large models that don't fit on a single node.

To learn more about configuring TPUs with KubeRay, see {ref}`kuberay-tpu`.

(jaxtrainer-api)=

## How does `JaxTrainer` work?

The {class}`~ray.train.v2.jax.JaxTrainer` is the core component for orchestrating distributed JAX training in Ray Train. It follows the single-program, multiple-data (SPMD) paradigm, where multiple workers run your training code simultaneously.

For TPUs, each worker runs on a separate TPU virtual machine within a TPU slice. Ray Train automatically reserves TPU slices atomically.

For GPUs, Ray automatically sets up the JAX distributed system on CUDA devices.

To create a `JaxTrainer`, pass it your training function as `train_loop_per_worker` and a `ScalingConfig` that specifies the distributed hardware layout. The `JaxTrainer` supports both Google Cloud TPUs and NVIDIA GPUs.

## Configure scale and accelerators

### TPU scaling configuration

For TPU training, use {class}`~ray.train.ScalingConfig` to define your TPU slice configuration. Key fields include the following:

* {class}`use_tpu <ray.train.ScalingConfig>`: A boolean flag that tells Ray Train to initialize the JAX backend for TPU execution. Ray 2.49.0 added this field to the V2 `ScalingConfig`.
* {class}`topology <ray.train.ScalingConfig>`: A string that defines the physical arrangement of the TPU chips, such as `"4x4"`. Multi-host training requires it, and it ensures Ray places workers correctly across the slice. Ray 2.49.0 added this field to the V2 `ScalingConfig`. For a list of supported TPU topologies by generation, see the [GKE documentation](https://cloud.google.com/kubernetes-engine/docs/concepts/plan-tpus#topology).
* {class}`num_workers <ray.train.ScalingConfig>`: Set this to the total number of TPU VMs across all slices. For example, one v4-32 slice with a 2x2x4 topology uses 4 VMs, so set `num_workers` to 4. If you use two v4-32 slices, set `num_workers` to 8.
* {class}`resources_per_worker <ray.train.ScalingConfig>`: A dictionary specifying the resources each worker needs. For TPUs, you typically request the number of chips per VM, such as `{"TPU": 4}`.
* {class}`accelerator_type <ray.train.ScalingConfig>`: For TPUs, `accelerator_type` specifies the TPU generation you're using, such as `"TPU-V6E"`, so that Ray schedules your workload on the desired TPU slice.

```{testcode}
:skipif: True

from ray.train import ScalingConfig
tpu_scaling_config = ScalingConfig(num_workers=4, use_tpu=True, topology="4x4", accelerator_type="TPU-V6E")
```

### GPU scaling configuration

For GPU training, use {class}`~ray.train.ScalingConfig` to define your GPU configuration. Each worker is one Ray Train process. When you set `use_gpu=True` without `resources_per_worker`, Ray Train requests one GPU per worker. Key fields include the following:

* {class}`num_workers <ray.train.ScalingConfig>`: The number of distributed training worker processes.
* {class}`use_gpu <ray.train.ScalingConfig>`: Whether each worker uses a GPU.
* {class}`resources_per_worker <ray.train.ScalingConfig>`: A dictionary specifying the resources each worker needs.


```{testcode}
from ray.train import ScalingConfig
gpu_scaling_config = ScalingConfig(num_workers=4, use_gpu=True)
```


For details, see {ref}`train_scaling_config`.

## Quickstart

The following example shows the final code:

```{testcode}
:skipif: True

from ray.train.v2.jax import JaxTrainer
from ray.train import ScalingConfig

def train_func():
    # Your JAX training code here.

# Define the TPU scaling configuration with `use_tpu=True`.
scaling_config = ScalingConfig(num_workers=4, use_tpu=True, topology="4x4", accelerator_type="TPU-V6E")
# Define the GPU scaling configuration with `use_gpu=True`.
# scaling_config = ScalingConfig(num_workers=4, use_gpu=True)

# Choose one scaling config.
trainer = JaxTrainer(train_func, scaling_config=scaling_config)
result = trainer.fit()
```

- `train_func` is the training function, the Python code that runs on each distributed training worker.
- {class}`~ray.train.ScalingConfig` defines the number of distributed training workers and whether to use TPUs or GPUs.
- {class}`~ray.train.v2.jax.JaxTrainer` launches the distributed training job.

Compare a JAX training script with and without Ray Train.

::::{tab-set}
:::{tab-item} JAX with Ray Train
```{testcode}
:skipif: True

import jax
import jax.numpy as jnp
import optax
import ray.train

from ray.train.v2.jax import JaxTrainer
from ray.train import ScalingConfig

def train_func():
    """This function is run on each distributed worker."""
    key = jax.random.PRNGKey(jax.process_index())
    X = jax.random.normal(key, (100, 1))
    noise = jax.random.normal(key, (100, 1)) * 0.1
    y = 2 * X + 1 + noise

    def linear_model(params, x):
        return x @ params['w'] + params['b']

    def loss_fn(params, x, y):
        preds = linear_model(params, x)
        return jnp.mean((preds - y) ** 2)

    @jax.jit
    def train_step(params, opt_state, x, y):
        loss, grads = jax.value_and_grad(loss_fn)(params, x, y)
        updates, opt_state = optimizer.update(grads, opt_state)
        params = optax.apply_updates(params, updates)
        return params, opt_state, loss

    # Initialize parameters and optimizer.
    key, w_key, b_key = jax.random.split(key, 3)
    params = {'w': jax.random.normal(w_key, (1, 1)), 'b': jax.random.normal(b_key, (1,))}
    optimizer = optax.adam(learning_rate=0.01)
    opt_state = optimizer.init(params)

    # Training loop
    epochs = 100
    for epoch in range(epochs):
        params, opt_state, loss = train_step(params, opt_state, X, y)
        # Report metrics back to Ray Train.
        ray.train.report({"loss": float(loss), "epoch": epoch})

# Define the TPU scaling configuration for your distributed job.
scaling_config = ScalingConfig(
    num_workers=4,
    use_tpu=True,
    topology="4x4",
    accelerator_type="TPU-V6E",
    placement_strategy="SPREAD"
)

# Define the GPU scaling configuration with `use_gpu=True`.
# scaling_config = ScalingConfig(
#     num_workers=4,
#     use_gpu=True,
# )

# Define and run the JaxTrainer.
trainer = JaxTrainer(
    train_loop_per_worker=train_func,
    scaling_config=scaling_config,
)
result = trainer.fit()
print(f"Training finished. Final loss: {result.metrics['loss']:.4f}")
```
:::

:::{tab-item} JAX
<!-- This snippet isn't tested because it doesn't use any Ray code. -->

```{testcode}
:skipif: True

import jax
import jax.numpy as jnp
import optax

# In a non-Ray script, you would manually initialize the
# distributed environment for multi-host training.
# import jax.distributed
# jax.distributed.initialize()

# Generate synthetic data.
key = jax.random.PRNGKey(0)
X = jax.random.normal(key, (100, 1))
noise = jax.random.normal(key, (100, 1)) * 0.1
y = 2 * X + 1 + noise

# Model and loss function are standard JAX.
def linear_model(params, x):
    return x @ params['w'] + params['b']

def loss_fn(params, x, y):
    preds = linear_model(params, x)
    return jnp.mean((preds - y) ** 2)

@jax.jit
def train_step(params, opt_state, x, y):
    loss, grads = jax.value_and_grad(loss_fn)(params, x, y)
    updates, opt_state = optimizer.update(grads, opt_state)
    params = optax.apply_updates(params, updates)
    return params, opt_state, loss

# Initialize parameters and optimizer.
key, w_key, b_key = jax.random.split(key, 3)
params = {'w': jax.random.normal(w_key, (1, 1)), 'b': jax.random.normal(b_key, (1,))}
optimizer = optax.adam(learning_rate=0.01)
opt_state = optimizer.init(params)

# Training loop
epochs = 100
print("Starting training...")
for epoch in range(epochs):
    params, opt_state, loss = train_step(params, opt_state, X, y)
    if epoch % 10 == 0:
        print(f"Epoch {epoch}, Loss: {loss:.4f}")

print("Training finished.")
print(f"Learned parameters: w={params['w'].item():.4f}, b={params['b'].item():.4f}")
```
:::
::::

## Set up a training function

Ray Train automatically initializes the JAX distributed environment based on the `ScalingConfig` and the `JAX_PLATFORMS` environment variable. To adapt your existing JAX code, wrap your training logic in a Python function that you pass to the `JaxTrainer`.

This function is the entry point that Ray runs on each remote worker.

```diff
+from ray.train.v2.jax import JaxTrainer
+from ray.train import ScalingConfig, report

-def main_logic()
+def train_func():
    """This function is run on each distributed worker."""
    # ... (JAX model, data, and training step definitions) ...

    # Training loop
    for epoch in range(epochs):
        params, opt_state, loss = train_step(params, opt_state, X, y)
-       print(f"Epoch {epoch}, Loss: {loss:.4f}")
+       # In Ray Train, you can report metrics back to the trainer
+       report({"loss": float(loss), "epoch": epoch})

-if __name__ == "__main__":
-    main_logic()
+# Define the hardware configuration for your distributed job.
+scaling_config = ScalingConfig(
+    num_workers=4,
+    use_tpu=True,
+    topology="4x4",
+    accelerator_type="TPU-V6E",
+    placement_strategy="SPREAD"
+)
+
+# Define the GPU scaling configuration with `use_gpu=True`.
+# scaling_config = ScalingConfig(
+#     num_workers=4,
+#     use_gpu=True,
+#     resources_per_worker={"GPU": 1},
+# )
+
+# Define and run the JaxTrainer, which executes `train_func`.
+trainer = JaxTrainer(
+    train_loop_per_worker=train_func,
+    scaling_config=scaling_config
+)
+result = trainer.fit()
```

## Configure persistent storage

Create a {class}`~ray.train.RunConfig` object to specify the path where Ray Train saves results, including checkpoints and artifacts.

```{testcode}
from ray.train import RunConfig

# Local path (/some/local/path/unique_run_name)
run_config = RunConfig(storage_path="/some/local/path", name="unique_run_name")

# Shared cloud storage URI (s3://bucket/unique_run_name)
run_config = RunConfig(storage_path="s3://bucket", name="unique_run_name")

# Shared NFS path (/mnt/nfs/unique_run_name)
run_config = RunConfig(storage_path="/mnt/nfs", name="unique_run_name")
```


:::{warning}
A *shared storage location*, such as cloud storage or NFS, is *optional* for single-node clusters but **required for multi-node clusters**. On multi-node clusters, a local path {ref}`raises an error <multinode-local-storage-warning>` during checkpointing.
:::


For details, see {ref}`persistent-storage-guide`.

## Launch a training job

To launch a distributed training job, pass the training function, scaling configuration, and run configuration to a {class}`~ray.train.v2.jax.JaxTrainer`, then call `fit()`.

```{testcode}
:skipif: True

from ray.train import ScalingConfig

train_func = lambda: None
# Define the TPU scaling configuration with `use_tpu=True`.
scaling_config = ScalingConfig(num_workers=4, use_tpu=True, topology="4x4", accelerator_type="TPU-V6E")
# Define the GPU scaling configuration with `use_gpu=True`.
# scaling_config = ScalingConfig(num_workers=4, use_gpu=True)
run_config = None
```

```{testcode}
:skipif: True

from ray.train.v2.jax import JaxTrainer

trainer = JaxTrainer(
    train_func, scaling_config=scaling_config, run_config=run_config
)
result = trainer.fit()
```

## Access training results

When training completes, `fit()` returns a {class}`~ray.train.Result` object that contains information about the training run, including the metrics and checkpoints reported during training.

```{testcode}
:skipif: True

result.metrics     # The metrics reported during training.
result.checkpoint  # The latest checkpoint reported during training.
result.path        # The path where logs are stored.
result.error       # The exception that was raised, if training failed.
```

For more usage examples, see {ref}`train-inspect-results`.

## Next steps

After you convert your JAX training script to use Ray Train, explore the following resources:

* See {ref}`user guides <train-user-guides>` to learn how to perform specific tasks.
* Browse the {doc}`examples <examples>` for end-to-end Ray Train examples.
* See the {ref}`API reference <train-api>` for details on the classes and methods in this guide.
