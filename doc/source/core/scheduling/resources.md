---
myst:
  html_meta:
    description: "Ray physical and logical resources: define custom resources, set node capacity, and request fractional resources per task or actor."
---

(core-resources)=

# Resources

With Ray, you can scale your applications from a laptop to a cluster without changing your code. *Ray resources* are key to this capability. They abstract away physical machines, so you express your computation in terms of resources while Ray manages scheduling and autoscaling based on resource requests.

A resource in Ray is a key-value pair where the key is a resource name and the value is a float quantity. For convenience, Ray natively supports the CPU, GPU, and memory resource types, which Ray calls *pre-defined resources*. Ray also supports {ref}`custom resources <custom-resources>`.

(logical-resources)=

## Physical resources and logical resources

Physical resources are the resources a machine physically has, such as physical CPUs and GPUs. Logical resources are virtual resources that a system defines.

Ray resources are *logical* and don't need a one-to-one mapping with physical resources. For example, you can start a Ray head node with zero logical CPUs by running `ray start --head --num-cpus=0`, even if the machine physically has eight. This setting signals the Ray scheduler not to schedule any tasks or actors that require logical CPU resources on the head node, mainly to reserve the head node for running Ray system processes. Ray mainly uses logical resources for admission control during scheduling.

Because resources are logical, the following implications apply:

- Resource requirements of tasks or actors don't limit actual physical resource usage. For example, Ray doesn't prevent a `num_cpus=1` task from launching multiple threads and using multiple physical CPUs. You're responsible for making sure tasks or actors use no more resources than their resource requirements specify.
- Ray doesn't provide CPU isolation for tasks or actors. For example, Ray doesn't reserve a physical CPU exclusively and pin a `num_cpus=1` task to it. Instead, Ray leaves scheduling and running the task to the operating system. If needed, use operating system APIs such as `sched_setaffinity` to pin a task to a physical CPU.
- Ray does provide {ref}`GPU <gpu-support>` isolation in the form of *visible devices*. It automatically sets the `CUDA_VISIBLE_DEVICES` environment variable, which most machine learning frameworks respect for GPU assignment.

(omp-num-thread-note)=

:::{note}
If you set `num_cpus` on a task or actor through {func}`ray.remote() <ray.remote>` and {meth}`task.options() <ray.remote_function.RemoteFunction.options>` or {meth}`actor.options() <ray.actor.ActorClass.options>`, Ray sets the environment variable `OMP_NUM_THREADS=<num_cpus>`. If you don't specify `num_cpus`, Ray sets `OMP_NUM_THREADS=1` to avoid performance degradation with many workers. For background, see issue #6998. To override anything Ray sets by default, set `OMP_NUM_THREADS` explicitly. NumPy, PyTorch, and TensorFlow commonly use `OMP_NUM_THREADS` to perform multi-threaded linear algebra. In a multi-worker setting, you want one thread per worker instead of many threads per worker to avoid contention. Some other libraries might have their own way to configure parallelism. For example, if you're using OpenCV, set the number of threads manually with `cv2.setNumThreads(num_threads)`. To disable multi-threading, set the number of threads to `0`.
:::

```{figure} ../images/physical_resources_vs_logical_resources.svg
Physical resources versus logical resources
```

(custom-resources)=

## Custom resources

You can specify custom resources for a Ray node and reference them to control scheduling for your tasks or actors.

Use custom resources when you need to manage scheduling with numeric values. For simple label-based scheduling, use labels instead. See {doc}`labels`.

(specify-node-resources)=

## Specifying node resources

By default, Ray nodes start with pre-defined CPU, GPU, and memory resources. Ray sets the quantities of these logical resources on each node to the physical quantities it detects automatically. By default, Ray configures these resources with the following rules:

- **Number of logical CPUs**: Ray sets `num_cpus` to the number of CPUs of the machine or container.
- **Number of logical GPUs**: Ray sets `num_gpus` to the number of GPUs of the machine or container.
- **Memory**: Ray sets `memory` to 70% of "available memory" when the Ray runtime starts.
- **Object store memory**: Ray sets `object_store_memory` to 30% of "available memory" when the Ray runtime starts. Object store memory isn't a logical resource, and you can't use it for scheduling.

:::{warning}
You cannot dynamically update the resource capacities of a node after Ray starts on that node.
:::

You can override these defaults by manually specifying the quantities of pre-defined resources and adding custom resources. How you do that depends on how you start the Ray cluster:

::::{tab-set}
:::{tab-item} ray.init()
If you use {func}`ray.init() <ray.init>` to start a single-node Ray cluster, specify node resources manually as follows:

```{literalinclude} ../doc_code/resources.py
:language: python
:start-after: __specifying_node_resources_start__
:end-before: __specifying_node_resources_end__
```
:::

:::{tab-item} ray start
If you use {ref}`ray start <ray-start-doc>` to start a Ray node, run the following command:

```shell
ray start --head --num-cpus=3 --num-gpus=4 --resources='{"special_hardware": 1, "custom_label": 1}'
```
:::

:::{tab-item} ray up
If you use {ref}`ray up <ray-up-doc>` to start a Ray cluster, set the {ref}`resources field <cluster-configuration-resources-type>` in the YAML file:

```yaml
available_node_types:
  head:
    ...
    resources:
      CPU: 3
      GPU: 4
      special_hardware: 1
      custom_label: 1
```
:::

:::{tab-item} KubeRay
If you use {ref}`KubeRay <kuberay-index>` to start a Ray cluster, set the {ref}`rayStartParams field <rayStartParams>` in the YAML file:

```yaml
headGroupSpec:
  rayStartParams:
    num-cpus: "3"
    num-gpus: "4"
    resources: '"{\"special_hardware\": 1, \"custom_label\": 1}"'
```
:::
::::


(resource-requirements)=

## Specifying task or actor resource requirements

You can specify the logical resource requirements of a task or actor, such as CPU, GPU, and custom resources. A task or actor runs on a node only if the node has enough of the required logical resources available to execute it.

By default, Ray tasks use one logical CPU resource, and Ray actors use one logical CPU for scheduling and zero logical CPUs for running. As a result, by default, actors can't get scheduled on a zero-CPU node, but an infinite number of them can run on any non-zero-CPU node. The default resource requirements for actors exist for historical reasons. To avoid surprises, always set `num_cpus` explicitly for actors. If you specify resources explicitly, Ray requires them both at schedule time and at execution time.

You can also explicitly specify a task's or actor's logical resource requirements (for example, one task may require a GPU) instead of using default ones via {func}`ray.remote() <ray.remote>` and {meth}`task.options() <ray.remote_function.RemoteFunction.options>`/{meth}`actor.options() <ray.actor.ActorClass.options>`.

::::{tab-set}
:::{tab-item} Python
```{literalinclude} ../doc_code/resources.py
:language: python
:start-after: __specifying_resource_requirements_start__
:end-before: __specifying_resource_requirements_end__
```
:::

:::{tab-item} Java
```java
// Specify required resources.
Ray.task(MyRayApp::myFunction).setResource("CPU", 1.0).setResource("GPU", 1.0).setResource("special_hardware", 1.0).remote();

Ray.actor(Counter::new).setResource("CPU", 2.0).setResource("GPU", 1.0).remote();
```
:::

:::{tab-item} C++
```c++
// Specify required resources.
ray::Task(MyFunction).SetResource("CPU", 1.0).SetResource("GPU", 1.0).SetResource("special_hardware", 1.0).Remote();

ray::Actor(CreateCounter).SetResource("CPU", 2.0).SetResource("GPU", 1.0).Remote();
```
:::
::::

Task and actor resource requirements affect Ray's scheduling concurrency. The sum of the logical resource requirements of all concurrently executing tasks and actors on a node can't exceed the node's total logical resources. You can use this property to {ref}`limit the number of concurrently running tasks or actors to avoid issues such as OOM <core-patterns-limit-running-tasks>`.

(fractional-resource-requirements)=

### Fractional resource requirements

Ray supports fractional resource requirements. For example, if your task or actor is I/O-bound and has low CPU usage, you can specify a fractional CPU with `num_cpus=0.5` or even zero CPUs with `num_cpus=0`. The precision of a fractional resource requirement is 0.0001, so avoid specifying a double beyond that precision.

```{literalinclude} ../doc_code/resources.py
:language: python
:start-after: __specifying_fractional_resource_requirements_start__
:end-before: __specifying_fractional_resource_requirements_end__
```

:::{note}
GPU, TPU, and `neuron_cores` resource requirements greater than 1 must be whole numbers. For example, `num_gpus=1.5` is invalid.
:::

:::{tip}
Besides resource requirements, you can specify a runtime environment for a task or actor to run in. A runtime environment can include Python packages, local files, environment variables, and more. See {ref}`Runtime environments <runtime-environments>`.
:::
