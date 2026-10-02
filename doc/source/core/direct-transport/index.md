---
myst:
  html_meta:
    description: "Ray Direct Transport moves tensors between actors without going through the object store, using collective groups over Gloo or NCCL."
---

(direct-transport)=


# Ray Direct Transport (RDT)

Ray normally stores objects in its CPU-based object store, then copies and deserializes them when a Ray task or actor accesses them. For GPU data, this can cause unnecessary and expensive data transfers. For example, passing a CUDA `torch.Tensor` from one Ray task to another requires a copy from GPU to CPU memory, then back to GPU memory.

With *Ray Direct Transport (RDT)*, Ray stores objects and passes them directly between Ray actors. RDT augments the familiar Ray {class}`ObjectRef <ray.ObjectRef>` API in the following ways:

- Keeping GPU data in GPU memory until a transfer is necessary.
- Avoiding expensive serialization and copies to and from the Ray object store.
- Using efficient data transports, such as the [Gloo](https://github.com/pytorch/gloo) or [NCCL](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/index.html) collective communication libraries or point-to-point remote direct memory access (RDMA) through [NVIDIA's NIXL](https://github.com/ai-dynamo/nixl), to transfer data directly between devices, including CPUs and GPUs.

:::{note}
RDT is currently in alpha and doesn't support all Ray Core APIs yet. Future releases might introduce breaking API changes. For more details, see the {ref}`limitations <limitations>` section.
:::

## Getting started

:::{tip}
RDT currently supports `torch.Tensor` objects created by Ray actor tasks. Future releases might add support for other data types and for Ray non-actor tasks.
:::

This walkthrough shows how to create and use RDT with different *tensor transports*, which are the mechanisms that transfer tensors between actors. RDT currently supports the following tensor transports:

1. [Gloo](https://github.com/pytorch/gloo): A collective communication library for PyTorch and CPUs.
1. [NVIDIA NCCL](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/index.html): A collective communication library for NVIDIA GPUs.
1. [NVIDIA NIXL](https://github.com/ai-dynamo/nixl): A library for accelerating point-to-point transfers through RDMA, especially between various types of memory and NVIDIA GPUs. NIXL runs on [libfabric](https://ofiwg.github.io/libfabric/) on AWS instances with [Elastic Fabric Adapter (EFA)](https://aws.amazon.com/hpc/efa/), and on [Unified Communication X (UCX)](https://github.com/openucx/ucx) everywhere else.

To keep the walkthrough easy to follow, start with the [Gloo](https://github.com/pytorch/gloo) transport, which works without any physical GPUs.

(direct-transport-gloo)=

### Usage with Gloo (CPUs only)

#### Installation

:::{note}
Under construction.
:::

#### Walkthrough

To get started, define an actor class and a task that returns a `torch.Tensor`:

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __normal_example_start__
:end-before: __normal_example_end__
```

As written, when the actor task returns the `torch.Tensor`, Ray copies it into its CPU-based object store. For CPU-based tensors, this can require an expensive step to copy and serialize the object. GPU-based tensors also require a copy to and from CPU memory.

To enable RDT, use the `tensor_transport` option in the {func}`@ray.method <ray.method>` decorator.

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __gloo_example_start__
:end-before: __gloo_example_end__
```

You can add this decorator to any actor task that returns a `torch.Tensor`, or that returns `torch.Tensors` nested inside other Python objects. Adding this decorator changes Ray's behavior in the following ways:

1. When returning the tensor, Ray stores a *reference* to the tensor instead of copying it to CPU memory.
1. When you pass the {class}`ray.ObjectRef` to another task, Ray uses Gloo to transfer the tensor to the destination task.

For the second behavior to work, you only need to add the {func}`@ray.method(tensor_transport) <ray.method>` decorator to the actor task that *returns* the tensor. Don't add it to actor tasks that *consume* the tensor, unless those tasks also return tensors.

The second behavior also requires that you first create a *collective group* of actors.

#### Creating a collective group

To create a collective group for use with RDT, do the following:

1. Create multiple Ray actors.
1. Create a collective group on the actors with the {func}`ray.experimental.collective.create_collective_group <ray.experimental.collective.create_collective_group>` function. The `backend` you specify must match the `tensor_transport` in the {func}`@ray.method <ray.method>` decorator.

The following example shows both steps:

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __gloo_group_start__
:end-before: __gloo_group_end__
```

The actors can now communicate directly through Gloo. To destroy the group, use the {func}`ray.experimental.collective.destroy_collective_group <ray.experimental.collective.destroy_collective_group>` function. After you call this function, you can create a new collective group on the same actors.

#### Passing objects to other actors

With a collective group in place, you can create and pass RDT objects between the actors. The following example shows the full flow:

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __gloo_full_example_start__
:end-before: __gloo_full_example_end__
```

When you pass the {class}`ray.ObjectRef` to another task, Ray uses Gloo to transfer the tensor directly from the source actor to the destination actor instead of through the default object store. The {func}`@ray.method(tensor_transport) <ray.method>` decorator is only on the actor task that *returns* the tensor. Once you add this hint, the receiving actor task `receiver.sum` automatically uses Gloo to receive the tensor. In this example, because `MyActor.sum` doesn't have the {func}`@ray.method(tensor_transport) <ray.method>` decorator, it uses the default Ray object store transport to return `torch.sum(tensor)`.

RDT also supports passing tensors nested inside Python data structures, and actor tasks that return multiple tensors, as in the following example:

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __gloo_multiple_tensors_example_start__
:end-before: __gloo_multiple_tensors_example_end__
```

#### Passing RDT objects to the actor that produced them

You can also pass RDT {class}`ray.ObjectRefs <ray.ObjectRef>` to the actor that produced them. This avoids any copies and provides a reference to the same `torch.Tensor` that the actor created earlier, as in the following example:

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __gloo_intra_actor_start__
:end-before: __gloo_intra_actor_end__
```


:::{note}
Ray only keeps a reference to the tensor that your code creates, so the tensor objects are *mutable*. If `sender.sum` modified the tensor in the preceding example, `receiver.sum` would also see the changes. This differs from the normal Ray Core API, which always makes an immutable copy of data that actors return.
:::


#### `ray.get`

You can also use the {func}`ray.get <ray.get>` function as usual to retrieve the result of an RDT object. However, by default {func}`ray.get <ray.get>` uses the same tensor transport as the one specified in the {func}`@ray.method <ray.method>` decorator. For collective-based transports, this doesn't work if the caller isn't part of the collective group.

Therefore, specify the Ray object store as the tensor transport explicitly by setting `_use_object_store` in {func}`ray.get <ray.get>`.

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __gloo_get_start__
:end-before: __gloo_get_end__
```

#### Object mutability

Unlike objects in the Ray object store, RDT objects are *mutable*, meaning that Ray only holds a reference to the tensor and doesn't copy it until a transfer is requested. If the actor that returns a tensor also keeps a reference to the tensor, and later modifies it in place while Ray is still storing the tensor reference, receiving actors might see some or all of the changes.

The following example shows what can go wrong:

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __gloo_wait_tensor_freed_bad_start__
:end-before: __gloo_wait_tensor_freed_bad_end__
```

In this example, the sender actor returns a tensor to Ray, but it also keeps a reference to the tensor in its local state. Then, in `sender.increment_and_sum_stored_tensor`, the sender actor modifies the tensor in place while Ray is still holding the tensor reference. Then, the `receiver.increment_and_sum` task receives the modified tensor instead of the original, so the assertion fails.

To fix this kind of error, use the {func}`ray.experimental.wait_tensor_freed <ray.experimental.wait_tensor_freed>` function to wait for Ray to release all references to the tensor, so that the actor can safely write to the tensor again. {func}`wait_tensor_freed <ray.experimental.wait_tensor_freed>` unblocks once all tasks that depend on the tensor have finished executing and all corresponding `ObjectRefs` have gone out of scope. Ray identifies the tasks that depend on the tensor by tracking which tasks take the `ObjectRef` corresponding to the tensor as an argument.

Here's a fixed version of the earlier example.

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __gloo_wait_tensor_freed_start__
:end-before: __gloo_wait_tensor_freed_end__
```

The main changes are the following:

1. `sender` calls {func}`wait_tensor_freed <ray.experimental.wait_tensor_freed>` before modifying the tensor in place.
1. The driver skips {func}`ray.get <ray.get>` because {func}`wait_tensor_freed <ray.experimental.wait_tensor_freed>` blocks until all `ObjectRefs` pointing to the tensor are freed, so calling {func}`ray.get <ray.get>` here would cause a deadlock.
1. The driver calls `del tensor` to release its reference to the tensor. Again, this is necessary because {func}`wait_tensor_freed <ray.experimental.wait_tensor_freed>` blocks until all `ObjectRefs` pointing to the tensor are freed.

When you pass an RDT `ObjectRef` back to the same actor that produced it, Ray passes back a *reference* to the tensor instead of a copy, so the same kind of bug can occur. To help catch such cases, Ray prints a warning if you pass an RDT object to the actor that produced it and a different actor, as in the following example:

```{literalinclude} ../doc_code/direct_transport_gloo.py
:language: python
:start-after: __gloo_object_mutability_warning_start__
:end-before: __gloo_object_mutability_warning_end__
```


### Usage with NCCL (NVIDIA GPUs only)

Switching RDT to a different tensor transport takes only a few lines of code change. The following code is the {ref}`Gloo example <direct-transport-gloo>`, modified to use NVIDIA GPUs and the [NCCL](https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/index.html) library for collective GPU communication.

```{literalinclude} ../doc_code/direct_transport_nccl.py
:language: python
:start-after: __nccl_full_example_start__
:end-before: __nccl_full_example_end__
```

The main code differences are the following:

1. The {func}`@ray.method <ray.method>` uses `tensor_transport="nccl"` instead of `tensor_transport="gloo"`.
1. The code uses the {func}`ray.experimental.collective.create_collective_group <ray.experimental.collective.create_collective_group>` function to create a collective group.
1. The code creates the tensor on the GPU with the `.cuda()` method.

### Usage with NIXL (CPUs or NVIDIA GPUs)

#### Installation
First, install NIXL with `pip install nixl`. For maximum performance, run the [install_gdrcopy.sh](https://github.com/ray-project/ray/blob/master/doc/tools/install_gdrcopy.sh) script, for example `install_gdrcopy.sh "${GDRCOPY_OS_VERSION}" "12.8" "x64"`. For the available OS versions, see the [NVIDIA GDRCopy downloads for CUDA 12.8](https://developer.download.nvidia.com/compute/redist/gdrcopy/CUDA%2012.8/).

When Ray selects the `UCX` backend, as {ref}`NIXL backend selection <nixl-backend-selection>` describes, set the following UCX environment variables, either so that UCX chooses the right transport from all options or to set your preferred transport yourself. These variables apply only to the `UCX` backend and have no effect when Ray runs the `LIBFABRIC` backend.


```bash
# Example UCX configuration, adjust according to your environment
$ export UCX_TLS=all  # or specify specific transports like "rc,ud,sm,^cuda_ipc" ..etc
$ export UCX_NET_DEVICES=all  # or specify network devices like "mlx5_0:1,mlx5_1:1"
```

On the `LIBFABRIC` backend, use libfabric environment variables instead, such as `FI_PROVIDER=efa` to pin the provider and `FI_LOG_LEVEL=Debug` for diagnostics. See the [NIXL libfabric plugin documentation](https://github.com/ai-dynamo/nixl/blob/main/src/plugins/libfabric/README.md) for more configuration and troubleshooting information.

(nixl-backend-selection)=

#### NIXL backend selection

Ray automatically selects the NIXL transport backend based on the available hardware:

- **AWS instances with EFA**: Ray detects EFA devices and validates that a realistic-size CUDA memory registration succeeds before it selects the `LIBFABRIC` backend. If validation fails, GPUDirect is usually misconfigured, for example because `nvidia-peermem` or dmabuf support is missing. To detect EFA both on bare hosts and inside containers, Ray checks for the EFA network device at `/sys/class/net/efa*` and for rdma-verbs devices under `/sys/class/infiniband` that are bound to the kernel `efa` driver. The host network device isn't visible inside a pod, so the verbs check is what makes detection work under Kubernetes. Ordinary InfiniBand or RoCE NICs also expose verbs devices, so Ray confirms the `efa` driver binding to avoid treating them as EFA.
- **All other environments**: Ray uses the `UCX` backend.

This selection requires no configuration. On AWS EFA instances, make sure you've run the [EFA installer](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html), which installs both the EFA driver and libfabric. If libfabric validation fails at startup, see the [NIXL libfabric plugin documentation](https://github.com/ai-dynamo/nixl/blob/main/src/plugins/libfabric/README.md) for troubleshooting.

#### Walkthrough

NIXL can transfer data between different devices, including CPUs and NVIDIA GPUs, but doesn't require you to create a collective group ahead of time. Any actor that has NIXL installed in its environment can create and pass an RDT object.

Otherwise, usage is the same as in the {ref}`Gloo example <direct-transport-gloo>`.

The following example uses NIXL to transfer an RDT object between two actors:

```{literalinclude} ../doc_code/direct_transport_nixl.py
:language: python
:start-after: __nixl_full_example_start__
:end-before: __nixl_full_example_end__
```

Compared to the {ref}`Gloo example <direct-transport-gloo>`, the main code differences are:

1. The {func}`@ray.method <ray.method>` uses `tensor_transport="nixl"` instead of `tensor_transport="gloo"`.
1. No collective group is needed.

#### ray.put and ray.get with NIXL

Unlike with the collective-based tensor transports, Gloo and NCCL, the {func}`ray.get <ray.get>` function can use NIXL to retrieve a copy of the result. By default, {func}`ray.get <ray.get>` uses the tensor transport specified in the {func}`@ray.method <ray.method>` decorator.

```{literalinclude} ../doc_code/direct_transport_nixl.py
:language: python
:start-after: __nixl_get_start__
:end-before: __nixl_get_end__
```

You can also use NIXL to retrieve the result from references created by {func}`ray.put <ray.put>`.

```{literalinclude} ../doc_code/direct_transport_nixl.py
:language: python
:start-after: __nixl_put__and_get_start__
:end-before: __nixl_put__and_get_end__
```


### Summary

With RDT, Ray stores objects and passes them directly between Ray actors, using accelerated transports such as Gloo, NCCL, and NIXL. Keep the following main points in mind:

* If you use a collective-based tensor transport, Gloo or NCCL, you must create a collective group ahead of time. NIXL only requires all involved actors to have NIXL installed.
* Unlike objects in the Ray object store, RDT objects are *mutable*, meaning that Ray only holds a reference to the stored tensors, not a copy.
* Otherwise, you can use actors as normal.

For a full list of limitations, see the {ref}`limitations <limitations>` section.


## Microbenchmarks

:::{note}
Under construction.
:::

(limitations)=

## Limitations

RDT is currently in alpha and has the following limitations, which future releases might address:

* Support for `torch.Tensor` objects only.
* Support for Ray actors only, not Ray tasks.
* Support for the following transports: Gloo, NCCL, and NIXL.
* Support for CPUs and NVIDIA GPUs only.
* RDT objects are *mutable*. Ray only holds a reference to the tensor and doesn't copy it until a transfer is requested. If the application code also keeps a reference to a tensor before returning it and modifies the tensor in place, the receiving actor might see some or all of the changes.
* `await` on an RDT object ref is temporarily unsupported.

The collective-based, two-sided tensor transports, Gloo and NCCL, have the following limitations:

* Only the process that created the collective group can submit actor tasks that return and pass RDT objects. If the creating process passes the actor handles to other processes, those processes can submit actor tasks as usual, but can't use RDT objects.
* Similarly, the process that created the collective group can't serialize and pass RDT {class}`ray.ObjectRefs <ray.ObjectRef>` to other Ray tasks or actors. Instead, you can only pass the {class}`ray.ObjectRef`s as direct arguments to other actor tasks, and those actors must be in the same collective group.
* Each actor can only be in one collective group per tensor transport at a time.
* No support for {func}`ray.put <ray.put>`.
* No support for out-of-order actors such as async actors or actors with `max_concurrency` greater than 1.


Because of a known issue, NIXL doesn't currently support storing different GPU objects at the same actor when the objects contain an overlapping but not equal set of tensors. To support this pattern, make sure the first `ObjectRef` has gone out of scope before you store the same tensors again in a second object.

```{literalinclude} ../doc_code/direct_transport_nixl.py
:language: python
:start-after: __nixl_limitations_start__
:end-before: __nixl_limitations_end__
```

## Error handling

* Application-level errors, which are exceptions that your code raises, don't destroy the collective group. Instead, Ray propagates them to any dependent tasks, as for non-RDT Ray objects.

* If a system-level error occurs during a Gloo or NCCL collective operation, the collective group is destroyed and the actors are killed to prevent any hanging.

* If a system-level error occurs during a NIXL transfer, Ray or NIXL stops the transfer with an exception, and Ray raises the exception in the dependent task or in the `ray.get` call on the NIXL object ref.

* System-level errors include the following:
  : * Errors internal to the third-party transport, for example NCCL network errors.
    * Actor or node failures.
    * Transport errors from tensor device and transport mismatches, for example a CPU tensor when using NCCL.
    * Ray RDT object fetch timeouts. To override the timeout, set the `RAY_rdt_fetch_fail_timeout_milliseconds` environment variable.
    * Any unexpected system bugs.


## Advanced: Registering a custom tensor transport

You can register custom tensor transports at runtime for use with RDT. To add a custom tensor transport, implement the abstract interface {class}`ray.experimental.TensorTransportManager <ray.experimental.TensorTransportManager>` and register it with {func}`ray.experimental.register_tensor_transport <ray.experimental.register_tensor_transport>`.

For a complete guide on implementing custom tensor transports, including detailed documentation of all required methods, see {ref}`custom-tensor-transport`.


## Advanced: RDT internals

:::{note}
Under construction.
:::

### Table of contents

For more details about Ray Direct Transport, see the following pages:

```{toctree}
:maxdepth: 1

custom-tensor-transport
```
