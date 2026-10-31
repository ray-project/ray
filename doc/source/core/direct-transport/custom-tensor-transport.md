---
myst:
  html_meta:
    description: "Implement a custom tensor transport for Ray Direct Transport: metadata classes, communicator metadata, send and receive, and cleanup."
---

(custom-tensor-transport)=

# Implementing a custom tensor transport (Advanced)

You can register custom tensor transports with Ray Direct Transport (RDT) at runtime. This page explains how to build a custom tensor transport by implementing the {class}`TensorTransportManager <ray.experimental.TensorTransportManager>` abstract interface.

## Overview

To create a custom tensor transport, do the following:

1. Implement the abstract interface {class}`ray.experimental.TensorTransportManager <ray.experimental.TensorTransportManager>`.
1. Define custom metadata classes by extending {class}`TensorTransportMetadata <ray.experimental.TensorTransportMetadata>` and {class}`CommunicatorMetadata <ray.experimental.CommunicatorMetadata>`.
1. Register your transport with {func}`ray.experimental.register_tensor_transport <ray.experimental.register_tensor_transport>`.

When Ray transfers a tensor between actors with your transport, it calls methods on your `TensorTransportManager` implementation at different stages of the transfer lifecycle.


## Implementing TensorTransportManager

The {class}`TensorTransportManager <ray.experimental.TensorTransportManager>` abstract class defines the interface for custom tensor transports. You must implement all abstract methods.

The following diagram shows when Ray calls each method during a tensor transfer:

```text
Source Actor                    Owner Process                 Destination Actor
============                    =============                 =================
     |                               |                               |
1. Task returns tensor               |                               |
   ``extract_tensor_transport_metadata``                             |
     |                               |                               |
     | ---- transport_metadata ----> |                               |
     |                               |                               |
     |                     2. Prepare communicator                   |
     |                        ``get_communicator_metadata``          |
     |                               |                               |
     | <---- comm metadata --------- | ---- comm metadata -------->  |
     |                               |                               |
3. ``send_multiple_tensors``         |          3. ``recv_multiple_tensors``
                                     |                               |
     | ------------ tensors ---------------------------------------> |
     |                               |                               |
     |                         (transfer complete)                   |
     |                               |                               |
     |                      5. Ref goes out of scope                 |
     | <---------------------------- |                               |
5. Clean up resources                |                               |
   ``garbage_collect``               |                               |
```


Ray doesn't call `send_multiple_tensors` for one-sided transports. Only one-sided transports support the `ray.put` and `ray.get` case. The following diagram shows where Ray calls each method in that case:

```text
Source Actor                                                  Destination Actor
============                                                  =================
     |                                                               |
1. User ``ray.put``'s tensor                                         |
   ``extract_tensor_transport_metadata``                             |
     |                                                               |
     |                                                               |
2. User passes ref to another actor                                  |
     | ---- transport_metadata ---------------------------------->   |
     |                                                               |
     |                                                               |
     |                                          3. User ``ray.get``'s on object ref
                                                   ``get_communicator_metadata``
     |                                              ``recv_multiple_tensors``
     | ------------ tensors --------- -----------------------------> |
     |                                                               |
     |                         (transfer complete)                   |
     |                                                               |
4. Clean up resources                                                |
   ``garbage_collect``                                               |
(when ref goes out of scope)                                         |
```


For details on what each method does and how to implement it, see the API reference for {class}`TensorTransportManager <ray.experimental.TensorTransportManager>`. For the implementations of Ray's default transports, such as NCCL and NIXL, see the [python/ray/experimental/rdt/](https://github.com/ray-project/ray/tree/master/python/ray/experimental/rdt) directory. The following sections walk through implementing and using a custom tensor transport.

## Example: Shared memory tensor transport

The following example walks through a complete custom tensor transport that transfers `numpy` arrays through shared memory.


With shared memory, the receiver directly reads the memory block that the sender wrote to, so the transport is one-sided. As a result, `is_one_sided` returns `True` and Ray never calls `send_multiple_tensors`.

### Define metadata classes

Your transport uses two metadata classes that flow through different stages of the transfer:

- {class}`TensorTransportMetadata <ray.experimental.TensorTransportMetadata>` comes from `extract_tensor_transport_metadata`, which runs on the source actor. It carries the shape, dtype, and device of each tensor. It also carries any transport-specific identifiers that the receiver needs to locate and read the data, such as shared memory block names or remote direct memory access (RDMA) keys.

- {class}`CommunicatorMetadata <ray.experimental.CommunicatorMetadata>` comes from `get_communicator_metadata`, which runs on the owner or driver process. It carries any coordination information that both actors need, such as ranks in a collective group. For one-sided transports, where the receiver can read the sender's memory directly, an empty metadata object is typically sufficient.

Start by extending these classes to carry any transport-specific state. `ShmTransportMetadata` stores the shared memory block name and size so the receiver can locate and read the data. This transport doesn't need any communicator metadata, so `ShmCommunicatorMetadata` is empty.

```{literalinclude} ../doc_code/direct_transport_custom.py
:language: python
:start-after: __custom_metadata_start__
:end-before: __custom_metadata_end__
```

### Extract tensor transport metadata

Ray calls `extract_tensor_transport_metadata` on the source actor right after the task produces its result tensors. Record shapes and dtypes, then perform any transport-specific registration. In this example, the implementation serializes the tensors into a shared memory block and records the block name and size in the metadata so the receiver can find it.

```{literalinclude} ../doc_code/direct_transport_custom.py
:language: python
:start-after: __custom_extract_start__
:end-before: __custom_extract_end__
```

### Get communicator metadata

Ray calls `get_communicator_metadata` on the owner or driver process before it orchestrates the transfer. Return any information that both actors need to coordinate, such as ranks in a collective group. For one-sided transports such as shared memory, an empty metadata object is fine.

```{literalinclude} ../doc_code/direct_transport_custom.py
:language: python
:start-after: __custom_communicator_start__
:end-before: __custom_communicator_end__
```

### Transport properties

Define your `TensorTransportManager` subclass and implement the property methods. `tensor_transport_backend` returns the name that you pass to `@ray.method(tensor_transport=...)`. `is_one_sided` and `can_abort_transport` tell Ray how to orchestrate transfers and handle errors. Ray calls `actor_has_tensor_transport` to check whether a given actor can use this transport.

```{literalinclude} ../doc_code/direct_transport_custom.py
:language: python
:start-after: __custom_properties_start__
:end-before: __custom_properties_end__
```

### Send and receive

`recv_multiple_tensors` runs on the destination actor. For this shared memory transport, it opens the shared memory block by name and deserializes the tensors.

`send_multiple_tensors` runs on the source actor for two-sided transports. Because shared memory is one-sided, Ray never calls this method. This implementation raises `NotImplementedError` as a safety guard.

```{literalinclude} ../doc_code/direct_transport_custom.py
:language: python
:start-after: __custom_send_recv_start__
:end-before: __custom_send_recv_end__
```

### Cleanup

`garbage_collect` runs on the source actor when Ray's reference counting determines the object is out of scope. Release any transport resources here. In this example, `garbage_collect` closes the shared memory block and unlinks it.

If `can_abort_transport` returns `True`, `abort_transport` runs on both actors when a system error occurs during a transfer. This transport returns `False` for `can_abort_transport`, so Ray kills the involved actors instead, and `abort_transport` is a no-op.

```{literalinclude} ../doc_code/direct_transport_custom.py
:language: python
:start-after: __custom_cleanup_start__
:end-before: __custom_cleanup_end__
```

## Registering your transport

After you implement your transport, register it in the driver process with {func}`ray.experimental.register_tensor_transport <ray.experimental.register_tensor_transport>` before you create any actors that use it:

```{literalinclude} ../doc_code/direct_transport_custom.py
:language: python
:start-after: __custom_usage_start__
:end-before: __custom_usage_end__
```


## Limitations

Custom tensor transports have the following limitations:

- **Actor restarts aren't supported.** Your actor doesn't have access to the custom transport after a restart.

- **Register transports before actor creation.** If you register a transport after creating an actor, that actor can't use the new transport.

- **Out-of-order actors.** If you have an out-of-order actor, such as an async actor, and the process where you submit the actor task is different from where you created the actor, Ray can't guarantee it has registered your custom transport on the actor at task execution time.

- **Actor creation and task submission from different processes.** If the process where you submit an actor task is different from where you created the actor, Ray can't guarantee it has registered your custom transport on the actor at task execution time.

For general RDT limitations, see {ref}`limitations <limitations>`.

If you have questions, ask them through [GitHub issues](https://github.com/ray-project/ray/issues) or the [Ray Slack](https://docs.google.com/forms/d/e/1FAIpQLSfAcoiLCHOguOm8e7Jnn-JJdZaCxPGjgVCvFijHB5PLaQLeig/viewform).
