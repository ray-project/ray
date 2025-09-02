.. _gpu-objects:

GPU Objects
===========

Ray objects are normally stored in Ray's CPU-based object store and are copied to GPU memory as needed by the user.
This can lead to unnecessary and expensive data transfers.
For example, passing a CUDA ``torch.Tensor`` from one Ray task to another would require a copy from GPU to CPU memory, then back again to GPU memory.

GPU objects are a new feature that allows Ray to store and pass objects directly in GPU memory, enabling zero-copy or near-zero-copy data transfers.
GPU objects implement the familiar Ray :class:`ObjectRef <ray.ObjectRef>` API but they also:

- Keep data in GPU memory until a transfer is needed
- Avoid expensive serialization and copies to and from CPU memory
- Use efficient data transports like collective communication libraries or RDMA (via `NVIDIA's NIXL <https://github.com/ai-dynamo/nixl>`__) to transfer data between GPUs

.. note::
   The GPU objects feature is currently in **alpha**. Not all Ray Core APIs are supported yet. Future releases may introduce breaking API changes.

Getting started
---------------

GPU objects currently support ``torch.Tensor`` objects created by Ray actor tasks.
This walkthrough will show how to create and use GPU objects using different *tensor transports*, i.e. the mechanism used to transfer the tensor between actors.
Currently, we support:

1. `Gloo <https://github.com/pytorch/gloo>`__: A collective communication library for PyTorch and CPUs.
2. `NVIDIA NCCL <https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/index.html>`__: A collective communication library for NVIDIA GPUs.
3. `NVIDIA NIXL <https://github.com/ai-dynamo/nixl>`__ (backed by `UCX <https://github.com/openucx/ucx>`__): A library for accelerating point-to-point transfers via RDMA, especially between various types of memory and NVIDIA GPUs.

For ease of following along, we'll start with the `Gloo <https://github.com/pytorch/gloo>`__ transport, which can be used without any physical GPUs.


Installation
------------

.. note::
    Under construction.

.. _gpu-objects-gloo:
Usage with gloo (CPUs only)
---------------------------

Installation
************

.. note::
    Under construction.

Walkthrough
***********

To get started, define an actor class and a task that returns a ``torch.Tensor``:

.. literalinclude:: doc_code/gpu_objects_gloo.py
   :language: python
   :start-after: __normal_example_start__
   :end-before: __normal_example_end__

As written, when the ``torch.Tensor`` is returned, it will be copied into Ray's CPU-based object store.
For CPU-based tensors, this can require an expensive step to copy and serialize the object, while GPU-based tensors additionally require a copy to and from CPU memory.

To enable GPU objects, use the ``tensor_transport`` option in the :func:`@ray.method <ray.method>` decorator.

.. literalinclude:: doc_code/gpu_objects_gloo.py
   :language: python
   :start-after: __gloo_example_start__
   :end-before: __gloo_example_end__

This decorator can be added to any actor tasks that return a ``torch.Tensor``, or that return ``torch.Tensors`` nested inside other Python objects.
Adding this decorator will change Ray's behavior in the following ways:

1. When returning the tensor, Ray will store a *reference* to the tensor instead of copying it to CPU memory. If multiple tensors are found, then Ray will store a reference for each tensor.
2. When the :class:`ray.ObjectRef` is passed to another task, Ray will use Gloo to transfer the tensor to the destination task.

For (2) to work, we must first create a *collective group* of actors.

Creating a collective group
***************************

.. Add API pages for collective group APIs.

To create a collective group for use with GPU objects, we must:

1. Create multiple Ray actors.
2. Create a collective group on the actors using the :func:`ray.experimental.collective.create_collective_group <ray.experimental.collective.create_collective_group>` function. The tensor transport specified should match the one used in the :func:`@ray.method <ray.method>` decorator.

Here is an example:

.. literalinclude:: doc_code/gpu_objects_gloo.py
   :language: python
   :start-after: __gloo_group_start__
   :end-before: __gloo_group_end__

The actors can now communicate directly via gloo.
The group can also be destroyed using the :func:`ray.experimental.collective.destroy_collective_group <ray.experimental.collective.destroy_collective_group>` function.
After calling this function, the actors can be reused as normal and new collective groups can be created on them.

Passing GPU objects to other actors
***********************************

Now that we have a collective group, we can create and pass GPU objects between the actors.
Here is a full example:

.. literalinclude:: doc_code/gpu_objects_gloo.py
   :language: python
   :start-after: __gloo_full_example_start__
   :end-before: __gloo_full_example_end__

When the :class:`ray.ObjectRef` is passed to another task, Ray will use Gloo to transfer the tensor directly from the source actor to the destination actor instead of the default object store.
This is done by submitting an additional Ray task to each actor, which executes the send and receive operations on a background thread.

Passing GPU objects to the actor that produced them
*************************************

GPU :class:`ray.ObjectRefs <ray.ObjectRef>` can also be passed to the actor that produced them.
This avoids any copies and just returns the ``torch.Tensors`` directly back to the same actor.
For example:

.. literalinclude:: doc_code/gpu_objects_gloo.py
   :language: python
   :start-after: __gloo_intra_actor_start__
   :end-before: __gloo_intra_actor_end__


.. note::
    Ray only keeps a reference to the tensor created by the user, so the tensor objects are *mutable*.
    If ``sender.sum`` were to modify the tensor in the above example, the changes would also be seen by ``receiver.sum``.
    This differs from the normal Ray Core API, which always makes an immutable copy of data returned by actors.


``ray.get``
***********

The :func:`ray.get <ray.get>` function can be used to retrieve the result of a GPU object.
Note that for collective-based tensor transports (Gloo and NCCL), the :func:`ray.get <ray.get>` function will use the Ray object store to retrieve a copy of the result.

.. literalinclude:: doc_code/gpu_objects_gloo.py
   :language: python
   :start-after: __gloo_get_start__
   :end-before: __gloo_get_end__

Usage with NCCL (NVIDIA GPUs only)
----------------------------------

GPU objects require just a few lines of code change to switch tensor transports. Here is the same example as above, modified to use NVIDIA GPUs and the `NCCL <https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/index.html>`__ library for collective GPU communication.

.. literalinclude:: doc_code/gpu_objects_nccl.py
   :language: python
   :start-after: __nccl_full_example_start__
   :end-before: __nccl_full_example_end__

The main code differences are:

1. The :func:`@ray.method <ray.method>` uses ``tensor_transport="nccl"`` instead of ``tensor_transport="gloo"``.
2. The :func:`ray.experimental.collective.create_collective_group <ray.experimental.collective.create_collective_group>` function is used to create a collective group with ``tensor_transport="nccl"``.
3. The tensor is created on the GPU using the ``.cuda()`` method.

Usage with NIXL (CPUs or NVIDIA GPUs)
-------------------------------------

NIXL uses RDMA to transfer data between devices, including CPUs and NVIDIA GPUs.
One advantage of this tensor transport is that it doesn't require a collective group to be created ahead of time.
This means that any actor that has NIXL installed in its environment can be used to create and pass a GPU object.

Here is an example showing how to use NIXL to transfer a GPU object between two actors:

.. literalinclude:: doc_code/gpu_objects_nixl.py
   :language: python
   :start-after: __nixl_full_example_start__
   :end-before: __nixl_full_example_end__

Compared to the :ref:`Gloo example <gpu-objects-gloo>`, the main code differences are:

1. The :func:`@ray.method <ray.method>` uses ``tensor_transport="nixl"`` instead of ``tensor_transport="gloo"``.
2. No collective group is needed.

``ray.get``
***********

Unlike the collective-based tensor transports (Gloo and NCCL), the :func:`ray.get <ray.get>` function can use NIXL or the Ray object store to retrieve a copy of the result.
By default, the tensor transport for :func:`ray.get <ray.get>` will be the one specified in the :func:`@ray.method <ray.method>` decorator.

.. literalinclude:: doc_code/gpu_objects_nixl.py
   :language: python
   :start-after: __nixl_get_start__
   :end-before: __nixl_get_end__

Microbenchmarks
---------------

.. note::
    Under construction.

Limitations
-----------

GPU objects are in alpha and currently have the following limitations, which may be addressed in future releases:

* Support for ``torch.Tensor`` objects only.
* Support for Ray actors only, not Ray tasks.
* Support for the following transports: Gloo, NCCL, and NIXL.
* Support for CPUs and NVIDIA GPUs only.
* GPU objects are *mutable*. This means that Ray only holds a reference to the tensor, and will not copy it until a transfer is requested. Thus, if the application code also keeps a reference to a tensor before returning it, and modifies the tensor in place, then some or all of the changes may be seen by the receiving actor.

For collective-based tensor transports (Gloo and NCCL):

* Only the process that created the collective group can submit actor tasks that return and pass GPU objects. If the creating process passes the actor handles to other processes, those processes can submit actor tasks as usual, but will not be able to use GPU objects.
* Similarly, the process that created the collective group cannot serialize and pass GPU :class:`ray.ObjectRefs <ray.ObjectRef>` to other Ray tasks or actors. Instead, the :class:`ray.ObjectRef`\s can only be passed as direct arguments to other actor tasks, and those actors must be in the same collective group.
* Each actor can only be in one collective group per tensor transport at a time.
* No support for :func:`ray.put <ray.put>`.
* If a system-level error occurs during a collective operation, the collective group will be destroyed and the actors will no longer be able to communicate via the collective group.