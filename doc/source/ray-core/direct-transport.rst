.. _direct-transport:


**************************
Ray Direct Transport (RDT)
**************************

Ray objects are normally stored in Ray's CPU-based object store and copied and deserialized when accessed by a Ray task or actor.
For GPU data specifically, this can lead to unnecessary and expensive data transfers.
For example, passing a CUDA ``torch.Tensor`` from one Ray task to another would require a copy from GPU to CPU memory, then back again to GPU memory.

*Ray Direct Transport (RDT)* is a new feature that allows Ray to store and pass objects directly between Ray actors.
This feature augments the familiar Ray :class:`ObjectRef <ray.ObjectRef>` API by:

- Keeping GPU data in GPU memory until a transfer is needed
- Avoiding expensive serialization and copies to and from the Ray object store
- Using efficient data transports like collective communication libraries (`Gloo <https://github.com/pytorch/gloo>`__ or `NCCL <https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/index.html>`__) or point-to-point RDMA (via `NVIDIA's NIXL <https://github.com/ai-dynamo/nixl>`__) to transfer data directly between devices, including both CPU and GPUs

.. note::
   RDT is currently in **alpha**. Not all Ray Core APIs are supported yet. Future releases may introduce breaking API changes. See the :ref:`limitations <limitations>` section for more details.

Getting started
===============

.. tip::
   RDT currently supports ``torch.Tensor`` objects created by Ray actor tasks. Other datatypes and Ray non-actor tasks may be supported in future releases.

This walkthrough will show how to create and use RDT with different *tensor transports*, i.e. the mechanism used to transfer the tensor between actors.
Currently, RDT supports the following tensor transports:

1. `Gloo <https://github.com/pytorch/gloo>`__: A collective communication library for PyTorch and CPUs.
2. `NVIDIA NCCL <https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/index.html>`__: A collective communication library for NVIDIA GPUs.
3. `NVIDIA NIXL <https://github.com/ai-dynamo/nixl>`__ (backed by `UCX <https://github.com/openucx/ucx>`__): A library for accelerating point-to-point transfers via RDMA, especially between various types of memory and NVIDIA GPUs.

For ease of following along, we'll start with the `Gloo <https://github.com/pytorch/gloo>`__ transport, which can be used without any physical GPUs.

.. _direct-transport-gloo:

Usage with Gloo (CPUs only)
---------------------------

Installation
^^^^^^^^^^^^

.. note::
    Under construction.

Walkthrough
^^^^^^^^^^^

To get started, define an actor class and a task that returns a ``torch.Tensor``:

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __normal_example_start__
   :end-before: __normal_example_end__

As written, when the ``torch.Tensor`` is returned, it will be copied into Ray's CPU-based object store.
For CPU-based tensors, this can require an expensive step to copy and serialize the object, while GPU-based tensors additionally require a copy to and from CPU memory.

To enable RDT, use the ``tensor_transport`` option in the :func:`@ray.method <ray.method>` decorator.

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __gloo_example_start__
   :end-before: __gloo_example_end__

This decorator can be added to any actor tasks that return a ``torch.Tensor``, or that return ``torch.Tensors`` nested inside other Python objects.
Adding this decorator will change Ray's behavior in the following ways:

1. When returning the tensor, Ray will store a *reference* to the tensor instead of copying it to CPU memory.
2. When the :class:`ray.ObjectRef` is passed to another task, Ray will use Gloo to transfer the tensor to the destination task.

Note that for (2) to work, the :func:`@ray.method(tensor_transport) <ray.method>` decorator only needs to be added to the actor task that *returns* the tensor. It should not be added to actor tasks that *consume* the tensor (unless those tasks also return tensors).

Also, for (2) to work, we must first create a *collective group* of actors.

Creating a collective group
^^^^^^^^^^^^^^^^^^^^^^^^^^^

To create a collective group for use with RDT:

1. Create multiple Ray actors.
2. Create a collective group on the actors using the :func:`ray.experimental.collective.create_collective_group <ray.experimental.collective.create_collective_group>` function. The `backend` specified must match the `tensor_transport` used in the :func:`@ray.method <ray.method>` decorator.

Here is an example:

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __gloo_group_start__
   :end-before: __gloo_group_end__

The actors can now communicate directly via gloo.
The group can also be destroyed using the :func:`ray.experimental.collective.destroy_collective_group <ray.experimental.collective.destroy_collective_group>` function.
After calling this function, a new collective group can be created on the same actors.

Passing objects to other actors
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Now that we have a collective group, we can create and pass RDT objects between the actors.
Here is a full example:

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __gloo_full_example_start__
   :end-before: __gloo_full_example_end__

When the :class:`ray.ObjectRef` is passed to another task, Ray will use Gloo to transfer the tensor directly from the source actor to the destination actor instead of the default object store.
Note that the :func:`@ray.method(tensor_transport) <ray.method>` decorator is only added to the actor task that *returns* the tensor; once this hint has been added, the receiving actor task `receiver.sum` will automatically use Gloo to receive the tensor.
In this example, because `MyActor.sum` does not have the :func:`@ray.method(tensor_transport) <ray.method>` decorator, it will use the default Ray object store transport to return `torch.sum(tensor)`.

RDT also supports passing tensors nested inside Python data structures, as well as actor tasks that return multiple tensors, like in this example:

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __gloo_multiple_tensors_example_start__
   :end-before: __gloo_multiple_tensors_example_end__

Passing RDT objects to the actor that produced them
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

RDT :class:`ray.ObjectRefs <ray.ObjectRef>` can also be passed to the actor that produced them.
This avoids any copies and just provides a reference to the same ``torch.Tensor`` that was previously created.
For example:

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __gloo_intra_actor_start__
   :end-before: __gloo_intra_actor_end__


.. note::
    Ray only keeps a reference to the tensor created by the user, so the tensor objects are *mutable*.
    If ``sender.sum`` were to modify the tensor in the above example, the changes would also be seen by ``receiver.sum``.
    This differs from the normal Ray Core API, which always makes an immutable copy of data returned by actors.


``ray.get``
^^^^^^^^^^^

The :func:`ray.get <ray.get>` function can also be used as usual to retrieve the result of an RDT object. However, :func:`ray.get <ray.get>` will by default use the same tensor transport as the one specified in the :func:`@ray.method <ray.method>` decorator. For collective-based transports, this will not work if the caller is not part of the collective group.

Therefore, users need to specify the Ray object store as the tensor transport explicitly by setting ``_tensor_transport`` in :func:`ray.get <ray.get>`.

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __gloo_get_start__
   :end-before: __gloo_get_end__

Object mutability
^^^^^^^^^^^^^^^^^

Unlike objects in the Ray object store, RDT objects are *mutable*, meaning that Ray only holds a reference to the tensor and will not copy it until a transfer is requested.
This means that if the actor that returns a tensor also keeps a reference to the tensor, and the actor later modifies it in place while Ray is still storing the tensor reference, it's possible that some or all of the changes may be seen by receiving actors.

Here is an example of what can go wrong:

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __gloo_wait_tensor_freed_bad_start__
   :end-before: __gloo_wait_tensor_freed_bad_end__

In this example, the sender actor returns a tensor to Ray, but it also keeps a reference to the tensor in its local state.
Then, in `sender.increment_and_sum_stored_tensor`, the sender actor modifies the tensor in place while Ray is still holding the tensor reference.
Then, the `receiver.increment_and_sum` task receives the modified tensor instead of the original, so the assertion fails.

To fix this kind of error, use the :func:`ray.experimental.wait_tensor_freed <ray.experimental.wait_tensor_freed>` function to wait for Ray to release all references to the tensor, so that the actor can safely write to the tensor again.
:func:`wait_tensor_freed <ray.experimental.wait_tensor_freed>` will unblock once all tasks that depend on the tensor have finished executing and all corresponding `ObjectRefs` have gone out of scope.
Ray tracks tasks that depend on the tensor by keeping track of which tasks take the `ObjectRef` corresponding to the tensor as an argument.

Here's a fixed version of the earlier example.

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __gloo_wait_tensor_freed_start__
   :end-before: __gloo_wait_tensor_freed_end__

The main changes are:
1. `sender` calls :func:`wait_tensor_freed <ray.experimental.wait_tensor_freed>` before modifying the tensor in place.
2. The driver skips :func:`ray.get <ray.get>` because :func:`wait_tensor_freed <ray.experimental.wait_tensor_freed>` blocks until all `ObjectRefs` pointing to the tensor are freed, so calling :func:`ray.get <ray.get>` here would cause a deadlock.
3. The driver calls `del tensor` to release its reference to the tensor. Again, this is necessary because :func:`wait_tensor_freed <ray.experimental.wait_tensor_freed>` blocks until all `ObjectRefs` pointing to the tensor are freed.

When an RDT `ObjectRef` is passed back to the same actor that produced it, Ray passes back a *reference* to the tensor instead of a copy. Therefore, the same kind of bug can occur.
To help catch such cases, Ray will print a warning if an RDT object is passed to the actor that produced it and a different actor, like so:

.. literalinclude:: doc_code/direct_transport_gloo.py
   :language: python
   :start-after: __gloo_object_mutability_warning_start__
   :end-before: __gloo_object_mutability_warning_end__


Usage with NCCL (NVIDIA GPUs only)
----------------------------------

RDT requires just a few lines of code change to switch tensor transports. Here is the :ref:`Gloo example <direct-transport-gloo>`, modified to use NVIDIA GPUs and the `NCCL <https://docs.nvidia.com/deeplearning/nccl/user-guide/docs/index.html>`__ library for collective GPU communication.

.. literalinclude:: doc_code/direct_transport_nccl.py
   :language: python
   :start-after: __nccl_full_example_start__
   :end-before: __nccl_full_example_end__

The main code differences are:

1. The :func:`@ray.method <ray.method>` uses ``tensor_transport="nccl"`` instead of ``tensor_transport="gloo"``.
2. The :func:`ray.experimental.collective.create_collective_group <ray.experimental.collective.create_collective_group>` function is used to create a collective group.
3. The tensor is created on the GPU using the ``.cuda()`` method.

Usage with NIXL (CPUs or NVIDIA GPUs)
-------------------------------------

Installation
^^^^^^^^^^^^

For maximum performance, run the `install_gdrcopy.sh <https://github.com/ray-project/ray/blob/master/doc/tools/install_gdrcopy.sh>`__ script (e.g., ``install_gdrcopy.sh "${GDRCOPY_OS_VERSION}" "12.8" "x64"``). You can find available OS versions `here <https://developer.download.nvidia.com/compute/redist/gdrcopy/CUDA%2012.8/>`__. If `gdrcopy` is not installed, things will still work with a plain ``pip install nixl``, just with lower performance. `nixl` and `ucx` are installed as dependencies via pip.

Walkthrough
^^^^^^^^^^^

NIXL can transfer data between different devices, including CPUs and NVIDIA GPUs, but doesn't require a collective group to be created ahead of time.
This means that any actor that has NIXL installed in its environment can be used to create and pass an RDT object.

Otherwise, the usage is the same as in the :ref:`Gloo example <direct-transport-gloo>`.

Here is an example showing how to use NIXL to transfer an RDT object between two actors:

.. literalinclude:: doc_code/direct_transport_nixl.py
   :language: python
   :start-after: __nixl_full_example_start__
   :end-before: __nixl_full_example_end__

Compared to the :ref:`Gloo example <direct-transport-gloo>`, the main code differences are:

1. The :func:`@ray.method <ray.method>` uses ``tensor_transport="nixl"`` instead of ``tensor_transport="gloo"``.
2. No collective group is needed.

ray.put and ray.get with NIXL
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Unlike the collective-based tensor transports (Gloo and NCCL), the :func:`ray.get <ray.get>` function can use NIXL to retrieve a copy of the result.
By default, the tensor transport for :func:`ray.get <ray.get>` will be the one specified in the :func:`@ray.method <ray.method>` decorator.

.. literalinclude:: doc_code/direct_transport_nixl.py
   :language: python
   :start-after: __nixl_get_start__
   :end-before: __nixl_get_end__

You can also use NIXL to retrieve the result from references created by :func:`ray.put <ray.put>`.

.. literalinclude:: doc_code/direct_transport_nixl.py
   :language: python
   :start-after: __nixl_put__and_get_start__
   :end-before: __nixl_put__and_get_end__

Summary
-------

RDT allows Ray to store and pass objects directly between Ray actors, using accelerated transports like GLOO, NCCL, and NIXL.
Here are the main points to keep in mind:

* If using a collective-based tensor transport (Gloo or NCCL), a collective group must be created ahead of time. NIXL just requires all involved actors to have NIXL installed.
* Unlike objects in the Ray object store, RDT objects are *mutable*, meaning that Ray only holds a reference, not a copy, to the stored tensor(s). 
* Otherwise, actors can be used as normal.

For a full list of limitations, see the :ref:`limitations <limitations>` section.


Microbenchmarks
===============

.. note::
    Under construction.

.. _limitations:

Limitations
===========

RDT is currently in alpha and currently has the following limitations, which may be addressed in future releases:

* Support for ``torch.Tensor`` objects only.
* Support for Ray actors only, not Ray tasks.
* Not yet compatible with `asyncio <https://docs.python.org/3/library/asyncio.html>`__. Follow the `tracking issue <https://github.com/ray-project/ray/issues/56398>`__ for updates.
* Support for the following transports: Gloo, NCCL, and NIXL.
* Support for CPUs and NVIDIA GPUs only.
* RDT objects are *mutable*. This means that Ray only holds a reference to the tensor, and will not copy it until a transfer is requested. Thus, if the application code also keeps a reference to a tensor before returning it, and modifies the tensor in place, then some or all of the changes may be seen by the receiving actor.

For collective-based tensor transports (Gloo and NCCL):

* Only the process that created the collective group can submit actor tasks that return and pass RDT objects. If the creating process passes the actor handles to other processes, those processes can submit actor tasks as usual, but will not be able to use RDT objects.
* Similarly, the process that created the collective group cannot serialize and pass RDT :class:`ray.ObjectRefs <ray.ObjectRef>` to other Ray tasks or actors. Instead, the :class:`ray.ObjectRef`\s can only be passed as direct arguments to other actor tasks, and those actors must be in the same collective group.
* Each actor can only be in one collective group per tensor transport at a time.
* No support for :func:`ray.put <ray.put>`.
* If a system-level error occurs during a collective operation, the collective group will be destroyed and the actors will no longer be able to communicate via the collective group. Note that application-level errors, i.e. exceptions raised by user code, will not destroy the collective group and will instead be propagated to any dependent task(s), as for non-RDT Ray objects. System-level errors include:

   * Errors internal to the third-party transport, e.g., NCCL network errors
   * Actor and node failure
   * Tensors returned by the user that are located on an unsupported device, e.g., a CPU tensor when using NCCL
   * Any unexpected system bugs


Due to a known issue, for NIXL, we currently do not support storing different GPU objects at the same actor, where the objects contain an overlapping but not equal set of tensors. To support this pattern, ensure that the first `ObjectRef` has gone out of scope before storing the same tensor(s) again in a second object.

.. literalinclude:: doc_code/direct_transport_nixl.py
   :language: python
   :start-after: __nixl_limitations_start__
   :end-before: __nixl_limitations_end__

Advanced: RDT Internals
=======================

.. note::
    Under construction.
