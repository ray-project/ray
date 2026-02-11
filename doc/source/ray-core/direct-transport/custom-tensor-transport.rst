.. _custom-tensor-transport:

*************************************************
Implementing a custom tensor transport (Advanced)
*************************************************

Ray Direct Transport (RDT) allows you to register custom tensor transports at runtime.
This page explains how to implement a custom tensor transport by implementing the :class:`TensorTransportManager <ray.experimental.TensorTransportManager>` abstract interface.

Overview
========

To create a custom tensor transport:

1. Implement the abstract interface :class:`ray.experimental.TensorTransportManager <ray.experimental.TensorTransportManager>`.
2. Define custom metadata classes by extending :class:`TensorTransportMetadata <ray.experimental.TensorTransportMetadata>` and :class:`CommunicatorMetadata <ray.experimental.CommunicatorMetadata>`.
3. Register your transport using :func:`ray.experimental.register_tensor_transport <ray.experimental.register_tensor_transport>`.

When Ray needs to transfer a tensor between actors using your transport, it calls specific methods on your ``TensorTransportManager`` implementation at different stages of the transfer lifecycle.


Implementing TensorTransportManager
===================================

The :class:`TensorTransportManager <ray.experimental.TensorTransportManager>` abstract class defines the interface for custom tensor transports. You must implement all abstract methods.

The following diagram shows when each method is called during a tensor transfer:

.. code-block:: text

   Source Actor                    Owner Process                 Destination Actor
   ============                    =============                 =================
        |                               |                               |
   1. Task returns tensor               |                               |
        |                               |                               |
   2. extract_tensor_transport_metadata |                               |
        |                               |                               |
        | ---- transport_metadata ----> |                               |
        |                               |                               |
        |                     3. get_communicator_metadata              |
        |                               |                               |
        | <---- comm metadata --------- | ---- comm metadata -------->  |
        |                               |                               |
   4. send_multiple_tensors             |          5. recv_multiple_tensors
        | ------------ tensors ---------------------------------------> |
        |                               |                               |
        |                         (transfer complete)                   |
        |                               |                               |
   6. garbage_collect                   |                               |
   (when ref goes out of scope)         |                               |


Note that Ray will not call `send_multiple_tensors` for one-sided transports.


Transport identification methods
--------------------------------

tensor_transport_backend
^^^^^^^^^^^^^^^^^^^^^^^^

Returns the name of your tensor transport backend.
Ray uses this name to match your transport with the ``tensor_transport`` argument in :func:`@ray.method <ray.method>`.

is_one_sided
^^^^^^^^^^^^

Indicates whether your transport uses one-sided communication where only the receiver initiates the transfer.

- **One-sided transports**: The receiver can directly read the sender's memory without the sender actively participating. NIXL and CUDA-IPC are examples.
- **Two-sided transports**: Both sender and receiver must actively participate in the transfer. Collective communication libraries like NCCL and GLOO are examples.

This affects how Ray orchestrates the transfer and handles failures. Two-sided transports also have extra limitations described in :ref:`limitations <limitations>`.
Ray will not call `send_multiple_tensors` for one-sided transports. The transfer is expected to happen through just `recv_multiple_tensors`.

can_abort_transport
^^^^^^^^^^^^^^^^^^^

Indicates whether your transport can safely abort an in-progress transfer.

- If ``True``: Ray calls `abort_transport` on both the source and destination actors when a send / recv error, allowing your transport to clean up gracefully.
- If ``False``: Ray kills the involved actors to prevent deadlocks when errors occur during transfer.

Return ``True`` only if your transport can reliably interrupt an in-progress send or receive operation without leaving either party in a blocked state.

Metadata extraction methods
---------------------------

TensorTransportMetadata
^^^^^^^^^^^^^^^^^^^^^^^

:class:`TensorTransportMetadata <ray.experimental.TensorTransportMetadata>` stores information about the tensors being transferred.
Ray creates this metadata on the source actor when the tensor is first created, and passes it to all subsequent send and recv tasks.

.. code-block:: python

    @dataclass
    class TensorTransportMetadata:
        # list of tuples, each containing the shape and dtype of a tensor
        tensor_meta: List[Tuple[torch.Size, torch.dtype]]
        # the device of the tensor, currently, all tensors in the list must have the same device type
        tensor_device: Optional[torch.device] = None

You can extend this class to store transport-specific metadata. For example, if your send or recv needs a source address, you can store it here.

.. code-block:: python

    @dataclass
    class MyCustomTransportMetadata(TensorTransportMetadata):
        buffer_ids: Optional[List[Any]] = None

extract_tensor_transport_metadata
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Implement this method to create the TensorTransportMetadata you defined previously.
Ray calls this on the source actor immediately after the actor task creates the result tensors.
Implement this to:

1. Record tensor shapes, dtypes, and devices.
2. Perform any transport-specific registration such as registering memory for RDMA.
3. Store any handles or identifiers needed for the transfer.

The following example shows basic metadata extraction:

.. code-block:: python

    def extract_tensor_transport_metadata(
        self,
        obj_id: str,
        gpu_object: List[torch.Tensor],
    ) -> MyCustomTransportMetadata:
        tensor_meta = [(t.shape, t.dtype) for t in gpu_object]
        tensor_device = gpu_object[0].device if gpu_object else None

        # Transport-specific: register memory buffers
        buffer_ids = [self._register_buffer(t) for t in gpu_object]

        return MyCustomTransportMetadata(
            tensor_meta=tensor_meta,
            tensor_device=tensor_device,
            buffer_ids=buffer_ids,
        )

CommunicatorMetadata
^^^^^^^^^^^^^^^^^^^^

:class:`CommunicatorMetadata <ray.experimental.CommunicatorMetadata>` stores any information about the actors that are doing the communication.
This is used when your owner / driver process has specific information about the actors involved in the transfer that needs to be sent.
Note that for many non-collective transports, this may be empty and unused, e.g. for one-sided RDMA transports like NIXL.

.. code-block:: python

    @dataclass
    class MyCustomCommunicatorMetadata(CommunicatorMetadata):
        communicator_name: str = None
        src_rank: int = -1
        dst_rank: int = -1

get_communicator_metadata
^^^^^^^^^^^^^^^^^^^^^^^^^

Gets the CommunicatorMetadata for a send/recv. Ray calls this on the owner/driver process before orchestrating the transfer.
You can typically implement this to return information both actors need to identify each other such as ranks in a collective group.
Many forms of transports such as one-sided RDMA reads may be ok just returning empty CommunicatorMetadata here.


.. code-block:: python

    def get_communicator_metadata(
        self,
        src_actor: ray.actor.ActorHandle,
        dst_actor: ray.actor.ActorHandle,
        backend: Optional[str] = None,
    ) -> MyCustomCommunicatorMetadata:
        group_name = self._find_collective_group(src_actor, dst_actor)
        src_rank = self._get_rank(src_actor, group_name)
        dst_rank = self._get_rank(dst_actor, group_name)

        return MyCustomCommunicatorMetadata(
            communicator_name=group_name,
            src_rank=src_rank,
            dst_rank=dst_rank,
        )

Data transfer methods
---------------------

send_multiple_tensors
^^^^^^^^^^^^^^^^^^^^^

Sends tensors from the source actor to the destination actor. Ray calls this on the source actor during the transfer.
Implement this to perform the actual data transfer using your transport's send mechanism. For one-sided transports,
you can simply avoid implementing this method or even raise a NotImplementedError to assure it's not being called.
For collective-based transports:

.. code-block:: python

    def send_multiple_tensors(
        self,
        tensors: List[torch.Tensor],
        tensor_transport_metadata: TensorTransportMetadata,
        communicator_metadata: MyCustomCommunicatorMetadata,
    ):
        comm = self._get_communicator(communicator_metadata.communicator_name)
        dst_rank = communicator_metadata.dst_rank
        comm.send(tensors, dst_rank)

recv_multiple_tensors
^^^^^^^^^^^^^^^^^^^^^

Receives tensors on the destination actor. Ray calls this on the destination actor during the transfer.

.. code-block:: python

    def recv_multiple_tensors(
        self,
        obj_id: str,
        tensor_transport_meta: TensorTransportMetadata,
        communicator_metadata: MyCustomCommunicatorMetadata,
    ):
        comm = self._get_communicator(communicator_metadata.communicator_name)
        src_rank = communicator_metadata.src_rank
        tensors = []
        device = tensor_transport_meta.tensor_device
        for meta in tensor_transport_meta.tensor_meta:
            shape, dtype = meta
            tensor = torch.empty(shape, dtype=dtype, device=device)
            tensors.append(tensor)
        comm.recv(tensors, src_rank)
        return tensors

Lifecycle management methods
----------------------------

garbage_collect
^^^^^^^^^^^^^^^

Cleans up resources when Ray decides to free the RDT object. Ray calls this on the source actor after Ray's distributed reference counting protocol determines the object is out of scope.
Use this to release any resources your transport allocated, such as deregistering memory buffers. Ray doesn't hold the tensor after returning it to the user on the recv side.

.. code-block:: python

    def garbage_collect(
        self,
        obj_id: str,
        tensor_transport_meta: MyCustomTransportMetadata,
    ):
        if tensor_transport_meta.buffer_ids:
            for buffer_id in tensor_transport_meta.buffer_ids:
                self._deregister_buffer(buffer_id)

abort_transport
^^^^^^^^^^^^^^^

Aborts an in-progress transfer. Ray calls this on both the source and destination actors when a system error occurs if `can_abort_transport` returns ``True``.

.. code-block:: python

    def abort_transport(
        self,
        obj_id: str,
        communicator_metadata: MyCustomCommunicatorMetadata,
    ):
        # Cancel any pending operations
        comm = self._get_communicator(communicator_metadata.communicator_name)
        comm.abort()

        # Clean up partial state
        self._cleanup_partial_transfer(obj_id)


Registering your transport
==========================

After implementing your transport, register it using :func:`ray.experimental.register_tensor_transport <ray.experimental.register_tensor_transport>`:

.. code-block:: python

   from ray.experimental import register_tensor_transport

   register_tensor_transport(
       "MY_TRANSPORT",    # Transport name
       ["cuda", "cpu"],   # List of supported device types
       MyCustomTransport, # Your TensorTransportManager class
   )

You can then use your transport in actor methods:

.. code-block:: python

    @ray.remote
    class MyActor:
        @ray.method(tensor_transport="MY_TRANSPORT")
        def create_tensor(self):
            return torch.randn(1000, 1000)

        def sum_tensor(self, tensor: torch.Tensor):
            return torch.sum(tensor).item()

    actors = [MyActor.remote() for _ in range(2)]
    ref = actors[0].create_tensor.remote()
    result = actors[1].sum_tensor.remote(ref)
    ray.get(result)


Limitations
===========

Custom tensor transports have the following limitations:

- **Actor restarts aren't supported.** Your actor doesn't have access to the custom transport after a restart.

- **Registration must happen before actor creation.** You must create your actor on the same process where you registered the custom tensor transport. For example, you can't use your custom transport with an actor if you registered the custom transport on the driver and created the actor inside a task.

- **Actors only access transports registered before their creation.** If you register a transport after creating an actor, that actor can't use the new transport.

- **Out-of-order actors** If you have an out-of-order actor (such as an async actor) and the process where you submit the actor task is different from where you created the actor, Ray can't guarantee it has registered your custom transport on the actor at task submission time.

For general RDT limitations, see :ref:`limitations <limitations>`.

Also feel free to reach out through GitHub issues or the OSS Ray Slack to ask any questions.
