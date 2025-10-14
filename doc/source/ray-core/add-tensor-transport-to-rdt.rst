.. _add-tensor-transport-to-rdt:

Add a new tensor transport to RDT
===========================================

To add a new tensor transport to RDT, the steps are:

1. Add the backend type in ``Backend`` enum in `types.py <https://github.com/ray-project/ray/blob/master/python/ray/util/collective/types.py>`__ and update the `tensor_transport_to_collective_backend <https://github.com/ray-project/ray/blob/master/python/ray/experimental/gpu_object_manager/gpu_object_store.py>`__ function to map the transport to the collective backend.
2. Add the transport type in ``TensorTransportEnum`` enum in `custom_types.py <https://github.com/ray-project/ray/blob/master/python/ray/_private/custom_types.py>`__ to map the transport to the tensor transport enum and modify the TENSOR_TRANSPORT_TO_COLLECTIVE_BACKEND in `gpu_object_store.py <https://github.com/ray-project/ray/blob/master/python/ray/experimental/gpu_object_manager/gpu_object_store.py>`__ to use the new transport type.
3. Add the transport metadata type in `types.py <https://github.com/ray-project/ray/blob/master/python/ray/util/collective/types.py>`__ to store the metadata for the transport. There are two examples: :class:`CollectiveTransportMetadata <ray.util.collective.types.CollectiveTransportMetadata>` and :class:`NixlTransportMetadata <ray.util.collective.types.NixlTransportMetadata>`.
4. Add a new transport class that inherits from the `TensorTransportManager <https://github.com/ray-project/ray/blob/master/python/ray/experimental/collective/tensor_transport_manager.py>`__ class. There are two examples: `CollectiveTensorTransport <https://github.com/ray-project/ray/blob/master/python/ray/experimental/collective/collective_tensor_transport.py>`__ and `NixlTensorTransport <https://github.com/ray-project/ray/blob/master/python/ray/experimental/collective/nixl_tensor_transport.py>`__.
5. Register the transport class in the `get_tensor_transport_manager <https://github.com/ray-project/ray/blob/master/python/ray/experimental/collective/util.py>`__ function.