import logging
import ray
import cupy

from ray.util.collective.collective_group import nccl_util
from ray.util.collective.collective_group.base_collective_group import BaseGroup
from ray.util.collective.types import AllReduceOptions, BarrierOptions, named_actor_suffix


# TODO(Hao):
# (1) stream management, instead of using the default stream, using a dedicate stream
# (2) communicator management, adding a caching mechanism to enable


class NCCLGroup(BaseGroup):
    def __init__(self, world_size, rank, group_name):
        """Init an NCCL collective group."""
        super(NCCLGroup, self).__init__(world_size, rank, group_name)
        self._nccl_uid_store = None
        self._nccl_uid = None

        # TODO(Hao): change this to a be a cache
        self._nccl_comm = None

        # Check NCCL version
        if nccl_util.get_nccl_build_version() < 2000:
            raise RuntimeError('NCCL in Ray requires NCCL>=2.0.')

        # TODO(Hao): check version here
        if nccl_util.get_nccl_runtime_version() < 2704:
            logging.warning('NCCL send/recv calls requires NCCL>=2.7.4')

        # Setup the nccl uid using the store
        self._init_nccl_unique_id()

        # Setup a tensor for barrier calls
        self._barrier_tensor = cupy.array([1])

    def _init_nccl_unique_id(self):
        """Init the NCCL unique ID required for setting up NCCL communicator."""
        # using group_name to query the UniqueIDActor
        unique_actor_name = self.group_name + named_actor_suffix

        # Assuming this named actor has been created.
        print('reach here...1')
        self._nccl_uid_store = ray.get_actor(unique_actor_name)
        print('reach here...1')
        self._nccl_uid = ray.get(self._nccl_uid_store.get_id.remote())

    @property
    def nccl_uid(self):
        return self._nccl_uid

    def destory_group(self):
        """Destroy the group and release the NCCL communicators safely."""
        if self._nccl_comm is not None:
            self.barrier()
            # We also need a barrier call here.
            stream = self._get_cuda_stream()
            stream.synchronize()
            # destroy the communicator
            self._nccl_comm.destory()
            self._nccl_comm = None
        super(NCCLGroup, self).destroy_group()

    @classmethod
    def backend(cls):
        return 'nccl'

    def allreduce(self, tensor, allreduce_options=AllReduceOptions()):
        """
        AllReduce a list of tensors following options.

        Args:
            tensor: the tensor to be reduced, each tensor locates on a GPU
            allreduce_options:

        Returns:
        """
        # obtain the communicator
        comm = self._get_nccl_communicator()
        # obtain the stream: using default stream by now
        # TODO(Hao): implement a simple stream manager here
        stream = self._get_cuda_stream()

        dtype = nccl_util.get_nccl_tensor_dtype(tensor)
        ptr = nccl_util.get_tensor_ptr(tensor)
        n_elems = nccl_util.get_tensor_n_elements(tensor)
        reduce_op = nccl_util.get_nccl_reduce_op(allreduce_options.reduceOp)

        # in-place allreduce
        comm.allReduce(ptr,
                       ptr,
                       n_elems,
                       dtype,
                       reduce_op,
                       stream.ptr)

    def barrier(self, barrier_options=BarrierOptions()):
        """
        Blocks until all processes in the communicator have reached this barrier call.

        Args:
            barrier_options:

        Returns:
        """
        self.allreduce(self._barrier_tensor)

    def _get_nccl_communicator(self):
        """Create a new NCCL communicator for the collective task, or using a cached communicator."""
        # TODO(Hao): later change this to use device keys and query from cache.
        # TODO(Hao): implement a thin wrapper
        if not self._nccl_comm:
            self._nccl_comm = nccl_util.create_nccl_communicator(
                self.world_size, self.nccl_uid, self.rank)
        return self._nccl_comm

    def _get_cuda_stream(self):
        """Obtain an idle stream from a stream pool for the collective task."""
        # TODO: implement a simple stream manager.
        return cupy.cuda.Stream.null

    # def _collective_call(self, *args):
    #     """Private method to encapsulate all collective calls"""
    #     pass
