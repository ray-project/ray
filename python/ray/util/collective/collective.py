"""APIs exposed under the namespace ray.util.collective."""
import logging

import numpy as np
import ray
from ray.util.collective import types
from ray.util.collective.const import get_nccl_store_name

_MPI_AVAILABLE = False
_NCCL_AVAILABLE = True

# try:
#     from ray.util.collective.collective_group.mpi_collective_group \
#     import MPIGroup
# except ImportError:
#     _MPI_AVAILABLE = False
try:
    from ray.util.collective.collective_group import NCCLGroup
    from ray.util.collective.collective_group import nccl_util
except ImportError:
    _NCCL_AVAILABLE = False

logger = logging.getLogger(__name__)


def nccl_available():
    return _NCCL_AVAILABLE


def mpi_available():
    return _MPI_AVAILABLE


class GroupManager(object):
    """Use this class to manage the collective groups we created so far.

    Each process will have an instance of `GroupManager`. Each process
    could belong to multiple collective groups. The membership information
    and other metadata are stored in the global `_group_mgr` object.
    """

    def __init__(self):
        self._name_group_map = {}
        self._group_name_map = {}

    def create_collective_group(self, backend, world_size, rank, group_name):
        """The entry to create new collective groups in the manager.

        Put the registration and the group information into the manager
        metadata as well.
        """
        backend = types.Backend(backend)
        if backend == types.Backend.MPI:
            raise NotImplementedError()
        elif backend == types.Backend.NCCL:
            # create the ncclUniqueID
            if rank == 0:
                # availability has been checked before entering here.
                group_uid = nccl_util.get_nccl_unique_id()
                store_name = get_nccl_store_name(group_name)
                # Avoid a potential circular dependency in ray/actor.py
                from ray.util.collective.util import NCCLUniqueIDStore
                store = NCCLUniqueIDStore.options(
                    name=store_name, lifetime="detached").remote(store_name)
                ray.wait([store.set_id.remote(group_uid)])

            logger.debug("creating NCCL group: '{}'".format(group_name))
            g = NCCLGroup(world_size, rank, group_name)
            self._name_group_map[group_name] = g
            self._group_name_map[g] = group_name
        return self._name_group_map[group_name]

    def is_group_exist(self, group_name):
        return group_name in self._name_group_map

    def get_group_by_name(self, group_name):
        """Get the collective group handle by its name."""
        if not self.is_group_exist(group_name):
            logger.warning(
                "The group '{}' is not initialized.".format(group_name))
            return None
        return self._name_group_map[group_name]

    def destroy_collective_group(self, group_name):
        """Group destructor."""
        if not self.is_group_exist(group_name):
            logger.warning("The group '{}' does not exist.".format(group_name))
            return

        # release the collective group resource
        g = self._name_group_map[group_name]
        rank = g.rank
        backend = g.backend()

        # clean up the dicts
        del self._group_name_map[g]
        del self._name_group_map[group_name]
        if backend == types.Backend.NCCL:
            # release the named actor
            if rank == 0:
                store_name = get_nccl_store_name(group_name)
                store = ray.get_actor(store_name)
                ray.wait([store.__ray_terminate__.remote()])
                ray.kill(store)
        # Release the communicator resources
        g.destroy_group()


_group_mgr = GroupManager()


def is_group_initialized(group_name):
    """Check if the group is initialized in this process by the group name."""
    return _group_mgr.is_group_exist(group_name)


def init_collective_group(world_size: int,
                          rank: int,
                          backend=types.Backend.NCCL,
                          group_name: str = "default"):
    """Initialize a collective group inside an actor process.

    Args:
        world_size (int): the total number of processed in the group.
        rank (int): the rank of the current process.
        backend: the CCL backend to use, NCCL or MPI.
        group_name (str): the name of the collective group.

    Returns:
        None
    """
    _check_inside_actor()
    backend = types.Backend(backend)
    _check_backend_availability(backend)
    global _group_mgr
    # TODO(Hao): implement a group auto-counter.
    if not group_name:
        raise ValueError("group_name '{}' needs to be a string."
                         .format(group_name))

    if _group_mgr.is_group_exist(group_name):
        raise RuntimeError("Trying to initialize a group twice.")

    assert (world_size > 0)
    assert (rank >= 0)
    assert (rank < world_size)
    _group_mgr.create_collective_group(backend, world_size, rank, group_name)


def destroy_collective_group(group_name: str = "default") -> None:
    """Destroy a collective group given its group name."""
    _check_inside_actor()
    global _group_mgr
    _group_mgr.destroy_collective_group(group_name)


def get_rank(group_name: str = "default") -> int:
    """Return the rank of this process in the given group.

    Args:
        group_name (str): the name of the group to query

    Returns:
        the rank of this process in the named group,
        -1 if the group does not exist or the process does
        not belong to the group.
    """
    _check_inside_actor()
    if not is_group_initialized(group_name):
        return -1
    g = _group_mgr.get_group_by_name(group_name)
    return g.rank


def get_world_size(group_name: str = "default") -> int:
    """Return the size of the collective gropu with the given name.

    Args:
        group_name: the name of the group to query

    Returns:
        The world size of the collective group，
        -1 if the group does not exist or the process does
        not belong to the group.
    """
    _check_inside_actor()
    if not is_group_initialized(group_name):
        return -1
    g = _group_mgr.get_group_by_name(group_name)
    return g.world_size


def allreduce(tensor, group_name: str = "default", op=types.ReduceOp.SUM):
    """Collective allreduce the tensor across the group.

    Args:
        tensor: the tensor to be all-reduced on this process.
        group_name (str): the collective group name to perform allreduce.
        op: The reduce operation.

    Returns:
        None
    """
    _check_single_tensor_input(tensor)
    g = _check_and_get_group(group_name)
    opts = types.AllReduceOptions
    opts.reduceOp = op
    g.allreduce(tensor, opts)


def barrier(group_name: str = "default"):
    """Barrier all processes in the collective group.

    Args:
        group_name (str): the name of the group to barrier.

    Returns:
        None
    """
    g = _check_and_get_group(group_name)
    g.barrier()


def reduce(tensor,
           dst_rank: int = 0,
           group_name: str = "default",
           op=types.ReduceOp.SUM):
    """Reduce the tensor across the group to the destination rank.

    Args:
        tensor: the tensor to be reduced on this process.
        dst_rank: the rank of the destination process.
        group_name: the collective group name to perform reduce.
        op: The reduce operation.

    Returns:
        None
    """
    _check_single_tensor_input(tensor)
    g = _check_and_get_group(group_name)

    # check dst rank
    _check_rank_valid(g, dst_rank)
    opts = types.ReduceOptions()
    opts.reduceOp = op
    opts.root_rank = dst_rank
    g.reduce(tensor, opts)


def broadcast(tensor, src_rank: int = 0, group_name: str = "default"):
    """Broadcast the tensor from a source process to all others.

    Args:
        tensor: the tensor to be broadcasted (src) or received (destination).
        src_rank: the rank of the source process.
        group_name: he collective group name to perform broadcast.

    Returns:
        None
    """
    _check_single_tensor_input(tensor)
    g = _check_and_get_group(group_name)

    # check src rank
    _check_rank_valid(g, src_rank)
    opts = types.BroadcastOptions()
    opts.root_rank = src_rank
    g.broadcast(tensor, opts)


def allgather(tensor_list: list, tensor, group_name: str = "default"):
    """Allgather tensors from each process of the group into a list.

    Args:
        tensor_list (list): the results, stored as a list of tensors.
        tensor: the tensor (to be gathered) in the current process
        group_name: the name of the collective group.

    Returns:
        None
    """
    _check_single_tensor_input(tensor)
    _check_tensor_list_input(tensor_list)
    g = _check_and_get_group(group_name)
    if len(tensor_list) != g.world_size:
        # Typically CLL lib requires len(tensor_list) >= world_size;
        # Here we make it more strict: len(tensor_list) == world_size.
        raise RuntimeError(
            "The length of the tensor list operands to allgather "
            "must not be equal to world_size.")
    opts = types.AllGatherOptions()
    g.allgather(tensor_list, tensor, opts)


def reducescatter(tensor,
                  tensor_list: list,
                  group_name: str = "default",
                  op=types.ReduceOp.SUM):
    """Reducescatter a list of tensors across the group.

    Reduce the list of the tensors across each process in the group, then
    scatter the reduced list of tensors -- one tensor for each process.

    Args:
        tensor: the resulted tensor on this process.
        tensor_list (list): The list of tensors to be reduced and scattered.
        group_name (str): the name of the collective group.
        op: The reduce operation.

    Returns:
        None
    """
    _check_single_tensor_input(tensor)
    _check_tensor_list_input(tensor_list)
    g = _check_and_get_group(group_name)
    if len(tensor_list) != g.world_size:
        raise RuntimeError(
            "The length of the tensor list operands to reducescatter "
            "must not be equal to world_size.")
    opts = types.ReduceScatterOptions()
    opts.reduceOp = op
    g.reducescatter(tensor, tensor_list, opts)


def _check_and_get_group(group_name):
    """Check the existence and return the group handle."""
    _check_inside_actor()
    if not is_group_initialized(group_name):
        raise RuntimeError("The collective group '{}' is not "
                           "initialized in the process.".format(group_name))
    g = _group_mgr.get_group_by_name(group_name)
    return g


def _check_backend_availability(backend: types.Backend):
    """Check whether the backend is available."""
    if backend == types.Backend.MPI:
        if not mpi_available():
            raise RuntimeError("MPI is not available.")
    elif backend == types.Backend.NCCL:
        if not nccl_available():
            raise RuntimeError("NCCL is not available.")


def _check_single_tensor_input(tensor):
    """Check if the tensor is with a supported type."""
    if isinstance(tensor, np.ndarray):
        return
    if types.cupy_available():
        if isinstance(tensor, types.cp.ndarray):
            return
    if types.torch_available():
        if isinstance(tensor, types.th.Tensor):
            return
    raise RuntimeError("Unrecognized tensor type '{}'. Supported types are: "
                       "np.ndarray, torch.Tensor, cupy.ndarray.".format(
                           type(tensor)))


def _check_inside_actor():
    """Check if currently it is inside a Ray actor/task."""
    worker = ray.worker.global_worker
    if worker.mode == ray.WORKER_MODE:
        return
    else:
        raise RuntimeError("The collective APIs shall be only used inside "
                           "a Ray actor or task.")


def _check_rank_valid(g, rank: int):
    if rank < 0:
        raise ValueError("rank '{}' is negative.".format(rank))
    if rank > g.world_size:
        raise ValueError("rank '{}' is greater than world size "
                         "'{}'".format(rank, g.world_size))


def _check_tensor_list_input(tensor_list):
    """Check if the input is a list of supported tensor types."""
    if not isinstance(tensor_list, list):
        raise RuntimeError("The input must be a list of tensors. "
                           "Got '{}'.".format(type(tensor_list)))
    if not tensor_list:
        raise RuntimeError("Got an empty list of tensors.")
    for t in tensor_list:
        _check_single_tensor_input(t)
