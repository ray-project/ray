"""APIs exposed under the namespace ray.util.collective."""
import logging
import os
from typing import List

import numpy as np
import ray
from ray.util.collective import types

_MPI_AVAILABLE = False
_NCCL_AVAILABLE = True

# try:
#     from ray.util.collective.collective_group.mpi_collective_group \
#     import MPIGroup
# except ImportError:
#     _MPI_AVAILABLE = False
try:
    from ray.util.collective.collective_group import NCCLGroup
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
        # clean up the dicts
        del self._group_name_map[g]
        del self._name_group_map[group_name]
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


def declare_collective_group(actors,
                             world_size: int,
                             ranks: List[int],
                             backend=types.Backend.NCCL,
                             group_name: str = "default"):
    """Declare a list of actors as a collective group.

    Note: This function should be called in a driver process.

    Args:
        actors (list): a list of actors to be set in a collective group.
        group_options (dict): a dictionary that contains group_name(str),
                              world_size(int), rank(list of int, e.g. [0,1]
                              means the first actor is rank 0, and the second
                              actor is rank 1), backend(str).
    """
    backend = types.Backend(backend)
    _check_backend_availability(backend)

    name = "info_" + group_name
    try:
        ray.get_actor(name)
        raise RuntimeError("Trying to initialize a group twice.")
    except ValueError:
        pass

    if len(ranks) != len(actors):
        raise RuntimeError(
            "Each actor should correspond to one rank. Got '{}' "
            "ranks but '{}' actors".format(len(ranks), len(actors)))

    if set(ranks) != set(range(len(ranks))):
        raise RuntimeError(
            "Ranks must be a permutation from 0 to '{}'. Got '{}'.".format(
                len(ranks), "".join([str(r) for r in ranks])))

    assert world_size > 0
    assert all(ranks) >= 0 and all(ranks) < world_size

    # avoid a circular dependency
    from ray.util.collective.util import Info
    # store the information into a NamedActor that can be accessed later/
    name = "info_" + group_name
    actors_id = [a._ray_actor_id for a in actors]
    info = Info.options(name=name, lifetime="detached").remote()
    ray.get([info.set_info.remote(actors_id, world_size, ranks, backend)])


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


def send(tensor, dst_rank: int, group_name: str = "default"):
    """Send a tensor to a remote processes synchronously.

    Args:
        tensor: the tensor to send.
        dst_rank (int): the rank of the destination process.
        group_name (str): the name of the collective group.

    Returns:
        None
    """
    _check_single_tensor_input(tensor)
    g = _check_and_get_group(group_name)
    _check_rank_valid(g, dst_rank)
    if dst_rank == g.rank:
        raise RuntimeError(
            "The destination rank '{}' is self.".format(dst_rank))
    g.send(tensor, dst_rank)


def recv(tensor, src_rank: int, group_name: str = "default"):
    """Receive a tensor from a remote process synchronously.

    Args:
        tensor: the received tensor.
        src_rank (int): the rank of the source process.
        group_name (str): the name of the collective group.

    Returns:
        None
    """
    _check_single_tensor_input(tensor)
    g = _check_and_get_group(group_name)
    _check_rank_valid(g, src_rank)
    if src_rank == g.rank:
        raise RuntimeError(
            "The destination rank '{}' is self.".format(src_rank))
    g.recv(tensor, src_rank)


def _check_and_get_group(group_name):
    """Check the existence and return the group handle."""
    _check_inside_actor()
    global _group_mgr
    if not is_group_initialized(group_name):
        # try loading from remote info store
        try:
            # if the information is stored in an Info object,
            # get and create the group.
            name = "info_" + group_name
            mgr = ray.get_actor(name=name)
            ids, world_size, rank, backend = ray.get(mgr.get_info.remote())
            worker = ray.worker.global_worker
            id_ = worker.core_worker.get_actor_id()
            r = rank[ids.index(id_)]
            _group_mgr.create_collective_group(backend, world_size, r,
                                               group_name)
        except ValueError as exc:
            # check if this group is initialized using options()
            if "collective_group_name" in os.environ and \
                    os.environ["collective_group_name"] == group_name:
                rank = int(os.environ["collective_rank"])
                world_size = int(os.environ["collective_world_size"])
                backend = os.environ["collective_backend"]
                _group_mgr.create_collective_group(backend, world_size, rank,
                                                   group_name)
            else:
                raise RuntimeError(
                    "The collective group '{}' is not "
                    "initialized in the process.".format(group_name)) from exc
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
    """Check the rank: 0 <= rank < world_size."""
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
