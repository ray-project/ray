"""APIs exposed under the namespace ray.util.collective."""
import logging
import os

import ray
from ray.util.collective import types
from ray.util.collective.const import get_nccl_store_name

# Get the availability information first by importing information
_MPI_AVAILABLE = False
_NCCL_AVAILABLE = True

# try:
#     from ray.util.collective.collective_group.mpi_collective_group
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
    """
    Use this class to manage the collective groups we created so far.

    """

    def __init__(self):
        """Put some necessary meta information here."""
        self._name_group_map = {}
        self._group_name_map = {}

    def create_collective_group(self, backend, world_size, rank, group_name):
        """
        The entry to create new collective groups and register in the manager.

        Put the registration and the group information into the manager
        metadata as well.
        """
        backend = types.Backend(backend)
        if backend == types.Backend.MPI:
            raise NotImplementedError()
        elif backend == types.Backend.NCCL:
            # create the ncclUniqueID
            if rank == 0:
                import cupy.cuda.nccl as nccl
                group_uid = nccl.get_unique_id()
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
        if group_name in self._name_group_map:
            return True
        return False

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
    if not _group_mgr.is_group_exist(group_name):
        return False
    return True


def init_collective_group(world_size: int,
                          rank: int,
                          backend=types.Backend.NCCL,
                          group_name: str = "default"):
    """
    Initialize a collective group inside an actor process.

    This is an
    Args:
        world_size:
        rank:
        backend:
        group_name:

    Returns:
        None
    """
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

def declare_collective_group(actors, group_options):
    """
    Declare a list of actors in a collective group with group options. This function
    should be called in a driver process.
    Args:
        actors (list): a list of actors to be set in a collective group.
        group_options (dict): a dictionary that contains group_name(str), world_size(int),
                              rank(list of int, e.g. [0,1] means the first actor is rank 0, and
                              the second actor is rank 1), backend(str)
    """
    try:
        group_name = group_options["group_name"]
        world_size = group_options["world_size"]
        rank = group_options["rank"]
        backend = group_options["backend"]
    except:
        raise ValueError("group options incomplete.")

    backend = types.Backend(backend)
    _check_backend_availability(backend)

    name = "info" + group_name
    try:
        ray.get_actor(name)
        raise RuntimeError('Trying to initialize a group twice.')
    except:
        pass

    if len(rank) != len(actors):
        raise RuntimeError("Each actor should correspond to one rank.")

    if set(rank) != set(range(len(rank))):
        raise RuntimeError("Rank must be a permutation from 0 to len-1.")

    assert world_size > 0
    assert all(rank) >= 0 and all(rank) < world_size
    
    from ray.util.collective.util import Info
    # store the information into a NamedActor that can be accessed later/
    name = "info" + group_name
    actors_id = [a._ray_actor_id for a in actors]
    info = Info.options(name=name, lifetime="detached").remote()
    ray.wait([info.set_info.remote(actors_id, world_size, rank, backend)])

def destroy_collective_group(group_name: str = "default") -> None:
    """Destroy a collective group given its group name."""
    global _group_mgr
    _group_mgr.destroy_collective_group(group_name)


def get_rank(group_name: str = "default") -> int:
    """
    Return the rank of this process in the given group.

    Args:
        group_name (str): the name of the group to query

    Returns:
        the rank of this process in the named group,
        -1 if the group does not exist or the process does
        not belong to the group.
    """
    if not is_group_initialized(group_name):
        return -1
    g = _group_mgr.get_group_by_name(group_name)
    return g.rank


def get_world_size(group_name="default") -> int:
    """
    Return the size of the collective gropu with the given name.

    Args:
        group_name: the name of the group to query

    Returns:
        The world size of the collective group，
        -1 if the group does not exist or the process does
        not belong to the group.
    """
    if not is_group_initialized(group_name):
        return -1
    g = _group_mgr.get_group_by_name(group_name)
    return g.world_size


def allreduce(tensor, group_name: str, op=types.ReduceOp.SUM):
    """
    Collective allreduce the tensor across the group with name group_name.

    Args:
        tensor:
        group_name (str):
        op:

    Returns:
        None
    """
    _check_single_tensor_input(tensor)
    g = _check_and_get_group(group_name)
    opts = types.AllReduceOptions
    opts.reduceOp = op
    g.allreduce(tensor, opts)


def barrier(group_name):
    """
    Barrier all collective process in the group with name group_name.

    Args:
        group_name (string):

    Returns:
        None
    """
    g = _check_and_get_group(group_name)
    g.barrier()


def _check_and_get_group(group_name):
    """Check the existence and return the group handle."""
    global _group_mgr
    if not is_group_initialized(group_name):
        # try loading from remote info store
        try:
            # if the information is stored in an Info object, get and create the group.
            name = "info" + group_name
            mgr = ray.get_actor(name=name)
            ids, world_size, rank, backend = ray.get(mgr.get_info.remote())
            worker = ray.worker.global_worker
            id_ = worker.core_worker.get_actor_id()
            r = rank[ids.index(id_)]
            _group_mgr.create_collective_group(backend, world_size, r, group_name)
        except:
            # check if this group is initialized using options()
            if os.environ["collective_group_name"] == group_name:
                rank = int(os.environ["collective_rank"])
                world_size = int(os.environ["collective_world_size"])
                backend = os.environ["collective_backend"]
                _group_mgr.create_collective_group(backend, world_size, rank, group_name)
            else:
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
    if types.numpy_available():
        if isinstance(tensor, types.np.ndarray):
            return
    if types.cupy_available():
        if isinstance(tensor, types.cp.ndarray):
            return
    if types.torch_available():
        if isinstance(tensor, types.th.Tensor):
            return
    raise RuntimeError("Unrecognized tensor type. Supported types are: "
                       "np.ndarray, torch.Tensor, cupy.ndarray.")
