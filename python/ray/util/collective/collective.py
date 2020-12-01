"""APIs exposed under the namespace ray.util.collective."""
import logging

import ray
from ray.util.collective import types

# Get the availability information first by importing information
_MPI_AVAILABLE = True
_NCCL_AVAILABLE = True

try:
    from ray.util.collective.collective_group.mpi_collective_group import MPIGroup
except ImportError:
    _MPI_AVAILABLE = False

try:
    from ray.util.collective.collective_group.nccl_collective_group import NCCLGroup
except ImportError:
    _NCCL_AVAILABLE = False


def nccl_available():
    return _NCCL_AVAILABLE


def mpi_available():
    return _MPI_AVAILABLE


def _backend_check(backend):
    if backend == 'mpi':
        if not mpi_available():
            raise RuntimeError()
        raise NotImplementedError()
    elif backend == 'nccl':
        if not nccl_available():
            raise RuntimeError()


@ray.remote
class NCCLUniqueIDStore(object):
    """NCCLUniqueID. This class should be used as a named actor."""
    def __init__(self, name):
        self.name = name
        self.nccl_id = None

    def set_id(self, uid):
        self.nccl_id = uid
        return self.nccl_id

    def get_id(self):
        if not self.nccl_id:
            logging.warning('The NCCL ID has not been set yet for store {}'.format(self.name))
        return self.nccl_id


class GroupManager(object):
    """
    Use this class to manage the collective groups we created so far;

    """
    def __init__(self):
        """Put some necessary meta information here."""
        self._name_group_map = {}
        self._group_name_map = {}

    def create_collective_group(self,
                                backend,
                                world_size,
                                rank,
                                group_name):
        """
        The only entry to create new collective groups and register to the manager.

        Put the registration and the group information into the manager metadata as well.
        """
        if backend == 'mpi':
            raise NotImplementedError()
        elif backend == 'nccl':
            # create the ncclUniqueID
            if rank == 0:
                import cupy.cuda.nccl as nccl
                group_uid = nccl.get_unique_id()
                store_name = group_name + types.named_actor_suffix
                store = NCCLUniqueIDStore.options(name=store_name, lifetime="detached").remote()
                ray.wait([store.set_id.remote(group_uid)])

            logging.info('creating NCCL group: {}'.format(group_name))
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
        if group_name not in self._name_group_map:
            return None
        return self._name_group_map[group_name]

    def destroy_collective_group(self, group_name):
        """Group destructor."""
        if group_name not in self._name_group_map:
            logging.warning('The group {} does not exist'.format(group_name))
            return

        # release the collective group resource
        g = self._name_group_map[group_name]

        rank = g.rank
        backend = g.backend()

        # clean up the dicts
        del self._group_name_map[g]
        del self._name_group_map[group_name]

        if backend == 'nccl':
            # release the named actor
            if rank == 0:
                store_name = group_name + types.named_actor_suffix
                store = ray.get_actor(store_name)
                ray.wait([store.__ray_terminate__.remote()])
                ray.kill(store)
        g.destroy()


class GroupManager_2(object):
    """
    Use this class to manage the collective groups we created so far;
    For interface 2.2

    """
    def __init__(self):
        """Put some necessary meta information here."""
        self._name_group_map = {}
        self._group_name_map = {}

    def create_collective_group(self,
                                backend,
                                world_size,
                                rank,
                                group_name):
        """
        The only entry to create new collective groups and register to the manager.

        Put the registration and the group information into the manager metadata as well.
        """
        if backend == 'mpi':
            raise NotImplementedError()
        elif backend == 'nccl':
            # create the ncclUniqueID
            #if rank == 0:
            import cupy.cuda.nccl as nccl
            group_uid = nccl.get_unique_id()

            for r in rank:
                g = NCCLGroup(world_size, r, group_name)
                self._name_group_map[group_name] = g
                self._group_name_map[g] = group_name
        return self._name_group_map[group_name]

    def is_group_exist(self, group_name):
        if group_name in self._name_group_map:
            return True
        return False

    def get_group_by_name(self, group_name):
        """Get the collective group handle by its name."""
        if group_name not in self._name_group_map:
            return None
        return self._name_group_map[group_name]

    def destroy_collective_group(self, group_name):
        """Group destructor."""
        if group_name not in self._name_group_map:
            logging.warning('The group {} does not exist'.format(group_name))
            return

        # release the collective group resource
        g = self._name_group_map[group_name]

        rank = g.rank
        backend = g.backend()

        # clean up the dicts
        del self._group_name_map[g]
        del self._name_group_map[group_name]

        if backend == 'nccl':
            # release the named actor
            if rank == 0:
                store_name = group_name + types.named_actor_suffix
                store = ray.get_actor(store_name)
                ray.wait([store.__ray_terminate__.remote()])
                ray.kill(store)
        g.destroy()

_group_mgr = GroupManager()
_group_mgr2 = GroupMagager_2()

def init_collective_group(backend,
                          world_size,
                          rank,
                          group_name='default'):
    """
    Initialize a collective group inside an actor process.

    This is an
    Args:
        backend:
        world_size:
        rank:
        group_name:

    Returns:

    """
    _backend_check(backend)
    global _group_mgr
    # TODO(Hao): implement a group auto-counter.
    if not group_name:
        raise ValueError('group_name: {},  needs to be a string.'.format(group_name))

    if _group_mgr.is_group_exist(group_name):
        raise RuntimeError('Trying to initialize a group twice.')

    assert(world_size > 0)
    assert(rank >= 0 )
    assert(rank < world_size)
    _group_mgr.create_collective_group(backend, world_size, rank, group_name)


def declare_collective_group(actors, group_options):
    """
    # Frontend API #2:
    # This API is supported to work in the driver program - the users declare a list of actors as a collective group
    # @Dacheng: This API is not in the right shape, need to work with ray.remote(), please figure out.
    Args:
        actors:
        group_options:

    Returns:

    """
    global _group_mgr_2
    try:
        group_name = group_options["group_name"]
        world_size = group_options["world_size"]
        rank = group_options["rank"]
        backend = group_options["backend"]
    except:
        raise ValueError("group options incomplete")
    
    _backend_check(backend)
    if _group_mgr_2.is_group_exist(group_name):
        raise RuntimeError('Trying to initialize a group twice.')

    assert(world_size > 0)
    assert(rank >= 0 and rank < world_size)

    _group_mgr_2.create_collective_group(backend, world_size, rank, group_name, actors)

def allreduce(tensor,
              group_name,
              op=types.ReduceOp.SUM):
    """
    Collective allreduce the tensor across the group with name group_name.

    Args:
        tensor:
        group_name (string):
        op:

    Returns:
        None
    """
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
    if not _group_mgr.is_group_exist(group_name):
        raise ValueError('The collective group {} is not initialized.'.format(group_name))
    # TODO(Hao): check if this rank is in the group.
    g = _group_mgr.get_group_by_name(group_name)
    return g


