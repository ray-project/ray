"""APIs exposed under the namespace ray.util.collective."""
import ray

# Get the availability information first by importing information
_MPI_AVAILABLE = True
_NCCL_AVAILABLE = True

try:
    from collective.collectivegoup.mpi_collective_group import MPICollectiveGroup
except ImportError:
    _MPI_AVAILABLE = False

try:
    from collective.collectivegoup.nccl_collective_group import NCCLCollectiveGroup
except ImportError:
    _NCCL_AVAILABLE = False


def nccl_available():
    return _NCCL_AVAILABLE

def mpi_available():
    return _MPI_AVAILABLE


@ray.remote
class NCCLUniqueIDStore(object):
    """NCCLUniqueID. This class should be used as a named actor."""
    def __init__(self):
        self.nccl_id = None

    def set_id(self, id):
        self.nccl_id = id

    def get_id(self):
        return self.nccl_id


class GroupManager(object):
    """
    Use this class to manage the collective groups we created so far;

    """
    def __init__(self):
        """Put some necessary meta information here."""
        self._default_group = None

    def get_default_group(self):
        pass

    def set_default_group(self):
        pass

    def create_collective_group(self,
                                backend,
                                group_name,
                                world_size,
                                rank):
        """
        The only entry to create new collective groups, construct CollectiveGroup here.

        Put the registration and the group information into the manager metadata as well.
        """
        pass

    def destroy_collective_group(self, name):
        """Group destructor."""
        pass

_group_mgr = GroupManager()


# Frontend API #1:
# This API is supposed to work within the actor or task program:
# See the RFC for an example.
def init_collective_group(backend,
                          group_name='default',
                          world_size=-1,
                          rank=-1):
    # do some check on the validaty of the arguments.
    # see: https://github.com/pytorch/pytorch/blob/master/torch/distributed/distributed_c10d.py
    if backend == 'mpi':
        if not mpi_available():
            raise RuntimeError()
        raise NotImplementedError()
    elif backend == 'nccl':
        if not nccl_available():
            raise RuntimeError()

    global _group_mgr
    _group_mgr.create_collective_group(group_name, world_size, rank)


# Frontend API #2:
# This API is supported to work in the driver program - the users declare a list of actors as a collective group
# @Dacheng: This API is not in the right shape, need to work with ray.remote(), please figure out.
def declare_collective_group(actors, group_options):
    pass


#collective APIs:
def send(tensor, dst, group_name='default'):
    pass

def recv(tensor, src, group_name='default'):
    pass

def broadcast(tensor, src, group_name='default'):
    pass

def allreduce(tensor, op=SUM, group_name='default'):
    pass

def reduce(tensor, dst, op=SUM, group_name='default'):
    pass

def allgather(tensor_list, tensor, gropu_name='default'):
    pass

def gather(tensor, gather_list=None, dst=0, group_name='default'):
    pass

def scatter(tensor, scatter_list=None, src=0, group_name='default'):
    pass

def barrier(group_name='default'):
    pass
