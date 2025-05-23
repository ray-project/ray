from ray.experimental.communicator import Communicator


_local_communicator_manager: "Optional[LocalCommunicatorManager]" = None
_local_communicator_manager_lock = threading.Lock()

_remote_communicator_manager: "Optional[RemoteCommunicatorManager]" = None
_remote_communicator_manager_lock = threading.Lock()


class RemoteCommunicatorManager:
    # Handles to communicators that we created. Key is a user-provided
    # name or UUID.
    remote_communicators: Dict[str, CommunicatorHandle]

    def __init__(self):
        pass

    @staticmethod
    def get() -> "RemoteCommunicatorManager":
        global _remote_communicator_manager
        with _remote_communicator_manager_lock:
            if _remote_communicator_manager is None:
                _remote_communicator_manager = RemoteCommunicatorManager()
            return _remote_communicator_manager

    def add_communicator(self, comm: Communicator):
        self.communicators[comm.name] = comm

    def remove_communicator(self, name: str):
        self.communicators.pop(name)

    def add_remote_communicator(self, comm_handle: CommunicatorHandle):
        self.remote_communicators[comm_handle.name] = comm_handle

    def remove_remote_communicator(self, name: str):
        self.remote_communicators.pop(name)



class LocalCommunicatorManager:
    """
    Class to manage all communicators that we are a member of.
    """
    # Communicators that we are a member of. Key is a user-provided
    # name or UUID.
    communicators: Dict[str, Communicator]

    def __init__(self):
        pass

    @staticmethod
    def get() -> "LocalCommunicatorManager":
        global _local_communicator_manager
        with _local_communicator_manager_lock:
            if _local_communicator_manager is None:
                _local_communicator_manager = LocalCommunicatorManager()
            return _local_communicator_manager

    def add_communicator(self, comm: Communicator):
        self.communicators[comm.name] = comm

    def remove_communicator(self, name: str):
        self.communicators.pop(name)

    def add_remote_communicator(self, comm_handle: CommunicatorHandle):
        self.remote_communicators[comm_handle.name] = comm_handle

    def remove_remote_communicator(self, name: str):
        self.remote_communicators.pop(name)


def _do_init_communicator(
    self,
    name,
    world_size,
    comm_id,
    rank,
    actor_handles,
    use_communication_streams,
    custom_communicator: Optional[Communicator] = None,
):
    if not custom_communicator:
        assert (
            AcceleratorContext.get().accelerator_count > 0
        ), "Actors participating in Communication group must have at least one Accelerator assigned"

    ctx = ChannelContext.get_current()
    if custom_communicator is not None:
        custom_communicator.initialize(rank)
        ctx.communicators[name] = custom_communicator
    else:
        # default to NcclGroup
        ctx.communicators[name] = AcceleratorContext.get().create_communicator(
            world_size,
            comm_id,
            rank,
            actor_handles,
            AcceleratorContext.get().current_stream(),
            use_communication_streams,
        )


def _do_destroy_communicator(self, name):
    ctx = ChannelContext.get_current()
    if name not in ctx.communicators:
        return
    ctx.communicators[name].destroy()

    # Keep the NCCL group in the map after destruction in case there is still a
    # task loop running.


def _do_check_has_accelerators(self) -> str:
    return AcceleratorContext.get().accelerator_count > 0


def _do_register_accelerator_context(self, name: str, communicator: Type[Communicator]):
    register_accelerator_context(name, communicator)


def _do_get_unique_communication_id(self) -> bool:
    return AcceleratorContext.get().generate_communicator_id()


def _get_ranks(
    actors: List[ray.actor.ActorHandle], custom_nccl_group: Optional[Communicator]
) -> List[int]:
    """
    Get ranks for the NCCL group to use. If custom_nccl_group is specified,
    return the ranks of the actors in the custom NCCL group, in the same
    order of the actors; otherwise, return list(range(len(actors))).

    Args:
        actors: A list of actors that participate in the NCCL group.
        custom_nccl_group: The custom NCCL group to use.
    """
    if custom_nccl_group is None:
        return list(range(len(actors)))

    assert len(actors) == custom_nccl_group.get_world_size(), (
        "The world size of the custom NCCL group does not match the number "
        "of actors."
    )
    ranks = []
    for actor in actors:
        rank = custom_nccl_group.get_rank(actor)
        assert rank not in ranks, "Duplicate rank in custom NCCL group"
        ranks.append(rank)
    assert custom_nccl_group.get_world_size() == len(actors), (
        "The world size of the custom NCCL group "
        f"({custom_nccl_group.get_world_size()}) "
        "does not match the number of actors "
        f"({len(actors)})."
    )
    return ranks


def _init_communicator(
    actors: List[ray.actor.ActorHandle],
    name: Optional[str] = None,
    custom_communicator: Optional[Communicator] = None,
    use_communication_streams: bool = False,
    accelerator_module_name: Optional[str] = None,
    accelerator_communicator_cls: Optional[Type[Communicator]] = None,
) -> str:
    """
    Initialize a NCCL group with the given actors. If a custom NCCL group is
    provided, then it will be used, otherwise a new NCCL group will be created.

    Args:
        actors: A list of actors that participate in the NCCL group.
        name: An optional name for the communicator. A unique string will be
            generated if none is provided.
        custom_communicator: A custom NCCL group to initialize.
        use_communication_streams: Whether to use dedicated send and recv
                streams for communication. If True, communication and computation
                can be overlapped to improve performance.
        accelerator_module_name: Optional name of the accelerator module to use.
        accelerator_communicator_cls: Optional communicator class for the accelerator.
    """
    ctx = ChannelContext.get_current()

    if name is None:
        name = str(uuid.uuid4())

    is_cpu_communicator = custom_communicator and isinstance(
        custom_communicator, CPUCommunicator
    )

    # Register accelerator context for all actors if accelerator is not default
    if accelerator_module_name and accelerator_communicator_cls:
        if is_accelerator_context_registered():
            ray.get(
                [
                    actor.__ray_call__.remote(
                        _do_register_accelerator_context,
                        accelerator_module_name,
                        accelerator_communicator_cls,
                    )
                    for actor in actors
                ]
            )

    has_accelerators = ray.get(
        [actor.__ray_call__.remote(_do_check_has_accelerators) for actor in actors]
    )
    for has_accelerator, actor in zip(has_accelerators, actors):
        if not has_accelerator and not is_cpu_communicator:
            raise ValueError(
                f"Actor {actor} returns a tensor with type hint "
                'TorchTensor(transport="nccl") or '
                "TorchTensor(transport=nccl_group_handle) "
                "but actor does not have an accelerator assigned by Ray."
            )

    actor_ids = {actor._ray_actor_id for actor in actors}
    assert len(actor_ids) == len(actors), "Actors must be unique"

    # Allocate a communicator ID on one of the actors that will participate in
    # the group. This is in case the driver is not on the same node as one of
    # the NCCL actors.
    comm_id = ray.get(actors[0].__ray_call__.remote(_do_get_unique_communication_id))

    if custom_communicator is not None:
        logger.info(f"Initializing custom NCCL group {name} on actors: {actors}")
    else:
        logger.info(f"Creating NCCL group {name} on actors: {actors}")

    world_size = len(actors)
    ranks = _get_ranks(actors, custom_communicator)
    init_tasks = [
        actor.__ray_call__.remote(
            _do_init_communicator,
            name,
            world_size,
            comm_id,
            rank,
            actors,
            use_communication_streams,
            custom_communicator,
        )
        for rank, actor in zip(ranks, actors)
    ]
    try:
        ray.get(init_tasks, timeout=30)
    except ray.exceptions.GetTimeoutError:
        logger.warning(
            "NCCL group creation not done after 30s. NCCL group creation may be hung."
        )
        ray.get(init_tasks)

    logger.info("NCCL group initialized.")

    if custom_communicator is not None:
        ctx.communicator_handles[name] = CommunicatorHandle(
            actor_handles=custom_communicator.get_actor_handles(),
        )
    else:
        ctx.communicator_handles[name] = CommunicatorHandle(
            actor_handles=actors,
        )

    return name


def _destroy_communicator(name: str) -> None:
    """
    Destroy the NCCL group with the given ID.
    """
    ctx = ChannelContext.get_current()
    if name not in ctx.communicator_handles:
        return

    group = ctx.communicator_handles[name]
    actors = group.get_actor_handles()
    destroy_tasks = [
        actor.__ray_call__.remote(
            _do_destroy_communicator,
            name,
        )
        for actor in actors
    ]

    _, unready = ray.wait(destroy_tasks, timeout=30, num_returns=len(destroy_tasks))
    if unready:
        logger.warning(
            "NCCL group destruction not done after 30s. NCCL group destruction "
            "may be hung."
        )

    del ctx.communicator_handles[name]


def get_collective_groups(actors: List[ActorHandle],
                          backend: Optional[str] = None) -> List[CommunicatorHandle]:
    actors = set(actors)
    manager = RemoteCommunicatorManager.get()
    collectives = []
    # Find all collective groups that the given actors are a subset
    # of, with the matching backend if provided.
    for collective in manager.remote_communicators.values():
        if actors.issubset(set(collective.actors)):
            if backend is None or collective.backend == backend:
                collectives.append(collective)
    return collectives


def create_collective_group(
        actors: List[ray.actor.ActorHandle],
        name: Optional[str] = None,
        backend: Optional[str] = None,
        ) -> CommunicatorHandle:
    accelerator_module_name = None
    if backend is not None:
        if backend not in ["nccl", "gloo"]:
            raise ValueError("Expected `backend` to be one of [\"nccl\", \"gloo\"]")
        if backend == "nccl":
            accelerator_module_name = "cuda"
            accelerator_communicator_cls = _NcclGroup
        else:
            accelerator_module_name = "cpu"
            assert False

    if name is None:
        name = str(uuid.uuid4())

    _init_communicator(actors,
                       accelerator_module_name=accelerator_module_name,
                       accelerator_communicator_cls=accelerator_communicator_cls)
    comm = CommunicatorHandle(actors, name, backend)
    manager = RemoteCommunicatorManager.get()
    manager.add_remote_collective(comm)
    return comm


def destroy_collective_group(name):
    manager = RemoteCommunicatorManager.get()
    _destroy_communicator(name)
    manager.remove_remote_collective(name)


def destroy_all_collective_groups():
    manager = RemoteCommunicatorManager.get()
    for name in list(manager.remote_communicators):
        destroy_collective_group(name)
