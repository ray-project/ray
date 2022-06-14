from ray.util.annotations import Deprecated


@Deprecated
class DockerSyncer:
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "DockerSyncer has been fully deprecated. There is no need to "
            "use this syncer anymore - data syncing will happen automatically "
            "using the Ray object store. You can just remove passing this class."
        )


@Deprecated
class DockerSyncClient:
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "DockerSyncClient has been fully deprecated. There is no need to "
            "use this syncer anymore - data syncing will happen automatically "
            "using the Ray object store. You can just remove passing this class."
        )
