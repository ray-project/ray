from ray.util.annotations import Deprecated


@Deprecated
class KubernetesSyncer:
    def __init__(self, *args, **kwargs):

        raise DeprecationWarning(
            "KubernetesSyncer has been fully deprecated. There is no need to "
            "use this syncer anymore - data syncing will happen automatically "
            "using the Ray object store. You can just remove passing this class."
        )


@Deprecated
class KubernetesSyncClient:
    def __init__(self, *args, **kwargs):
        raise DeprecationWarning(
            "KubernetesSyncClient has been fully deprecated. There is no need to "
            "use this syncer anymore - data syncing will happen automatically "
            "using the Ray object store. You can just remove passing this class."
        )


def NamespacedKubernetesSyncer(namespace: str):
    raise DeprecationWarning(
        "NamespacedKubernetesSyncer has been fully deprecated. There is no need to "
        "use this syncer anymore - data syncing will happen automatically "
        "using the Ray object store. You can just remove passing this class."
    )
