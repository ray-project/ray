import os

import ray
from ray.air.constants import COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV
from ray.train.constants import RAY_CHDIR_TO_TRIAL_DIR

DEFAULT_ENV_VARS = {
    # https://github.com/ray-project/ray/issues/28197
    "PL_DISABLE_FORK": "1"
}
ENV_VARS_TO_PROPAGATE = {
    COPY_DIRECTORY_CHECKPOINTS_INSTEAD_OF_MOVING_ENV,
    RAY_CHDIR_TO_TRIAL_DIR,
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SECURITY_TOKEN",
    "AWS_SESSION_TOKEN",
}


class _ActorClassCache:
    """Caches actor classes.

    ray.remote is a registration call. It sends the serialized object to the
    key value store (redis), and will be fetched at an arbitrary worker
    later. Registration does not use any Ray scheduling resources.

    Later, class.remote() actually creates the remote actor. The
    actor will be instantiated on some arbitrary machine,
    according to the underlying Ray scheduler.

    Without this cache, you would register the same serialized object
    over and over again. Naturally, since redis doesn’t spill to disk,
    this can easily nuke the redis instance (and basically blow up Ray).
    This cache instead allows us to register once and only once.

    Note that we assume there can be multiple trainables in the
    system at once.
    """

    def __init__(self):
        self._cache = {}

    def get(self, trainable_cls):
        """Gets the wrapped trainable_cls, otherwise calls ray.remote."""
        env_vars = DEFAULT_ENV_VARS.copy()

        for env_var_to_propagate in ENV_VARS_TO_PROPAGATE:
            if env_var_to_propagate in os.environ:
                env_vars[env_var_to_propagate] = os.environ[env_var_to_propagate]

        runtime_env = {"env_vars": env_vars}
        if trainable_cls not in self._cache:
            remote_cls = ray.remote(runtime_env=runtime_env)(trainable_cls)
            self._cache[trainable_cls] = remote_cls
        return self._cache[trainable_cls]
