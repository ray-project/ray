from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class UnsupportedSpaceException(Exception):
    """Error for an unsupported action or observation space."""
    pass


@PublicAPI
class EnvError(Exception):
    """Error if we encounter an error during RL environment validation."""
    pass
