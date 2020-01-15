from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class UnsupportedSpaceException(Exception):
    """Error for an unsupported action or observation space."""
    pass
