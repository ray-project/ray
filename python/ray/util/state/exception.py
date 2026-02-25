from ray.util.annotations import DeveloperAPI

"""Internal Error"""


@DeveloperAPI
class DataSourceUnavailable(Exception):
    pass


"""User-facing Error"""


@DeveloperAPI
class RayStateApiException(Exception):
    pass


@DeveloperAPI
class ServerUnavailable(RayStateApiException):
    """Thrown when failing to connect to dashboard server"""

    pass
