"""Internal Error"""


class DataSourceUnavailable(Exception):
    pass


"""User-facing Error"""


class RayStateApiException(Exception):
    pass


class ServerUnavailable(RayStateApiException):
    """Thrown when failing to connect to dashboard server"""

    pass
