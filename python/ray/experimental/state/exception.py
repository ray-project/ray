"""Internal Error"""


class DataSourceUnavailable(Exception):
    pass


"""User-facing Error"""


class RayStateApiException(Exception):
    pass
