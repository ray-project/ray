"""Internal Error"""


from ray.experimental.state.common import STATE_OBS_ALPHA_FEEDBACK_MSG


class DataSourceUnavailable(Exception):
    pass


"""User-facing Error"""


class RayStateApiException(Exception):
    def __init__(self, err_msg, *args):
        err_msg += "\n".join(STATE_OBS_ALPHA_FEEDBACK_MSG)
        super().__init__(err_msg, *args)


class ServerUnavailable(RayStateApiException):
    """Thrown when failing to connect to dashboard server"""

    pass
