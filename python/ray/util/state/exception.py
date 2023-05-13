"""Internal Error"""


STATE_OBS_ALPHA_FEEDBACK_MSG = [
    "\n==========ALPHA, FEEDBACK NEEDED ===============",
    "State Observability APIs is currently in Alpha. ",
    "If you have any feedback, you could do so at either way as below:",
    "    1. Report bugs/issues with details: https://forms.gle/gh77mwjEskjhN8G46",
    "    2. Follow up in #ray-state-observability-dogfooding slack channel of Ray: "
    "https://tinyurl.com/2pm26m4a",
    "==========================================================",
]


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
