import inspect


class RequestInObject:
    """
    Request Arguments required for enqueuing a request to the service
    queue.
    Args:
        service(str): A registered service endpoint.
        request_context(TaskContext): Context of a request.
        request_slo_ms(float): Expected time for the query to get
            completed.
        is_wall_clock_time(bool): if True, router won't add wall clock
            time to `request_slo_ms`.
    """

    def __init__(self,
                 service,
                 request_context,
                 request_slo_ms=None,
                 is_wall_clock_time=False):

        self.service = service
        self.request_context = request_context
        self.request_slo_ms = request_slo_ms
        self.is_wall_clock_time = is_wall_clock_time
        if request_slo_ms is None:
            self.is_wall_clock_time = False

    @classmethod
    def get_kwargs(cls):
        signature = inspect.signature(cls)
        params = signature.parameters
        kwargs_keys = [
            k for k in params.keys()
            if params[k].default is not inspect.Parameter.empty
        ]
        kwargs_keys.pop(kwargs_keys.index("request_slo_ms"))
        kwargs_keys.append("slo_ms")
        return kwargs_keys
