import time
from ray.serve.constants import DEFAULT_LATENCY_SLO_MS


class RequestMetadata:
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
                 relative_slo_ms=None,
                 absolute_slo_ms=None):

        self.service = service
        self.request_context = request_context
        self.relative_slo_ms = relative_slo_ms
        self.absolute_slo_ms = absolute_slo_ms

    def adjust_relative_slo_ms(self) -> float:
        """Normalize the input latency objective to absoluate timestamp.

        """
        slo_ms = self.relative_slo_ms
        if slo_ms is None:
            slo_ms = DEFAULT_LATENCY_SLO_MS
        current_time_ms = time.time() * 1000
        return current_time_ms + slo_ms
