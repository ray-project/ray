import ray
from ray import serve
from ray.serve.context import TaskContext
from ray.serve.exceptions import RayServeException
from ray.serve.request_params import RequestMetadata


class RayServeHandle:
    """A handle to a service endpoint.

    Invoking this endpoint with .remote is equivalent to pinging
    an HTTP endpoint.

    Example:
       >>> handle = serve.get_handle("my_endpoint")
       >>> handle
       RayServeHandle(
            Endpoint="my_endpoint",
            URL="...",
            Traffic=...
       )
       >>> handle.remote(my_request_content)
       ObjectID(...)
       >>> ray.get(handle.remote(...))
       # result
       >>> ray.get(handle.remote(let_it_crash_request))
       # raises RayTaskError Exception
    """

    def __init__(
            self,
            router_handle,
            endpoint_name,
            relative_slo_ms=None,
            absolute_slo_ms=None,
            method_name=None,
            shard_key=None,
    ):
        self.router_handle = router_handle
        self.endpoint_name = endpoint_name
        assert relative_slo_ms is None or absolute_slo_ms is None, (
            "Can't specify both "
            "relative and absolute "
            "slo's together!")
        self.relative_slo_ms = self._check_slo_ms(relative_slo_ms)
        self.absolute_slo_ms = self._check_slo_ms(absolute_slo_ms)
        self.method_name = method_name
        self.shard_key = shard_key

    def _check_slo_ms(self, slo_value):
        if slo_value is not None:
            try:
                slo_value = float(slo_value)
                if slo_value < 0:
                    raise ValueError(
                        "Request SLO must be positive, it is {}".format(
                            slo_value))
                return slo_value
            except ValueError as e:
                raise RayServeException(str(e))
        return None

    def remote(self, *args, **kwargs):
        if len(args) != 0:
            raise RayServeException(
                "handle.remote must be invoked with keyword arguments.")

        method_name = self.method_name
        if method_name is None:
            method_name = "__call__"

        # create RequestMetadata instance
        request_in_object = RequestMetadata(
            self.endpoint_name,
            TaskContext.Python,
            self.relative_slo_ms,
            self.absolute_slo_ms,
            call_method=method_name,
            shard_key=self.shard_key,
        )
        return self.router_handle.enqueue_request.remote(
            request_in_object, **kwargs)

    def options(self,
                method_name=None,
                shard_key=None,
                relative_slo_ms=None,
                absolute_slo_ms=None):
        # If both the slo's are None then then we use a high default
        # value so other queries can be prioritize and put in front of these
        # queries.
        assert not all([absolute_slo_ms, relative_slo_ms
                        ]), ("Can't specify both "
                             "relative and absolute "
                             "slo's together!")

        # Don't override existing method
        if method_name is None and self.method_name is not None:
            method_name = self.method_name

        if shard_key is None and self.shard_key is not None:
            shard_key = self.shard_key

        return RayServeHandle(
            self.router_handle,
            self.endpoint_name,
            relative_slo_ms,
            absolute_slo_ms,
            method_name=method_name,
            shard_key=shard_key,
        )

    def get_traffic_policy(self):
        master_actor = serve.api._get_master_actor()
        return ray.get(
            master_actor.get_traffic_policy.remote(self.endpoint_name))

    def __repr__(self):
        return """
RayServeHandle(
    Endpoint="{endpoint_name}",
    Traffic={traffic_policy}
)
""".format(
            endpoint_name=self.endpoint_name,
            traffic_policy=self.get_traffic_policy(),
        )
