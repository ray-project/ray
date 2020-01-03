from ray.streaming.runtime.gateway_client import GatewayClient


class StreamingContext:
    """Wrapper of java org.ray.streaming.api.context.StreamingContext"""

    class Builder:

        def __init__(self):
            self._options = {}

        def config(self, key=None, value=None, conf=None):
            if conf is None:
                self._options[key] = str(value)
            else:
                for k, v in conf.items():
                    self._options[k] = v
            return self

        def build(self):
            ctx = StreamingContext()
            ctx._gateway_client.with_config(self._options)
            return ctx

    def __init__(self):
        self._python_gateway_actor = self.__create_python_gateway_actor()
        self.__gateway_client = GatewayClient(self._python_gateway_actor)
        self._j_ctx = self._gateway_client.create_streaming_context()

    def execute(self):
        """Construct job DAG, and execute the job.
        """
        self._gateway_client.execute()

    def _do_init(self):
        pass

    def __create_python_gateway_actor(self):
        pass

    @property
    def _gateway_client(self):
        return self.__gateway_client
