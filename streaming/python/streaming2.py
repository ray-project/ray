from ray.streaming.gateway_client import GatewayClient


class StreamingContext:
    """Wrapper of java org.ray.streaming.api.context.StreamingContext"""

    def __init__(self):
        self._python_gateway_actor = self.__create_python_gateway_actor()
        self.__gateway_client = GatewayClient(self._python_gateway_actor)
        self._j_ctx = self._gateway_client.create_streaming_context()

    @staticmethod
    def build_context():
        return StreamingContext()

    def with_config(self, job_config):
        self._gateway_client.with_config(job_config)

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


