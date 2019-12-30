class StreamingContext:
    """Wrapper of java org.ray.streaming.api.context.StreamingContext"""

    def __init__(self):
        self.python_gateway_actor = self.__create_python_gateway_actor()

    @staticmethod
    def build_context():
        return StreamingContext()

    def with_config(self, job_config):
        pass

    def execute(self):
        """Construct job DAG, and execute the job.
        """
        pass

    def _do_init(self):
        pass

    def __create_python_gateway_actor(self):
        pass
