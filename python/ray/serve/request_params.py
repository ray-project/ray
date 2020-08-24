import ray.cloudpickle as pickle


class RequestMetadata:
    """
    Request arguments required for enqueuing a request to the endpoint queue.

    Args:
        endpoint(str): A registered endpoint.
        request_context(TaskContext): Context of a request.
    """

    def __init__(self,
                 endpoint,
                 request_context,
                 call_method="__call__",
                 shard_key=None):

        self.endpoint = endpoint
        self.request_context = request_context
        self.call_method = call_method
        self.shard_key = shard_key

    def ray_serialize(self):
        return pickle.dumps(self.__dict__)

    @staticmethod
    def ray_deserialize(value):
        kwargs = pickle.loads(value)
        return RequestMetadata(**kwargs)
