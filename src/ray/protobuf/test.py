from google.protobuf.any_pb2 import Any

from ray.serve.generated import serve_pb2

from ray import serve


@serve.deployment
class Hi:
    def __call__(self, any_input: Any):
        # Everything done here can be transparently performed in the Serve replica
        # (assuming they use type annotations for the input type).
        request = serve_pb2.TestIn()
        any_input.Unpack(request)
        output = self.call(request)
        any_output = Any()
        any_output.Pack(output)
        return any_output

    # This is what a user-defined method would look like.
    def call(self, request: serve_pb2.TestIn) -> serve_pb2.TestOut:
        return serve_pb2.TestOut(greeting=f"Hello {request.name}")


h = Hi.options(name="test-name").bind()
