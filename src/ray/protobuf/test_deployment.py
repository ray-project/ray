from google.protobuf.any_pb2 import Any

from ray import serve
from ray.serve.generated import serve_pb2


@serve.deployment
class Hi:
    def __call__(self, any_input: Any):
        # Everything done here can be transparently performed in the Serve replica
        # (assuming they use type annotations for the input type).

        # TODO: this packing and unpacking would go into replica
        request = serve_pb2.TestIn()
        any_input.Unpack(request)
        # output = self.call(request)
        output = serve_pb2.TestOut(greeting=f"Hello {request.name}")
        any_output = Any()
        any_output.Pack(output)
        return any_output


h = Hi.options(name="test-name").bind()
