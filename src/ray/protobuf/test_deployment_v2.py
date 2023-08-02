from ray import serve
from ray.serve.generated import serve_pb2
from ray.serve.generated.serve_pb2 import TestIn, TestOut


@serve.deployment
class GrpcDeployment:
    def __call__(self, request: "TestIn") -> "TestOut":
        greeting = f"Hello {request.name} from {request.foo}"
        num_x2 = request.num * 2
        output = serve_pb2.TestOut(
            greeting=greeting,
            num_x2=num_x2,
        )
        return output

    def method1(self, request: "TestIn") -> "TestOut":
        greeting = f"Hello {request.foo} from method1"
        num_x2 = request.num * 3
        output = serve_pb2.TestOut(
            greeting=greeting,
            num_x2=num_x2,
        )
        return output

    def method2(self, request: "TestIn") -> "TestOut":
        greeting = "This is from method2"
        output = serve_pb2.TestOut(greeting=greeting)
        return output


g = GrpcDeployment.options(name="grpc-deployment").bind()
