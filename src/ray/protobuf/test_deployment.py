import time

from starlette.requests import Request

from ray import serve
from ray.serve.generated import serve_pb2


@serve.deployment
class GrpcDeployment:
    def __call__(self, my_input: bytes) -> bytes:
        request = serve_pb2.TestIn()
        request.ParseFromString(my_input)
        greeting = f"Hello {request.name} from {request.foo}"
        num_x2 = request.num * 2
        output = serve_pb2.TestOut(
            greeting=greeting,
            num_x2=num_x2,
        )
        return output.SerializeToString()


g = GrpcDeployment.options(name="test-name").bind()


@serve.deployment
class HttpDeployment:
    async def __call__(self, request: Request) -> str:
        body = await request.body()
        print("request.body()", body)
        return f"Hello {body} {time.time()}"


h = HttpDeployment.options(name="http-deployment").bind()
