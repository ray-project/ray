import pickle
import time
from typing import Generator

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


g = GrpcDeployment.options(name="grpc-deployment").bind()


@serve.deployment
class GrpcDeploymentNoProto:
    def __call__(self, my_input: bytes) -> bytes:
        request = pickle.loads(my_input)
        greeting = f"Hello {request['name']} from {request['foo']}"
        num_x2 = request["num"] * 2
        output = {
            "greeting": greeting,
            "num_x2": num_x2,
        }
        return pickle.dumps(output)


g2 = GrpcDeploymentNoProto.options(name="grpc-deployment-no-proto").bind()


@serve.deployment
class GrpcDeploymentStreamingResponse:
    def __call__(self, my_input: bytes) -> Generator[bytes, None, None]:
        print("my_input", my_input)
        request = serve_pb2.TestIn()
        request.ParseFromString(my_input)
        for i in range(10):
            greeting = f"{i}: Hello {request.name} from {request.foo}"
            num_x2 = request.num * 2 + i
            output = serve_pb2.TestOut(
                greeting=greeting,
                num_x2=num_x2,
            )
            yield output.SerializeToString()

            time.sleep(0.1)


g3 = GrpcDeploymentStreamingResponse.options(
    name="grpc-deployment-streaming-response"
).bind()


@serve.deployment
class HttpDeployment:
    async def __call__(self, request: Request) -> str:
        body = await request.body()
        print("request.body()", body)
        return f"Hello {body} {time.time()}"


h = HttpDeployment.options(name="http-deployment").bind()
