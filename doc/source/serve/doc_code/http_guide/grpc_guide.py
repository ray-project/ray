# flake8: noqa
import ray


ray.init()

# __begin_start_grpc_proxy__
from ray import serve
from ray.serve.config import gRPCOptions


grpc_port = 9000
grpc_servicer_functions = [
    "user_defined_protos_pb2_grpc.add_UserDefinedServiceServicer_to_server",
    "user_defined_protos_pb2_grpc.add_FruitServiceServicer_to_server",
]
serve.start(
    http_options={"location": "EveryNode"},
    grpc_options=gRPCOptions(
        port=grpc_port,
        grpc_servicer_functions=grpc_servicer_functions,
    ),
)
# __end_start_grpc_proxy__

# __begin_grpc_deployment__
import time

from typing import Generator
from user_defined_protos_pb2 import (
    UserDefinedMessage,
    UserDefinedMessage2,
    UserDefinedResponse,
    UserDefinedResponse2,
)

import ray
from ray import serve


@serve.deployment
class GrpcDeployment:
    def __call__(self, user_message: UserDefinedMessage) -> UserDefinedResponse:
        greeting = f"Hello {user_message.name} from {user_message.origin}"
        num = user_message.num * 2
        user_response = UserDefinedResponse(
            greeting=greeting,
            num=num,
        )
        return user_response

    @serve.multiplexed(max_num_models_per_replica=1)
    async def get_model(self, model_id: str) -> str:
        return f"loading model: {model_id}"

    async def Multiplexing(
        self, user_message: UserDefinedMessage2
    ) -> UserDefinedResponse2:
        model_id = serve.get_multiplexed_model_id()
        model = await self.get_model(model_id)
        user_response = UserDefinedResponse2(
            greeting=f"Method2 called model, {model}",
        )
        return user_response

    def Streaming(
        self, user_message: UserDefinedMessage
    ) -> Generator[UserDefinedResponse, None, None]:
        for i in range(10):
            greeting = f"{i}: Hello {user_message.name} from {user_message.origin}"
            num = user_message.num * 2 + i
            user_response = UserDefinedResponse(
                greeting=greeting,
                num=num,
            )
            yield user_response

            time.sleep(0.1)


g = GrpcDeployment.bind()

# __end_grpc_deployment__

# __begin_deploy_grpc_app__
app1 = "app1"
serve.run(target=g, name=app1, route_prefix=f"/{app1}")

# __end_deploy_grpc_app__


# __begin_send_grpc_requests__
import grpc
from user_defined_protos_pb2_grpc import UserDefinedServiceStub
from user_defined_protos_pb2 import UserDefinedMessage


channel = grpc.insecure_channel("localhost:9000")
stub = UserDefinedServiceStub(channel)
request = UserDefinedMessage(name="foo", num=30, origin="bar")

response, call = stub.__call__.with_call(request=request)
print(f"status code: {call.code()}")  # grpc.StatusCode.OK
print(f"greeting: {response.greeting}")  # "Hello foo from bar"
print(f"num: {response.num}")  # 60

# __end_send_grpc_requests__

# __begin_health_check__
import grpc
from ray.serve.generated.serve_pb2_grpc import RayServeAPIServiceStub
from ray.serve.generated.serve_pb2 import HealthzRequest, ListApplicationsRequest


channel = grpc.insecure_channel("localhost:9000")
stub = RayServeAPIServiceStub(channel)
request = ListApplicationsRequest()
response = stub.ListApplications(request=request)
print(f"Applications: {response.application_names}")  # ["app1"]

request = HealthzRequest()
response = stub.Healthz(request=request)
print(f"Health: {response.message}")  # "success"

# __end_health_check__

# __begin_metadata__
import grpc
from user_defined_protos_pb2_grpc import UserDefinedServiceStub
from user_defined_protos_pb2 import UserDefinedMessage2


channel = grpc.insecure_channel("localhost:9000")
stub = UserDefinedServiceStub(channel)
request = UserDefinedMessage2()
app_name = "app1"
request_id = "123"
multiplexed_model_id = "999"
metadata = (
    ("application", app_name),
    ("request_id", request_id),
    ("multiplexed_model_id", multiplexed_model_id),
)

response, call = stub.Multiplexing.with_call(request=request, metadata=metadata)
print(f"greeting: {response.greeting}")  # "Method2 called model, loading model: 999"
for key, value in call.trailing_metadata():
    print(f"trailing metadata key: {key}, value {value}")  # "request_id: 123"

# __end_metadata__


# __begin_streaming__
import grpc
from user_defined_protos_pb2_grpc import UserDefinedServiceStub
from user_defined_protos_pb2 import UserDefinedMessage


channel = grpc.insecure_channel("localhost:9000")
stub = UserDefinedServiceStub(channel)
request = UserDefinedMessage(name="foo", num=30, origin="bar")
metadata = (("application", "app1"),)

responses = stub.Streaming(request=request, metadata=metadata)
for response in responses:
    print(f"greeting: {response.greeting}")
    print(f"num: {response.num}")

# __end_streaming__


# __begin_model_composition_deployment__
from typing import Dict
from user_defined_protos_pb2 import (
    FruitAmounts,
    FruitCosts,
)

import ray
from ray import serve
from ray.serve.handle import RayServeDeploymentHandle


@serve.deployment
class FruitMarket:
    def __init__(
        self,
        _orange_stand: RayServeDeploymentHandle,
        _apple_stand: RayServeDeploymentHandle,
    ):
        self.directory = {
            "ORANGE": _orange_stand,
            "APPLE": _apple_stand,
        }

    async def FruitStand(self, fruit_amounts_proto: FruitAmounts) -> FruitCosts:
        fruit_amounts = {}
        if fruit_amounts_proto.orange:
            fruit_amounts["ORANGE"] = fruit_amounts_proto.orange
        if fruit_amounts_proto.apple:
            fruit_amounts["APPLE"] = fruit_amounts_proto.apple
        if fruit_amounts_proto.banana:
            fruit_amounts["BANANA"] = fruit_amounts_proto.banana

        costs = await self.check_price(fruit_amounts)
        return FruitCosts(costs=costs)

    async def check_price(self, inputs: Dict[str, int]) -> float:
        costs = 0
        for fruit, amount in inputs.items():
            if fruit not in self.directory:
                return
            fruit_stand = self.directory[fruit]
            ref: ray.ObjectRef = await fruit_stand.remote(int(amount))
            result = await ref
            costs += result
        return costs


@serve.deployment
class OrangeStand:
    def __init__(self):
        self.price = 2.49

    def __call__(self, num_oranges: int):
        return num_oranges * self.price


@serve.deployment
class AppleStand:
    def __init__(self):
        self.price = 3.89

    def __call__(self, num_oranges: int):
        return num_oranges * self.price


orange_stand = OrangeStand.bind()
apple_stand = AppleStand.bind()
g2 = FruitMarket.bind(orange_stand, apple_stand)

# __end_model_composition_deployment__


# __begin_model_composition_deploy__
app2 = "app2"
serve.run(target=g2, name=app2, route_prefix=f"/{app2}")

# __end_model_composition_deploy__


# __begin_model_composition_client__
import grpc
from user_defined_protos_pb2_grpc import FruitServiceStub
from user_defined_protos_pb2 import FruitAmounts


channel = grpc.insecure_channel("localhost:9000")
stub = FruitServiceStub(channel)
request = FruitAmounts(orange=4, apple=8)
metadata = (("application", "app2"),)

response, call = stub.FruitStand.with_call(request=request, metadata=metadata)
print(f"status code: {call.code()}")  # grpc.StatusCode.OK
print(f"costs: {response.costs}")  # 41.08

# __end_model_composition_client__


# __begin_error_handle__
import grpc
from user_defined_protos_pb2_grpc import UserDefinedServiceStub
from user_defined_protos_pb2 import UserDefinedMessage


channel = grpc.insecure_channel("localhost:9000")
stub = UserDefinedServiceStub(channel)
request = UserDefinedMessage(name="foo", num=30, origin="bar")

try:
    response = stub.__call__(request=request)
except grpc.RpcError as rpc_error:
    print(f"status code {rpc_error.code()}")  # StatusCode.NOT_FOUND
    print(f"details {rpc_error.details()}")

# __end_error_handle__
