# flake8: noqa
import ray


ray.init()

# __begin_start_grpc_proxy__
from ray import serve
from ray.serve.config import gRPCOptions


grpc_port = 9000
grpc_servicer_functions = [
    "user_defined_protos_pb2_grpc.add_UserDefinedServiceServicer_to_server",
    "user_defined_protos_pb2_grpc.add_ImageClassificationServiceServicer_to_server",
]
serve.start(
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
    print(f"greeting: {response.greeting}")  # greeting: n: Hello foo from bar
    print(f"num: {response.num}")  # num: 60 + n
# __end_streaming__


# __begin_model_composition_deployment__
import requests
import torch
from typing import List
from PIL import Image
from io import BytesIO
from torchvision import transforms
from user_defined_protos_pb2 import (
    ImageClass,
    ImageData,
)

from ray import serve
from ray.serve.handle import DeploymentHandle


@serve.deployment
class ImageClassifier:
    def __init__(
        self,
        _image_downloader: DeploymentHandle,
        _data_preprocessor: DeploymentHandle,
    ):
        self._image_downloader = _image_downloader
        self._data_preprocessor = _data_preprocessor
        self.model = torch.hub.load(
            "pytorch/vision:v0.10.0", "resnet18", pretrained=True
        )
        self.model.eval()
        self.categories = self._image_labels()

    def _image_labels(self) -> List[str]:
        categories = []
        url = (
            "https://raw.githubusercontent.com/pytorch/hub/master/imagenet_classes.txt"
        )
        labels = requests.get(url).text
        for label in labels.split("\n"):
            categories.append(label.strip())
        return categories

    async def Predict(self, image_data: ImageData) -> ImageClass:
        # Download image
        image = await self._image_downloader.remote(image_data.url)

        # Preprocess image
        input_batch = await self._data_preprocessor.remote(image)
        # Predict image
        with torch.no_grad():
            output = self.model(input_batch)

        probabilities = torch.nn.functional.softmax(output[0], dim=0)
        return self.process_model_outputs(probabilities)

    def process_model_outputs(self, probabilities: torch.Tensor) -> ImageClass:
        image_classes = []
        image_probabilities = []
        # Show top categories per image
        top5_prob, top5_catid = torch.topk(probabilities, 5)
        for i in range(top5_prob.size(0)):
            image_classes.append(self.categories[top5_catid[i]])
            image_probabilities.append(top5_prob[i].item())

        return ImageClass(
            classes=image_classes,
            probabilities=image_probabilities,
        )


@serve.deployment
class ImageDownloader:
    def __call__(self, image_url: str):
        image_bytes = requests.get(image_url).content
        return Image.open(BytesIO(image_bytes)).convert("RGB")


@serve.deployment
class DataPreprocessor:
    def __init__(self):
        self.preprocess = transforms.Compose(
            [
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.ToTensor(),
                transforms.Normalize(
                    mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]
                ),
            ]
        )

    def __call__(self, image: Image):
        input_tensor = self.preprocess(image)
        return input_tensor.unsqueeze(0)  # create a mini-batch as expected by the model


image_downloader = ImageDownloader.bind()
data_preprocessor = DataPreprocessor.bind()
g2 = ImageClassifier.options(name="grpc-image-classifier").bind(
    image_downloader, data_preprocessor
)
# __end_model_composition_deployment__


# __begin_model_composition_deploy__
app2 = "app2"
serve.run(target=g2, name=app2, route_prefix=f"/{app2}")
# __end_model_composition_deploy__


# __begin_model_composition_client__
import grpc
from user_defined_protos_pb2_grpc import ImageClassificationServiceStub
from user_defined_protos_pb2 import ImageData


channel = grpc.insecure_channel("localhost:9000")
stub = ImageClassificationServiceStub(channel)
request = ImageData(url="https://github.com/pytorch/hub/raw/master/images/dog.jpg")
metadata = (("application", "app2"),)  # Make sure application metadata is passed.

response, call = stub.Predict.with_call(request=request, metadata=metadata)
print(f"status code: {call.code()}")  # grpc.StatusCode.OK
print(f"Classes: {response.classes}")  # ['Samoyed', ...]
print(f"Probabilities: {response.probabilities}")  # [0.8846230506896973, ...]
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
    print(f"status code: {rpc_error.code()}")  # StatusCode.NOT_FOUND
    print(f"details: {rpc_error.details()}")  # Application metadata not set...
# __end_error_handle__


# __begin_grpc_context_define_app__
from user_defined_protos_pb2 import UserDefinedMessage, UserDefinedResponse

from ray import serve
from ray.serve.grpc_util import RayServegRPCContext

import grpc
from typing import Tuple


@serve.deployment
class GrpcDeployment:
    def __init__(self):
        self.nums = {}

    def num_lookup(self, name: str) -> Tuple[int, grpc.StatusCode, str]:
        if name not in self.nums:
            self.nums[name] = len(self.nums)
            code = grpc.StatusCode.INVALID_ARGUMENT
            message = f"{name} not found, adding to nums."
        else:
            code = grpc.StatusCode.OK
            message = f"{name} found."
        return self.nums[name], code, message

    def __call__(
        self,
        user_message: UserDefinedMessage,
        grpc_context: RayServegRPCContext,  # to use grpc context, add this kwarg
    ) -> UserDefinedResponse:
        greeting = f"Hello {user_message.name} from {user_message.origin}"
        num, code, message = self.num_lookup(user_message.name)

        # Set custom code, details, and trailing metadata.
        grpc_context.set_code(code)
        grpc_context.set_details(message)
        grpc_context.set_trailing_metadata([("num", str(num))])

        user_response = UserDefinedResponse(
            greeting=greeting,
            num=num,
        )
        return user_response


g = GrpcDeployment.bind()
app1 = "app1"
serve.run(target=g, name=app1, route_prefix=f"/{app1}")
# __end_grpc_context_define_app__


# __begin_grpc_context_client__
import grpc
from user_defined_protos_pb2_grpc import UserDefinedServiceStub
from user_defined_protos_pb2 import UserDefinedMessage


channel = grpc.insecure_channel("localhost:9000")
stub = UserDefinedServiceStub(channel)
request = UserDefinedMessage(name="foo", num=30, origin="bar")
metadata = (("application", "app1"),)

# First call is going to page miss and return INVALID_ARGUMENT status code.
try:
    response, call = stub.__call__.with_call(request=request, metadata=metadata)
except grpc.RpcError as rpc_error:
    assert rpc_error.code() == grpc.StatusCode.INVALID_ARGUMENT
    assert rpc_error.details() == "foo not found, adding to nums."
    assert any(
        [key == "num" and value == "0" for key, value in rpc_error.trailing_metadata()]
    )
    assert any([key == "request_id" for key, _ in rpc_error.trailing_metadata()])

# Second call is going to page hit and return OK status code.
response, call = stub.__call__.with_call(request=request, metadata=metadata)
assert call.code() == grpc.StatusCode.OK
assert call.details() == "foo found."
assert any([key == "num" and value == "0" for key, value in call.trailing_metadata()])
assert any([key == "request_id" for key, _ in call.trailing_metadata()])
# __end_grpc_context_client__
