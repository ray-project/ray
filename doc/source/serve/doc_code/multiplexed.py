# __serve_deployment_example_begin__

from ray import serve
from ray.serve.handle import DeploymentHandle
import aioboto3
import torch
import starlette


# Multiplexing belongs on the downstream deployment, not the ingress.
@serve.deployment
class ModelInferencer:
    def __init__(self):
        self.bucket_name = "my_bucket"

    @serve.multiplexed(max_num_models_per_replica=3)
    async def get_model(self, model_id: str):
        session = aioboto3.Session()
        async with session.resource("s3") as s3:
            obj = await s3.Bucket(self.bucket_name)
            await obj.download_file(f"{model_id}/model.pt", f"model_{model_id}.pt")
            return torch.load(f"model_{model_id}.pt")

    async def __call__(self):
        model_id = serve.get_multiplexed_model_id()
        model = await self.get_model(model_id)
        return model.forward(torch.rand(64, 3, 512, 512))


@serve.deployment
class Ingress:
    def __init__(self, downstream: DeploymentHandle):
        self._downstream = downstream

    async def __call__(self, request: starlette.requests.Request):
        model_id = request.headers.get("serve_multiplexed_model_id", "1")
        return await self._downstream.options(multiplexed_model_id=model_id).remote()


entry = Ingress.bind(ModelInferencer.bind())

# __serve_deployment_example_end__

handle = serve.run(entry)

# __serve_request_send_example_begin__
import requests  # noqa: E402

resp = requests.get(
    "http://localhost:8000", headers={"serve_multiplexed_model_id": str("1")}
)
# __serve_request_send_example_end__

# __serve_handle_send_example_begin__
# Get a handle to the multiplexed deployment and set the model ID in options.
model_handle = serve.get_deployment_handle("ModelInferencer", "default")
obj_ref = model_handle.options(multiplexed_model_id="1").remote()
# __serve_handle_send_example_end__

# __serve_model_composition_example_begin__
# Ingress extracts model ID and forwards to multiplexed downstream.
@serve.deployment
class Downstream:
    @serve.multiplexed(max_num_models_per_replica=2)
    async def load_model(self, model_id: str):
        return model_id

    async def __call__(self):
        model_id = serve.get_multiplexed_model_id()
        return await self.load_model(model_id)


@serve.deployment
class Upstream:
    def __init__(self, downstream: DeploymentHandle):
        self._h = downstream

    async def __call__(self, request: starlette.requests.Request):
        model_id = request.headers.get("x-model-id", "default")
        return await self._h.options(multiplexed_model_id=model_id).remote()


serve.run(Upstream.bind(Downstream.bind()))
resp = requests.get("http://localhost:8000", headers={"x-model-id": "bar"})
# __serve_model_composition_example_end__


# __serve_multiplexed_batching_example_begin__
from typing import List  # noqa: E402
from starlette.requests import Request  # noqa: E402

# Multiplexed + batching: use on downstream, not ingress.
@serve.deployment(max_ongoing_requests=15)
class BatchedMultiplexModel:
    @serve.multiplexed(max_num_models_per_replica=3)
    async def get_model(self, model_id: str):
        # Load and return your model here
        return model_id

    @serve.batch(max_batch_size=10, batch_wait_timeout_s=0.1)
    async def batched_predict(self, inputs: List[str]) -> List[str]:
        # Get the model ID - this works correctly inside batched functions
        # because all requests in the batch target the same model
        model_id = serve.get_multiplexed_model_id()
        model = await self.get_model(model_id)

        # Process the batch with the loaded model
        return [f"{model}:{inp}" for inp in inputs]

    async def __call__(self, input_text: str):
        # Called from ingress with multiplexed_model_id set
        return await self.batched_predict(input_text)


@serve.deployment
class BatchingIngress:
    def __init__(self, downstream: DeploymentHandle):
        self._downstream = downstream

    async def __call__(self, request: Request):
        model_id = request.headers.get("serve_multiplexed_model_id", "1")
        input_text = (await request.body()).decode()
        return await self._downstream.options(
            multiplexed_model_id=model_id
        ).remote(input_text)


# __serve_multiplexed_batching_example_end__
