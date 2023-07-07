# __serve_deployment_example_begin__

from ray import serve
import aioboto3
import torch
import starlette


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

    async def __call__(self, request: starlette.requests.Request):
        model_id = serve.get_multiplexed_model_id()
        model = await self.get_model(model_id)
        return model.forward(torch.rand(64, 3, 512, 512))


entry = ModelInferencer.bind()

# __serve_deployment_example_end__

serve.run(entry)

# __serve_request_send_example_begin__
import requests  # noqa: E402

resp = requests.get(
    "http://localhost:8000", headers={"serve_multiplexed_model_id": str("1")}
)
# __serve_request_send_example_end__
