# __example_code_start__
from fastapi import FastAPI
import torch
from transformers import pipeline

from ray import serve
from ray.serve.handle import DeploymentHandle


app = FastAPI()


@serve.deployment(num_replicas=1)
@serve.ingress(app)
class APIIngress:
    def __init__(self, distilbert_model_handle: DeploymentHandle) -> None:
        self.handle = distilbert_model_handle

    @app.get("/classify")
    async def classify(self, sentence: str):
        return await self.handle.classify.remote(sentence)


@serve.deployment(
    ray_actor_options={"num_gpus": 1},
    autoscaling_config={"min_replicas": 0, "max_replicas": 2},
)
class DistilBertModel:
    def __init__(self):
        self.classifier = pipeline(
            "sentiment-analysis",
            model="distilbert-base-uncased",
            framework="pt",
            # Transformers requires you to pass device with index
            device=torch.device("cuda:0"),
        )

    def classify(self, sentence: str):
        return self.classifier(sentence)


entrypoint = APIIngress.bind(DistilBertModel.bind())

# __example_code_end__

if __name__ == "__main__":
    import requests
    import ray

    ray.init(runtime_env={"pip": ["transformers==4.27.1", "accelerate==0.17.1"]})
    serve.run(entrypoint)

    prompt = (
        "This was a masterpiece. Not completely faithful to the books, but "
        "enthralling  from beginning to end. Might be my favorite of the three."
    )
    input = "%20".join(prompt.split(" "))
    resp = requests.get(f"http://127.0.0.1:8000/classify?sentence={prompt}")
    print(resp.status_code, resp.json())

    assert resp.status_code == 200
