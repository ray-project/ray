from contextlib import contextmanager

# __example_code_start__
from transformers import pipeline
from fastapi import FastAPI
from ray import serve
import torch


app = FastAPI()


@serve.deployment(num_replicas=1, route_prefix="/")
@serve.ingress(app)
class APIIngress:
    def __init__(self, distilbert_model_handle) -> None:
        self.handle = distilbert_model_handle

    @app.get("/classify")
    async def classify(self, sentence: str):
        predict_ref = await self.handle.classify.remote(sentence)
        predict_result = await predict_ref
        return predict_result


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


@contextmanager
def serve_session(deployment):
    handle = serve.run(deployment)
    try:
        yield handle
    finally:
        serve.shutdown()


if __name__ == "__main__":
    import ray

    ray.init(runtime_env={"pip": ["transformers==4.27.1", "accelerate==0.17.1"]})

    with serve_session(entrypoint):
        # __example_client_start__
        import requests

        prompt = (
            "This was a masterpiece. Not completely faithful to the books, but "
            "enthralling  from beginning to end. Might be my favorite of the three."
        )
        input = "%20".join(prompt.split(" "))
        resp = requests.get(f"http://127.0.0.1:8000/classify?sentence={prompt}")
        print(resp.status_code, resp.json())
        # __example_client_end__

        assert resp.status_code == 200
