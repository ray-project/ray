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
            model="stevhliu/my_awesome_model",
            device=torch.device("cuda:0"),
        )

    def classify(self, sentence: str):
        return self.classifier(sentence)


entrypoint = APIIngress.bind(DistilBertModel.bind())
