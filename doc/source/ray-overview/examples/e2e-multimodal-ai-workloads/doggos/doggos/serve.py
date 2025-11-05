from urllib.parse import urlparse

import mlflow
import numpy as np
import torch
from doggos.infer import TorchPredictor
from doggos.model import collate_fn
from doggos.utils import url_to_array
from fastapi import FastAPI
from PIL import Image
from starlette.requests import Request
from transformers import CLIPModel, CLIPProcessor

from ray import serve


@serve.deployment(
    num_replicas="1",
    ray_actor_options={
        "num_gpus": 1,
        "accelerator_type": "T4",
    },
)
class ClassPredictor:
    def __init__(self, model_id, artifacts_dir, device="cuda"):
        """Initialize the model."""
        # Embedding model
        self.processor = CLIPProcessor.from_pretrained(model_id)
        self.model = CLIPModel.from_pretrained(model_id)
        self.model.to(device=device)
        self.device = device

        # Trained classifier
        self.predictor = TorchPredictor.from_artifacts_dir(artifacts_dir=artifacts_dir)
        self.preprocessor = self.predictor.preprocessor

    def get_probabilities(self, url):
        image = Image.fromarray(np.uint8(url_to_array(url=url))).convert("RGB")
        inputs = self.processor(images=[image], return_tensors="pt", padding=True).to(
            self.device
        )
        with torch.inference_mode():
            embedding = self.model.get_image_features(**inputs).cpu().numpy()
        outputs = self.predictor.predict_probabilities(
            collate_fn({"embedding": embedding}, device=self.device)
        )
        return {"probabilities": outputs["probabilities"][0]}


# Define app
api = FastAPI(
    title="doggos",
    description="classify your dog",
    version="0.1",
)


@serve.deployment
@serve.ingress(api)
class Doggos:
    def __init__(self, classifier):
        self.classifier = classifier

    @api.post("/predict/")
    async def predict(self, request: Request):
        data = await request.json()
        probabilities = await self.classifier.get_probabilities.remote(url=data["url"])
        return probabilities


# Model registry.
model_registry = "/mnt/user_storage/mlflow/doggos"
experiment_name = "doggos"
mlflow.set_tracking_uri(f"file:{model_registry}")

# Get best_run's artifact_dir.
sorted_runs = mlflow.search_runs(
    experiment_names=[experiment_name], order_by=["metrics.val_loss ASC"]
)
best_run = sorted_runs.iloc[0]
artifacts_dir = urlparse(best_run.artifact_uri).path

# Define app
app = Doggos.bind(
    classifier=ClassPredictor.bind(
        model_id="openai/clip-vit-base-patch32",
        artifacts_dir=artifacts_dir,
        device="cuda",
    )
)

if __name__ == "__main__":
    # Run service locally
    serve.run(app, route_prefix="/")
