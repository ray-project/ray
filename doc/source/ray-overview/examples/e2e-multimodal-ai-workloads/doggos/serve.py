import os
from urllib.parse import urlparse

import mlflow
import ray
from fastapi import FastAPI
from ray import serve
from starlette.requests import Request

from doggos.embed import EmbeddingGenerator, get_top_matches
from doggos.infer import TorchPredictor
from doggos.utils import url_to_array

# Define app
api = FastAPI(
    title="doggos",
    description="classify your dog",
    version="0.1",
)


@serve.deployment(
    num_replicas="1",
    ray_actor_options={
        "num_cpus": 2,
        "num_gpus": 1,
        "accelerator_type": "L4",
    },
)
class ClassPredictor:
    def __init__(self, artifacts_dir):
        """Initialize the model."""
        self.predictor = TorchPredictor.from_artifacts_dir(artifacts_dir=artifacts_dir)
        self.preprocessor = self.predictor.preprocessor

    def get_probabilities(self, url):
        image = url_to_array(url=url)
        ds = ray.data.from_items([{"image": image, "url": url}])
        ds = self.preprocessor.transform(
            ds=ds,
            concurrency=1,
            batch_size=1,
            num_gpus=1,
        )
        ds = ds.map_batches(
            self.predictor.predict_probabilities,
            fn_kwargs={"device": "cuda"},
            concurrency=1,
            batch_size=1,
            num_gpus=1,
        )
        probabilities = ds.take_all()[0]["probabilities"]
        return probabilities


@serve.deployment(
    num_replicas="1",
    ray_actor_options={
        "num_cpus": 2,
        "num_gpus": 1,
        "accelerator_type": "L4",
    },
)
class EmbeddingSimilarity:
    def __init__(self, embeddings_path):
        self.embedding_generator = EmbeddingGenerator(
            model_id="openai/clip-vit-base-patch32"
        )
        self.embeddings_ds = ray.data.read_parquet(embeddings_path)  # use vector DB

    def get_top_matches(self, url, probabilities, k):
        # Top k class predictions
        sorted_probabilities = sorted(
            probabilities.items(), key=lambda x: x[1], reverse=True
        )
        top_k = [item[0] for item in sorted_probabilities[0:k]]

        # Generate embedding
        image = url_to_array(url=url)
        embedding = self.embedding_generator({"image": [image]})["embedding"][0]

        # Filter for top matches
        top_matches = get_top_matches(
            query_embedding=embedding,
            embeddings_ds=self.embeddings_ds,
            class_filters=top_k,
            n=5,
        )
        return top_matches


@serve.deployment(num_replicas="1", ray_actor_options={"num_cpus": 2})
@serve.ingress(api)
class Doggos:
    def __init__(self, classifier, embedder):
        self.classifier = classifier
        self.embedder = embedder

    @api.post("/predict/")
    async def predict(self, request: Request):
        data = await request.json()
        probabilities = await self.classifier.get_probabilities.remote(url=data["url"])
        top_matches = await self.embedder.get_top_matches.remote(
            url=data["url"],
            probabilities=probabilities,
            k=data["k"],
        )
        return {
            "probabilities": probabilities,
            "top_matches": top_matches,
        }


# Model registry
model_registry = "/mnt/user_storage/mlflow/doggos"
experiment_name = "doggos"
mlflow.set_tracking_uri(f"file:{model_registry}")

# Best run
sorted_runs = mlflow.search_runs(
    experiment_names=[experiment_name],
    order_by=["metrics.val_loss ASC"],
)
best_run = sorted_runs.iloc[0]
artifacts_dir = urlparse(best_run.artifact_uri).path
embeddings_path = os.path.join("/mnt/user_storage", "doggos/embeddings")

# Define app
app = Doggos.bind(
    classifier=ClassPredictor.bind(artifacts_dir=artifacts_dir),
    embedder=EmbeddingSimilarity.bind(embeddings_path=embeddings_path),
)

if __name__ == "__main__":

    # Run service locally
    serve.run(app, route_prefix="/")
