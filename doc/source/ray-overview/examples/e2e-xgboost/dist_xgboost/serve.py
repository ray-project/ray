# Note: requires train.py to be run first for the model and preprocessor to be saved to MLFlow

import os

os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

import asyncio

import aiohttp
import pandas as pd
import requests
import xgboost
from ray import serve
from ray.serve.handle import DeploymentHandle
from starlette.requests import Request

from dist_xgboost.data import load_model_and_preprocessor


@serve.deployment(num_replicas=2, ray_actor_options={"num_cpus": 2})
class XGBoostModel:
    def __init__(self, loader):
        # pass in loader function from the outer context to
        # make it easier to mock during testing
        self.preprocessor, self.model = loader()

    @serve.batch(max_batch_size=16, batch_wait_timeout_s=0.1)
    async def predict_batch(self, input_data: list[dict]) -> list[float]:
        print(f"Batch size: {len(input_data)}")
        # Convert list of dictionaries to DataFrame
        input_df = pd.DataFrame(input_data)
        # Preprocess the input
        preprocessed_batch = self.preprocessor.transform_batch(input_df)
        # Create DMatrix for prediction
        dmatrix = xgboost.DMatrix(preprocessed_batch)
        # Get predictions
        predictions = self.model.predict(dmatrix)
        return predictions.tolist()

    async def __call__(self, request: Request):
        # Parse the request body as JSON
        input_data = await request.json()
        return await self.predict_batch(input_data)


xgboost_model = XGBoostModel.bind(load_model_and_preprocessor)
_handle: DeploymentHandle = serve.run(
    xgboost_model, name="xgboost-breast-cancer-classifier"
)


def main():
    sample_input = {
        "mean radius": 14.9,
        "mean texture": 22.53,
        "mean perimeter": 102.1,
        "mean area": 685.0,
        "mean smoothness": 0.09947,
        "mean compactness": 0.2225,
        "mean concavity": 0.2733,
        "mean concave points": 0.09711,
        "mean symmetry": 0.2041,
        "mean fractal dimension": 0.06898,
        "radius error": 0.253,
        "texture error": 0.8749,
        "perimeter error": 3.466,
        "area error": 24.19,
        "smoothness error": 0.006965,
        "compactness error": 0.06213,
        "concavity error": 0.07926,
        "concave points error": 0.02234,
        "symmetry error": 0.01499,
        "fractal dimension error": 0.005784,
        "worst radius": 16.35,
        "worst texture": 27.57,
        "worst perimeter": 125.4,
        "worst area": 832.7,
        "worst smoothness": 0.1419,
        "worst compactness": 0.709,
        "worst concavity": 0.9019,
        "worst concave points": 0.2475,
        "worst symmetry": 0.2866,
        "worst fractal dimension": 0.1155,
    }
    sample_target = 0

    # create a batch of 100 requests and send them at once

    url = "http://127.0.0.1:8000/"

    # Example with a single request (synchronous call)
    prediction = requests.post(url, json=sample_input).json()
    print(f"Prediction: {prediction:.4f}")
    print(f"Ground truth: {sample_target}")

    # Send many requests at once instead of blocking after each request
    # using asyncio and aiohttp

    sample_input_list = [sample_input] * 100

    async def fetch(session, url, data):
        async with session.post(url, json=data) as response:
            return await response.json()

    async def fetch_all(requests: list):
        async with aiohttp.ClientSession() as session:
            tasks = [fetch(session, url, input) for input in requests]
            responses = await asyncio.gather(*tasks)
            return responses

    responses = asyncio.run(fetch_all(sample_input_list))

    print(f"First prediction: {responses[0]:.4f}")


if __name__ == "__main__":
    main()
