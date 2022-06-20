import requests
from transformers import pipeline
from ray import serve


# 1: Wrap the pretrained sentiment analysis model in a Serve deployment.
@serve.deployment(route_prefix="/")
class SentimentAnalysisDeployment:
    def __init__(self):
        self._model = pipeline("sentiment-analysis")

    def __call__(self, request):
        return self._model(request.query_params["text"])[0]


# 2: Deploy the deployment.
serve.start()
SentimentAnalysisDeployment.deploy()

# 3: Query the deployment and print the result.
print(
    requests.get(
        "http://localhost:8000/", params={"text": "Ray Serve is great!"}
    ).json()
)
# {'label': 'POSITIVE', 'score': 0.9998476505279541}
