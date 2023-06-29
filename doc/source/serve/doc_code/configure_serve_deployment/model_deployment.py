# flake8: noqa

# __deployment_start__
import ray
from ray import serve
from fastapi import FastAPI

from transformers import pipeline

app = FastAPI()


@serve.deployment(
    name="Translator",
    route_prefix="/",
    num_replicas=2,
    ray_actor_options={"num_cpus": 0.2, "num_gpus": 0},
    max_concurrent_queries=100,
    # autoscaling_config={"min_replicas": 1, "initial_replicas": 2, "max_replicas": 5, "target_num_ongoing_requests_per_replica": 10},
    # user_config={},
    health_check_period_s=10,
    health_check_timeout_s=30,
    graceful_shutdown_timeout_s=20,
    graceful_shutdown_wait_loop_s=2,
)
@serve.ingress(app)
class Translator:
    def __init__(self):
        # Load model
        self.model = pipeline("translation_en_to_fr", model="t5-small")

    @app.post("/")
    def translate(self, text: str) -> str:
        # Run inference
        model_output = self.model(text)

        # Post-process output to return only the translation text
        translation = model_output[0]["translation_text"]

        return translation


translator_app = Translator.bind()
# __deployment_end__

translator_app = Translator.options(ray_actor_options={}).bind()

# __options_end__
serve.run(translator_app)

# __client_function_start__
# File name: model_client.py
import requests

response = requests.post("http://127.0.0.1:8000/", params={"text": "Hello world!"})
french_text = response.json()

print(french_text)
# __client_function_end__

assert french_text == "Bonjour monde!"

serve.shutdown()
ray.shutdown()
