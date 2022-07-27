# flake8: noqa

# Stub functions for testing
import ray

ray.init()


def stub(*args, **kwargs):
    pass


ray.init = stub

# __deployment_full_start__
# File name: model_on_ray_serve.py
import ray
from ray import serve
from transformers import pipeline


@serve.deployment
class Translator:
    def __init__(self):
        # Load model
        self.model = pipeline("translation_en_to_fr", model="t5-small")

    def translate(self, text: str) -> str:
        # Run inference
        model_output = self.model(text)

        # Post-process output to return only the translation text
        translation = model_output[0]["translation_text"]

        return translation

    async def __call__(self, http_request) -> str:
        english_text: str = await http_request.json()
        return self.translate(english_text)


ray.init(address="auto")

serve.start(detached=True)

translator = Translator.bind()
serve.run(translator)
# __deployment_full_end__

import requests

response = requests.post("http://127.0.0.1:8000/", json="Hello world!").text
assert response == "Bonjour monde!"

serve.shutdown()
ray.shutdown()
