# flake8: noqa

# Stub functions for testing
import ray

ray.init()


def stub(*args, **kwargs):
    pass


ray.init = stub

# __fastapi_start__
# File name: serve_with_fastapi.py
import ray
from ray import serve
from fastapi import FastAPI
from transformers import pipeline

app = FastAPI()


@serve.deployment
@serve.ingress(app)
class Translator:
    def __init__(self):
        # Load model
        self.model = pipeline("translation_en_to_fr", model="t5-small")

    @app.get("/translate")
    def translate(self, text: str) -> str:
        # Run inference
        model_output = self.model(text)

        # Post-process output to return only the translation text
        translation = model_output[0]["translation_text"]

        return translation


ray.init(address="auto")

translator = Translator.bind()
serve.run(translator)
# __fastapi_end__

# __fastapi_client_start__
# File name: fastapi_client.py
import requests

response = requests.get("http://127.0.0.1:8000/translate?text=Hello world!").json()
print(response)
# __fastapi_client_end__

assert response == "Bonjour monde!"

serve.shutdown()
ray.shutdown()
