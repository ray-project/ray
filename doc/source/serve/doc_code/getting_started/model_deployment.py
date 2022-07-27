# flake8: noqa

# __import_start__
import ray
from ray import serve

# __import_end__

# __model_start__
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


# __model_end__


def stub(*args, **kwargs):
    pass


# Stub ray.init for testing
ray.init()
ray.init = stub

# __connect_to_ray_cluster_start__
ray.init(address="auto")
# __connect_to_ray_cluster_end__

# __model_deploy_start__
translator = Translator.bind()
serve.run(translator)
# __model_deploy_end__

# __client_function_start__
# File name: model_client.py
import requests

english_text = "Hello world!"

response = requests.post("http://127.0.0.1:8000/", json=english_text)
french_text = response.text

print(french_text)
# __client_function_end__

assert french_text == "Bonjour monde!"

serve.shutdown()
ray.shutdown()
