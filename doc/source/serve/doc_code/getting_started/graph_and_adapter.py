# flake8: noqa

# Stub functions for testing
import ray

ray.init()


def stub(*arsg, **kwargs):
    pass


ray.init = stub

# __start__
# File name: model_on_ray_serve.py
import ray
from ray import serve

from ray.serve.drivers import DAGDriver
from ray.serve.http_adapters import json_request
from ray.serve.deployment_graph import InputNode

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


ray.init(address="auto")

with InputNode() as json_data:
    translator_node = Translator.bind()
    translate_method_node = translator_node.translate.bind(json_data)

deployment_graph = DAGDriver.bind(translate_method_node, http_adapter=json_request)
serve.run(deployment_graph)
# __end__

import requests

response = requests.post("http://127.0.0.1:8000/", json="Hello world!").json()
assert response == "Bonjour monde!"

serve.shutdown()
ray.shutdown()
