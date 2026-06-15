# flake8: noqa

# __deployment_full_start__
# File name: serve_quickstart.py
from starlette.requests import Request

import ray
from ray import serve

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer


@serve.deployment(num_replicas=2, ray_actor_options={"num_cpus": 0.2, "num_gpus": 0})
class Translator:
    def __init__(self):
        # Load model
        self.tokenizer = AutoTokenizer.from_pretrained("t5-small")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("t5-small")

    def translate(self, text: str) -> str:
        # Run inference
        input_ids = self.tokenizer(
            f"translate English to French: {text}", return_tensors="pt"
        ).input_ids
        output_ids = self.model.generate(
            input_ids, num_beams=4, early_stopping=True, max_new_tokens=40
        )

        # Post-process output to return only the translation text
        translation = self.tokenizer.decode(output_ids[0], skip_special_tokens=True)

        return translation

    async def __call__(self, http_request: Request) -> str:
        english_text: str = await http_request.json()
        return self.translate(english_text)


translator_app = Translator.bind()
# __deployment_full_end__

translator_app = Translator.options(ray_actor_options={}).bind()
serve.run(translator_app)

import requests

response = requests.post("http://127.0.0.1:8000/", json="Hello world!").text
assert response == "Bonjour monde!", f"got {response!r}"

serve.shutdown()
ray.shutdown()
