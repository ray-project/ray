# flake8: noqa

# __start_graph__
# File name: graph.py
from starlette.requests import Request

import ray
from ray import serve
from ray.serve.handle import RayServeHandle

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


@serve.deployment
class Summarizer:
    def __init__(self, translator: RayServeHandle):
        # Load model
        self.model = pipeline("summarization", model="t5-small")
        self.translator = translator

    def summarize(self, text: str) -> str:
        # Run inference
        model_output = self.model(text, min_length=5, max_length=15)

        # Post-process output to return only the summary text
        summary = model_output[0]["summary_text"]

        return summary

    async def __call__(self, http_request: Request) -> str:
        english_text: str = await http_request.json()
        summary = self.summarize(english_text)

        translation_ref = await self.translator.translate.remote(summary)
        translation = await translation_ref

        return translation


app = Summarizer.bind(Translator.bind())
# __end_graph__

serve.run(app)

# __start_client__
# File name: composed_client.py
import requests

english_text = (
    "It was the best of times, it was the worst of times, it was the age "
    "of wisdom, it was the age of foolishness, it was the epoch of belief"
)
response = requests.post("http://127.0.0.1:8000/", json=english_text)
french_text = response.text

print(french_text)
# __end_client__

assert french_text == "c'était le meilleur des temps, c'était le pire des temps ."

serve.shutdown()
ray.shutdown()
