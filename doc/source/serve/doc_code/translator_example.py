import requests

# __serve_example_begin__
import starlette

from transformers import pipeline

from ray import serve


@serve.deployment
class Translator:
    def __init__(self):
        self.model = pipeline("translation_en_to_de", model="t5-small")

    def translate(self, text: str) -> str:
        return self.model(text)[0]["translation_text"]

    async def __call__(self, req: starlette.requests.Request):
        req = await req.json()
        return self.translate(req["text"])


app = Translator.options(route_prefix="/translate").bind()
# __serve_example_end__


serve.run(app, name="app2")
assert (
    requests.post(
        "http://localhost:8000/translate",
        json={"text": "Hello, the weather is quite fine today!"},
    ).text
    == "Hallo, das Wetter ist heute ziemlich gut!"
)
