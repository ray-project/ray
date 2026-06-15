# flake8: noqa

# __start_graph__
# File name: serve_quickstart_composed.py
from starlette.requests import Request

import ray
from ray import serve
from ray.serve.handle import DeploymentHandle

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer


@serve.deployment
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
            input_ids, num_beams=4, early_stopping=True, max_length=300
        )

        # Post-process output to return only the translation text
        translation = self.tokenizer.decode(
            output_ids[0], skip_special_tokens=True, clean_up_tokenization_spaces=False
        )

        return translation


@serve.deployment
class Summarizer:
    def __init__(self, translator: DeploymentHandle):
        self.translator = translator

        # Load model.
        self.tokenizer = AutoTokenizer.from_pretrained("t5-small")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("t5-small")

    def summarize(self, text: str) -> str:
        # Run inference
        input_ids = self.tokenizer(f"summarize: {text}", return_tensors="pt").input_ids
        output_ids = self.model.generate(
            input_ids,
            num_beams=4,
            early_stopping=True,
            length_penalty=2.0,
            no_repeat_ngram_size=3,
            min_length=5,
            max_length=15,
        )

        # Post-process output to return only the summary text
        summary = self.tokenizer.decode(
            output_ids[0], skip_special_tokens=True, clean_up_tokenization_spaces=False
        )

        return summary

    async def __call__(self, http_request: Request) -> str:
        english_text: str = await http_request.json()
        summary = self.summarize(english_text)

        translation = await self.translator.translate.remote(summary)
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
