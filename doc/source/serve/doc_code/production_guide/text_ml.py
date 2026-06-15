# flake8: noqa

# __example_start__
from starlette.requests import Request
from typing import Dict

import ray
from ray import serve
from ray.serve.handle import DeploymentHandle

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer, pipeline

# Map a language to the task prefix that t5-small expects.
LANGUAGE_TO_PREFIX = {
    "french": "translate English to French: ",
    "german": "translate English to German: ",
    "romanian": "translate English to Romanian: ",
}


@serve.deployment
class Translator:
    def __init__(self):
        self.language = "french"
        self.prefix = LANGUAGE_TO_PREFIX[self.language]
        self.tokenizer = AutoTokenizer.from_pretrained("t5-small")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("t5-small")

    def translate(self, text: str) -> str:
        input_ids = self.tokenizer(
            f"{self.prefix}{text}", return_tensors="pt"
        ).input_ids
        output_ids = self.model.generate(input_ids, max_new_tokens=40)

        translation = self.tokenizer.decode(output_ids[0], skip_special_tokens=True)

        return translation

    def reconfigure(self, config: Dict):
        self.language = config.get("language", "french")

        self.prefix = LANGUAGE_TO_PREFIX.get(self.language.lower())
        if self.prefix is None:
            self.prefix = LANGUAGE_TO_PREFIX["french"]


@serve.deployment
class Summarizer:
    def __init__(self, translator: DeploymentHandle):
        # Load model
        self.model = pipeline("summarization", model="t5-small")
        self.translator = translator
        self.min_length = 5
        self.max_length = 15

    def summarize(self, text: str) -> str:
        # Run inference
        model_output = self.model(
            text, min_length=self.min_length, max_length=self.max_length
        )

        # Post-process output to return only the summary text
        summary = model_output[0]["summary_text"]

        return summary

    async def __call__(self, http_request: Request) -> str:
        english_text: str = await http_request.json()
        summary = self.summarize(english_text)

        return await self.translator.translate.remote(summary)

    def reconfigure(self, config: Dict):
        self.min_length = config.get("min_length", 5)
        self.max_length = config.get("max_length", 15)


app = Summarizer.bind(Translator.bind())
# __example_end__

serve.run(app)

# __start_client__
import requests

english_text = (
    "It was the best of times, it was the worst of times, it was the age "
    "of wisdom, it was the age of foolishness, it was the epoch of belief"
)
response = requests.post("http://127.0.0.1:8000/", json=english_text)
french_text = response.text

print(french_text)
# 'c'était le meilleur des temps, c'était le pire des temps .'
# __end_client__

assert isinstance(french_text, str) and french_text

serve.run(
    Summarizer.bind(Translator.options(user_config={"language": "german"}).bind())
)


# __start_second_client__
import requests

english_text = (
    "It was the best of times, it was the worst of times, it was the age "
    "of wisdom, it was the age of foolishness, it was the epoch of belief"
)
response = requests.post("http://127.0.0.1:8000/", json=english_text)
german_text = response.text

print(german_text)
# 'Es war die beste Zeit, es war die schlimmste Zeit .'
# __end_second_client__

assert isinstance(german_text, str) and german_text

serve.shutdown()
ray.shutdown()
