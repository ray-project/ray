# flake8: noqa

# __deployment_start__
import ray
from ray import serve
from fastapi import FastAPI

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

app = FastAPI()


@serve.deployment(num_replicas=2, ray_actor_options={"num_cpus": 0.2, "num_gpus": 0})
@serve.ingress(app)
class Translator:
    def __init__(self):
        # Load model
        self.tokenizer = AutoTokenizer.from_pretrained("t5-small")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("t5-small")

    @app.post("/")
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


translator_app = Translator.bind()
# __deployment_end__

translator_app = Translator.options(ray_actor_options={}).bind()
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
