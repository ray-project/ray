# flake8: noqa

# __import_start__
from starlette.requests import Request

import ray
from ray import serve

# __import_end__

# __model_start__
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


# __model_end__

# __model_deploy_start__
translator_app = Translator.bind()
# __model_deploy_end__

translator_app = Translator.options(ray_actor_options={}).bind()
serve.run(translator_app)

# __client_function_start__
# File name: model_client.py
import requests

english_text = "Hello world!"

response = requests.post("http://127.0.0.1:8000/", json=english_text)
french_text = response.text

print(french_text)
# __client_function_end__

assert french_text == "Bonjour monde!", f"got {french_text!r}"

serve.shutdown()
ray.shutdown()
