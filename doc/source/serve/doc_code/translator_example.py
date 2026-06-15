import requests

# __serve_example_begin__
import starlette

from transformers import AutoModelForSeq2SeqLM, AutoTokenizer

from ray import serve


@serve.deployment
class Translator:
    def __init__(self):
        self.tokenizer = AutoTokenizer.from_pretrained("t5-small")
        self.model = AutoModelForSeq2SeqLM.from_pretrained("t5-small")

    def translate(self, text: str) -> str:
        input_ids = self.tokenizer(
            f"translate English to German: {text}", return_tensors="pt"
        ).input_ids
        output_ids = self.model.generate(
            input_ids, num_beams=4, early_stopping=True, max_new_tokens=40
        )
        return self.tokenizer.decode(output_ids[0], skip_special_tokens=True)

    async def __call__(self, req: starlette.requests.Request):
        req = await req.json()
        return self.translate(req["text"])


app = Translator.bind()
# __serve_example_end__


serve.run(app, name="app2", route_prefix="/translate")

# __request_begin__
text = "Hello, the weather is quite fine today!"
resp = requests.post("http://localhost:8000/translate", json={"text": text})

print(resp.text)
# 'Hallo, das Wetter ist heute ziemlich gut!'
# __request_end__

assert resp.text == "Hallo, das Wetter ist heute ziemlich gut!", f"got {resp.text!r}"
