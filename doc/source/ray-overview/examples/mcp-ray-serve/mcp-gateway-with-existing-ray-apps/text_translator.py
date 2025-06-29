from ray import serve
from starlette.requests import Request

@serve.deployment(
    name="text_translator",
    num_replicas=2,
    ray_actor_options={
        "num_gpus": 0.5,
        "runtime_env": {                 
            "pip": [
                "transformers",
                "torch"
                ]
            }
        },
)
class Translator:
    def __init__(self):
        from transformers import pipeline
        self.model = pipeline("translation_en_to_fr", model="t5-small")

    def translate(self, text: str) -> str:
        out = self.model(text)
        return out[0]["translation_text"]

    async def __call__(self, request: Request) -> str:
        english: str = await request.json()
        return self.translate(english)

# 3) Bind the deployment into an application for config-generation
app = Translator.bind()
