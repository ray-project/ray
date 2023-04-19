import ray
import pandas as pd
from transformers import pipeline


class HuggingFacePredictor:
    def __init__(self):
        self.model = pipeline("text-generation", model="gpt2")

    def __call__(self, batch):
        return self.model(list(batch["text"]), max_length=20)


prompts = pd.DataFrame(["Complete these sentences", "for me"], columns=["text"])
ds = ray.data.from_pandas(prompts)

scale = ray.data.ActorPoolStrategy(2)
predictions = ds.map_batches(HuggingFacePredictor, compute=scale)

predictions.show(limit=1)
# [{'generated_text': 'Complete these sentences until you understand them.'}]
