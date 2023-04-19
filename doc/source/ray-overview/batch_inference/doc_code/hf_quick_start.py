# flake8: noqa
# isort: skip_file

# __hf_quickstart_load_start__
import ray
import pandas as pd


prompts = pd.DataFrame(["Complete these sentences", "for me"], columns=["text"])
ds = ray.data.from_pandas(prompts)
# __bp_quickstart_load_end__


class HuggingFacePredictor:
    def __init__(self):
        from transformers import pipeline
        self.model = pipeline("text-generation", model="gpt2")

    def __call__(self, batch):
        return self.model(list(batch["text"]), max_length=20)


scale = ray.data.ActorPoolStrategy(2)
predictions = ds.map_batches(HuggingFacePredictor, compute=scale)

predictions.show(limit=1)
# [{'generated_text': 'Complete these sentences until you understand them.'}]
