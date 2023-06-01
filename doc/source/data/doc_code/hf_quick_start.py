# flake8: noqa
# isort: skip_file
# fmt: off

# __hf_super_quick_start__
import ray
import numpy as np
from typing import Dict

# Step 1: Create a Ray Dataset from in-memoruy Numpy arrays. 
ds = ray.data.from_numpy(np.asarray(["Complete this", "for me"]))

# Define a Predictor class 
class HuggingFacePredictor:
    def __init__(self):
        from transformers import pipeline
        self.model = pipeline("text-generation", model="gpt2")

    def __call__(self, batch: Dict[str, np.ndarray]):
        predictions = self.model(list(batch["data"]), max_length=20, num_return_sequences=1)
        # `predictions` is a list of length-one lists. For example:
        # [[{'generated_text': '...'}], ..., [{'generated_text': "..."}]]
        batch["output"] = [sequences[0]["generated_text"] for sequences in predictions]
        return batch

scale = ray.data.ActorPoolStrategy(size=2)
predictions = ds.map_batches(HuggingFacePredictor, compute=scale)
predictions.show(limit=1)
# __hf_super_quick_end__
