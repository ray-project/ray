# flake8: noqa
# isort: skip_file
# fmt: off

# __hf_super_quick_start__
import ray
import numpy as np
from typing import Dict

ds = ray.data.from_numpy(np.asarray(["Complete this", "for me"]))

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

# __hf_no_ray_start__
import numpy as np
from typing import Dict
from transformers import pipeline

batches = {"data": np.asarray(["Complete this", "for me"])}

model = pipeline("text-generation", model="gpt2")

def transform(batch: Dict[str, np.ndarray]):
    return model(list(batch["data"]), max_length=20)

results = transform(batches)
# __hf_no_ray_end__

# __hf_quickstart_load_start__
import ray
import numpy as np
from typing import Dict

ds = ray.data.from_numpy(np.asarray(["Complete this", "for me"]))
# __hf_quickstart_load_end__


# __hf_quickstart_model_start__
class HuggingFacePredictor:
    def __init__(self):  # <1>
        from transformers import pipeline
        self.model = pipeline("text-generation", model="gpt2")

    def __call__(self, batch: Dict[str, np.ndarray]):  # <2>
        predictions = self.model(list(batch["data"]), max_length=20, num_return_sequences=1)
        # `predictions` is a list of length-one lists. For example:
        # [[{'generated_text': '...'}], ..., [{'generated_text': "..."}]]
        batch["output"] = [sequences[0]["generated_text"] for sequences in predictions]
        return batch
# __hf_quickstart_model_end__


# __hf_quickstart_prediction_test_start__
hfp = HuggingFacePredictor()
batch = ds.take_batch(10)
test = hfp(batch)
# __hf_quickstart_prediction_test_end__


# __hf_quickstart_prediction_start__
scale = ray.data.ActorPoolStrategy(size=2)
predictions = ds.map_batches(HuggingFacePredictor, compute=scale)

predictions.show(limit=1)
# [{'generated_text': 'Complete these sentences until you understand them.'}]
# __hf_quickstart_prediction_end__

# __hf_quickstart_air_start__
import pandas as pd
from transformers import AutoConfig, AutoModelForCausalLM, AutoTokenizer
from transformers.pipelines import pipeline
from ray.train.huggingface import HuggingFacePredictor


tokenizer = AutoTokenizer.from_pretrained("sgugger/gpt2-like-tokenizer")
model_config = AutoConfig.from_pretrained("gpt2")
model = AutoModelForCausalLM.from_config(model_config)
pipeline = pipeline("text-generation", model=model, tokenizer=tokenizer)

predictor = HuggingFacePredictor(pipeline=pipeline)

prompts = pd.DataFrame(["Complete these sentences", "for me"], columns=["sentences"])
predictions = predictor.predict(prompts)
# __hf_quickstart_air_end__
# fmt: on
