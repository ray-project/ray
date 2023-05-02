# flake8: noqa
# isort: skip_file
# fmt: off

# __hf_quickstart_load_start__
import ray
import pandas as pd


prompts = pd.DataFrame(["Complete these sentences", "for me"], columns=["text"])
ds = ray.data.from_pandas(prompts)
# __hf_quickstart_load_end__


# __hf_quickstart_model_start__
class HuggingFacePredictor:
    def __init__(self):  # <1>
        from transformers import pipeline
        self.model = pipeline("text-generation", model="gpt2")

    def __call__(self, batch):  # <2>
        # TODO make this run with "numpy" format
        return self.model(list(batch["text"]), max_length=20)
# __hf_quickstart_model_end__


# __hf_quickstart_prediction_start__
hfp = HuggingFacePredictor()
batch = ds.take_batch(10)
test = hfp(batch)

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
