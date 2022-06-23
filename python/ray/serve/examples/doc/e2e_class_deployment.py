# __deployment_class_start__
# File name: summarizer_on_ray_serve.py
import ray
from ray import serve
from transformers import pipeline

ray.init(address="auto")
serve.start(detached=True)


@serve.deployment
class Summarizer:
    def __init__(self):
        self.summarize = pipeline("summarization", model="t5-small")

    def __call__(self, request):
        txt = request.query_params["txt"]
        summary_list = self.summarize(txt)
        summary = summary_list[0]["summary_text"]
        return summary


Summarizer.deploy()
# __deployment_class_end__
