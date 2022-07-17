# __deployment_full_start__
# File name: model_on_ray_serve.py
import ray
from ray import serve
from transformers import pipeline


def summarize(text):
    summarizer = pipeline("summarization", model="t5-small")
    summary_list = summarizer(text)
    summary = summary_list[0]["summary_text"]
    return summary


ray.init(address="auto")
serve.start(detached=True)


@serve.deployment
def router(request):
    txt = request.query_params["txt"]
    return summarize(txt)


router.deploy()
# __deployment_full_end__
