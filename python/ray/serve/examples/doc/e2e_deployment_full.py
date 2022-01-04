# __deployment_full_start__
import ray
from ray import serve
from transformers import pipeline

def summarize(text):
    summarizer = pipeline("summarization", model="t5-small")
    summary_list = summarizer(text)
    summary = summary_list[0]["summary_text"]
    return summary

ray.init(address="auto", namespace="serve")
serve.start()

@serve.deployment
def router(request):
    txt = request.query_params["txt"]
    return summarize(txt)

router.deploy()
# __deployment_full_end__