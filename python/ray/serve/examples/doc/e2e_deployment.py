# __import_start__
import ray
from ray import serve

# __import_end__

# __local_model_start__
from transformers import pipeline


def summarize(text):
    summarizer = pipeline("summarization", model="t5-small")
    summary_list = summarizer(text)
    summary = summary_list[0]["summary_text"]
    return summary


# __local_model_end__

# __start_ray_cluster_start__
ray.init(address="auto", namespace="serve")
# __start_ray_cluster_end__

# __start_serve_start__
serve.start(detached=True)
# __start_serve_end__


# __router_start__
@serve.deployment
def router(request):
    txt = request.query_params["txt"]
    return summarize(txt)


# __router_end__

# __router_deploy_start__
router.deploy()
# __router_deploy_end__
