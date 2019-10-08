import time

import requests

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json


def echo1(context):
	context["query_string"] += 'FROM MODEL1 -> '
    return context
def echo2(context):
	context += 'FROM MODEL2 -> '
    return context

serve.init(blocking=True)

serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)

serve.create_backend(echo, "echo:v1")
serve.create_no_http_service("serve1")
serve.link_service("serve1", "echo:v1")
serve.add_service_to_pipeline("pipeline1","serve1")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)