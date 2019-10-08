import time

import requests
from werkzeug import urls

from ray.experimental import serve
from ray.experimental.serve.utils import pformat_color_json
import json

def echo1(context):
	# query_string_dict = urls.url_decode(context["query_string"])
	message = ""
	# message += query_string_dict.get("message", "")
	# message += " "
	message += 'FROM MODEL1 -> '
	return message
	# context["query_string"] += 'FROM MODEL1 -> '
	# return context
def echo2(context):
	result = json.loads(context)
	result = result['result']
	result += 'FROM MODEL2 -> '
	return result

serve.init(blocking=True)

serve.create_endpoint_pipeline("pipeline1", "/echo", blocking=True)

serve.create_backend(echo1, "echo:v1")
serve.create_backend(echo2, "echo:v2")

serve.create_no_http_service("serve1")
serve.create_no_http_service("serve2")

serve.link_service("serve1", "echo:v1")
serve.link_service("serve2", "echo:v2")

serve.add_service_to_pipeline("pipeline1","serve1",blocking=True)
serve.add_service_to_pipeline("pipeline1","serve2",blocking=True)

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)