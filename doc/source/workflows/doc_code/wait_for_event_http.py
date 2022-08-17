import ray
from ray import workflow, serve
from ray.workflow.http_event_provider import HTTPListener

from time import sleep
import requests

ray.init(storage="/tmp/ray/workflow/data")
# Start a Ray Serve instance. This will automatically start
# or connect to an existing Ray cluster.
serve.start(detached=True)

# fmt: off
# __wait_for_event_begin__
# File name: wait_for_event_http.py
# Create a task waiting for an http event with a JSON message.
# The JSON message is expected to have an event_key field
# and an event_payload field.

event_task = workflow.wait_for_event(HTTPListener, event_key="my_event_key")

obj_ref = workflow.run_async(event_task, workflow_id="workflow_receive_event_by_http")

# __wait_for_event_end__

# wait for the backend to be ready
sleep(10)

# fmt: off
# __submit_event_begin__
# File name: wait_for_event_http.py
res = requests.post(
        "http://127.0.0.1:8000/event/send_event/"
        + "workflow_receive_event_by_http",
        json={"event_key": "my_event_key", "event_payload": "my_event_message"},
    )
if res.status_code == 200:
    print("event processed successfully")
elif res.status_code == 500:
    print("request sent but workflow event processing failed")
elif res.status_code == 404:
    print("request sent but either workflow_id or event_key is not found")

# __submit_event_end__

assert res.status_code == 200
key, message = ray.get(obj_ref)
assert key == "my_event_key"
assert message == "my_event_message"
