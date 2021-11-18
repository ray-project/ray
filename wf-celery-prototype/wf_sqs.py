import ray
from ray import workflow
from pathlib import Path
from sqs_listener import SQSEventListener
import json
#ray.init(address='auto')

workflow.init()
p = Path("/tmp/ray/record.txt")
p.unlink(missing_ok=True)
p.touch()



@ray.remote
def record(msg):
    with p.open(mode='a') as f:
        f.write(json.dumps(msg))
        f.write("\n")


@workflow.step
def process_event(e):
    return record.remote(e.get("Messages"))


pull_from_queue.step(None).run("sqs-job")
