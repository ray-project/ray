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
@workflow.step
def pull_from_queue(task):
    w = workflow.wait_for_event(SQSEventListener)
    print("Another pull")
    return pull_from_queue.step(process_event.step(w))

@workflow.step
def process_event(e):
    with p.open(mode='a') as f:
        f.write(json.dumps(e))
        f.write("\n")

pull_from_queue.step(None).run("sqs-job")
