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

max_concurrency = 10


@workflow.step
def pull_from_queue(ready, pending):
    for _ in range(max_concurrency - len(pending)):
        w = workflow.wait_for_event(SQSEventListener)
        p = process_event.step(w)
        pending.append(p)
    pull_from_queue.step(workflow.wait(pending))


@ray.remote
def record(msg):
    with p.open(mode='a') as f:
        f.write(json.dumps(msg))
        f.write("\n")


@workflow.step
def process_event(e):
    return record.remote(e.get("Messages"))


pull_from_queue.step([], []).run("sqs-job")
