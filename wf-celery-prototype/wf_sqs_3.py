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
    return pull_from_queue.step(start_event.step(w))


@ray.remote
def record(msg):
    with p.open(mode='a') as f:
        f.write(json.dumps(msg))
        f.write("\n")


@workflow.step
def start_event(e):
    @workflow.step
    def do_process(e):
        record.remote(e)

    do_process.step(e.get("Messages")).run_async(
        e["ResponseMetadata"]["RequestId"])


pull_from_queue.step(None).run("sqs-job")
