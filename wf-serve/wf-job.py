import ray
import uuid
from ray import serve
import ray.data
from ray import workflow
ray.init(address='auto')
serve.start(detached=True)

from fastapi import FastAPI
import time
app = FastAPI()


def process(v):
    time.sleep(2)
    return v


process_remote = ray.remote(process)
process_step = workflow.step(process)


@workflow.step
def pull_from_queue(task):
    from sqs_listener import SQSEventListener
    if task is not None:
        wf_id = "sqs_" + str(uuid.uuid1())
        # id should be fetched from task
        process_step.step(task.get("Messages")).run(workflow_id=wf_id)
    w = workflow.wait_for_event(SQSEventListener)
    return pull_from_queue.step(w)


@serve.deployment(route_prefix="/infer")
@serve.ingress(app)
class InferenceService:
    def __init__(self):
        import nest_asyncio
        nest_asyncio.apply()
        workflow.init()
        pull_from_queue.step(None).run_async("sqs-job")
        workflow.resume_all()

    @app.get("/get_result")
    def get_result(self, task_id):
        try:
            return ray.get(workflow.get_output(task_id))
        except ValueError as e:
            return str(e)

    @app.get("/get_status")
    def get_status(self, task_id):
        try:
            return workflow.get_status(task_id)
        except ray.workflow.common.WorkflowNotFoundError:
            return f"{task_id} doesn't exist\n"

    @app.get("/list_all")
    def list_all(self):
        return workflow.list_all()

    @app.get("/run_job")
    def run_job(self, e):
        return process(e)

    @app.get("/schedule")
    def schedule(self, e):
        job_id = "http_" + str(uuid.uuid1())
        # we can also put periodical job here
        process_step.step(e).run_async(workflow_id=job_id)
        return job_id


InferenceService.deploy()

# need to setup sqs queue and crediential in ray/wf-serve/sqs_listener.py

# >> python push-to-queue.py
# >> curl -X GET http://127.0.0.1:8000/infer/list_all
# [["sqs_12dd5164-4833-11ec-bfc2-06ac50ea2df5","RUNNING"],["sqs-job","RUNNING"]]
# # wait for 2 seconds
# >> curl -X GET http://127.0.0.1:8000/infer/list_all
# [["sqs_12dd5164-4833-11ec-bfc2-06ac50ea2df5","SUCCESSFUL"],["sqs-job","RUNNING"]]
# >> curl -X GET http://127.0.0.1:8000/infer/get_result?task_id=sqs_12dd5164-4833-11ec-bfc2-06ac50ea2df5
#
# >> curl -X GET http://127.0.0.1:8000/infer/schedule?e=ABC
# "http_f449184c-4836-11ec-bfc2-06ac50ea2df5"
# >> curl -X GET http://127.0.0.1:8000/infer/list_all
# [["sqs-job","RUNNING"],["http_f449184c-4836-11ec-bfc2-06ac50ea2df5","SUCCESSFUL"], ["sqs_12dd5164-4833-11ec-bfc2-06ac50ea2df5","SUCCESSFUL"]]
# >> curl -X GET http://127.0.0.1:8000/infer/get_result?task_id=http_f449184c-4836-11ec-bfc2-06ac50ea2df5
# "ABC"
# >> curl -X GET http://127.0.0.1:8000/infer/run_job?e=abc
# "abc"
# >> curl -X GET http://127.0.0.1:8000/infer/list_all

# [["sqs-job","RUNNING"],["http_f449184c-4836-11ec-bfc2-06ac50ea2df5","SUCCESSFUL"], ["sqs_12dd5164-4833-11ec-bfc2-06ac50ea2df5","SUCCESSFUL"]]
