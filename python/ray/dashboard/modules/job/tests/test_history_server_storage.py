from ray.dashboard.modules.job.common import JobInfo, JobStatus
from ray.dashboard.modules.job.history_server_storage import (
    append_job_event,
    append_actor_events,
    LocalFileStorage,
)
import time


def test_append_job_event():
    storage = LocalFileStorage("/tmp/", "test_cluster")
    submission_id = "submission_id_1"

    metadata = {"foo": "bar"}
    runtime_env = {"env_vars": {"TEST": "123"}}
    entrypoint_resources = {"Custom": 1}
    job_resources = {"replica": 2, "bundle": {"GPU": 1}}
    job_info: JobInfo = JobInfo(
        entrypoint="python test.py",
        status=JobStatus.QUEUED,
        start_time=int(time.time() * 1000),
        metadata=metadata,
        runtime_env=runtime_env,
        entrypoint_num_cpus=2,
        entrypoint_num_gpus=4,
        entrypoint_resources=entrypoint_resources,
        job_resources=job_resources,
        queue_timeout=60,
        gang_schedule_timeout=600,
    )

    append_job_event(storage, submission_id, job_info.to_json())

    job_info.status = JobStatus.RUNNING
    append_job_event(storage, submission_id, job_info.to_json())


def test_append_actor_event():
    storage = LocalFileStorage("/tmp/", "test_cluster")
    actor_info = {
        "actorId": "bce926b88c54ebb4e8b790bd01000000",
        "jobId": "01000000",
        "address": {
            "rayletId": "56b97436e67393f15565e024278867700c2bd0ec6ca9e0dac7261fd1",
            "ipAddress": "127.0.0.1",
            "port": 55428,
            "workerId": "5a1c3eceb99da5dc6a7d8769b8fec657b73be8bdbc8b97bf3fdaee9f",
        },
        "name": "SERVE_REPLICA::MyModelDeployment#wxgUmq_0",
        "className": "ServeReplica:MyModelDeployment",
        "state": "DEAD",
        "numRestarts": "0",
        "timestamp": 1689060097299.0,
        "pid": 29686,
        "startTime": 1689060085642,
        "endTime": 1689060097298,
        "actorClass": "ServeReplica:MyModelDeployment",
        "exitDetail": "The actor is dead because its node has died. Node Id: 56b97436e67393f15565e024278867700c2bd0ec6ca9e0dac7261fd1",
        "requiredResources": {"CPU": 1.0},
    }

    append_actor_events(storage, [actor_info, actor_info, actor_info])
