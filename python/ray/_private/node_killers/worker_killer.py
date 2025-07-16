import asyncio
import logging

import ray
from ray._private.test_utils import ResourceKillerActor


@ray.remote(num_cpus=0)
class WorkerKillerActor(ResourceKillerActor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Kill worker immediately so that the task does
        # not finish successfully on its own.
        self.kill_immediately_after_found = True

        from ray.util.state.api import StateApiClient
        from ray.util.state.common import ListApiOptions

        self.client = StateApiClient()
        self.task_options = ListApiOptions(
            filters=[
                ("state", "=", "RUNNING"),
                ("name", "!=", "WorkerKillActor.run"),
            ]
        )

    async def _find_resources_to_kill(self):
        from ray.util.state.common import StateResource

        process_to_kill_task_id = None
        process_to_kill_pid = None
        process_to_kill_node_id = None
        while process_to_kill_pid is None and self.is_running:
            tasks = self.client.list(
                StateResource.TASKS,
                options=self.task_options,
                raise_on_missing_output=False,
            )
            if self.kill_filter_fn is not None:
                tasks = list(filter(self.kill_filter_fn(), tasks))

            for task in tasks:
                if task.worker_id is not None and task.node_id is not None:
                    process_to_kill_task_id = task.task_id
                    process_to_kill_pid = task.worker_pid
                    process_to_kill_node_id = task.node_id
                    break

            # Give the cluster some time to start.
            await asyncio.sleep(0.1)

        return [(process_to_kill_task_id, process_to_kill_pid, process_to_kill_node_id)]

    def _kill_resource(
        self, process_to_kill_task_id, process_to_kill_pid, process_to_kill_node_id
    ):
        if process_to_kill_pid is not None:

            @ray.remote
            def kill_process(pid):
                import psutil

                proc = psutil.Process(pid)
                proc.kill()

            scheduling_strategy = (
                ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
                    node_id=process_to_kill_node_id,
                    soft=False,
                )
            )
            kill_process.options(scheduling_strategy=scheduling_strategy).remote(
                process_to_kill_pid
            )
            logging.info(
                f"Killing pid {process_to_kill_pid} on node {process_to_kill_node_id}"
            )
            # Store both task_id and pid because retried tasks have same task_id.
            self.killed.add((process_to_kill_task_id, process_to_kill_pid))
