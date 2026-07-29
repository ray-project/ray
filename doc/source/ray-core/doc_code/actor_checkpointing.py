# __actor_checkpointing_manual_restart_begin__
import os
import sys
import ray
import json
import tempfile
import shutil


@ray.remote(num_cpus=1)
class Worker:
    def __init__(self):
        self.state = {"num_tasks_executed": 0}

    def execute_task(self, crash=False):
        if crash:
            sys.exit(1)

        # Execute the task
        # ...

        # Update the internal state
        self.state["num_tasks_executed"] = self.state["num_tasks_executed"] + 1

    def checkpoint(self):
        return self.state

    def restore(self, state):
        self.state = state


class Controller:
    def __init__(self):
        self.worker = Worker.remote()
        self.worker_state = ray.get(self.worker.checkpoint.remote())

    def execute_task_with_fault_tolerance(self):
        i = 0
        while True:
            i = i + 1
            try:
                ray.get(self.worker.execute_task.remote(crash=(i % 2 == 1)))
                # Checkpoint the latest worker state
                self.worker_state = ray.get(self.worker.checkpoint.remote())
                return
            except ray.exceptions.RayActorError:
                print("Actor crashes, restarting...")
                # Restart the actor and restore the state
                self.worker = Worker.remote()
                ray.get(self.worker.restore.remote(self.worker_state))


controller = Controller()
controller.execute_task_with_fault_tolerance()
controller.execute_task_with_fault_tolerance()
assert ray.get(controller.worker.checkpoint.remote())["num_tasks_executed"] == 2
# __actor_checkpointing_manual_restart_end__


# __actor_checkpointing_auto_restart_begin__
@ray.remote(max_restarts=-1, max_task_retries=-1)
class ImmortalActor:
    def __init__(self, checkpoint_file):
        self.checkpoint_file = checkpoint_file

        if os.path.exists(self.checkpoint_file):
            # Restore from a checkpoint
            with open(self.checkpoint_file, "r") as f:
                self.state = json.load(f)
        else:
            self.state = {}

    def update(self, key, value):
        import random

        if random.randrange(10) < 5:
            sys.exit(1)

        self.state[key] = value

        # Checkpoint the latest state
        with open(self.checkpoint_file, "w") as f:
            json.dump(self.state, f)

    def get(self, key):
        return self.state[key]


checkpoint_dir = tempfile.mkdtemp()
actor = ImmortalActor.remote(os.path.join(checkpoint_dir, "checkpoint.json"))
ray.get(actor.update.remote("1", 1))
ray.get(actor.update.remote("2", 2))
assert ray.get(actor.get.remote("1")) == 1
shutil.rmtree(checkpoint_dir)
# __actor_checkpointing_auto_restart_end__
