import time

import pytest
import ray

from ray.experimental.state.api import (
    summarize_tasks,
    summarize_actors,
    # summarize_objects,
)
from ray._private.test_utils import wait_for_condition


def test_task_summary(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=2)

    @ray.remote
    def run_long_time_task():
        time.sleep(30)
        return True

    @ray.remote
    def task_wait_for_dep(dep):
        print(dep)

    a = task_wait_for_dep.remote(run_long_time_task.remote())  # noqa
    b = task_wait_for_dep.remote(run_long_time_task.remote())  # noqa

    def verify():
        # task_name -> states
        task_summary = summarize_tasks()
        assert "task_wait_for_dep" in task_summary
        assert "run_long_time_task" in task_summary
        assert (
            task_summary["task_wait_for_dep"]["state_counts"][
                "WAITING_FOR_DEPENDENCIES"
            ]
            == 2
        )
        assert task_summary["run_long_time_task"]["state_counts"]["RUNNING"] == 2
        assert task_summary["task_wait_for_dep"]["required_resources"] == {"CPU": 1.0}
        assert task_summary["task_wait_for_dep"]["type"] == "NORMAL_TASK"
        return True

    wait_for_condition(verify)


def test_actor_summary(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node(num_cpus=2)
    ray.init(address=cluster.address)
    cluster.add_node(num_cpus=2)

    @ray.remote(num_gpus=1)
    class Infeasible:
        pass

    @ray.remote(num_cpus=2)
    class Actor:
        pass

    infeasible = Infeasible.remote()  # noqa
    running = [Actor.remote() for _ in range(2)]  # noqa
    pending = Actor.remote()  # noqa

    def verify():
        summary = summarize_actors()
        actor_summary = None
        infeasible_summary = None
        for actor_class_name, s in summary.items():
            if ".Actor" in actor_class_name:
                actor_summary = s
            elif ".Infeasible" in actor_class_name:
                infeasible_summary = s

        assert actor_summary["state_counts"]["PENDING_CREATION"] == 1
        assert actor_summary["state_counts"]["ALIVE"] == 2
        assert infeasible_summary["state_counts"]["PENDING_CREATION"] == 1
        return True

    wait_for_condition(verify)


# TODO(sang): Currently,
# def test_object_summary(monkeypatch, ray_start_cluster):
#     with monkeypatch.context() as m:
#         m.setenv("RAY_record_ref_creation_sites", "1")
#         cluster = ray_start_cluster
#         cluster.add_node(num_cpus=4)
#         ray.init(address=cluster.address)

#         dep = ray.put(1) # noqa

#         @ray.remote
#         def task_wait_for_dep(dep):
#             time.sleep(30)

#         a = [task_wait_for_dep.remote(dep) for _ in range(2)] # noqa

#         def verify():
#             print(summarize_objects())

#         # wait_for_condition(verify)
#         time.sleep(300)


# def test_summary_cli(ray_start_cluster):
#     cluster = ray_start_cluster
#     cluster.add_node(num_cpus=2)
#     ray.init(address=cluster.address)
#     cluster.add_node(num_cpus=2)

#     @ray.remote
#     def run_long_time_task():
#         time.sleep(30)
#         return True

#     @ray.remote
#     def task_wait_for_dep(dep):
#         print(dep)

#     a = task_wait_for_dep.remote(run_long_time_task.remote()) # noqa
#     b = task_wait_for_dep.remote(run_long_time_task.remote()) # noqa

#     def verify():
#         print(summarize_tasks())

#     wait_for_condition(verify)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
