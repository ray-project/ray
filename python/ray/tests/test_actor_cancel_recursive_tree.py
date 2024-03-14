import asyncio
import os
import sys
import time
from collections import defaultdict

import pytest

import ray
from ray._private.test_utils import SignalActor, wait_for_condition
from ray.util.state import list_tasks


def test_cancel_recursive_tree(shutdown_only):
    """Verify recursive cancel works for tree-nested tasks.

    Task A -> Task B
           -> Task C
    """
    ray.init(num_cpus=16)

    # Test the tree structure.
    @ray.remote
    def child():
        for _ in range(5):
            time.sleep(1)
        return True

    @ray.remote
    class ChildActor:
        async def child(self):
            await asyncio.sleep(5)
            return True

    @ray.remote
    class Actor:
        def __init__(self):
            self.children_refs = defaultdict(list)

        def get_children_refs(self, task_id):
            return self.children_refs[task_id]

        async def run(self, child_actor, sig):
            ref1 = child.remote()
            ref2 = child_actor.child.remote()
            task_id = ray.get_runtime_context().get_task_id()
            self.children_refs[task_id].append(ref1)
            self.children_refs[task_id].append(ref2)

            await sig.wait.remote()
            await ref1
            await ref2

    sig = SignalActor.remote()
    child_actor = ChildActor.remote()
    a = Actor.remote()
    ray.get(a.__ray_ready__.remote())

    """
    Test the basic case.
    """
    run_ref = a.run.remote(child_actor, sig)
    task_id = run_ref.task_id().hex()
    wait_for_condition(
        lambda: list_tasks(filters=[("task_id", "=", task_id)])[0].state == "RUNNING"
    )
    ray.cancel(run_ref, recursive=True)
    ray.get(sig.send.remote())

    children_refs = ray.get(a.get_children_refs.remote(task_id))

    for ref in children_refs + [run_ref]:
        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(ref)

    """
    Test recursive = False
    """
    run_ref = a.run.remote(child_actor, sig)
    task_id = run_ref.task_id().hex()
    wait_for_condition(
        lambda: list_tasks(filters=[("task_id", "=", task_id)])[0].state == "RUNNING"
    )
    ray.cancel(run_ref, recursive=False)
    ray.get(sig.send.remote())

    children_refs = ray.get(a.get_children_refs.remote(task_id))

    for ref in children_refs:
        assert ray.get(ref)

    with pytest.raises(ray.exceptions.TaskCancelledError):
        ray.get(run_ref)

    """
    Test concurrent cases.
    """
    run_refs = [a.run.remote(ChildActor.remote(), sig) for _ in range(10)]
    task_ids = []

    for i, run_ref in enumerate(run_refs):
        task_id = run_ref.task_id().hex()
        task_ids.append(task_id)
        wait_for_condition(
            lambda task_id=task_id: list_tasks(filters=[("task_id", "=", task_id)])[
                0
            ].state
            == "RUNNING"
        )
        children_refs = ray.get(a.get_children_refs.remote(task_id))
        for child_ref in children_refs:
            task_id = child_ref.task_id().hex()
            wait_for_condition(
                lambda task_id=task_id: list_tasks(filters=[("task_id", "=", task_id)])[
                    0
                ].state
                == "RUNNING"
            )
        recursive = i % 2 == 0
        ray.cancel(run_ref, recursive=recursive)

    ray.get(sig.send.remote())

    for i, task_id in enumerate(task_ids):
        children_refs = ray.get(a.get_children_refs.remote(task_id))

        if i % 2 == 0:
            for ref in children_refs:
                with pytest.raises(ray.exceptions.TaskCancelledError):
                    ray.get(ref)
        else:
            for ref in children_refs:
                assert ray.get(ref)

        with pytest.raises(ray.exceptions.TaskCancelledError):
            ray.get(run_ref)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
