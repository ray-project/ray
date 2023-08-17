import ray
from ray.util.state import list_tasks
from ray._private.test_utils import wait_for_condition

@ray.remote
def child():
    pass

@ray.remote
class A:
    @ray.method(num_returns=2)
    async def parent(self):
        core_worker = ray._private.worker.global_worker.core_worker
        ref = child.remote()
        task_id = ray.get_runtime_context().task_id
        print(task_id)
        children_task_ids = core_worker.get_pending_children_task_ids(task_id)
        actual_children = [ref.task_id()]
        return children_task_ids, actual_children
        # return 1, 1

a = A.remote()
result, expected = ray.get(a.parent.remote())
print(result)
print(expected)
from pprint import pprint
pprint(list_tasks())
