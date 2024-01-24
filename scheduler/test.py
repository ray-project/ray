import ray
from ray import serve
from api_server import Apiserver
from custom_controller import Controller
from ray.util.scheduling_strategies import (
    In,
    NotIn,
    Exists,
    DoesNotExist,
    NodeLabelSchedulingStrategy,
)

ray.init()

dir = "/tmp/apple"

@ray.remote()
def user_function(working_dir):
    print(ray.get_runtime_context().get_node_info())
    return "ok"

task_id = user_function.options(
    scheduling_strategy=NodeLabelSchedulingStrategy(
        hard={dir: In(dir)}
    )
).remote(working_dir=dir)

print(ray.get(task_id))




# pip install fastapi==0.104.1
# pip install pydantic==1.8
#  pip install aiorwlock