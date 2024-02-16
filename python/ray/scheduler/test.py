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

# ray.init()

# dir = "/tmp/apple/333"

# @ray.remote
# def user_function(working_dir):
    # print(ray.get_runtime_context().get_node_info())
    # return "ok"

# task_id = user_function.options(
#     scheduling_strategy=NodeLabelSchedulingStrategy(
#         hard={dir: In(dir)}
#     )
# ).remote(working_dir=dir)

# task_id = user_function.remote(working_dir=dir)

# print(task_id)
# print(ray.get(task_id))


# from ray.util.state import list_tasks

# tasks = list_tasks()
# for task in tasks:
#     print(task["task_id"])
#     print(task)
#     break

# pip install fastapi==0.104.1
# pip install pydantic==1.8
#  pip install aiorwlock
# pip install uvicorn




from ray.util.state import get_task
from ray.util.state import list_tasks
from time import sleep

# ray.init()
# dir = "/tmp/test/5"

# @ray.remote(num_cpus=1)
# def user_function(working_dir):
#     print(ray.get_runtime_context().get_task_id())
#     return "okrefjiweofjoijwr"

# task_id = user_function.remote(working_dir=dir)
# print(task_id)
# print(ray.get(task_id))
# sleep(10)
# print(task_id.hex()[:-8])
# print(get_task(task_id.hex()[:-8]))


# https://docs.ray.io/en/latest/ray-observability/reference/doc/ray.util.state.get_task.html#ray-util-state-get-task


# tasks = list_tasks()
# for task in tasks:
#     print(task["task_id"])

# print(get_task("cbd431224396f2d6ffffffffffffffffffffffff0f000000"))
# cbd431224396f2d6ffffffffffffffffffffffff0f000000
# ray stop --force
# ray start --head
# ray start --address='34.199.201.49:6379'






# ray.init()
# dir = "/tmp/apple/1" 

# @ray.remote(num_cpus=1)
# def user_function(working_dir, complexity_score, time):
#     # print(ray.get_runtime_context().get_task_id())
#     print(ray.get_runtime_context().get_node_id())
#     sleep(time)
#     return "ok"

# task_id_1 = user_function.remote(working_dir=dir, complexity_score=1, time=1)
# sleep(5)

# task_id_2 = user_function.remote(working_dir=dir + "2", complexity_score=1, time=2)
# print("second task")
# sleep(5)


# task_id_3 = user_function.remote(working_dir=dir + "3", complexity_score=1, time=1)
# print("third task")

# print(ray.get(task_id_1))
# print(ray.get(task_id_2))
# print(ray.get(task_id_3))





ray.init()

dir = "/tmp/apple/9" 
# ray.get_runtime_context().set_label({dir: dir})

@ray.remote(num_cpus=1)
def user_function(working_dir, complexity_score, time):
    # print(ray.get_runtime_context().get_task_id())
    # print(ray.get_runtime_context().get_node_id())
    sleep(time)
    return "ok"

task_id_1 = user_function.remote(working_dir=dir, complexity_score=1, time=30)


task_id_2 = user_function.remote(working_dir=dir, complexity_score=1, time=1)
print(ray.get(task_id_1))
print(ray.get(task_id_2))
