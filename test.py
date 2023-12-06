import ray

# @ray.remote
# class Actor:
#     def get_task_id(self):
#         return ray.get_runtime_context().get_task_id()

@ray.remote
def get_task_id():
    

    # ray.get_runtime_context().get_node_info()
    ray.get_runtime_context().set_label({b"gpu_type":b"A100"})
    ray.get_runtime_context().get_node_info()
# # All the below code generates different task ids.
# a = Actor.remote()
# Task ids are available for actor tasks.
# print(ray.get(a.get_task_id.remote()))
# Task ids are available for normal tasks.
print(ray.get(get_task_id.remote()))