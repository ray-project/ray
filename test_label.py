import ray
import time
from func_timeout import func_set_timeout

from ray.util.scheduling_strategies import (
    In,
    NotIn,
    Exists,
    DoesNotExist,
    NodeLabelSchedulingStrategy,
)
ray.init()


# @ray.remote
# class MyActor:
#     def __init__(self):
#         self.value = 0

#     def value(self):
#         return self.value

#     def get_node_id(self):
#         return ray.get_runtime_context().get_node_id()

# # start=time.time()
# # actor = MyActor.options(
# #     scheduling_strategy=NodeLabelSchedulingStrategy(
# #         hard={"gpu_type": In("A100")}
# #     )
# #     ).remote()

# # end=time.time()
# @func_set_timeout(3)
# def create_actor():
#     actor=MyActor.options(
#         scheduling_strategy=NodeLabelSchedulingStrategy(
#             hard={"gpu_type": In("D10")}
#         )
#     ).remote()
    
#     node_id = ray.get(actor.get_node_id.remote())
    
#     return actor

# def main():
#     start_time=time.time()

#     while(True):
#         try:
#             # Attempt to create the actor with a timeout
#             # with timeout.timeout(3):
#             #     actor = MyActor.options(
#             #         scheduling_strategy=NodeLabelSchedulingStrategy(
#             #             hard={"gpu_type": In("A100")}
#             #         )
#             #     ).remote()
            
#             actor=create_actor()
#             break
#             # If the actor creation was successful, break out of the loop
            
#         except :
#             # Handle timeout (you can log it or print an error message)
#             print("Timeout occurred during actor creation.")
#     end_time=time.time()
#     print("total time to connect",end_time-start_time)
#     node_id = ray.get(actor.get_node_id.remote())
#     print(node_id)

# main()

@ray.remote
def bind_label():
	        #TODO: how to guarantee transfer data finished 
            #TODO: set home address
            # os.system("ls")
            # if os.path.exists(directory):
            #     ray.get_runtime_context().set_label({label: label})
            # else:
            #     os.system("rsync -a -P {} {}".format(label,node_ip+":"+label))
    time.sleep(1)
            
    return False
task_id = bind_label.options(
            scheduling_strategy=NodeLabelSchedulingStrategy(
                hard={"gpu": In("10")}
            )
        ).remote()
print(task_id)