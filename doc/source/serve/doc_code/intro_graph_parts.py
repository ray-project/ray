# flake8: noqa

# __deployments_start__
import ray
from ray import serve
from ray.serve.deployment_graph import InputNode

@serve.deployment
class AddCls:

    def __init__(self, addend: float):
        self.addend = addend
    
    def add(self, number: float) -> float:
        return number + self.addend
    
    async def unpack_request(self, http_request) -> float:
        return await http_request.json()

@serve.deployment
def subtract_one_fn(number: float) -> float:
    return number - 1

@serve.deployment
async def unpack_request(http_request) -> float:
    return await http_request.json()
# __deployments_end__


# __graph_start__

# Bind class-based DeploymentNodes to their constructor's args and kwargs
add_2 = AddCls.bind(2)
add_3 = AddCls.bind(3)

# Write the call graph inside the InputNode context manager
with InputNode() as http_request:
    request_number = unpack_request.bind(http_request)
    add_2_output = add_2.add.bind(request_number)
    subtract_1_output = subtract_one_fn.bind(add_2_output)
    add_3_output = add_3.add.bind(subtract_1_output)

serve.run(add_3_output)
print("Finished run")
