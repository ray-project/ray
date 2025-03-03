# __simple_actor_start__
import ray

@ray.remote
class SimpleActor:
    def echo(self, msg):
        return msg
# __simple_actor_end__

# __simple_actor_usage_start__
import time

a = SimpleActor.remote()

# warmup
for _ in range(5):
    msg_ref = a.echo.remote("hello")
    ray.get(msg_ref)

start = time.perf_counter()
msg_ref = a.echo.remote("hello")
ray.get(msg_ref)
end = time.perf_counter()
print(f"Execution takes {(end - start) * 1000 * 1000} us")
# __simple_actor_usage_end__

# __dag_usage_start__
import ray.dag

with ray.dag.InputNode() as inp:
    # Note that it uses `bind` instead of `remote`.
    # This returns a ray.dag.DAGNode, instead of the usual ray.ObjectRef.
    dag = a.echo.bind(inp)

# warmup
for _ in range(5):
    msg_ref = dag.execute("hello")
    ray.get(msg_ref)

start = time.perf_counter()
# `dag.execute` runs the DAG and returns an ObjectRef. You can use `ray.get` API.
msg_ref = dag.execute("hello")
ray.get(msg_ref)
end = time.perf_counter()
print(f"Execution takes {(end - start) * 1000 * 1000} us")
# __dag_usage_end__

# __cgraph_usage_start__
dag = dag.experimental_compile()

# warmup
for _ in range(5):
    msg_ref = dag.execute("hello")
    ray.get(msg_ref)

start = time.perf_counter()
# `dag.execute` runs the DAG and returns CompiledDAGRef. Similar to
# ObjectRefs, you can use the ray.get API.
msg_ref = dag.execute("hello")
ray.get(msg_ref)
end = time.perf_counter()
print(f"Execution takes {(end - start) * 1000 * 1000} us")
# __cgraph_usage_end__

# __teardown_start__
dag.teardown()
# __teardown_end__

# __cgraph_dependency_start__
a = SimpleActor.remote()
b = SimpleActor.remote()

with ray.dag.InputNode() as inp:
    # Note that it uses `bind` instead of `remote`.
    # This returns a ray.dag.DAGNode, instead of the usual ray.ObjectRef.
    dag = a.echo.bind(inp)
    dag = b.echo.bind(dag)

dag = dag.experimental_compile()
print(ray.get(dag.execute("hello")))
# __cgraph_dependency_end__

# __cgraph_multi_output_start__
import ray.dag

a = SimpleActor.remote()
b = SimpleActor.remote()

with ray.dag.InputNode() as inp:
    # Note that it uses `bind` instead of `remote`.
    # This returns a ray.dag.DAGNode, instead of the usual ray.ObjectRef.
    dag = ray.dag.MultiOutputNode([a.echo.bind(inp), b.echo.bind(inp)])

dag = dag.experimental_compile()
print(ray.get(dag.execute("hello")))
# __cgraph_multi_output_end__

# __cgraph_actor_death_start__
from ray.dag import InputNode, MultiOutputNode

@ray.remote
class EchoActor:
def echo(self, msg):
    return msg

actors = [EchoActor.remote() for _ in range(4)]
with InputNode() as inp:
    outputs = [actor.echo.bind(inp) for actor in actors]
    dag = MultiOutputNode(outputs)

compiled_dag = dag.experimental_compile()
# Kill one of the actors to simulate unexpected actor death.
ray.kill(actors[0])
ref = compiled_dag.execute(1)

live_actors = []
try:
    ray.get(ref)
except ray.exceptions.ActorDiedError:
    # At this point, the Compiled Graph is shutting down.
    for actor in actors:
        try:
            # Check for live actors.
            ray.get(actor.echo.remote("ping"))
            live_actors.append(actor)
        except ray.exceptions.RayActorError:
            pass

# Optionally, use the live actors to create a new Compiled Graph.
assert live_actors == actors[1:]
# __cgraph_actor_death_end__

# __cgraph_gpu_to_gpu_start__
import torch
from ray.experimental.channel.torch_tensor_type import TorchTensorType

ray.init()
# Note that the following example requires at least 2 GPUs.
assert ray.available_resources().get("GPU") >= 2, "At least 2 GPUs are required to run this example."

@ray.remote(num_gpus=1)
class GPUSender:
    def send(self, shape):
        return torch.zeros(shape, device="cuda")

@ray.remote(num_gpus=1)
class GPUReceiver:
    def recv(self, tensor: torch.Tensor):
        assert tensor.device.type == "cuda"
        return tensor.shape

sender = GPUSender.remote()
receiver = GPUReceiver.remote()
# __cgraph_gpu_to_gpu_end__

# __cgraph_nccl_start__
with ray.dag.InputNode() as inp:
    dag = sender.send.bind(inp)
    # Add a type hint that the return value of `send` should use NCCL.
    dag = dag.with_tensor_transport("nccl")
    # NOTE: With ray<2.42, use `with_type_hint()` instead.
    # dag = dag.with_type_hint(TorchTensorType(transport="nccl"))
    dag = receiver.recv.bind(dag)

# Compile API prepares the NCCL communicator across all workers and schedule operations
# accordingly.
dag = dag.experimental_compile()
assert ray.get(dag.execute((10, ))) == (10, )
# __cgraph_nccl_end__
