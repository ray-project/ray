# __cgraph_visualize_start__
import ray
from ray.dag import InputNode, MultiOutputNode


@ray.remote
class Worker:
    def inc(self, x):
        return x + 1

    def double(self, x):
        return x * 2

    def echo(self, x):
        return x


sender1 = Worker.remote()
sender2 = Worker.remote()
receiver = Worker.remote()

with InputNode() as inp:
    w1 = sender1.inc.bind(inp)
    w1 = receiver.echo.bind(w1)
    w2 = sender2.double.bind(inp)
    w2 = receiver.echo.bind(w2)
    dag = MultiOutputNode([w1, w2])

compiled_dag = dag.experimental_compile()
compiled_dag.visualize()
# __cgraph_visualize_end__
