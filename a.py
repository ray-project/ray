import ray
from ray.dag import InputNode, MultiOutputNode

@ray.remote
class Worker:
    def w(self, x):
        return 1

worker = Worker.remote()
with InputNode() as inp:    
    dag = worker.w.bind(inp, y=inp)

adag = dag.experimental_compile()
