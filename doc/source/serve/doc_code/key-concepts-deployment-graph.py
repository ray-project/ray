import ray
from ray import serve
from ray.serve.dag import InputNode
from ray.serve.drivers import DAGDriver


@serve.deployment
def preprocess(inp: int):
    return inp + 1


@serve.deployment
class Model:
    def __init__(self, increment: int):
        self.increment = increment

    def predict(self, inp: int):
        return inp + self.increment


with InputNode() as inp:
    model = Model.bind(increment=2)
    output = model.predict.bind(preprocess.bind(inp))
    serve_dag = DAGDriver.bind(output)

handle = serve.run(serve_dag)
assert ray.get(handle.predict.remote(1)) == 4
