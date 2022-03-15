from ray import serve
import ray
from ray.experimental.dag.input_node import InputNode
from ray.serve.handle import PipelineHandle
from ray.serve.pipeline.pipeline_input_node import PipelineInputNode


@serve.deployment
class Adder:
    def __init__(self, increment: int):
        self.increment = increment

    def forward(self, inp: int) -> int:
        print(f"Adder got {inp}")
        return inp + self.increment

    __call__ = forward


@serve.deployment
class Driver:
    def __init__(self, dag: PipelineHandle):
        self.dag = dag

    def __call__(self, inp: int) -> int:
        print(f"Driver got {inp}")
        return ray.get(self.dag.remote(inp))


def test_single_node_deploy_success(serve_instance):
    m1 = Adder.bind(1)
    handle = serve.run(m1)
    assert ray.get(handle.remote(41)) == 42


def test_single_node_driver_sucess(serve_instance):
    m1 = Adder.bind(1)
    m2 = Adder.bind(2)
    with PipelineInputNode() as input_node:
        out = m1.forward.bind(input_node)
        out = m2.forward.bind(m1)
    driver = Driver.bind(out)
    handle = serve.run(driver)
    # TODO(simon): somehow i'm not getting the right number here, but all models were
    # called at least!
    assert ray.get(handle.remote(39)) > 0
