import pytest
import os
import sys
from typing import Any

from ray import serve
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.dag import InputNode
from ray.serve.deployment_graph_build import build as pipeline_build


@serve.deployment(name="counter", num_replicas=2, user_config={"count": 123, "b": 2})
class Counter:
    def __init__(self):
        self.count = 10

    def __call__(self, *args):
        return self.count, os.getpid()

    def reconfigure(self, config):
        self.count = config["count"]


@serve.deployment
class Model:
    def __init__(self, weight: int, ratio: float = None):
        self.weight = weight
        self.ratio = ratio or 1

    def forward(self, input: int):
        return self.ratio * self.weight * input

    def __call__(self, request):
        input_data = request
        return self.ratio * self.weight * input_data


@serve.deployment
class Driver:
    def __init__(self, dag: RayServeDAGHandle):
        self.dag = dag

    async def __call__(self, inp: Any) -> Any:
        print(f"Driver got {inp}")
        return await self.dag.remote(inp)


@serve.deployment
def combine(m1_output, m2_output, kwargs_output=0):
    return m1_output + m2_output + kwargs_output


def test_deploment_options_func_class_with_class_method():
    with InputNode() as dag_input:
        counter = Counter.bind()
        m1 = Model.options(name="m1", max_concurrent_queries=3).bind(1)
        m2 = Model.options(name="m2", max_concurrent_queries=5).bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        combine_output = combine.options(num_replicas=3, max_concurrent_queries=7).bind(
            m1_output, m2_output, kwargs_output=dag_input[2]
        )
        dag = counter.__call__.bind(combine_output)
        serve_dag = Driver.bind(dag)

    deployments = pipeline_build(serve_dag)
    hit_count = 0
    for deployment in deployments:
        if deployment.name == "counter":
            assert deployment.num_replicas == 2
            assert deployment.user_config == {"count": 123, "b": 2}
            hit_count += 1
        elif deployment.name == "m1":
            assert deployment.max_concurrent_queries == 3
            hit_count += 1
        elif deployment.name == "m2":
            assert deployment.max_concurrent_queries == 5
            hit_count += 1
        elif deployment.name == "combine":
            assert deployment.num_replicas == 3
            assert deployment.max_concurrent_queries == 7
            hit_count += 1
    assert hit_count == 4, "Not all deployments with expected name were found."


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
