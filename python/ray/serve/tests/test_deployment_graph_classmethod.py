import pytest
import os
import sys
from typing import TypeVar, Union

import numpy as np
import requests

import ray
from ray import serve
from ray.serve.application import Application
from ray.serve.api import build as build_app
from ray.serve.deployment_graph import RayServeDAGHandle
from ray.serve.pipeline.api import build as pipeline_build
from ray.serve.deployment_graph import ClassNode, InputNode
from ray.serve.drivers import DAGDriver
import starlette.requests


def maybe_build(
    node: ClassNode, use_build: bool
) -> Union[Application, ClassNode]:
    if use_build:
        return Application.from_dict(build_app(node).to_dict())
    else:
        return node


async def json_resolver(request: starlette.requests.Request):
    return await request.json()


@serve.deployment
class Counter:
    def __init__(self, val=0):
        self.val = val

    def inc(self, inc=1):
        self.val += inc

    def get(self):
        return self.val


def test_two_dags_shared_instance(serve_instance):
    """Test classmethod chain behavior is consistent across core and serve dag.

    Note this only works if serve also has one replica given each class method
    call mutates its internal state, but forming class method call chains that
    mutate replica state should be considered anti-pattern in serve, given
    request could be routed to different replicas each time.
    """
    counter = Counter.bind(0)

    with InputNode() as input_1:
        # Will be carried over to second dag if counter reused
        counter.inc.bind(2)
        # Only applicable to current execution
        counter.inc.bind(input_1)
        dag = counter.get.bind()
        serve_dag = DAGDriver.options(route_prefix="/serve_dag").bind(
            dag, input_schema=json_resolver
        )

    with InputNode() as _:
        counter.inc.bind(10)
        counter.inc.bind(20)
        other_dag = counter.get.bind()
        other_serve_dag = DAGDriver.options(
            route_prefix="/other_serve_dag"
        ).bind(other_dag, input_schema=json_resolver)

    # First DAG
    assert ray.get(dag.execute(3)) == 5  # 0 + 2 + input(3)
    serve_handle = serve.run(serve_dag)
    assert ray.get(serve_handle.predict.remote(3)) == 5  # 0 + 2 + input(3)

    # Second DAG with shared counter ClassNode
    assert ray.get(other_dag.execute(0)) == 32  # 0 + 2 + 10 + 20
    other_serve_handle = serve.run(other_serve_dag)
    assert ray.get(serve_handle.predict.remote(0)) == 32  # 0 + 2 + 10 + 20


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
