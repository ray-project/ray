import os
import sys

import gradio as gr
import pytest
import requests

import ray
from ray import serve
from ray.serve.gradio_integrations import GradioIngress, GradioServer


@pytest.fixture
def serve_start_shutdown():
    ray.init()
    serve.start()
    yield
    serve.shutdown()
    ray.shutdown()


@pytest.mark.parametrize("use_user_defined_class", [False, True])
def test_gradio_ingress_correctness(serve_start_shutdown, use_user_defined_class: bool):
    """
    Ensure a Gradio app deployed to a cluster through GradioIngress still
    produces the correct output.
    """

    def greet(name):
        return f"Good morning {name}!"

    if use_user_defined_class:

        @serve.deployment
        class UserDefinedGradioServer(GradioIngress):
            def __init__(self):
                super().__init__(
                    lambda: gr.Interface(fn=greet, inputs="text", outputs="text")
                )

        app = UserDefinedGradioServer.bind()
    else:
        app = GradioServer.bind(
            lambda: gr.Interface(fn=greet, inputs="text", outputs="text")
        )

    serve.run(app)

    test_input = "Alice"
    response = requests.post(
        "http://127.0.0.1:8000/api/predict/", json={"data": [test_input]}
    )
    assert response.status_code == 200 and response.json()["data"][0] == greet(
        test_input
    )


def test_gradio_ingress_scaling(serve_start_shutdown):
    """
    Check that a Gradio app that has been deployed to a cluster through
    GradioIngress scales as needed, i.e. separate client requests are served by
    different replicas.
    """

    def f(*args):
        return os.getpid()

    app = GradioServer.options(num_replicas=2).bind(
        lambda: gr.Interface(fn=f, inputs="text", outputs="text")
    )
    serve.run(app)

    pids = []
    for _ in range(3):
        response = requests.post(
            "http://127.0.0.1:8000/api/predict/", json={"data": ["input"]}
        )
        assert response.status_code == 200
        pids.append(response.json()["data"][0])

    assert len(set(pids)) == 2


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
