import os
import sys

import gradio as gr
from gradio_client import Client
import pytest

from ray._private.test_utils import wait_for_condition

from ray import serve
from ray.serve.gradio_integrations import GradioIngress, GradioServer


@pytest.mark.parametrize("use_user_defined_class", [False, True])
def test_gradio_ingress_correctness(serve_instance, use_user_defined_class: bool):
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

    client = Client("http://localhost:8000")
    assert client.predict("Alice") == "Good morning Alice!"


def test_gradio_ingress_scaling(serve_instance):
    """
    Check that a Gradio app that has been deployed to a cluster through
    GradioIngress scales as needed, i.e. separate client requests are served by
    different replicas.
    """

    def f(*args):
        return os.getpid()

    serve.run(
        GradioServer.options(num_replicas=2).bind(
            lambda: gr.Interface(fn=f, inputs="text", outputs="text")
        )
    )

    client = Client("http://localhost:8000")
    # Verify that the requests are handled by two separate replicas.
    wait_for_condition(lambda: len({client.predict("input") for _ in range(3)}) == 2)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
