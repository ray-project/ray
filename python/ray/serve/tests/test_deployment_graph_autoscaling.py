import sys
import pytest

from python.ray.serve import constants as serve_constants

import ray
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.experimental.dag.input_node import InputNode
from ray.serve.deployment_state import ReplicaState
from ray._private.test_utils import SignalActor, wait_for_condition


def get_num_running_replicas(controller, deployment_name):
    replicas = ray.get(
        controller._dump_replica_states_for_testing.remote(deployment_name)
    )
    running_replicas = replicas.get([ReplicaState.RUNNING])
    print(deployment_name, len(running_replicas))
    return len(running_replicas)


def test_autoscaling_0_replica(serve_instance):
    serve_constants.HANDLE_METRIC_PUSH_INTERVAL_S = 1
    autoscaling_config = {
        "metrics_interval_s": 0.1,
        "min_replicas": 0,
        "max_replicas": 2,
        "look_back_period_s": 0.4,
        "downscale_delay_s": 0,
        "upscale_delay_s": 0,
    }

    @serve.deployment(
        _autoscaling_config=autoscaling_config,
    )
    class Model1:
        def __init__(self, weight):
            self.weight = weight

        def forward(self, input):
            return input + self.weight

    with InputNode() as user_input:
        model1 = Model1.bind(1)
        output = model1.forward.bind(user_input)
        serve_dag = DAGDriver.options(
            route_prefix="/my-dag",
            _autoscaling_config=autoscaling_config,
        ).bind(output)
    dag_handle = serve.run(serve_dag)
    assert 2 == ray.get(dag_handle.predict.remote(1))


@pytest.mark.parametrize("min_replicas", [0, 1])
def test_autoscaling_with_chain_nodes(min_replicas, serve_instance):

    serve_constants.HANDLE_METRIC_PUSH_INTERVAL_S = 1

    signal1 = SignalActor.remote()
    signal2 = SignalActor.remote()
    autoscaling_config = {
        "metrics_interval_s": 0.1,
        "min_replicas": min_replicas,
        "max_replicas": 2,
        "look_back_period_s": 0.4,
        "downscale_delay_s": 0,
        "upscale_delay_s": 0,
    }

    @serve.deployment(
        _autoscaling_config=autoscaling_config,
        _graceful_shutdown_timeout_s=1,
    )
    class Model1:
        def __init__(self, weight):
            self.weight = weight

        def forward(self, input):
            ray.get(signal1.wait.remote())
            return input + self.weight

    @serve.deployment(
        _autoscaling_config=autoscaling_config,
        _graceful_shutdown_timeout_s=1,
    )
    class Model2:
        def __init__(self, weight):
            self.weight = weight

        def forward(self, input):
            ray.get(signal2.wait.remote())
            return input + self.weight

    with InputNode() as user_input:

        model1 = Model1.bind(0)
        model2 = Model2.bind(1)
        output = model1.forward.bind(user_input)
        output2 = model2.forward.bind(output)
        serve_dag = DAGDriver.options(
            route_prefix="/my-dag",
            _autoscaling_config=autoscaling_config,
            _graceful_shutdown_timeout_s=1,
        ).bind(output2)

    dag_handle = serve.run(serve_dag)

    [dag_handle.predict.remote(w) for w in range(100)]

    controller = serve_instance._controller

    wait_for_condition(
        lambda: get_num_running_replicas(controller, DAGDriver.name) >= 1
    )
    signal1.send.remote()
    wait_for_condition(lambda: get_num_running_replicas(controller, Model1.name) >= 1)
    signal2.send.remote()

    wait_for_condition(
        lambda: get_num_running_replicas(controller, DAGDriver.name) == min_replicas
    )
    wait_for_condition(
        lambda: get_num_running_replicas(controller, Model1.name) == min_replicas
    )
    wait_for_condition(
        lambda: get_num_running_replicas(controller, Model2.name) == min_replicas
    )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
