from ray.serve.tests.resources.test_modules import (
    Model,
    Combine,
    combine,
    NESTED_HANDLE_KEY,
)
from ray.dag.input_node import InputNode


def get_simple_func_dag():
    with InputNode() as dag_input:
        ray_dag = combine.bind(dag_input[0], dag_input[1], kwargs_output=1)

    return ray_dag, dag_input


def get_simple_class_with_class_method_dag():
    with InputNode() as dag_input:
        model = Model.bind(2, ratio=0.3)
        ray_dag = model.forward.bind(dag_input)

    return ray_dag, dag_input


def get_func_class_with_class_method_dag():
    with InputNode() as dag_input:
        m1 = Model.bind(1)
        m2 = Model.bind(2)
        m1_output = m1.forward.bind(dag_input[0])
        m2_output = m2.forward.bind(dag_input[1])
        ray_dag = combine.bind(m1_output, m2_output, kwargs_output=dag_input[2])

    return ray_dag, dag_input


def get_multi_instantiation_class_deployment_in_init_args_dag():
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2=m2)
        ray_dag = combine.__call__.bind(dag_input)

    return ray_dag, dag_input


def get_shared_deployment_handle_dag():
    with InputNode() as dag_input:
        m = Model.bind(2)
        combine = Combine.bind(m, m2=m)
        ray_dag = combine.__call__.bind(dag_input)

    return ray_dag, dag_input


def get_multi_instantiation_class_nested_deployment_arg_dag():
    with InputNode() as dag_input:
        m1 = Model.bind(2)
        m2 = Model.bind(3)
        combine = Combine.bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
        ray_dag = combine.__call__.bind(dag_input)

    return ray_dag, dag_input
