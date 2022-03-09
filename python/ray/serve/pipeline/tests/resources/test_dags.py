
from ray.serve.pipeline.tests.resources.test_modules import (
    Model,
    Combine,
    NESTED_HANDLE_KEY,
    request_to_data_int,
)
from ray.serve.pipeline.pipeline_input_node import PipelineInputNode

def get_multi_instantiation_class_deployment_in_init_args_dag():
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        m1 = Model._bind(2)
        m2 = Model._bind(3)
        combine = Combine._bind(m1, m2=m2)
        ray_dag = combine.__call__._bind(dag_input)

    return ray_dag, dag_input

def get_shared_deployment_handle_dag():
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        m = Model._bind(2)
        combine = Combine._bind(m, m2=m)
        ray_dag = combine.__call__._bind(dag_input)

    return ray_dag, dag_input

def get_multi_instantiation_class_nested_deployment_arg_dag():
    with PipelineInputNode(preprocessor=request_to_data_int) as dag_input:
        m1 = Model._bind(2)
        m2 = Model._bind(3)
        combine = Combine._bind(m1, m2={NESTED_HANDLE_KEY: m2}, m2_nested=True)
        ray_dag = combine.__call__._bind(dag_input)

    return ray_dag, dag_input