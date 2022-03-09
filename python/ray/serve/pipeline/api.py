from ray.util.annotations import PublicAPI
from ray.experimental.dag import DAGNode
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
    get_pipeline_input_node,
    get_ingress_deployment,
)


@PublicAPI
def build(ray_dag_root_node: DAGNode):
    """Do all the DAG transformation, extraction and generation needed to
    produce a runnable and deployable serve pipeline application from a valid
    DAG authored with Ray DAG API.

    This should be the only user facing API that user interacts with.

    Args:
        ray_dag_root_node: DAGNode acting as root of a Ray authored DAG. It
            should be executable via `ray_dag_root_node.execute(user_input)`
            and should have `PipelineInputNode` in it.

    Returns:
        app: The Ray Serve application object that wraps all deployments needed
            along with ingress deployment for an e2e runnable serve pipeline,
            accessible via python .remote() call and HTTP.

    Examples:
        >>> with ServeInputNode(preprocessor=request_to_data_int) as dag_input:
        ...    m1 = Model.bind(1)
        ...    m2 = Model.bind(2)
        ...    m1_output = m1.forward.bind(dag_input[0])
        ...    m2_output = m2.forward.bind(dag_input[1])
        ...    ray_dag = ensemble.bind(m1_output, m2_output)

        >>> app = serve.pipeline.build(ray_dag)
    """
    serve_root_dag = ray_dag_root_node.apply_recursive(
        transform_ray_dag_to_serve_dag
    )
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    pipeline_input_node = get_pipeline_input_node(serve_root_dag)
    ingress_deployment = get_ingress_deployment(
        serve_root_dag, pipeline_input_node
    )
    deployments.insert(0, ingress_deployment)

    # TODO (jiaodong): Call into Application once Shreyas' PR is merged
    return deployments
