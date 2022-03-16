from ray.experimental.dag import DAGNode
from ray.serve.pipeline.generate import (
    transform_ray_dag_to_serve_dag,
    extract_deployments_from_serve_dag,
    get_pipeline_input_node,
    get_ingress_deployment,
)


def build(ray_dag_root_node: DAGNode, inject_ingress=True):
    """Do all the DAG transformation, extraction and generation needed to
    produce a runnable and deployable serve pipeline application from a valid
    DAG authored with Ray DAG API.

    This should be the only user facing API that user interacts with.

    Assumptions:
        Following enforcements are only applied at generating and applying
        pipeline artifact, but not blockers for local development and testing.

        - ALL args and kwargs used in DAG building should be JSON serializable.
            This means in order to ensure your pipeline application can run on
            a remote cluster potentially with different runtime environment,
            among all options listed:

                1) binding in-memory objects
                2) Rely on pickling
                3) Enforce JSON serialibility on all args used

            We believe both 1) & 2) rely on unstable in-memory objects or
            cross version pickling / closure capture, where JSON serialization
            provides the right contract needed for proper deployment.

        - ALL classes and methods used should be visible on top of the file and
            importable via a fully qualified name. Thus no inline class or
            function definitions should be used.

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

        Assuming we have non-JSON serializable or inline defined class or
        function in local pipeline development.

        >>> app = serve.pipeline.build(ray_dag) # This works
        >>> handle = app.deploy()
        >>> # This also works, we're simply executing the transformed serve_dag.
        >>> ray.get(handle.remote(data)
        >>> # This will fail where enforcements are applied.
        >>> deployment_yaml = app.to_yaml()
    """
    serve_root_dag = ray_dag_root_node.apply_recursive(transform_ray_dag_to_serve_dag)
    deployments = extract_deployments_from_serve_dag(serve_root_dag)
    if inject_ingress:
        pipeline_input_node = get_pipeline_input_node(serve_root_dag)
        ingress_deployment = get_ingress_deployment(serve_root_dag, pipeline_input_node)
        deployments.insert(0, ingress_deployment)

    # TODO (jiaodong): Call into Application once Shreyas' PR is merged
    # TODO (jiaodong): Apply enforcements at serve app to_yaml level
    return deployments
