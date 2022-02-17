
import ray

class DAGRunner:
    def __init__(self, serve_dag_node_json: str):
        import json
        from ray.serve.pipeline.json_serde import dagnode_from_json
        # TODO: (jiaodong) Make this class take JSON serialized dag

        print(f"serve_dag_node_json: {serve_dag_node_json}")
        self.dag = json.loads(
            serve_dag_node_json, object_hook=dagnode_from_json
        )
        print(f"self.dag: {str(self.dag)}")

    def __call__(self, request):
        data = int(request.query_params["input"])
        print(f"data received: {data}")
        ref = self.dag.execute(data)
        return ray.get(ref)