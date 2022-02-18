from fastapi import Depends, FastAPI
import ray
from ray._private.utils import import_attr
from ray.serve.http_util import ASGIHTTPSender
import starlette
import inspect


class DAGRunner:
    def __init__(
        self,
        serve_dag_node_json: str,
        input_schema: str = "ray.serve.http_adapters.serve_api_resolver",
    ):
        import json
        from ray.serve.pipeline.json_serde import dagnode_from_json

        # TODO: (jiaodong) Make this class take JSON serialized dag

        print(f"serve_dag_node_json: {serve_dag_node_json}")
        self.dag = json.loads(serve_dag_node_json, object_hook=dagnode_from_json)
        print(f"self.dag: {str(self.dag)}")

        if input_schema is None:
            from ray.serve.http_adapters import serve_api_resolver

            input_schema = serve_api_resolver
        elif isinstance(input_schema, str):
            input_schema = import_attr(input_schema)
        assert callable(input_schema), "input schema must be callable"

        self.app = FastAPI()

        @self.app.post("/")
        async def handle_request(inp=Depends(input_schema)):
            return await self.dag.execute(inp)

    async def __call__(self, request: starlette.requests.Request):
        sender = ASGIHTTPSender()
        await self.app(request.scope, receive=request.receive, send=sender)
        return sender.build_asgi_response()
