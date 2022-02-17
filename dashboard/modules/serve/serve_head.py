import aiohttp.web
from aiohttp.web import Request, Response
import json
import logging
from typing import Optional

import ray.dashboard.utils as dashboard_utils
import ray.dashboard.optional_utils as optional_utils

from ray import serve
from ray.serve.api import Deployment, deploy_group

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

routes = optional_utils.ClassMethodRouteTable


class ServeHead(dashboard_utils.DashboardHeadModule):
    def __init__(self, dashboard_head):
        super().__init__(dashboard_head)

    def _get_deployment_by_name(self, name: str) -> Optional[serve.api.Deployment]:
        try:
            return serve.get_deployment(name)
        except KeyError:
            return None

    def _create_deployment_object(self, params):
        if "import_path" not in params.keys():
            raise ValueError("An import path must be specified for each deployment.")
        if "name" not in params.keys():
            raise ValueError("A name must be specified for every deployment.")

        import_path = params["import_path"]
        del params["import_path"]

        for key in list(params.keys()):
            if params[key] is None:
                del params[key]

        return serve.deployment(**params)(import_path)

    def _get_deployment_dict(self, deployment: Deployment):
        return dict(
            name=deployment.name,
            version=deployment.version,
            prev_version=deployment.prev_version,
            func_or_class=deployment.func_or_class,
            num_replicas=deployment.num_replicas,
            user_config=deployment.user_config,
            max_concurrent_queries=deployment.max_concurrent_queries,
            route_prefix=deployment.route_prefix,
            ray_actor_options=deployment.ray_actor_options,
            init_args=deployment.init_args,
            init_kwargs=deployment.init_kwargs,
            url=deployment.url,
        )

    @routes.get("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_all_deployments(self, req: Request) -> Response:
        dict_response = {
            name: self._get_deployment_dict(deployment)
            for name, deployment in serve.list_deployments().items()
        }

        return Response(text=json.dumps(dict_response), content_type="application/json")

    @routes.get("/api/serve/deployments/{name}")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def get_single_deployment(self, req: Request) -> Response:
        name = req.match_info["name"]
        deployment = self._get_deployment_by_name(name)
        if deployment is None:
            return Response(
                text=f"Deployment {name} does not exist.",
                status=aiohttp.web.HTTPNotFound.status_code,
            )
        return Response(
            text=json.dumps(str(deployment)), content_type="application/json"
        )

    @routes.delete("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def delete_all_deployments(self, req: Request) -> Response:
        serve.shutdown()

    @routes.delete("/api/serve/deployments/{name}")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def delete_single_deployment(self, req: Request) -> Response:
        name = req.match_info["name"]
        deployment = self._get_deployment_by_name(name)
        if deployment is None:
            return Response(
                text=f"Deployment {name} does not exist.",
                status=aiohttp.web.HTTPNotFound.status_code,
            )

        deployment.delete()
        return Response()

    @routes.put("/api/serve/deployments/")
    @optional_utils.init_ray_and_catch_exceptions(connect_to_serve=True)
    async def put_all_deployments(self, req: Request) -> Response:
        deployment_data = await req.json()

        deployments, new_names = [], set()
        for deployment_dict in deployment_data:
            deployments.append(self._create_deployment_object(deployment_dict))
            new_names.add(deployment_dict["name"])
        deploy_group(deployments, _blocking=False)

        all_deployments = serve.list_deployments()
        all_names = set(all_deployments.keys())
        names_to_delete = all_names.difference(new_names)
        for name in names_to_delete:
            all_deployments[name].delete()

        return Response()

    async def run(self, server):
        pass

    @staticmethod
    def is_minimal_module():
        return False
