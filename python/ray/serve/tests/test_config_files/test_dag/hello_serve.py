from starlette.requests import Request

from ray import serve
from ray.serve.tests.test_config_files.test_dag.utils.test import hello


@serve.deployment
class HelloModel:
    async def __call__(self, starlette_request: Request) -> None:
        return hello()


model = HelloModel.bind()
