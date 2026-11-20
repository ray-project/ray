# flake8: noqa

# __tracing_app_start__
from starlette.requests import Request

from ray import serve


@serve.deployment
class Greeter:
    def __call__(self, request: Request) -> str:
        return "Hello!"


app = Greeter.bind()
# __tracing_app_end__
