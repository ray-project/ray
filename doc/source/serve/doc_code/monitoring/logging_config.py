# flake8: noqa
#  __deployment_json_start__
import requests
from ray import serve
from ray.serve.schema import LoggingConfig


@serve.deployment(logging_config=LoggingConfig(encoding="JSON"))
class Model:
    def __call__(self) -> int:
        return "hello world"


serve.run(Model.bind())

resp = requests.get("http://localhost:8000/")

# __deployment_json_end__


#  __serve_run_json_start__
import requests
from ray import serve
from ray.serve.schema import LoggingConfig


@serve.deployment
class Model:
    def __call__(self) -> int:
        return "hello world"


serve.run(Model.bind(), logging_config=LoggingConfig(encoding="JSON"))

resp = requests.get("http://localhost:8000/")

# __serve_run_json_end__


#  __level_start__


@serve.deployment(logging_config=LoggingConfig(log_level="DEBUG"))
class Model:
    def __call__(self) -> int:
        logger = logging.getLogger("ray.serve")
        logger.debug("This debug message is from the router.")
        return "hello world"


# __level_end__

serve.run(Model.bind())

resp = requests.get("http://localhost:8000/")


# __logs_dir_start__
@serve.deployment(logging_config=LoggingConfig(logs_dir="/my_dirs"))
class Model:
    def __call__(self) -> int:
        return "hello world"


# __logs_dir_end__


# __enable_access_log_start__
import requests
import logging
from ray import serve


@serve.deployment(logging_config={"enable_access_log": False})
class Model:
    def __call__(self):
        logger = logging.getLogger("ray.serve")
        logger.info("hello world")


serve.run(Model.bind())

resp = requests.get("http://localhost:8000/")

# __enable_access_log_end__


# __application_and_deployment_start__
import requests
import logging
from ray import serve


@serve.deployment
class Router:
    def __init__(self, handle):
        self.handle = handle

    async def __call__(self):
        logger = logging.getLogger("ray.serve")
        logger.debug("This debug message is from the router.")
        return await self.handle.remote()


@serve.deployment(logging_config={"log_level": "INFO"})
class Model:
    def __call__(self) -> int:
        logger = logging.getLogger("ray.serve")
        logger.debug("This debug message is from the model.")
        return "hello world"


serve.run(Router.bind(Model.bind()), logging_config={"log_level": "DEBUG"})
resp = requests.get("http://localhost:8000/")
# __application_and_deployment_end__

# __configure_serve_component_start__
from ray import serve


serve.start(
    logging_config={
        "encoding": "JSON",
        "log_level": "DEBUG",
        "enable_access_log": False,
    }
)

# __configure_serve_component_end__
