import ray
from ray import serve
from ray.serve.drivers import DAGDriver
from ray.dag.input_node import InputNode
from ray.serve.handle import RayServeDeploymentHandle
from ray.serve.handle import RayServeSyncHandle

import requests
from starlette.requests import Request

serve.start()


# __raw_handle_graph_start__


@serve.deployment
class Model:
    def forward(self, input) -> str:
        # do some inference work
        return "done"


@serve.deployment
class Preprocess:
    def __init__(self, model_handle: RayServeSyncHandle):
        self.model_handle = model_handle

    async def __call__(self, input):
        # do some preprocessing works for your inputs
        return await self.model_handle.forward.remote(input)


Model.deploy()
model_handle = Model.get_handle()

Preprocess.deploy(model_handle)
preprocess_handle = Preprocess.get_handle()
ray.get(preprocess_handle.remote(1))

# __raw_handle_graph_end__

serve.shutdown()
serve.start()


# __single_deployment_old_api_start__
@serve.deployment
class Model:
    def __call__(self, input: int):
        # some inference work
        return


Model.deploy()
handle = Model.get_handle()
handle.remote(1)
# __single_deployment_old_api_end__

serve.shutdown()
serve.start()


# __multi_deployments_old_api_start__
@serve.deployment
class Model:
    def forward(self, input: int):
        # some inference work
        return


@serve.deployment
class Model2:
    def forward(self, input: int):
        # some inference work
        return


Model.deploy()
Model2.deploy()
handle = Model.get_handle()
handle.forward.remote(1)

handle2 = Model2.get_handle()
handle2.forward.remote(1)
# __multi_deployments_old_api_end__

serve.shutdown()
serve.start()


# __customized_route_old_api_start__
@serve.deployment(route_prefix="/my_model1")
class Model:
    def __call__(self, req: Request) -> str:
        # some inference work
        return "done"


Model.deploy()
resp = requests.get("http://localhost:8000/my_model1", data="321")
# __customized_route_old_api_end__

serve.shutdown()


# __single_deployment_new_api_start__
@serve.deployment
class Model:
    def __call__(self, input: int):
        # some inference work
        return


handle = serve.run(Model.bind())
handle.remote(1)
# __single_deployment_new_api_end__

serve.shutdown()


# __multi_deployments_new_api_start__
@serve.deployment
class Model:
    def forward(self, input: int):
        # some inference work
        return


@serve.deployment
class Model2:
    def forward(self, input: int):
        # some inference work
        return


with InputNode() as dag_input:
    model = Model.bind()
    model2 = Model2.bind()
    d = DAGDriver.bind(
        {
            "/model1": model.forward.bind(dag_input),
            "/model2": model2.forward.bind(dag_input),
        }
    )
handle = serve.run(d)
handle.predict_with_route.remote("/model1", 1)
handle.predict_with_route.remote("/model2", 1)

resp = requests.get("http://localhost:8000/model1", data="1")
resp = requests.get("http://localhost:8000/model2", data="1")
# __multi_deployments_new_api_end__

serve.shutdown()


# __customized_route_old_api_1_start__
@serve.deployment
class Model:
    def __call__(self, req: Request) -> str:
        # some inference work
        return "done"


d = DAGDriver.options(route_prefix="/my_model1").bind(Model.bind())
handle = serve.run(d)
resp = requests.get("http://localhost:8000/my_model1", data="321")
# __customized_route_old_api_1_end__

serve.shutdown()


# __customized_route_old_api_2_start__
@serve.deployment
class Model:
    def __call__(self, req: Request) -> str:
        # some inference work
        return "done"


@serve.deployment
class Model2:
    def __call__(self, req: Request) -> str:
        # some inference work
        return "done"


d = DAGDriver.bind({"/my_model1": Model.bind(), "/my_model2": Model2.bind()})
handle = serve.run(d)
resp = requests.get("http://localhost:8000/my_model1", data="321")
resp = requests.get("http://localhost:8000/my_model2", data="321")
# __customized_route_old_api_2_end__

serve.shutdown()


# __graph_with_new_api_start__
@serve.deployment
class Model:
    def forward(self, input) -> str:
        # do some inference work
        return "done"


@serve.deployment
class Preprocess:
    def __init__(self, model_handle: RayServeDeploymentHandle):
        self.model_handle = model_handle

    async def __call__(self, input):
        # do some preprocessing works for your inputs
        ref = await self.model_handle.forward.remote(input)
        result = await ref
        return result


handle = serve.run(Preprocess.bind(Model.bind()))
ray.get(handle.remote(1))
# __graph_with_new_api_end__
