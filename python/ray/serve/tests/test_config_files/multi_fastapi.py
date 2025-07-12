from fastapi import FastAPI

from ray import serve
from ray.serve.handle import DeploymentHandle

app1 = FastAPI()
app2 = FastAPI()


@serve.deployment
@serve.ingress(app2)
class SubModel:
    def add(self, a: int):
        return a + 1


@serve.deployment
@serve.ingress(app1)
class Model:
    def __init__(self, submodel: DeploymentHandle):
        self.submodel = submodel

    @app1.get("/{a}")
    async def func(self, a: int):
        return await self.submodel.add.remote(a)


invalid_model = Model.bind(SubModel.bind())
