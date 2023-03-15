from fastapi import FastAPI
from ray import serve

app = FastAPI(docs_url="/my_docs")


@serve.deployment
@serve.ingress(app)
class FastAPIDeployment:
    @app.get("/hello")
    def incr(self):
        return "Hello world!"


node = FastAPIDeployment.bind()
