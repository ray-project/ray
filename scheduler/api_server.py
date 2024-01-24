import requests
from fastapi import FastAPI
from ray import serve
from custom_resource import Resource
import queue
from fastapi import Body
import logging

app = FastAPI()

@serve.deployment(route_prefix="/")
@serve.ingress(app)
class Apiserver:
    def __init__(self):
        self.instances = queue.Queue()

    @app.post("/apply")
    def apply(self, spec: dict = Body(...)):
        instance = Resource(spec=spec)
        self.instances.put(instance)
        print("apiserver: apply a new instance", instance)

    @app.get("/get")
    def get(self):
        if self.instances.empty():
            return Resource().to_dict()
        else:
            return self.instances.get().to_dict()

    @app.post("/update-status")
    def update_status(self, instance: dict = Body(...)):
        instance = Resource(**instance)
        self.instances.put(instance)
        print("apiserver: update status", instance)
   
        

