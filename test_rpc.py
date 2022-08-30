import asyncio
import time

import grpc

import test_service_pb2
import test_service_pb2_grpc
from test_service_pb2_grpc import TestServiceServicer

import ray
from ray import serve
from ray.serve.drivers import GrpcDriver, ServeGrpcIngress


@serve.deployment
class MyIngress(ServeGrpcIngress, TestServiceServicer):
    def __init__(self, dag=None, port="50001") -> None:
        self.dag = dag
        ServeGrpcIngress.__init__(self)

    async def Ping(self, request, context):
        print("Ping called!")
        res = await (await self.dag.remote())
        return test_service_pb2.PingReply()


@serve.deployment
class D1:
    def __call__(self, input):
        return input["a"]


@serve.deployment
class D2:
    def __call__(self):
        print("D2 called")
        return


# internal schema
serve.run(GrpcDriver.bind(D1.bind()))


# customer schema
# serve.run(MyIngress.bind(D2.bind()))

time.sleep(36000)
