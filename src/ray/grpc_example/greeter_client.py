from __future__ import print_function
import logging

import grpc

import example.proto.greeter_pb2 as greeter_pb2
import example.proto.greeter_pb2_grpc as greeter_pb2_grpc


def run():
    with grpc.insecure_channel("localhost:10086") as channel:
        stub = greeter_pb2_grpc.GreeterStub(channel)
        response = stub.SayHello(greeter_pb2.HelloRequest(name="ray-client"))
    print("Greeter client received: " + response.message)


if __name__ == "__main__":
    logging.basicConfig()
    run()
