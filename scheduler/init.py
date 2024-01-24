import ray
from ray import serve
from api_server import Apiserver
from custom_controller import Controller



serve.run(Apiserver.bind())
Controller().start_loop()















