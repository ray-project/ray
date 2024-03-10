import ray
from ray import serve
from api_server import Apiserver
from custom_controller import Controller
import os

cur_file_path = os.path.dirname(os.path.realpath(__file__))

print(cur_file_path)

ray.init(address="auto", runtime_env={"working_dir": cur_file_path})

serve.start(http_options={"host": "0.0.0.0"})
serve.run(Apiserver.bind())
Controller().start_loop()
