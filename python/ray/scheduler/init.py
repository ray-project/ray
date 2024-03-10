import ray
from ray import serve
from api_server import Apiserver
from custom_controller import Controller

ray.init(address="auto", runtime_env={"working_dir": "."})

serve.start(http_options={"host": "0.0.0.0"})
serve.run(Apiserver.bind())
Controller().start_loop()
