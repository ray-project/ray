import ray
from ray import serve


@ray.remote
def download(uri):
    return uri


@serve.deployment
class MyModel:
    def __init__(self, arg):
        self._arg = arg
    
    def __call__(self, *args):
        return self._arg


m = MyModel.bind([download.bind("Hello world")])
