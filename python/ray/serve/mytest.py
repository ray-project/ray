import ray
import time 

from ray import serve
from starlette.responses import Response

ray.init()

client = serve.start()

def f(_):
    response = Response('Hello, world!', media_type='text/plain')
    return response

client.create_backend("f", f)
client.create_endpoint("f", backend="f", route="/f")

time.sleep(30)