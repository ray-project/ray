from random import random
import requests
import ray
from ray import serve

#
# Our pipeline will be structured as follows:
# - Input comes in, the composed model sends it to model_one
# - model_one outputs a random number between 0 and 1, if the value is
#   greater than 0.5, then the data is sent to model_two
# - otherwise, the data is returned to the user.

# Let's define two models that just print out the data they received.


@serve.deployment
def model_one(data):
    print(f"Model 1 called with data:{data}")
    return random()


@serve.deployment
def model_two(data):
    print(f"Model 2 called with data:{data}")
    # Use this data sent from model_one.
    return data


# max_concurrent_queries is optional. By default, if you pass in an async
# function, Ray Serve sets the limit to a high number.
@serve.deployment(max_concurrent_queries=10, route_prefix="/composed")
class ComposedModel:
    def __init__(self):
        # Use the Python ServeHandle APIs.
        # Set sync=False to override default, which is use this in a
        # synchronous mode.  We want these deployments to be run within an
        # asynchronous event loop for concurrency. See documentation for Sync
        # and Async ServeHandle APIs for details:
        # https://docs.ray.io/en/latest/serve/http-servehandle.html
        self.model_one = model_one.get_handle(sync=False)
        self.model_two = model_two.get_handle(sync=False)

    # This method can be called concurrently.
    async def __call__(self, starlette_request):
        # At this point you are yielding to the event loop to take in another
        # request.
        data = await starlette_request.body()

        # Use await twice here for two reasons:
        # 1. Since we are running within a async def callable function and we
        # want to use this model_one deployment to run in an asynchronous
        # fashion, this is standard async-await pattern. This await call will
        # return an ObjectRef.
        # 2. The second await waits on the ObjectRef to do an implicit
        # ray.get(Object) to fetch the actual value returned.
        # Hence two awaits.
        score = await (await self.model_one.remote(data=data))
        if score > 0.5:
            await (await self.model_two.remote(data=data))
            result = {"model_used: 1 & 2;  score": score}
        else:
            result = {"model_used: 1 ; score": score}

        return result


if __name__ == "__main__":

    # Start ray with 8 processes.
    if ray.is_initialized():
        ray.shutdown()
    ray.init(num_cpus=8)
    serve.start()
    # Start deployment instances.
    model_one.deploy()
    model_two.deploy()
    ComposedModel.deploy()

    # Now send requests.
    for _ in range(8):
        resp = requests.get("http://127.0.0.1:8000/composed", data="Hey!")
        print(resp.json())

    ray.shutdown()

# Output
# {'model_used: 1 ; score': 0.20814435670233788}
# {'model_used: 1 ; score': 0.02964993348224776}
# {'model_used: 1 & 2;  score': 0.7570845154147877}
# {'model_used: 1 & 2;  score': 0.8166808845518793}
# {'model_used: 1 ; score': 0.28354556740137904}
# {'model_used: 1 & 2;  score': 0.5826064390148368}
# {'model_used: 1 ; score': 0.4460146836937825}
# {'model_used: 1 ; score': 0.37099434069129833}
