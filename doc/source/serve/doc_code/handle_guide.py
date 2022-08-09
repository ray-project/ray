import ray
from ray import serve
import requests

# __basic_example_start__
@serve.deployment
class Deployment:
    def method1(self, arg):
        return f"Method1: {arg}"

    def __call__(self, arg):
        return f"__call__: {arg}"


handle = serve.run(Deployment.bind())

ray.get(handle.remote("hi"))  # Defaults to calling the __call__ method.
ray.get(handle.method1.remote("hi"))  # Call a different method.
# __basic_example_end__


# __async_handle_start__
@serve.deployment(route_prefix="/api")
class Deployment:
    def say_hello(self, name: str):
        return f"Hello {name}!"

    def __call__(self, request):
        return self.say_hello(request.query_params["name"])


handle = serve.run(Deployment.bind())

# __async_handle_end__


# __async_handle_print_start__
print(requests.get("http://localhost:8000/api?name=Alice"))
# Hello Alice!

print(ray.get(handle.say_hello.remote("Alice")))
# Hello Alice!
# __async_handle_print_end__
