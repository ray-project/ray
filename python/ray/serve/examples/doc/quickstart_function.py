from ray import serve
import requests

serve.init()


def echo(flask_request):
    return "hello " + flask_request.args.get("name", "serve!")


serve.create_endpoint("hello", "/hello")
serve.create_backend("hello", echo)
serve.set_traffic("hello", {"hello": 1.0})

requests.get("http://127.0.0.1:8000/hello").text
# > "hello serve!"
