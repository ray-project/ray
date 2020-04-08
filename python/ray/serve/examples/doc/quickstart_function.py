from ray import serve

serve.init()


@serve.route("/hello")
def echo(flask_request):
    return "hello " + flask_request.args.get("name", "serve!")


requests.get("http://127.0.0.1:8000/hello").text
# > "hello serve!"