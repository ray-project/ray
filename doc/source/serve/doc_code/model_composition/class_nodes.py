# flake8: noqa

import requests

# __echo_class_start__
# File name: echo.py
from starlette.requests import Request

from ray import serve


@serve.deployment
class EchoClass:
    def __init__(self, echo_str: str):
        self.echo_str = echo_str

    def __call__(self, request: Request) -> str:
        return self.echo_str


# You can create ClassNodes from the EchoClass deployment
foo_node = EchoClass.bind("foo")
bar_node = EchoClass.bind("bar")
baz_node = EchoClass.bind("baz")
# __echo_class_end__

for node, echo in [(foo_node, "foo"), (bar_node, "bar"), (baz_node, "baz")]:
    serve.run(node)
    assert requests.get("http://localhost:8000/").text == echo

# __echo_client_start__
# File name: echo_client.py
import requests

response = requests.get("http://localhost:8000/")
echo = response.text
print(echo)
# __echo_client_end__

# __hello_start__
# File name: hello.py
import ray
from ray import serve


@serve.deployment
class LanguageClassifer:
    def __init__(self, spanish_responder, french_responder):
        self.spanish_responder = spanish_responder
        self.french_responder = french_responder

    async def __call__(self, http_request):
        request = await http_request.json()
        language, name = request["language"], request["name"]

        if language == "spanish":
            ref = await self.spanish_responder.say_hello.remote(name)
        elif language == "french":
            ref = await self.french_responder.say_hello.remote(name)
        else:
            return "Please try again."

        return await ref


@serve.deployment
class SpanishResponder:
    def say_hello(self, name: str):
        return f"Hola {name}"


@serve.deployment
class FrenchResponder:
    def say_hello(self, name: str):
        return f"Bonjour {name}"


spanish_responder = SpanishResponder.bind()
french_responder = FrenchResponder.bind()
language_classifier = LanguageClassifer.bind(spanish_responder, french_responder)
# __hello_end__

serve.run(language_classifier)

# __hello_client_start__
# File name: hello_client.py
import requests

response = requests.post(
    "http://localhost:8000", json={"language": "spanish", "name": "Dora"}
)
greeting = response.text
print(greeting)
# __hello_client_end__

assert greeting == "Hola Dora"
