# flake8: noqa

import requests

# __echo_class_start__
# File name: echo.py
from ray import serve

@serve.deployment
class EchoClass:

    def __init__(self, echo_str: str):
        self.echo_str = echo_str
    
    def __call__(self, request) -> str:
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
