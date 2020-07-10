"""
Example service that prints out http context.
"""

import json
import time

from pygments import formatters, highlight, lexers

import requests

from ray import serve


def pformat_color_json(d):
    """Use pygments to pretty format and colorize dictionary"""
    formatted_json = json.dumps(d, sort_keys=True, indent=4)

    colorful_json = highlight(formatted_json, lexers.JsonLexer(),
                              formatters.TerminalFormatter())

    return colorful_json


def echo(flask_request):
    return "hello " + flask_request.args.get("name", "serve!")


serve.init()

serve.create_backend("echo:v1", echo)
serve.create_endpoint("my_endpoint", backend="echo:v1", route="/echo")

while True:
    resp = requests.get("http://127.0.0.1:8000/echo").json()
    print(pformat_color_json(resp))

    print("...Sleeping for 2 seconds...")
    time.sleep(2)
