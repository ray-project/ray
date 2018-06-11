import click
from flask import Flask, request
import pickle
import pyarrow
import sys

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        data = request.files['value'].read()
        ctx = pyarrow.default_serialization_context()

        value = ctx.deserialize(data)
        print(value)
        return '''
        hi!
        ''', 400
    else:
        return '''
        <html><body><h1>hi!</h1></body></html>
        '''


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option(
    "--address",
    "-a",
    type=str,
    required=False,
    default="0.0.0.0",
    help="Gateway host address")
@click.option(
    "--port",
    "-p",
    type=int,
    required=False,
    default=5000,
    help="Gateway data port")
@click.option(
    "--debug",
    is_flag=True,
    default=False,
    help="Start Flask server in debug mode")
def start(address, port, debug):
    app.run(host=address,
            port=port,
            debug=debug)

if __name__ == '__main__':
    start()
