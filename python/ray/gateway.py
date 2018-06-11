import click
from flask import Flask, request
import pickle
import pyarrow
from pyarrow import plasma as plasma
import sys

app = Flask(__name__)
client = None

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        id = request.files['id'].read()
        data = request.files['value'].read()
        ctx = pyarrow.default_serialization_context()

        value = ctx.deserialize(data)
        print(value, id)
        client.put(
            value,
            object_id=pyarrow.plasma.ObjectID(id),
            memcopy_threads=12,
            serialization_context=ctx)
        return id, 402
    else:
        return '''
        <html><body><h1>hi!</h1></body></html>
        '''


@click.command(context_settings=dict(help_option_names=['-h', '--help']))
@click.option(
    "--plasma-store-socket-name",
    "-s",
    type=str,
    required=True,
    help="Plasma store socket name")
@click.option(
    "--plasma-manager-socket-name",
    "-m",
    type=str,
    required=True,
    help="Plasma manager socket name")
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
def start(plasma_store_socket_name,
          plasma_manager_socket_name,
          address,
          port,
          debug):
    app.run(host=address,
            port=port,
            debug=debug)
    client = plasma.connect(
        plasma_store_socket_name,
        plasma_manager_socket_name,
        64)
    print(plasma_store_socket_name, plasma_manager_socket_name, file=sys.stderr)

if __name__ == '__main__':
    start()
