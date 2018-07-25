import click

# TODO (dsuo): eventually remove this dependency
from flask import Flask, request, send_file
import io
import pyarrow
from pyarrow import plasma as plasma

from ray.utils import hex_to_binary

app = Flask(__name__)
ctx = pyarrow.default_serialization_context()

@app.route('/', methods=['GET', 'POST'])
def index():
    """TODO (dsuo): Add comments.

    TODO (dsuo): Maybe care about batching
    TODO (dsuo): REST is nice conceptually, but probably too much overhead
    TODO (dsuo): Move logic to C++ (keep as head node process)

    NOTE: We do an extra serialize during get and extra deserialize during
        put.
    """
    if request.method == 'POST':
        object_id = request.files['object_id'].read()
        data = request.files['value'].read()
        value = ctx.deserialize(data)

        plasma_client.put(
            value,
            object_id=plasma.ObjectID(object_id),
            memcopy_threads=12,
            serialization_context=ctx)

        return object_id, 402
    elif request.method == 'GET' and 'object_ids' in request.args:
        object_ids = [plasma.ObjectID(hex_to_binary(object_id)) for object_id in \
            request.args['object_ids'].split(",")]

        # Fetch remote objects
        # TODO (dsuo): maybe care about batching
        plasma_client.fetch(object_ids)

        # Get local objects
        # TODO (dsuo): Handle object not available
        results = plasma_client.get(
            object_ids,
            5000,
            ctx)

        # TODO (dsuo): serialize results into one object
        # TODO (dsuo): this may cause problems if the object we're getting back
        # is an array
        data = ctx.serialize(results).to_buffer().to_pybytes()

        return send_file(io.BytesIO(data), mimetype="application/octet-stream")
        
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
    global plasma_client
    plasma_client = plasma.connect(
        plasma_store_socket_name,
        plasma_manager_socket_name,
        64)
    app.run(host=address,
            port=port,
            debug=debug)
    
if __name__ == '__main__':
    start()
