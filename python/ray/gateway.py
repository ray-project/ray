import click

# TODO (dsuo): eventually remove this dependency
from flask import Flask, request, send_file
import io
import ray

from ray.utils import hex_to_binary

app = Flask(__name__)


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
        raw_object_id = request.files['object_id'].read()
        object_id = ray.pyarrow.plasma.ObjectID(raw_object_id)
        data = request.files['value'].read()

        # Get a memoryview buffer of type unsigned bytes
        buf = memoryview(plasma_client.create(object_id, len(data))).cast("B")

        for i in range(len(data)):
            buf[i] = data[i]
        plasma_client.seal(object_id)
        return raw_object_id, 402

    elif request.method == 'GET' and 'object_ids' in request.args:
        object_ids = [ray.pyarrow.plasma.ObjectID(hex_to_binary(object_id))
                      for object_id in request.args['object_ids'].split(",")]

        # Fetch remote objects
        # TODO (dsuo): maybe care about batching
        # NOTE: this is a really flaky test for "simple value"
        # NOTE: we don't support retrieving multiple objectIDs at a time
        data = plasma_client.get_buffers(object_ids)[0]

        # Return an appropriate return code?
        return send_file(io.BytesIO(data.to_pybytes()), mimetype="application/octet-stream")
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
    default=5002,
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
    plasma_client = ray.pyarrow.plasma.connect(
        plasma_store_socket_name,
        plasma_manager_socket_name,
        64)
    app.run(host=address,
            port=port,
            debug=debug)


if __name__ == '__main__':
    start()
