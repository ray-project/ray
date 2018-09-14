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

        # NOTE (dsuo): we should use readinto in the future if possible.
        # Otherwise, we create a throwaway buffer when we read the whole
        # stream of data. Might look something like this:
        #
        #   request.files['value'].readinto(buf)
        #
        # Unfortunately, SpooledTemporaryFile request.files['value']
        # doesn't implement. See here: https://bugs.python.org/issue32600.
        data = request.files['value'].read()

        # Get a memoryview buffer of type unsigned bytes
        buf = memoryview(plasma_client.create(object_id, len(data))).cast("B")

        # Copy data into plasma buffer
        buf[:] = data

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
        return send_file(io.BytesIO(data.to_pybytes()),
                         mimetype="application/octet-stream")
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
@click.option(
    "--use-reloader",
    is_flag=True,
    default=False,
    help="Reload on file changes")
def start(plasma_store_socket_name,
          plasma_manager_socket_name,
          address,
          port,
          debug,
          use_reloader):
    global plasma_client
    plasma_client = ray.pyarrow.plasma.connect(
        plasma_store_socket_name,
        plasma_manager_socket_name,
        64)
    app.run(host=address,
            port=port,
            debug=debug,
            use_reloader=use_reloader)


if __name__ == '__main__':
    start()
