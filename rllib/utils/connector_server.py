import six.moves.queue as queue
import threading
import traceback

from http.server import SimpleHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

import ray.cloudpickle as pickle
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.connector_client import ConnectorClient


class ConnectorServer(ThreadingMixIn, HTTPServer, InputReader):
    """REST server for the application connector.

    This launches a multi-threaded server that listens on the specified host
    and port to serve policy requests and forward experiences to RLlib. For
    high performance experience collection, it implements InputReader. 

    Examples:
        >>> pg = PGTrainer(
        ...     env="CartPole-v0", config={"input": ConnectorServer})
        >>> while True:
                pg.train()

        >>> client = ConnectorClient("localhost:9900")
        >>> eps_id = client.start_episode()
        >>> action = client.get_action(eps_id, obs)
        >>> ...
        >>> client.log_returns(eps_id, reward)
        >>> ...
        >>> client.log_returns(eps_id, reward)
    """

    @PublicAPI
    def __init__(self, ioctx, address, port):
        self.rollout_worker = ioctx.worker
        self.queue = queue.Queue()
        handler = _make_handler(self.rollout_worker, self.queue)
        HTTPServer.__init__(self, (address, port), handler)
        print("---")
        print("--- Starting connector server at {}:{}".format(address, port))
        print("---")
        thread = threading.Thread(name="server", target=self.serve_forever)
        thread.start()

    @override(InputReader)
    def next(self):
        return self.queue.get()


def _make_handler(rollout_worker, queue):
    class Handler(SimpleHTTPRequestHandler):
        def do_POST(self):
            content_len = int(self.headers.get("Content-Length"), 0)
            raw_body = self.rfile.read(content_len)
            parsed_input = pickle.loads(raw_body)
            try:
                response = self.execute_command(parsed_input)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(pickle.dumps(response))
            except Exception:
                self.send_error(500, traceback.format_exc())

        def execute_command(self, args):
            command = args["command"]
            response = {}
            if command == ConnectorClient.GET_WORKER_ARGS:
                print("Sending worker creation args to client.")
                response["worker_args"] = rollout_worker.creation_args()
            elif command == ConnectorClient.GET_WEIGHTS:
                print("Sending worker weights to client.")
                response["weights"] = rollout_worker.get_weights()
            elif command == ConnectorClient.REPORT_SAMPLES:
                print("Got sample batch of size {} from client.".format(
                    args["samples"].count))
                queue.put(args["samples"])
            else:
                raise Exception("Unknown command: {}".format(command))
            return response

    return Handler
