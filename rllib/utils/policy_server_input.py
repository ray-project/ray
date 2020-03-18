import logging
import six.moves.queue as queue
import threading
import traceback

from http.server import SimpleHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

import ray.cloudpickle as pickle
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.utils.annotations import override, PublicAPI
from ray.rllib.utils.policy_client import PolicyClient

logger = logging.getLogger(__name__)
logger.setLevel("INFO")  # TODO(ekl) this is needed for cartpole_server.py


class PolicyServerInput(ThreadingMixIn, HTTPServer, InputReader):
    """REST policy server that acts as an offline data source.

    This launches a multi-threaded server that listens on the specified host
    and port to serve policy requests and forward experiences to RLlib. For
    high performance experience collection, it implements InputReader.

    For an example, run `examples/cartpole_server.py` along
    with `examples/cartpole_client.py [--local-inference].

    Examples:
        >>> pg = PGTrainer(
        ...     env="CartPole-v0", config={
        ...         "input": lambda ioctx:
        ...             PolicyServerInput(ioctx, addr, port),
        ...         "num_workers": 0,  # Run just 1 server, in the trainer.
        ...     }
        >>> while True:
                pg.train()

        >>> client = PolicyClient("localhost:9900", inference_mode="local")
        >>> eps_id = client.start_episode()
        >>> action = client.get_action(eps_id, obs)
        >>> ...
        >>> client.log_returns(eps_id, reward)
        >>> ...
        >>> client.log_returns(eps_id, reward)
    """

    @PublicAPI
    def __init__(self, ioctx, address, port):
        """Create a PolicyServerInput.

        This class implements rllib.offline.InputReader, and can be used with
        any Trainer by configuring

            {"num_workers": 0,
             "input": lambda ioctx: PolicyServerInput(ioctx, addr, port)}

        Note that by setting num_workers: 0, the trainer will only create one
        rollout worker / PolicyServerInput. Clients can connect to the launched
        server using rllib.utils.PolicyClient.

        Args:
            ioctx (IOContext): IOContext provided by RLlib.
            address (str): Server addr (e.g., "localhost").
            port (int): Server port (e.g., 9900).
        """

        self.rollout_worker = ioctx.worker
        self.samples_queue = queue.Queue()
        self.metrics_queue = queue.Queue()

        def get_metrics():
            completed = []
            while True:
                try:
                    completed.append(self.metrics_queue.get_nowait())
                except queue.Empty:
                    break
            return completed

        # Forwards client-reported rewards directly into the local rollout
        # worker. This is a bit of a hack since it is patching the get_metrics
        # function of the sampler.
        self.rollout_worker.sampler.get_metrics = get_metrics

        handler = _make_handler(self.rollout_worker, self.samples_queue,
                                self.metrics_queue)
        HTTPServer.__init__(self, (address, port), handler)
        logger.info("---")
        logger.info("--- Starting connector server at {}:{}".format(
            address, port))
        logger.info("---")
        thread = threading.Thread(name="server", target=self.serve_forever)
        thread.start()

    @override(InputReader)
    def next(self):
        return self.samples_queue.get()


def _make_handler(rollout_worker, samples_queue, metrics_queue):
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
            if command == PolicyClient.GET_WORKER_ARGS:
                logger.info("Sending worker creation args to client.")
                response["worker_args"] = rollout_worker.creation_args()
            elif command == PolicyClient.GET_WEIGHTS:
                logger.info("Sending worker weights to client.")
                response["weights"] = rollout_worker.get_weights()
            elif command == PolicyClient.REPORT_SAMPLES:
                logger.info("Got sample batch of size {} from client.".format(
                    args["samples"].count))
                samples_queue.put(args["samples"])
                for rollout_metric in args["metrics"]:
                    metrics_queue.put(rollout_metric)
            else:
                raise Exception("Unknown command: {}".format(command))
            return response

    return Handler
