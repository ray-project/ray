from http.server import HTTPServer, SimpleHTTPRequestHandler
import logging
import queue
from socketserver import ThreadingMixIn
import threading
import time
import traceback

import ray.cloudpickle as pickle
from ray.rllib.env.policy_client import PolicyClient, \
    _create_embedded_rollout_worker
from ray.rllib.offline.input_reader import InputReader
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override, PublicAPI

logger = logging.getLogger(__name__)


class PolicyServerInput(ThreadingMixIn, HTTPServer, InputReader):
    """REST policy server that acts as an offline data source.

    This launches a multi-threaded server that listens on the specified host
    and port to serve policy requests and forward experiences to RLlib. For
    high performance experience collection, it implements InputReader.

    For an example, run `examples/serving/cartpole_server.py` along
    with `examples/serving/cartpole_client.py --inference-mode=local|remote`.

    Examples:
        >>> pg = PGTrainer(
        ...     env="CartPole-v0", config={
        ...         "input": lambda ioctx:
        ...             PolicyServerInput(ioctx, addr, port),
        ...         "num_workers": 0,  # Run just 1 server, in the trainer.
        ...     }
        >>> while True:
        >>>     pg.train()

        >>> client = PolicyClient("localhost:9900", inference_mode="local")
        >>> eps_id = client.start_episode()
        >>> action = client.get_action(eps_id, obs)
        >>> ...
        >>> client.log_returns(eps_id, reward)
        >>> ...
        >>> client.log_returns(eps_id, reward)
    """

    @PublicAPI
    def __init__(self, ioctx, address, port, idle_timeout=3.0):
        """Create a PolicyServerInput.

        This class implements rllib.offline.InputReader, and can be used with
        any Trainer by configuring

            {"num_workers": 0,
             "input": lambda ioctx: PolicyServerInput(ioctx, addr, port)}

        Note that by setting num_workers: 0, the trainer will only create one
        rollout worker / PolicyServerInput. Clients can connect to the launched
        server using rllib.env.PolicyClient.

        Args:
            ioctx (IOContext): IOContext provided by RLlib.
            address (str): Server addr (e.g., "localhost").
            port (int): Server port (e.g., 9900).
        """

        self.rollout_worker = ioctx.worker
        self.samples_queue = queue.Queue()
        self.metrics_queue = queue.Queue()
        self.idle_timeout = idle_timeout

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
        if self.rollout_worker.sampler is not None:
            self.rollout_worker.sampler.get_metrics = get_metrics

        # Create a request handler that receives commands from the clients
        # and sends data and metrics into the queues.
        handler = _make_handler(self.rollout_worker, self.samples_queue,
                                self.metrics_queue)
        try:
            import time
            time.sleep(1)
            HTTPServer.__init__(self, (address, port), handler)
        except OSError:
            print(f"Creating a PolicyServer on {address}:{port} failed!")
            import time
            time.sleep(1)
            raise

        logger.info("Starting connector server at "
                    f"{self.server_name}:{self.server_port}")

        # Start the serving thread, listening on socket and handling commands.
        serving_thread = threading.Thread(
            name="server", target=self.serve_forever)
        serving_thread.daemon = True
        serving_thread.start()

        # Start a dummy thread that puts empty SampleBatches on the queue, just
        # in case we don't receive anything from clients (or there aren't
        # any). The latter would block sample collection entirely otherwise,
        # even if other workers' PolicyServerInput receive incoming data from
        # actual clients.
        heart_beat_thread = threading.Thread(
            name="heart-beat", target=self._put_empty_sample_batch_every_n_sec)
        heart_beat_thread.daemon = True
        heart_beat_thread.start()

    @override(InputReader)
    def next(self):
        return self.samples_queue.get()

    def _put_empty_sample_batch_every_n_sec(self):
        # Places an empty SampleBatch every `idle_timeout` seconds onto the
        # `samples_queue`. This avoids hanging of all RolloutWorkers parallel
        # to this one in case this PolicyServerInput does not have incoming
        # data (e.g. no client connected).
        while True:
            time.sleep(self.idle_timeout)
            self.samples_queue.put(SampleBatch())


def _make_handler(rollout_worker, samples_queue, metrics_queue):
    # Only used in remote inference mode. We must create a new rollout worker
    # then since the original worker doesn't have the env properly wrapped in
    # an ExternalEnv interface.
    child_rollout_worker = None
    inference_thread = None
    lock = threading.Lock()

    def setup_child_rollout_worker():
        nonlocal lock
        nonlocal child_rollout_worker
        nonlocal inference_thread

        with lock:
            if child_rollout_worker is None:
                (child_rollout_worker,
                 inference_thread) = _create_embedded_rollout_worker(
                     rollout_worker.creation_args(), report_data)
                child_rollout_worker.set_weights(rollout_worker.get_weights())

    def report_data(data):
        nonlocal child_rollout_worker

        batch = data["samples"]
        batch.decompress_if_needed()
        samples_queue.put(batch)
        for rollout_metric in data["metrics"]:
            metrics_queue.put(rollout_metric)

        if child_rollout_worker is not None:
            child_rollout_worker.set_weights(rollout_worker.get_weights(),
                                             rollout_worker.get_global_vars())

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *a, **kw):
            super().__init__(*a, **kw)

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

            # Local inference commands:
            if command == PolicyClient.GET_WORKER_ARGS:
                logger.info("Sending worker creation args to client.")
                response["worker_args"] = rollout_worker.creation_args()
            elif command == PolicyClient.GET_WEIGHTS:
                logger.info("Sending worker weights to client.")
                response["weights"] = rollout_worker.get_weights()
                response["global_vars"] = rollout_worker.get_global_vars()
            elif command == PolicyClient.REPORT_SAMPLES:
                logger.info("Got sample batch of size {} from client.".format(
                    args["samples"].count))
                report_data(args)

            # Remote inference commands:
            elif command == PolicyClient.START_EPISODE:
                setup_child_rollout_worker()
                assert inference_thread.is_alive()
                response["episode_id"] = (
                    child_rollout_worker.env.start_episode(
                        args["episode_id"], args["training_enabled"]))
            elif command == PolicyClient.GET_ACTION:
                assert inference_thread.is_alive()
                response["action"] = child_rollout_worker.env.get_action(
                    args["episode_id"], args["observation"])
            elif command == PolicyClient.LOG_ACTION:
                assert inference_thread.is_alive()
                child_rollout_worker.env.log_action(
                    args["episode_id"], args["observation"], args["action"])
            elif command == PolicyClient.LOG_RETURNS:
                assert inference_thread.is_alive()
                if args["done"]:
                    child_rollout_worker.env.log_returns(
                        args["episode_id"], args["reward"], args["info"],
                        args["done"])
                else:
                    child_rollout_worker.env.log_returns(
                        args["episode_id"], args["reward"], args["info"])
            elif command == PolicyClient.END_EPISODE:
                assert inference_thread.is_alive()
                child_rollout_worker.env.end_episode(args["episode_id"],
                                                     args["observation"])
            else:
                raise ValueError("Unknown command: {}".format(command))
            return response

    return Handler
