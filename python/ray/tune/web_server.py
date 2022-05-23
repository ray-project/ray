import json
import logging
import threading
from typing import Tuple, List, TYPE_CHECKING

from urllib.parse import urljoin, urlparse
from http.server import SimpleHTTPRequestHandler, HTTPServer

import ray.cloudpickle as cloudpickle
from ray.tune import TuneError
from ray.tune.suggest import BasicVariantGenerator
from ray._private.utils import binary_to_hex, hex_to_binary

if TYPE_CHECKING:
    from ray.tune.trial_runner import TrialRunner

logger = logging.getLogger(__name__)

try:
    import requests  # `requests` is not part of stdlib.
except ImportError:
    requests = None
    logger.exception(
        "Couldn't import `requests` library. "
        "Be sure to install it on the client side."
    )


class TuneClient:
    """Client to interact with an ongoing Tune experiment.

    Requires a TuneServer to have started running.

    Attributes:
        tune_address: Address of running TuneServer
        port_forward: Port number of running TuneServer
    """

    def __init__(self, tune_address: str, port_forward: int):
        self._tune_address = tune_address
        self._port_forward = port_forward
        self._path = "http://{}:{}".format(tune_address, port_forward)

    def get_all_trials(self, timeout=None):
        """Returns a list of all trials' information."""
        response = requests.get(urljoin(self._path, "trials"), timeout=timeout)
        return self._deserialize(response)

    def get_trial(self, trial_id, timeout=None):
        """Returns trial information by trial_id."""
        response = requests.get(
            urljoin(self._path, "trials/{}".format(trial_id)), timeout=timeout
        )
        return self._deserialize(response)

    def add_trial(self, name, specification):
        """Adds a trial by name and specification (dict)."""
        payload = {"name": name, "spec": specification}
        response = requests.post(urljoin(self._path, "trials"), json=payload)
        return self._deserialize(response)

    def stop_trial(self, trial_id):
        """Requests to stop trial by trial_id."""
        response = requests.put(urljoin(self._path, "trials/{}".format(trial_id)))
        return self._deserialize(response)

    def stop_experiment(self):
        """Requests to stop the entire experiment."""
        response = requests.put(urljoin(self._path, "stop_experiment"))
        return self._deserialize(response)

    @property
    def server_address(self):
        return self._tune_address

    @property
    def server_port(self):
        return self._port_forward

    def _load_trial_info(self, trial_info):
        trial_info["config"] = cloudpickle.loads(hex_to_binary(trial_info["config"]))
        trial_info["result"] = cloudpickle.loads(hex_to_binary(trial_info["result"]))

    def _deserialize(self, response):
        parsed = response.json()

        if "trial" in parsed:
            self._load_trial_info(parsed["trial"])
        elif "trials" in parsed:
            for trial_info in parsed["trials"]:
                self._load_trial_info(trial_info)

        return parsed


def RunnerHandler(runner):
    class Handler(SimpleHTTPRequestHandler):
        """A Handler is a custom handler for TuneServer.

        Handles all requests and responses coming into and from
        the TuneServer.
        """

        def _do_header(self, response_code: int = 200, headers: List[Tuple] = None):
            """Sends the header portion of the HTTP response.

            Parameters:
                response_code: Standard HTTP response code
                headers: Standard HTTP response headers
            """
            if headers is None:
                headers = [("Content-type", "application/json")]

            self.send_response(response_code)
            for key, value in headers:
                self.send_header(key, value)
            self.end_headers()

        def do_HEAD(self):
            """HTTP HEAD handler method."""
            self._do_header()

        def do_GET(self):
            """HTTP GET handler method."""
            response_code = 200
            message = ""
            try:
                result = self._get_trial_by_url(self.path)
                resource = {}
                if result:
                    if isinstance(result, list):
                        infos = [self._trial_info(t) for t in result]
                        resource["trials"] = infos
                    else:
                        resource["trial"] = self._trial_info(result)
                message = json.dumps(resource)
            except TuneError as e:
                response_code = 404
                message = str(e)

            self._do_header(response_code=response_code)
            self.wfile.write(message.encode())

        def do_PUT(self):
            """HTTP PUT handler method."""
            response_code = 200
            message = ""
            try:
                resource = {}

                if self.path.endswith("stop_experiment"):
                    runner.request_stop_experiment()
                    trials = list(runner.get_trials())
                else:
                    trials = self._get_trial_by_url(self.path)
                    if trials:
                        if not isinstance(trials, list):
                            trials = [trials]
                        for t in trials:
                            runner.request_stop_trial(t)

                resource["trials"] = [self._trial_info(t) for t in trials]
                message = json.dumps(resource)
            except TuneError as e:
                response_code = 404
                message = str(e)

            self._do_header(response_code=response_code)
            self.wfile.write(message.encode())

        def do_POST(self):
            """HTTP POST handler method."""
            response_code = 201

            content_len = int(self.headers.get("Content-Length"), 0)
            raw_body = self.rfile.read(content_len)
            parsed_input = json.loads(raw_body.decode())
            resource = self._add_trials(parsed_input["name"], parsed_input["spec"])

            headers = [("Content-type", "application/json"), ("Location", "/trials/")]
            self._do_header(response_code=response_code, headers=headers)
            self.wfile.write(json.dumps(resource).encode())

        def _trial_info(self, trial):
            """Returns trial information as JSON."""
            if trial.last_result:
                result = trial.last_result.copy()
            else:
                result = None
            info_dict = {
                "id": trial.trial_id,
                "trainable_name": trial.trainable_name,
                "config": binary_to_hex(cloudpickle.dumps(trial.config)),
                "status": trial.status,
                "result": binary_to_hex(cloudpickle.dumps(result)),
            }
            return info_dict

        def _get_trial_by_url(self, url):
            """Parses url to get either all trials or trial by trial_id."""
            parts = urlparse(url)
            path = parts.path

            if path == "/trials":
                return list(runner.get_trials())
            else:
                trial_id = path.split("/")[-1]
                return runner.get_trial(trial_id)

        def _add_trials(self, name, spec):
            """Add trial by invoking TrialRunner."""
            resource = {}
            resource["trials"] = []
            trial_generator = BasicVariantGenerator()
            trial_generator.add_configurations({name: spec})
            while not trial_generator.is_finished():
                trial = trial_generator.next_trial()
                if not trial:
                    break
                runner.add_trial(trial)
                resource["trials"].append(self._trial_info(trial))
            return resource

    return Handler


class TuneServer(threading.Thread):
    """A TuneServer is a thread that initializes and runs a HTTPServer.

    The server handles requests from a TuneClient.

    Attributes:
        runner: Runner that modifies and accesses trials.
        port_forward: Port number of TuneServer.
    """

    DEFAULT_PORT = 4321

    def __init__(self, runner: "TrialRunner", port: int = None):
        """Initialize HTTPServer and serve forever by invoking self.run()"""
        threading.Thread.__init__(self)
        self._port = port if port else self.DEFAULT_PORT
        address = ("localhost", self._port)
        logger.info("Starting Tune Server...")
        self._server = HTTPServer(address, RunnerHandler(runner))
        self.daemon = True
        self.start()

    def run(self):
        self._server.serve_forever()

    def shutdown(self):
        """Shutdown the underlying server."""
        self._server.shutdown()
