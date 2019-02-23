from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import sys
import threading

import ray.cloudpickle as cloudpickle
from ray.tune.error import TuneError, TuneManagerError
from ray.tune.suggest import BasicVariantGenerator
from ray.utils import binary_to_hex, hex_to_binary

if sys.version_info[0] == 2:
    from urlparse import urljoin, urlparse
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    from SocketServer import TCPServer as HTTPServer
elif sys.version_info[0] == 3:
    from urllib.parse import urljoin, urlparse
    from http.server import SimpleHTTPRequestHandler, HTTPServer

logger = logging.getLogger(__name__)

try:
    import requests  # `requests` is not part of stdlib.
except ImportError:
    requests = None
    logger.exception("Couldn't import `requests` library. "
                     "Be sure to install it on the client side.")


def load_trial_info(trial_info):
    trial_info["config"] = cloudpickle.loads(
        hex_to_binary(trial_info["config"]))
    trial_info["result"] = cloudpickle.loads(
        hex_to_binary(trial_info["result"]))


class TuneClient(object):
    """A TuneClient interacts with an ongoing Tune experiment by sending requests
    to a TuneServer. Requires server to have started running.

    Attributes:
        tune_address (str): Address of running TuneServer
        port_forward (int): Port number of running TuneServer
    """

    def __init__(self, tune_address, port_forward):
        self._path = "http://{}:{}".format(tune_address, port_forward)

    def get_all_trials(self):
        """Returns a list of all trials' information."""
        return self._get_response(requests.get, urljoin(self._path, "trials"))

    def get_trial(self, trial_id):
        """Returns trial information by trial_id."""
        return self._get_response(
            requests.get, urljoin(self._path, "trials/{}".format(trial_id)))

    def add_trial(self, name, specification):
        """Adds a trial by name and specification (dict)."""
        payload = {"name": name, "spec": specification}
        return self._get_response(
            requests.post, urljoin(self._path, "trials"), payload=payload)

    def stop_trial(self, trial_id):
        """Requests to stop trial by trial_id."""
        return self._get_response(
            requests.put, urljoin(self._path, "trials/{}".format(trial_id)))

    def _get_response(self, requests_fn, url, payload=None):
        """Make HTTP request and parse the response as JSON.

        Also load trial information.
        """
        response = requests_fn(url, json=payload)
        parsed = response.json()

        if "trial" in parsed:
            load_trial_info(parsed["trial"])
        elif "trials" in parsed:
            for trial_info in parsed["trials"]:
                load_trial_info(trial_info)

        return parsed


def RunnerHandler(runner):
    class Handler(SimpleHTTPRequestHandler):
        """A Handler is a custom handler that handles all requests and responses
        coming into and from the TuneServer. Built off SimpleHTTPRequestHandler
        which provides methods for handling HTTP requests.
        """

        def _do_header(self,
                       response_code=200,
                       headers=[('Content-type', 'application/json')]):
            """Sends the header portion of the HTTP response.

            Parameters:
                response_code (int): Standard HTTP response code
                headers (list[tuples]): Standard HTTP response headers
            """
            self.send_response(response_code)
            for h in headers:
                self.send_header(h[0], h[1])
            self.end_headers()

        def do_HEAD(self):
            """HTTP HEAD handler method."""
            self._do_header()

        def do_GET(self):
            """HTTP GET handler method."""
            response_code = 200
            resource, message = {}, ""
            try:
                result = self._get_trial_by_url(self.path)
                if result:
                    if isinstance(result, list):
                        infos = [self._trial_info(t) for t in result]
                        resource["trials"] = infos
                    else:
                        resource["trial"] = self._trial_info(result)
            except TuneError as e:
                response_code = 404
                message = str(e)

            self._do_header(response_code=response_code)
            if response_code == 200:
                self.wfile.write(json.dumps(resource).encode())
            else:
                self.wfile.write(message.encode())

        def do_PUT(self):
            """HTTP PUT handler method."""
            response_code = 200
            resource, message = {}, ""
            try:
                result = self._get_trial_by_url(self.path)
                if result:
                    if isinstance(result, list):
                        infos = [self._trial_info(t) for t in result]
                        resource["trials"] = infos
                        for t in result:
                            runner.request_stop_trial(t)
                    else:
                        resource["trial"] = self._trial_info(result)
                        runner.request_stop_trial(result)
            except TuneError as e:
                response_code = 404
                message = str(e)

            self._do_header(response_code=response_code)
            if response_code == 200:
                self.wfile.write(json.dumps(resource).encode())
            else:
                self.wfile.write(message.encode())

        def do_POST(self):
            """HTTP POST handler method."""
            response_code = 201

            content_len = int(self.headers.get('Content-Length'), 0)
            raw_body = self.rfile.read(content_len)
            parsed_input = json.loads(raw_body.decode())
            resource = self._add_trials(parsed_input["name"],
                                        parsed_input["spec"])

            headers = [('Content-type', 'application/json'), ('Location',
                                                              '/trials/')]
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
                "result": binary_to_hex(cloudpickle.dumps(result))
            }
            return info_dict

        def _get_trial_by_url(self, url):
            """Parses url to get either all trials or trial by trial_id."""
            parts = urlparse(url)
            path = parts.path
            result = None

            if path == "/trials":
                trials = [t for t in runner.get_trials()]
                result = trials
            else:
                trial_id = path.split("/")[-1]
                trial = runner.get_trial(trial_id)
                result = trial

            return result

        def _add_trials(self, name, spec):
            """Add trial by invoking TrialRunner."""
            resource = {}
            resource["trials"] = []
            trial_generator = BasicVariantGenerator()
            trial_generator.add_configurations({name: spec})
            for trial in trial_generator.next_trials():
                runner.add_trial(trial)
                resource["trials"].append(self._trial_info(trial))
            return resource

    return Handler


class TuneServer(threading.Thread):
    """A TuneServer is a thread that initializes and runs a HTTPServer. The
    server handles requests from a TuneClient.

    Attributes:
        runner (TrialRunner): Runner that modifies and accesses trials.
        port_forward (int): Port number of TuneServer.
    """

    DEFAULT_PORT = 4321

    def __init__(self, runner, port=None):
        """Initialize HTTPServer and serve forever by invoking self.run()"""
        threading.Thread.__init__(self)
        self._port = port if port else self.DEFAULT_PORT
        address = ('localhost', self._port)
        logger.info("Starting Tune Server...")
        self._server = HTTPServer(address, RunnerHandler(runner))
        self.start()

    def run(self):
        self._server.serve_forever()

    def shutdown(self):
        self._server.shutdown()
