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

from sys import version as python_version
from cgi import parse_header, parse_multipart

if sys.version_info[0] == 2:
    from urlparse import parse_qs, urljoin, urlparse
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    from SocketServer import TCPServer as HTTPServer
elif sys.version_info[0] == 3:
    from urllib.parse import parse_qs, urljoin, urlparse
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
    """Client to interact with ongoing Tune experiment.

    Requires server to have started running."""

    def __init__(self, tune_address, port_forward):
        self._tune_address = tune_address
        self._port_forward = port_forward
        self._path = "http://{}:{}".format(tune_address, port_forward)

    def get_all_trials(self):
        """Returns a list of all trials (trial_id, config, status)."""
        response = requests.get(urljoin(self._path, "trials"))
        parsed = response.json()

        if "trial" in parsed:
            load_trial_info(parsed["trial"])
        elif "trials" in parsed:
            for trial_info in parsed["trials"]:
                load_trial_info(trial_info)

        return parsed

    def get_trial(self, trial_id):
        """Returns the last result for queried trial."""
        response = requests.get(urljoin(self._path, "trials/{}".format(trial_id)))
        parsed = response.json()

        if "trial" in parsed:
            load_trial_info(parsed["trial"])
        elif "trials" in parsed:
            for trial_info in parsed["trials"]:
                load_trial_info(trial_info)

        return parsed

    def add_trial(self, name, trial_spec):
        """Adds a trial of `name` with configurations."""
        payload = {
            "name": name,
            "spec": trial_spec
        }
        response = requests.post(urljoin(self._path, "trials"), json=payload)
        parsed = response.json()

        if "trial" in parsed:
            load_trial_info(parsed["trial"])
        elif "trials" in parsed:
            for trial_info in parsed["trials"]:
                load_trial_info(trial_info)

        return parsed

    def stop_trial(self, trial_id):
        """Requests to stop trial."""
        response = requests.put(urljoin(self._path, "trials/{}".format(trial_id)))
        parsed = response.json()

        if "trial" in parsed:
            load_trial_info(parsed["trial"])
        elif "trials" in parsed:
            for trial_info in parsed["trials"]:
                load_trial_info(trial_info)

        return parsed


def RunnerHandler(runner):
    class Handler(SimpleHTTPRequestHandler):
        # Send HTTP Response header, using response code and headers (tuples)
        def _do_header(self, 
                       response_code=200, 
                       headers=[('Content-type', 'application/json')]):
            self.send_response(response_code)
            for h in headers:
                self.send_header(h[0], h[1])
            self.end_headers()

        def do_HEAD(self):
            self._do_header()

        # GET trial(s) information
        def do_GET(self):
            response_code = 200
            resource, message = {}, ""
            try:
                result = self._get_trial_by_uri(self.path)
                if result:
                    if isinstance(result, list):
                        resource["trials"] = [self.trial_info(t) for t in result]
                    else:
                        resource["trial"] = self.trial_info(result)
            except TuneError as e:
                response_code = 404
                message = str(e)

            self._do_header(response_code=response_code)
            if response_code == 200:
                self.wfile.write(json.dumps(resource).encode())
            else:
                self.wfile.write(message.encode())

        # STOP trial(s)
        def do_PUT(self):
            response_code = 200
            resource, message = {}, ""
            try:
                result = self._get_trial_by_uri(self.path)
                if result:
                    if isinstance(result, list):
                        resource["trials"] = [self.trial_info(t) for t in result]
                        for t in result:
                            runner.request_stop_trial(t)
                    else:
                        resource["trial"] = self.trial_info(result)
                        runner.request_stop_trial(result)
            except TuneError as e:
                response_code = 404
                message = str(e)

            self._do_header(response_code=response_code)
            if response_code == 200:
                self.wfile.write(json.dumps(resource).encode())
            else:
                self.wfile.write(message.encode())

        """def parse_POST(self):
            ctype, pdict = parse_header(self.headers['content-type'])
            if ctype == 'multipart/form-data':
                postvars = parse_multipart(self.rfile, pdict)
            elif ctype == 'application/x-www-form-urlencoded':
                length = int(self.headers['content-length'])
                postvars = parse_qs(
                        self.rfile.read(length), 
                        keep_blank_values=1)
            else:
                postvars = {}
            return postvars"""
        
        # ADD trial
        def do_POST(self):
            response_code = 201

            content_len = int(self.headers.get('Content-Length'), 0)
            raw_body = self.rfile.read(content_len)
            parsed_input = json.loads(raw_body.decode())
            resource = self._add_trials(parsed_input["name"], parsed_input["spec"])

            self._do_header(response_code=response_code, headers=[('Content-type', 'application/json'), ('Location', '/trials/')])
            self.wfile.write(json.dumps(resource).encode())

        # Representation of trial information
        def trial_info(self, trial):
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
        
        def _get_trial_by_id(self, trial_id):
            trial = runner.get_trial(trial_id)
            if trial is None:
                error = "Trial ({}) not found.".format(trial_id)
                raise TuneManagerError(error)
            else:
                return trial

        def _get_trial_by_uri(self, uri):
            parts = urlparse(uri)
            path, query = parts.path, parse_qs(parts.query)
            result = None

            if path == "/trials":
                # GET ALL TRIALS
                trials = [t for t in runner.get_trials()]
                result = trials
            else:
                # GET ONE TRIAL
                trial_id = path.split("/")[-1]
                try:
                    trial = self._get_trial_by_id(trial_id)
                except TuneError as e:
                    return None
                result = trial
            
            return result
        
        def _add_trials(self, name, spec):
            resource = {}
            resource["trials"] = []
            trial_generator = BasicVariantGenerator()
            trial_generator.add_configurations({name: spec})
            for trial in trial_generator.next_trials():
                runner.add_trial(trial)
                resource["trials"].append(self.trial_info(trial))
            return resource

    return Handler


class TuneServer(threading.Thread):

    DEFAULT_PORT = 4321

    def __init__(self, runner, port=None):

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
