from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import requests
import json
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from six.moves.queue import Queue

import ray
from ray.tune.error import TuneError, TuneManagerError
from ray.tune.variant_generator import generate_trials


class ExpManager(object):
    STOP = "STOP"
    ADD = "ADD"
    GET_LIST = "GET_LIST"
    GET_TRIAL = "GET_TRIAL"

    def __init__(self, tune_address):
        # TODO(rliaw): Have some way of specifying address and doing port forwarding
        self._tune_address = tune_address
        self._path = "http://{}".format(tune_address)

    def get_all_trials(self):
        """Returns a list of all trials (trial_id, config, status)"""
        return self._get_response(
            {"command": ExpManager.GET_LIST})

    def get_trial(self, trial_id):
        """Returns the last result for queried trial"""
        return self._get_response(
            {"command": ExpManager.GET_TRIAL,
             "trial_id": trial_id})

    def add_trial(self, name, trial_spec):
        """Adds a trial of `name` with configurations"""
        # TODO(rliaw): have better way of specifying a new trial
        return self._get_response(
            {"command": ExpManager.ADD,
             "name": name,
             "spec": trial_spec})

    def stop_trial(self, trial_id):
        return self._get_response(
            {"command": ExpManager.STOP,
             "trial_id": trial_id})

    def _get_response(self, data):
        payload = json.dumps(data).encode() # don't know if needed
        response = requests.get(self._path, data=payload)
        parsed = response.json()
        print("Status:", response)
        return parsed


def QueueHandler(in_queue, out_queue):
    class Handler(BaseHTTPRequestHandler):

        def do_GET(self):
            content_len = int(self.headers.get('Content-Length'), 0)
            raw_body = self.rfile.read(content_len)
            parsed_input = json.loads(raw_body.decode())
            in_queue.put(parsed_input)
            status, response = out_queue.get()
            if status:
                self.send_response(200)
            else:
                self.send_response(400)
            self.end_headers()
            self.wfile.write(json.dumps(
                response).encode())
    return Handler


class TuneManager(threading.Thread):

    def __init__(self, port=4321):
        threading.Thread.__init__(self)
        self._port = port
        self._server = None
        self._inqueue = Queue()
        self._outqueue = Queue()
        self.start()

    def run(self):
        address = ('localhost', self._port)
        print("Starting Tune Server...")
        self.server = HTTPServer(
            address, QueueHandler(self._inqueue, self._outqueue))
        self.server.serve_forever()

    def respond_msgs(self, runner):
        while not self._inqueue.empty():
            commands = self._inqueue.get_nowait()
            response = self.execute_command(
                runner, commands)
            self._outqueue.put(response)

    def shutdown(self):
        self.server.shutdown()

    def execute_command(self, runner, args):
        def get_trial():
            trial = runner.get_trial(args["trial_id"])
            if trial is None:
                error = "Trial ({}) not found.".format(args["trial_id"])
                raise TuneManagerError(error)
            else:
                return trial

        command = args["command"]
        response = {}
        try:
            if command == ExpManager.GET_LIST:
                response["trials"] = [t.info() for t in runner.get_trials()]
            elif command == ExpManager.GET_TRIAL:
                trial = get_trial()
                response["trial_info"] = trial.info()
            elif command == ExpManager.STOP:
                trial = get_trial()
                runner.stop_trial(trial)
            elif command == ExpManager.ADD:
                name = args["name"]
                spec = args["spec"]
                for trial in generate_trials(spec, name):
                    runner.add_trial(trial)
            else:
                raise TuneManagerError("Unknown command.")
            status = True
        except TuneError as e:
            status = False
            response["message"] = str(e)

        return status, response

