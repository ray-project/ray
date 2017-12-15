from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import requests
import json

STOP = "STOP"
PAUSE = "PAUSE"
UNPAUSE = "UNPAUSE"
ADD = "ADD"
GET_LIST = "GET_LIST"
GET_TRIAL = "GET_TRIAL"

class ExpManager(object):

    def __init__(self, tune_address):
        self._tune_address = tune_address
        self._path = "http://{}".format(tune_address)

    def get_all_trials(self):
        """Returns a list of all trials (tid, config, status)"""
        return self._get_response({"command": GET_LIST})

    def get_trial_result(self, trial_id):
        """Returns the last result for queried trial"""
        return self._get_response({"command": GET_TRIAL, "tid": trial_id})

    def add_trial(self, trainable_name, trial_kwargs):
        """Adds a trial of `trainable_name` with specified configurations
        to the TrialRunner"""
        return self._get_response(
            {"command": ADD,
             "trainable_name": trainable_name,
             "kwargs": trial_kwargs})

    def stop_trial(self, trial_id):
        return self._get_response({"command": STOP, "tid": trial_id})

    def pause_trial(self, trial_id):
        return self._get_response({"command": PAUSE, "tid": trial_id})

    def unpause_trial(self, trial_id):
        return self._get_response({"command": UNPAUSE, "tid": trial_id})

    def _get_response(self, data):
        payload = json.dumps(data).encode() # don't know if needed
        response = requests.get(self._path, data=payload)
        parsed = response.json()
        return parsed


from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from six.moves import queue

SUCCESS = True
FAILURE = False


def QueueHandler(in_queue, out_queue):
    class Handler(BaseHTTPRequestHandler):

        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            content_len = int(self.headers.get('Content-Length'), 0)
            raw_body = self.rfile.read(content_len)
            parsed_input = json.loads(raw_body.decode())
            in_queue.put(parsed_input)
            response = out_queue.get()
            self.wfile.write(json.dumps(
                response).encode())
    return Handler

class Interface(threading.Thread):

    def __init__(self, port=4321):
        threading.Thread.__init__(self)
        self._port = port
        self._server = None
        self._inq = queue.Queue()
        self._outq = queue.Queue()
        self.start()

    def run(self):
        address = ('localhost', self._port)
        print("Starting server...")
        self.server = HTTPServer(address, QueueHandler(self._inq, self._outq))
        self.server.serve_forever()

    def respond_msgs(self, runner):
        while not self._inq.empty():
            commands = self._inq.get_nowait()
            # print("Responding to ", commands["command"])
            response = parse_command(
                runner, commands)
            self._outq.put(response)

    def shutdown(self):
        self.server.shutdown()


def parse_command(runner, args):
    command = args["command"]
    try:
        if command == GET_LIST:
            return SUCCESS, [t.info() for t in runner.get_trials()]
        elif command == GET_TRIAL:
            tid = args["tid"]
            trial = runner.get_trial(tid)
            if trial is None:
                return FAILURE, "Trial ({}) not found!".format(tid)
            return SUCCESS, trial.info(), trial.last_result._asdict()
        elif command == STOP:
            tid = args["tid"]
            trial = runner.get_trial(tid)
            if trial is None:
                return FAILURE, "Trial ({}) not found!".format(tid)
            runner.stop_trial(trial)
            return SUCCESS, None
        elif command == PAUSE:
            tid = args["tid"]
            trial = runner.get_trial(tid)
            if trial is None:
                return FAILURE, "Trial ({}) not found!".format(tid)
            runner.pause_trial(trial)  # TODO(rliaw): not implemented
            return SUCCESS, None
        elif command == UNPAUSE:
            tid = args["tid"]
            trial = runner.get_trial(tid)
            if trial is None:
                return FAILURE, "Trial ({}) not found!".format(tid)
            if trial.status == Trial.PAUSE:
                runner.unpause_trial(trial)  # TODO(rliaw): not implemented
                return SUCCESS, None
            else:
                return FAILURE, "Unpause request not valid for {}".format(tid)
        elif command == ADD:
            raise NotImplementedError
            trainable_name = args["trainable_name"]
            kwargs = args["kwargs"]
            trial = Trial(config, **kwargs)
            runner.add_trial(trial)
            return SUCCESS, None
        else:
            return FAILURE, "Unknown Command"
    except Exception as e:
        print(e)
        return FAILURE, "Errored!"
