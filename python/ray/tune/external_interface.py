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
        """Returns a list of all trials (trialstr, config, status)"""
        return self._get_response({"command": GET_LIST})

    def get_trial_result(self, trialstr):
        """Returns the last result for queried trial"""
        return self._get_response({"command": GET_TRIAL, "trialstr": trialstr})

    def add_trial(self, trainable_name, trial_kwargs):
        """Adds a trial of `trainable_name` with specified configurations
        to the TrialRunner"""
        return self._get_response(
            {"command": ADD,
             "trainable_name": trainable_name,
             "kwargs": trial_kwargs})

    def stop_trial(self, trialstr):
        return self._get_response({"command": STOP, "trialstr": trialstr})

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
            response = execute_command(
                runner, commands)
            self._outq.put(response)

    def shutdown(self):
        self.server.shutdown()


def execute_command(runner, args):
    def get_trial():
        trial = runner.get_trial(args["trialstr"])
        if trial is None:
            error = "Trial ({}) not found!".format(trial)
            raise TuneInterfaceError  # TODO (add error)
        else:
            return trial

    command = args["command"]
    response = {}
    try:
        if command == GET_LIST:
            response["return"] = [t.info() for t in runner.get_trials()]
        elif command == GET_TRIAL:
            trial = get_trial()
            response["trial_info"] = trial.info()
            response["last_result"] = trial.last_result._asdict()
        elif command == STOP:
            trial = get_trial()
            runner.stop_trial(trial)
        elif command == ADD:
            trainable_name = args["trainable_name"]
            kwargs = args["kwargs"]
            trial = Trial(config, **kwargs)
            runner.add_trial(trial)
        else:
            response["message"] = "Unknown command"
            raise TuneInterfaceError

        response["status"] = SUCCESS
    except Exception as e:
        import ipdb; ipdb.set_trace()
        response["status"] = FAILURE
        # TODO(rliaw): get message as part of exception?

    return response

