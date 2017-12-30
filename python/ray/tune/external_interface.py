from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
import requests
import json
from ray.tune.error import TuneError, TuneInterfaceError


class ExpManager(object):
    STOP = "STOP"
    ADD = "ADD"
    GET_LIST = "GET_LIST"
    GET_TRIAL = "GET_TRIAL"

    def __init__(self, tune_address):
        self._tune_address = tune_address
        self._path = "http://{}".format(tune_address)

    def get_all_trials(self):
        """Returns a list of all trials (trialstr, config, status)"""
        return self._get_response(
            {"command": ExpManager.GET_LIST})

    def get_trial_result(self, trialstr):
        """Returns the last result for queried trial"""
        return self._get_response(
            {"command": ExpManager.GET_TRIAL,
             "trialstr": trialstr})

    def add_trial(self, trainable_name, trial_kwargs):
        """Adds a trial of `trainable_name` with configurations"""
        return self._get_response(
            {"command": ExpManager.ADD,
             "trainable_name": trainable_name,
             "kwargs": trial_kwargs})

    def stop_trial(self, trialstr):
        return self._get_response(
            {"command": ExpManager.STOP,
             "trialstr": trialstr})

    def _get_response(self, data):
        payload = json.dumps(data).encode() # don't know if needed
        response = requests.get(self._path, data=payload)
        parsed = response.json()
        return parsed


from http.server import HTTPServer, BaseHTTPRequestHandler
import threading
from six.moves import queue


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

class Interface(threading.Thread):

    def __init__(self, port=4321):
        threading.Thread.__init__(self)
        self._port = port
        self._server = None
        self._inqueue = queue.Queue()
        self._outqueue = queue.Queue()
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
            trial = runner.get_trial(args["trialstr"])
            if trial is None:
                error = "Trial ({}) not found.".format(args["trialstr"])
                raise TuneInterfaceError(error)
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
                trainable_name = args["trainable_name"]
                kwargs = args["kwargs"]
                trial = Trial(config, **kwargs)
                runner.add_trial(trial)
            else:
                raise TuneInterfaceError("Unknown command.")
            status = True
        except TuneError as e:
            status = False
            response["message"] = str(e)

        return status, response

