from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import ray
from ray.utils import random_string
import redis

class ExpManager(object):

    STOP = "STOP"
    PAUSE = "PAUSE"
    UNPAUSE = "UNPAUSE"
    ADD = "ADD"
    GET_LIST = "GET_LIST"
    GET_TRIAL = "GET_TRIAL"

    def __init__(self, tune_address):
        pass

    def get_all_trials(self):
        # Get a list of all trials (tid, config, status) (not result)
        command = ExpManager.GET_LIST
        return self._get_response([command])

    def get_trial_result(self, trial_id):
        command = ExpManager.GET_TRIAL
        return self._get_response([command])

    def add_trial(self, trial_config):
        command = ExpManager.ADD
        return self._get_response([command, trial_config])

    def stop_trial(self, trial_id):
        command = ExpManager.STOP
        return self._get_response([command, trial_id])

    def pause_trial(self, trial_id):
        command = ExpManager.PAUSE
        return self._get_response([command, trial_id])

    def unpause_trial(self, trial_id):
        command = ExpManager.UNPAUSE
        return self._get_response([command, trial_id])

    def _get_response(self, command):
        send_request(command)
        get_request(server, command)
        assert response[0] == command[0]
        return response


class ExternalInterface(object):
    """Interface to respond to Experiment Manager. Currently only takes
    1 manager at once."""

    def __init__(self):
        self._server = None
        self.reset()

    def run():
        http.serve_forever()

    def respond_msgs(self, runner):
        get_messages_from_http_queue()

        response = parse_command(runner, *commands)
        self._server.send_response(code, response)


def parse_command(runner, command, *args):
    import ipdb; ipdb.set_trace()
    try:
        if command == ExpManager.GET_LIST:
            return [t.info() for t in runner.get_trials()]
        elif command == ExpManager.GET_TRIAL:
            t = runner.get_trial()
            return t.info()
        elif command == ExpManager.STOP:
            tid = args[0]
            t = runner.get_trial(tid)
            runner.stop_trial(t)
            return SUCCESS
        elif command == ExpManager.PAUSE:
            tid = args[0]
            t = runner.get_trial(tid)
            runner.pause_trial(t)
            return SUCCESS
        elif command == ExpManager.UNPAUSE:
            tid = args[0]
            t = runner.get_trial(tid)
            if t.status == Trial.PAUSE:
                runner.unpause_trial(t)
                return SUCCESS
            else:
                return ERROR
        elif command == ExpManager.ADD:
            config = args[0]
            t = Trial(config)
            runner.add_trial(t)
            return SUCCESS
        else:
            "Unknown Command"
            return ERROR
    except Exception as e:
        return ERROR


if __name__ == '__main__':
    pass
