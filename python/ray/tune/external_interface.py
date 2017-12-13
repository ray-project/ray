import ray

import redis

CLIENT_KEY_CHANNEL = b"Tune:CLKey"
SERVER_KEY_CHANNEL = b"Tune:SRKey"

## TO get this to work:
   # Server needs to expose redis

class ExpManager(object):

    STOP = "STOP"
    PAUSE = "PAUSE"
    UNPAUSE = "UNPAUSE"
    ADD = "ADD"
    GET_LIST = "GET_LIST"
    GET_TRIAL = "GET_TRIAL"

    def __init__(self, redis_address):
        self._interface = None # Interface to running trial_runner process
        self._rclient = None
        self._push_key = None
        if not self._rclient.hkeys(b"Tune:"):
            raise Exception  # TODO(rliaw): Tune is not currently running!
        self._rclient.lpush(CLIENT_KEY_CHANNEL, self._push_key)
        self._pull_key = self._rclient.blpop(SERVER_KEY_CHANNEL, 0)

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
        self._rclient.lpush(self._push_key, command)
        response = self._rclient.blpop(self._pull_key, 0)
        assert response[0] == command[0]
        return response


class ExternalInterface(object):
    """Interface to respond to Experiment Manager.
    Currently only takes 1 manager at once."""

    def __init__(self):
        self._rclient = ray.global_state.redis_client
        self._has_connection = False
        self._request_key = None
        self._response_key = None

    def has_connection(self):
        # check if client is still alive:
            # if not, self._has_connection = False
        return self._has_connection

    def check_conn_reqs(self):
        # check if TunePushKey is not empty
        # if not empty,
        if not self.has_connection():
            self._request_key = self._rclient.lpop(CLIENT_KEY_CHANNEL)
            if self._request_key:
                self._response_key = "ASDF"  # TODO(rliaw): generate responsekey
                self._has_connection = True

    def respond_msgs(self, runner):
        commands = self._rclient.lpop(self._request_key)
        if commands:
            response = parse_command(runner, *commands)
            self._rclient.lpush(self._response_key, response)


def parse_command(runner, command, *args):
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
            return 0
        elif command == ExpManager.PAUSE:
            tid = args[0]
            t = runner.get_trial(tid)
            runner.pause_trial(t)
            return 0
        elif command == ExpManager.UNPAUSE:
            tid = args[0]
            t = runner.get_trial(tid)
            if t.status == Trial.PAUSE:
                runner.unpause_trial(t)
                return 0
            else:
                return 1
        elif command == ExpManager.ADD:
            config = args[0]
            t = Trial(config)
            runner.add_trial(t)
            return 0
        else:
            "Unknown Command"
            return 1
    except Exception e:
        return 1
