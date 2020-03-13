import ray


@ray.remote(num_cpus=0)
class _BatchLogsReporter:
    def __init__(self):
        # we need the new_data field to allow sending back None as the legs
        self._logs = {"new_data": False, "data": None}
        self._setup = {"new_data": False, "data": None}

    def _send_setup(self, data):
        self._setup = {"new_data": True, "data": data}

    def _send(self, data):
        self._logs = {"new_data": True, "data": data}

    def _read(self):
        res = self._logs

        self._logs = {"new_data": False, "data": None}

        return res

    def _read_setup(self):
        res = self._setup

        self._setup = {"new_data": False, "data": None}

        return res
