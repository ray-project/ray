import subprocess


class ProcessRunnerInterceptor:
    """A fd level interceptor for running processes.

    An instance of the ProcessRunnerInterceptor class conforms the the subset
    of the `subprocess` module necessary to act as a process runner. It
    implemented by overriding the default stdout/err streams for the process
    runner.

    """

    def __init__(self, stream, err_stream=None, process_runner=None):
        self.stream = stream
        self.err_stream = err_stream or stream
        self.process_runner = process_runner or subprocess

    def check_output(self, *args, **kwargs):
        with_defaults = {"stdout": self.stream, "stderr": self.err_stream}
        with_defaults.update(kwargs)
        return self.process_runner.check_output(*args, **with_defaults)

    def check_call(self, *args, **kwargs):
        with_defaults = {"stdout": self.stream, "stderr": self.err_stream}
        with_defaults.update(kwargs)
        return self.process_runner.check_call(*args, **with_defaults)
