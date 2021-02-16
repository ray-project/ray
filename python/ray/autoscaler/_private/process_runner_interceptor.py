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
        with_defaults = {"stderr": self.err_stream}
        with_defaults.update(kwargs)
        output = self.process_runner.check_output(*args, **with_defaults)
        # Poor man's `tee`
        # TODO (Alex): We're currently abusing check_output by trying to print
        # its output and parse it at the same time.
        self.stream.write(output)
        self.stream.flush()
        return output

    def check_call(self, *args, **kwargs):
        with_defaults = {"stdout": self.stream, "stderr": self.err_stream}
        with_defaults.update(kwargs)
        return_code = self.process_runner.check_call(*args, **with_defaults)
        return return_code
