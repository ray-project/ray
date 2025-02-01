import ray.includes
import tempfile
import sys
import os


def test_redirection():
    with tempfile.TemporaryFile(mode="w+") as sink_file:
        stdout_redirection_opt = ray.includes.StreamRedirectionOption(
            sink_file,
            sys.maxsize,
            1,
            False,
            False,
        )
        fd = ray.includes.get_fd_for_stream_redirection(stdout_redirection_opt)
        with os.fdopen(fd, "w") as write_file:
            write_file.write("helloworld")
        sink_file.flush()

        sink_file.seek(0)
        content = sink_file.read()
        assert content == "helloworld"
