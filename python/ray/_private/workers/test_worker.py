from ray._raylet import StreamRedirector

import logging
import sys
import atexit

new_fd = StreamRedirector.redirect_stdout_and_get_fd(
    "/tmp/hjiang.out",
    1000000,
    1,
    False,
    False,
)
new_fd = StreamRedirector.redirect_stderr_and_get_fd(
    "/tmp/hjiang.err",
    1000000,
    1,
    False,
    False,
)

print("helloworld")


def flush_stdout():
    sys.stdout.flush()


atexit.register(flush_stdout)

sys.stdout.write("another helloworld")
logging.info("hello world from logger")
