from enum import IntEnum

from ray.experimental.serve.exceptions import RayServeException


class TaskContext(IntEnum):
    """TaskContext constants for queue.enqueue method"""
    Web = 1
    Python = 2


# Global variable will be modified in worker
# web == True: currrently processing a request from web server
# web == False: currently processing a request from python
web = False

_not_in_web_context_error = """
Accessing the request object outside of the web context. Please use
"serve.context.web" to determine when the function is called within
a web context.
"""


class FakeFlaskQuest:
    def __getattribute__(self, name):
        raise RayServeException(_not_in_web_context_error)

    def __setattr__(self, name, value):
        raise RayServeException(_not_in_web_context_error)
