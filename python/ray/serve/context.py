from enum import IntEnum

from ray.serve.exceptions import RayServeException


class TaskContext(IntEnum):
    """TaskContext constants for queue.enqueue method"""
    Web = 1
    Python = 2


# Global variable will be modified in worker
# web == True: currrently processing a request from web server
# web == False: currently processing a request from python
web = False

# batching information in serve context
# batch_size == None : the backend doesn't support batching
# batch_size(int)    : the number of elements of input list
batch_size = None

_not_in_web_context_error = """
Accessing the request object outside of the web context. Please use
"serve.context.web" to determine when the function is called within
a web context.
"""


class FakeFlaskRequest:
    def __getattribute__(self, name):
        raise RayServeException(_not_in_web_context_error)

    def __setattr__(self, name, value):
        raise RayServeException(_not_in_web_context_error)
