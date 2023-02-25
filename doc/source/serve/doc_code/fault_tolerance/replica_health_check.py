# flake8: noqa

from ray import serve

# Stubs
def connect_to_db(*args, **kwargs):
    pass


# __health_check_start__
@serve.deployment(health_check_period_s=10, health_check_timeout_s=30)
class MyDeployment:
    def __init__(self, db_addr: str):
        self._my_db_connection = connect_to_db(db_addr)

    def __call__(self, request):
        return self._do_something_cool()

    # Called by Serve to check the replica's health.
    def check_health(self):
        if not self._my_db_connection.is_connected():
            # The specific type of exception is not important.
            raise RuntimeError("uh-oh, DB connection is broken.")
            # __health_check_end__
