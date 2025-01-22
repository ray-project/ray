import time

from ray import serve


@serve.deployment
class A:
    def __init__(self):
        time.sleep(5)
        raise ZeroDivisionError


node = A.bind()
