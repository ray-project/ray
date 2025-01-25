import time

from ray import serve


@serve.deployment
class A:
    def __init__(self):
        time.sleep(5)
        _ = 1 / 0


node = A.bind()
