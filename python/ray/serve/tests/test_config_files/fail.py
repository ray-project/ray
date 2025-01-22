from ray import serve


@serve.deployment
class A:
    def __init__(self):
        raise ZeroDivisionError


node = A.bind()
