from ray import serve


@serve.deployment
class A:
    def __init__(self):
        1 / 0


node = A.bind()
