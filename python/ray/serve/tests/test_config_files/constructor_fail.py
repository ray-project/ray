from ray import serve

@serve.deployment
class ConstructorFailure:
    def __init__(self):
        raise RuntimeError("Intentionally failing.")

app = ConstructorFailure.bind()