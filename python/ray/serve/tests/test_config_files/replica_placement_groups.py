from ray import serve

@serve.deployment
class D:
    def __call__(self, *args):
        return "hi"

app = D.bind()
