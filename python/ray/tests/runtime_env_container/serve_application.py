from ray import serve


@serve.deployment
class Model:
    def __call__(self):
        with open("file.txt") as f:
            return f.read().strip()


app = Model.bind()
