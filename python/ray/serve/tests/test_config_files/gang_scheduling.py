from ray import serve


@serve.deployment
class GangApp:
    def __call__(self, *args):
        return "hello_from_gang_scheduling"


app = GangApp.bind()
