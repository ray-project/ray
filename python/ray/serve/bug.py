from ray import serve

class DummyEndpoint:
    def __call__(self, flask_request):
        pass

class DummyEndpoint2:
    def __init__(self):
        self.handle = serve.get_handle("endpoint", missing_ok=True)
    def __call__(self):
        return None

serve.init()

serve.create_endpoint("endpoint", "/endpoint", methods=["GET", "POST"])
serve.create_backend(DummyEndpoint, "endpoint:v0")
serve.link("endpoint", "endpoint:v0")

serve.create_endpoint("endpoint2", "/endpoint2", methods=["GET", "POST"])
serve.create_backend(DummyEndpoint2, "endpoint2:v0")
serve.link("endpoint2", "endpoint2:v0")
