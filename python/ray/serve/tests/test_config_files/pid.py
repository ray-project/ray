from ray import serve
import os


@serve.deployment
class f:
    def __init__(self, name: str = "default_name"):
        self.name = name

    def reconfigure(self, config: dict):
        self.name = config.get("name", "default_name")

    async def __call__(self):
        return os.getpid()


node = f.bind()
