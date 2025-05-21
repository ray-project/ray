from ray.serve.llm import LLMServer


async def create_server(*args, **kwargs):
    """Asynchronously create an LLMServer instance."""

    # _ = LLMServer(...) will raise TypeError("__init__() should return None")
    # so we do __new__ then __init__
    server = LLMServer.__new__(LLMServer)
    await server.__init__(*args, **kwargs)
    return server
