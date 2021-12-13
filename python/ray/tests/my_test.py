import ray
ray.init("ray://localhost:10001", runtime_env={"pip": ["ray[serve]"]})


def f():
    from ray import serve
    serve.start()
