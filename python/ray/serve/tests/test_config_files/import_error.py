from ray import serve

1 / 0


@serve.deployment(ray_actor_options={"num_cpus": 0.1})
def f(*args):
    return "hello world"


app = f.bind()
