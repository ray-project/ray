import ray

runtime_env = {"pip": ["emoji"]}

ray.init(runtime_env=runtime_env)


@ray.remote
def f():
    import emoji

    return emoji.emojize("Python is :thumbs_up:")


print(ray.get(f.remote()))
