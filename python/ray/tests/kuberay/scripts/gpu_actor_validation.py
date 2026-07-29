import ray


def main():
    """Confirms placement of a GPU actor."""
    gpu_actor = ray.get_actor("gpu_actor")
    actor_response = ray.get(gpu_actor.where_am_i.remote())
    return actor_response


if __name__ == "__main__":
    ray.init("auto", namespace="gpu-test")
    out = main()
    print(out)
