import ray


def main():
    """Confirms placement of a ACC actor."""
    acc_actor = ray.get_actor("acc_actor")
    actor_response = ray.get(acc_actor.where_am_i.remote())
    return actor_response


if __name__ == "__main__":
    ray.init("auto", namespace="acc-test")
    out = main()
    print(out)
