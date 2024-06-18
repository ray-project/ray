import ray


def main():
    """Requests placement of a ACC actor."""

    @ray.remote(num_accs=1, num_cpus=1)
    class ACCActor:
        def where_am_i(self):
            assert len(ray.get_acc_ids()) == 1
            return "on-a-acc-node"

    ACCActor.options(name="acc_actor", lifetime="detached").remote()


if __name__ == "__main__":
    ray.init("auto", namespace="acc-test")
    main()
