import ray
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
import time

@ray.remote(num_cpus=1)
class Actor:
    def __init__(self) -> None:
        # add own SIGTERM handler
        # self._add_sigterm_handler()
        # print("sigterm handler added"
        self.p = None

    def sleep(self):
        # Fork and sleep together.
        import os
        import multiprocessing
        mp_context = multiprocessing.get_context('spawn')
        import time
        self.p = mp_context.Process(target=time.sleep, args=(1000,))
        self.p.daemon= True
        self.p.start()

        print(f"[pid={os.getpid()}ppid={os.getppid()}]sleeping for 1")
        # self.p.join()
        print("done")
        time.sleep(30)

    def __del__(self):
        print("destructor")
        if self.p is not None:
            self.p.terminate()


def main():
    ray.init(num_cpus=1)

    # Create a placement group
    pg = ray.util.placement_group([{"CPU": 1}])
    ray.get(pg.ready())

    # Create an actor to a placement group.
    actor = Actor.options(
        scheduling_strategy=PlacementGroupSchedulingStrategy(
            placement_group=pg,
        )
    ).remote()

    ray.get(actor.__ray_ready__.remote())
    actor.sleep.remote()
    time.sleep(1)

    # Remove the placement group
    ray.util.remove_placement_group(pg)
    time.sleep(999)


if __name__ == "__main__":
    main()