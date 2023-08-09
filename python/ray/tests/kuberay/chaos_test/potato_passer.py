import asyncio
import argparse
import ray
ray.init()

"""
Potato passer is a test script that lets multiple actors call each other's methods.
Actors are wired in a round-trip fashion: actor 0 calls actor 1, which calls actor 2.
The last actor calls actor 0. In each call, the actor sleeps for a time, occationally
prints, and calls next actor.

Note the number of tasks on-the-fly can go up to `pass-times` because the next call is
made before exiting current call.
"""


@ray.remote
class PotatoPasser:
    def __init__(self, name, next_name, sleep_secs):
        self.count = 0
        self.name = name
        self.next_name = next_name
        self.sleep_secs = sleep_secs
        self.print_every = 100

    async def pass_potato(self, potato: int, target: int):
        self.count += 1
        if potato % self.print_every == 0:
            print(
                f"running, name {self.name}, count {self.count}, "
                f"potato {potato}, target {target}")
        if potato >= target:
            print(f"target reached! name = {self.name}, count = {self.count}")
            return target
        next_actor = ray.get_actor(self.next_name)
        await asyncio.sleep(self.sleep_secs)
        return await next_actor.pass_potato.remote(potato + 1, target)


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-actors", type=int, help="Make this many actors")
    parser.add_argument("--pass-times", type=int, help="Pass this many messages")
    parser.add_argument("--sleep-secs", type=float, help="Sleep seconds before sending "
                        "message to next actor")
    args = parser.parse_args()

    actors = []
    for i in range(args.num_actors):
        this_actor = "actor" + str(i)
        next_actor = "actor" + str((i + 1) % args.num_actors)
        actor = PotatoPasser.options(
            name=this_actor, scheduling_strategy="SPREAD").remote(
            this_actor, next_actor, args.sleep_secs)
        actors.append(actor)

    ret = await actors[0].pass_potato.remote(0, args.pass_times)
    assert ret == args.pass_times


asyncio.run(main())
