from ray.util.anyscale.zmq_channel import RouterChannel
import pytest
import ray.cluster_utils
import sys
import time
import os
from ray._private.utils import get_or_create_event_loop


pytestmark = [
    pytest.mark.skipif(
        sys.platform != "linux" and sys.platform != "darwin",
        reason="Requires Linux or MacOS",
    ),
    pytest.mark.timeout(200),
]


@ray.remote
class Actor:
    def __init__(self, initial_message):
        self.initial_message = initial_message

    def run(self, dealer_channel):
        next_message = self.initial_message
        while True:
            dealer_channel.write(next_message)
            next_message = dealer_channel.read()


def test_rllib_seed_pattern(ray_start_regular):
    actors = [Actor.remote(0) for _ in range(5)]
    router_channel = RouterChannel()
    refs = []
    for actor in actors:
        dealer_channel = router_channel.create_dealer(actor)
        refs.append(actor.run.remote(dealer_channel))

    last_message = {actor: -1 for actor in actors}

    start_time = time.time()
    num_iterations = 0
    while num_iterations < 50 and time.time() - start_time < 1:
        message, actor = router_channel.read()
        assert message > last_message[actor]
        last_message[actor] = message
        router_channel.write(actor, message + 1)
        num_iterations += 1
    assert time.time() - start_time < 1
    assert num_iterations == 50


def test_rllib_seed_pattern_async(ray_start_regular):
    async def main():
        @ray.remote
        class AsyncActor:
            async def run(self, dealer_channel):
                next_message = 0
                while True:
                    await dealer_channel.async_write(next_message)
                    next_message = await dealer_channel.async_read()

        actors = [AsyncActor.remote() for _ in range(5)]
        router_channel = RouterChannel(_asyncio=True)
        refs = []
        for actor in actors:
            dealer_channel = router_channel.create_dealer(actor, _asyncio=True)
            refs.append(actor.run.remote(dealer_channel))

        last_message = {actor: -1 for actor in actors}

        start_time = time.time()
        num_iterations = 0
        while num_iterations < 50 and time.time() - start_time < 1:
            message, actor = await router_channel.async_read()
            assert message > last_message[actor]
            last_message[actor] = message
            await router_channel.async_write(actor, message + 1)
            num_iterations += 1
        assert time.time() - start_time < 1
        assert num_iterations == 50

    get_or_create_event_loop().run_until_complete(main())


def test_writing_to_correct_actors(ray_start_regular):
    actors = [Actor.remote(i) for i in range(5)]
    router_channel = RouterChannel()
    refs = []
    for actor in actors:
        dealer_channel = router_channel.create_dealer(actor)
        refs.append(actor.run.remote(dealer_channel))

    actor_to_actor_idx = {actor: i for i, actor in enumerate(actors)}

    start_time = time.time()
    num_iterations = 0
    while num_iterations < 50 and time.time() - start_time < 1:
        message, actor = router_channel.read()
        assert message == actor_to_actor_idx[actor]
        router_channel.write(actor, message)
        num_iterations += 1
    assert time.time() - start_time < 1
    assert num_iterations == 50


def test_multi_node_tcp_channel(ray_start_cluster):
    cluster = ray_start_cluster
    cluster.add_node()
    ray.init(address=cluster.address)
    worker_node = cluster.add_node()
    cluster.wait_for_nodes()

    put_on_worker = ray.util.scheduling_strategies.NodeAffinitySchedulingStrategy(
        worker_node.node_id, soft=False
    )

    actors = [
        Actor.options(scheduling_strategy=put_on_worker).remote(i) for i in range(1)
    ]
    router_channel = RouterChannel()
    refs = []
    for actor in actors:
        dealer_channel = router_channel.create_dealer(actor)
        refs.append(actor.run.remote(dealer_channel))

    actor_to_actor_idx = {actor: i for i, actor in enumerate(actors)}

    start_time = time.time()
    num_iterations = 0
    while num_iterations < 50 and time.time() - start_time < 1:
        message, actor = router_channel.read()
        assert message == actor_to_actor_idx[actor]
        router_channel.write(actor, message)
        num_iterations += 1
    assert time.time() - start_time < 1
    assert num_iterations == 50


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
