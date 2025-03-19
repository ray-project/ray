import time
import threading

import gymnasium as gym
import msgpack
import numpy as np
import tree  # pip install dm_tree

import ray
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger, Stats
from ray.util.anyscale.zmq_channel import RouterChannel


class Algo:
    def __init__(
        self,
        *,
        observation_space,
        num_env_runners=2,
        num_envs_per_env_runner=1,
        rollout_fragment_length=50,
        num_aggregator_actors=2,
        train_batch_size_per_learner=500,
        num_learners=1,
        zmq_asyncio=False,
        min_time_s_per_iteration=1,
        env_step_time=0.015,
    ):
        self.observation_space = observation_space
        self.num_env_runners = num_env_runners
        self.num_aggregator_actors = num_aggregator_actors
        self.train_batch_size_per_learner = train_batch_size_per_learner
        self.num_learners = num_learners
        self.zmq_asyncio = zmq_asyncio
        self.min_time_s_per_iteration = min_time_s_per_iteration

        self.metrics = MetricsLogger()

        self.router_channel = RouterChannel(
            _asyncio=self.zmq_asyncio,
            max_num_actors=1_000,
            max_outbound_messages=100_000,
            max_inbound_messages=100_000,
        )

        # Create the env runners.
        self.env_runners = [
            ray.remote(EnvRunner).remote(
                observation_space=self.observation_space,
                num_envs_per_env_runner=num_envs_per_env_runner,
                rollout_fragment_length=rollout_fragment_length,
                env_step_time=env_step_time,
            ) for _ in range(self.num_env_runners)
        ]
        self._env_runner_idx = 0
        # Create the agg. actors.
        self.aggregator_actors = [
            AggregatorActor.remote(
                observation_space=self.observation_space,
                batch_size=self.train_batch_size_per_learner,
            ) for _ in range(self.num_aggregator_actors)
        ]
        self._aggregator_actor_idx = 0
        _agg_dealer_channels = {
            f"AGG{aid}": self.router_channel.create_dealer(
                actor=actor,
                _asyncio=self.zmq_asyncio,
            )
            for aid, actor in enumerate(self.aggregator_actors)
        }
        # Add dealer channels to agg. actors.
        for aid, agg in enumerate(self.aggregator_actors):
            agg.start_zmq.remote(dealer_channel=_agg_dealer_channels[f"AGG{aid}"])

        # Add dealer channels to env runners.
        _er_dealer_channels = {
            f"ER{aid}": self.router_channel.create_dealer(
                actor=actor,
                _asyncio=self.zmq_asyncio,
            )
            for aid, actor in enumerate(self.env_runners)
        }
        # Kick off env runners infinite sampling loops.
        for aid, er in enumerate(self.env_runners):
            er.start_zmq.remote(dealer_channel=_er_dealer_channels[f"ER{aid}"])
            er.add_aggregator_actors.remote(self.aggregator_actors)
            # Kick off infinite sampling loop.
            er.sample.remote()

    def train(self) -> dict:
        t0 = time.time()
        # While the iteration is ongoing ...
        while time.time() - t0 < self.min_time_s_per_iteration:
            # Ping one env runner and query its metrics.
            env_runner = self.env_runners[self._env_runner_idx % len(self.env_runners)]
            self.router_channel.write(
                actor=env_runner,
                message="get_metrics",
            )
            time.sleep(0.01)
            response = self.router_channel.read()
            self.metrics.merge_and_log_n_dicts(
                [tree.map_structure(
                    lambda s: Stats.from_state(msgpack.unpackb(s)),
                    response[0],
                )],
                key="env_runners",
            )
            self._env_runner_idx += 1

            # Ping one env runner and query its metrics.
            agg_actor = self.aggregator_actors[self._aggregator_actor_idx % len(self.aggregator_actors)]
            self.router_channel.write(
                actor=agg_actor,
                message="get_metrics",
            )
            time.sleep(0.01)
            response = self.router_channel.read()
            self.metrics.merge_and_log_n_dicts(
                [tree.map_structure(
                    lambda s: Stats.from_state(msgpack.unpackb(s)),
                    response[0],
                )],
                key="aggregator_actors",
            )
            self._aggregator_actor_idx += 1

        return self.metrics.reduce()

class EnvRunner:
    def __init__(
        self,
        observation_space,
        num_envs_per_env_runner=1,
        rollout_fragment_length=50,
        env_step_time=0.015,
    ):
        self.observation_space = observation_space
        self.num_envs_per_env_runner = num_envs_per_env_runner
        self.rollout_fragment_length = rollout_fragment_length
        self.env_step_time = env_step_time
        # Assuming synchronous/sequential env stepping.
        self._sample_time = (
            self.env_step_time
            * self.num_envs_per_env_runner
            * self.rollout_fragment_length
        )

        self.metrics = MetricsLogger()
        self.metrics._threading_lock = threading.RLock()

        self._size_per_sample_kb = (
            np.prod(self.observation_space.shape)
            * self.observation_space.dtype.itemsize
            * self.num_envs_per_env_runner
            * self.rollout_fragment_length
        )
        # 0.1 for "all the reset" (actions, rewards, terminateds, etc..)
        self._size_per_sample_kb += 0.1 * self._size_per_sample_kb
        self._size_per_sample_kb = int(self._size_per_sample_kb / 1024)

        self._curr_agg_idx = 0
        self._aggregator_actor_refs = []

    def start_zmq(self, dealer_channel):
        self._dealer_channel = dealer_channel
        # Start a dealer channel thread.
        DealerChannelThread(self._dealer_channel, self.metrics).start()

    def add_aggregator_actors(self, aggregator_actor_refs):
        self._aggregator_actor_refs = aggregator_actor_refs

    def sample(self):
        while True:
            self._sample()

    def _sample(self):
        time.sleep(self._sample_time)
        data = create_data(size_kb=self._size_per_sample_kb, n_components=5)
        # Send data directly to an aggregator actor.
        # Pick an aggregator actor round-robin.
        if not self._aggregator_actor_refs:
            return

        env_steps = self.rollout_fragment_length * self.num_envs_per_env_runner
        agg_actor = self._aggregator_actor_refs[self._curr_agg_idx % len(self._aggregator_actor_refs)]
        agg_actor.produce_batch.remote(
            data,
            env_steps=env_steps,
        )
        self.metrics.log_value(
            "num_env_steps_sampled_lifetime",
            env_steps,
            reduce="sum",
            with_throughput=True,
        )
        self._curr_agg_idx += 1


@ray.remote
class AggregatorActor:
    def __init__(
        self,
        observation_space,
        batch_size=500,
        process_time_per_env_step=0.01,
    ):
        self.observation_space = observation_space
        self.batch_size = batch_size
        self.process_time_per_env_step = process_time_per_env_step

        self.metrics = MetricsLogger()
        self.metrics._threading_lock = threading.RLock()

        self._size_per_batch_kb = (
            np.prod(observation_space.shape)
            * self.batch_size
        )
        # 0.5 for "all the reset" (actions, rewards, terminateds, etc..)
        self._size_per_batch_kb += 0.5 * self._size_per_batch_kb
        self._size_per_batch_kb = int(self._size_per_batch_kb / 1024)

    def start_zmq(self, dealer_channel):
        self._dealer_channel = dealer_channel
        # Start the dealer channel thread.
        DealerChannelThread(self._dealer_channel, self.metrics).start()

    def produce_batch(self, data, env_steps: int):
        time.sleep(self.process_time_per_env_step * env_steps)
        #data = create_data(size_kb=self._size_per_batch_kb, n_components=5)
        self.metrics.log_value(
            "num_env_steps_aggregated_lifetime",
            env_steps,
            reduce="sum",
            with_throughput=True,
        )
        return self._size_per_batch_kb * 1024


class DealerChannelThread(threading.Thread):
    def __init__(self, dealer_channel, metrics):
        super().__init__()
        self.dealer_channel = dealer_channel
        self.metrics = metrics

    def run(self):
        while True:
            # Receive the message from the RouterChannel.
            message = self.dealer_channel.read()
            if message == "get_metrics":
                response = tree.map_structure(
                    lambda s: msgpack.packb(s.get_state()),
                    self.metrics.reduce(),
                )
            # TODO: just mirror message for now ...
            else:
                response = message

            self.dealer_channel.write(response)


def create_data(size_kb, n_components, dtype=np.float32):
    bytes_per_element = np.dtype(dtype).itemsize
    total_bytes = size_kb * 1024

    # Divide bytes equally among components
    bytes_per_component = total_bytes // n_components

    if bytes_per_component < bytes_per_element:
        raise ValueError(
            "Target size too small for the given number of components and dtype.")

    elements_per_component = bytes_per_component // bytes_per_element

    # Adjust last component to account for rounding
    remaining_bytes = total_bytes - (
        elements_per_component * bytes_per_element * n_components
    )
    last_component_elements = remaining_bytes // bytes_per_element

    data = {}
    for i in range(n_components):
        size = (
            elements_per_component if i < n_components - 1
            else last_component_elements
        )
        data[f"component_{i}"] = np.zeros(size, dtype=dtype)

    return data


if __name__ == "__main__":
    algo = Algo(
        observation_space=gym.spaces.Box(-1.0, 1.0, (64, 64, 4), np.float32),
        num_env_runners=4,
        num_envs_per_env_runner=1,
        rollout_fragment_length=50,
        num_aggregator_actors=4,
        train_batch_size_per_learner=500,
        num_learners=1,
    )
    time.sleep(1.0)

    while True:
        results = algo.train()
        env_steps_sampled = results['env_runners']['num_env_steps_sampled_lifetime']
        print(
            f"sampled={env_steps_sampled.peek()} "
            f"({env_steps_sampled.peek(throughput=True)}/sec) "
        )
        if "aggregator_actors" in results:
            env_steps_aggregated = results['aggregator_actors']['num_env_steps_aggregated_lifetime']
            print(
                f"aggregated={env_steps_aggregated.peek()} "
                f"({env_steps_aggregated.peek(throughput=True)}/sec) "
            )
