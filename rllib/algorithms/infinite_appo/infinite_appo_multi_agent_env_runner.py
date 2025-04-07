import random

import ray
from ray.rllib.core import COMPONENT_RL_MODULE
from ray.rllib.env.multi_agent_env_runner import MultiAgentEnvRunner
from ray.rllib.utils.metrics import WEIGHTS_SEQ_NO


class InfiniteAPPOMultiAgentEnvRunner(MultiAgentEnvRunner):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.weights_server_actors = None
        self.sync_freq = 10

        self._curr_agg_idx = 0
        self._aggregator_actor_refs = []

    def add_aggregator_actors(self, aggregator_actor_refs):
        # aggregator_actor_refs must be list of lists. Outer index is the learner index,
        # inner index is the aggregator index (for that learner).
        self._aggregator_actor_refs = []
        for agg_idx, agg_0 in enumerate(aggregator_actor_refs[0]):
            self._aggregator_actor_refs.extend([agg_0] + [aggregator_actor_refs[i][agg_idx] for i in range(1, len(aggregator_actor_refs))])

    def start_infinite_sample(self):
        iteration = 0
        while True:
            # Pull new weights, every n times.
            if iteration % self.config.broadcast_interval == 0 and self.weights_server_actors:
                learner_state = ray.get(random.choice(self.weights_server_actors).get.remote())

                if learner_state is not None:
                    assert isinstance(learner_state[COMPONENT_RL_MODULE], ray.ObjectRef)
                    self.module.set_state(ray.get(learner_state[COMPONENT_RL_MODULE]))
                    self._weights_seq_no = learner_state[WEIGHTS_SEQ_NO]

            episodes = self.sample()

            # Send data directly to an aggregator actor.
            # Pick an aggregator actor round-robin.
            if not self._aggregator_actor_refs:
                return

            agg_actor = self._aggregator_actor_refs[
                self._curr_agg_idx % len(self._aggregator_actor_refs)
            ]
            agg_actor.push_episodes.remote(
                episodes,
                env_runner_metrics=self.get_metrics(),
            )

            # Sync with one aggregator actor.
            if iteration % self.sync_freq == 0:
                ray.get(agg_actor.sync.remote())

            self._curr_agg_idx += 1
            iteration += 1
