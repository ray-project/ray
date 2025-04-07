import random

import ray
from ray.rllib.algorithms.utils import AggregatorActor
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger


@ray.remote
class InfiniteAPPOAggregatorActor(AggregatorActor):
    def __init__(
        self,
        *,
        config,
        rl_module_spec,
        sync_freq,
    ):
        super().__init__(config=config, rl_module_spec=rl_module_spec)
        self.sync_freq = sync_freq
        self._batch_dispatchers = None
        self._metrics_actor = None
        self._learner_idx = None

        #self._FAKE_BATCH = _make_fake(
        #    self.config.train_batch_size_per_learner,
        #    return_ray_ref=False,
        #    observation_space=rl_module_spec.rl_module_specs["p0"].observation_space,
        #    action_space=rl_module_spec.rl_module_specs["p0"].action_space,
        #)

        self._num_batches_produced = 0
        self._ts = 0
        self._episodes = []
        self._env_runner_metrics = MetricsLogger()

    def set_other_actors(self, *, batch_dispatchers, metrics_actor, learner_idx):
        self._batch_dispatchers = batch_dispatchers
        self._metrics_actor = metrics_actor
        self._learner_idx = learner_idx

    # Synchronization helper method.
    def sync(self):
        return None

    def push_episodes(self, episodes, env_runner_metrics):
        self._env_runner_metrics.merge_and_log_n_dicts([env_runner_metrics])

        # `__fake` signal to create a fake batch and send that to our Learner
        # instead.
        #if episodes == "__fake":
        #    ma_batch = copy.deepcopy(self._FAKE_BATCH)
        #    batch_env_steps = ma_batch.env_steps()
        #else:
        # Make sure we count how many timesteps we already have and only produce a
        # batch, once we have enough episode data.
        self._episodes.extend(episodes)

        env_steps = sum(len(e) for e in episodes)
        self._ts += env_steps

        ma_batch = None

        if self._ts >= self.config.train_batch_size_per_learner:
            # If we have enough episodes collected to create a single train batch, pass
            # them at once through the connector to receive a single train batch.
            batch = self._learner_connector(
                episodes=self._episodes,
                rl_module=self._module,
                metrics=self.metrics,
            )
            batch_env_steps = sum(len(e) for e in self._episodes)
            self._ts = 0
            for e in self._episodes:
                del e
            self._episodes = []

            # Convert to a dict into a `MultiAgentBatch`.
            # TODO (sven): Try to get rid of dependency on MultiAgentBatch (once our mini-
            #  batch iterators support splitting over a dict).
            ma_batch = MultiAgentBatch(
                policy_batches={
                    pid: SampleBatch(pol_batch) for pid, pol_batch in batch.items()
                },
                env_steps=batch_env_steps,
            )

        if ma_batch:
            self.metrics.log_value(
                "num_env_steps_aggregated_lifetime",
                batch_env_steps,
                reduce="sum",
                with_throughput=True,
            )

            # Forward results to a Learner actor.
            batch_dispatch_actor = random.choice(self._batch_dispatchers)
            batch_dispatch_actor.add_batch.remote(
                batch_ref={"batch": ray.put(ma_batch)},
                learner_idx=self._learner_idx,
            )
            del ma_batch

            self._num_batches_produced += 1

            if self._num_batches_produced % 10 == 0:
                self._metrics_actor.add.remote(
                    env_runner_metrics=self._env_runner_metrics.reduce(),
                    aggregator_metrics=self.metrics.reduce(),
                )

            # Sync with one of the dispatcher actors.
            if self._num_batches_produced % self.sync_freq == 0:
                ray.get(batch_dispatch_actor.sync.remote())

