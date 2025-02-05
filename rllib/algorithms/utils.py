import platform
from typing import List

import tree  # pip install dm_tree

import ray
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.policy.sample_batch import MultiAgentBatch, SampleBatch
from ray.rllib.utils.actor_manager import FaultAwareApply
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.metrics.metrics_logger import MetricsLogger
from ray.rllib.utils.typing import EpisodeType
from ray.util.annotations import DeveloperAPI

torch, _ = try_import_torch()


@DeveloperAPI(stability="alpha")
class AggregatorActor(FaultAwareApply):
    """Runs episode lists through ConnectorV2 pipeline and creates train batches.

    The actor should be co-located with a Learner worker. Ideally, there should be one
    or two aggregator actors per Learner worker (having even more per Learner probably
    won't help. Then the main process driving the RL algo can perform the following
    execution logic:
    - query n EnvRunners to sample the environment and return n lists of episodes as
    Ray.ObjectRefs.
    - remote call the set of aggregator actors (in round-robin fashion) with these
    list[episodes] refs in async fashion.
    - gather the results asynchronously, as each actor returns refs pointing to
    ready-to-go train batches.
    - as soon as we have at least one train batch per Learner, call the LearnerGroup
    with the (already sharded) refs.
    - an aggregator actor - when receiving p refs to List[EpisodeType] - does:
    -- ray.get() the actual p lists and concatenate the p lists into one
    List[EpisodeType].
    -- pass the lists of episodes through its LearnerConnector pipeline
    -- buffer the output batches of this pipeline until enough batches have been
    collected for creating one train batch (matching the config's
    `train_batch_size_per_learner`).
    -- concatenate q batches into a train batch and return that train batch.
    - the algo main process then passes the ray.ObjectRef to the ready-to-go train batch
    to the LearnerGroup for calling each Learner with one train batch.
    """

    def __init__(self, config: AlgorithmConfig, rl_module_spec):
        self.config = config

        # Set device and node.
        self._node = platform.node()
        self._device = torch.device(
            f"cuda:{ray.get_gpu_ids()[0]}"
            if self.config.num_gpus_per_learner > 0
            else "cpu"
        )
        self.metrics = MetricsLogger()

        # Create the RLModule.
        # TODO (sven): For now, this RLModule (its weights) never gets updated.
        #  The reason the module is needed is for the connector to know, which
        #  sub-modules are stateful (and what their initial state tensors are), and
        #  which IDs the submodules have (to figure out, whether its multi-agent or
        #  not).
        self._module = rl_module_spec.build()
        self._module = self._module.as_multi_rl_module()

        # Create the Learner connector pipeline.
        self._learner_connector = self.config.build_learner_connector(
            input_observation_space=None,
            input_action_space=None,
            device=self._device,
        )

    def get_batch(self, episode_refs: List[ray.ObjectRef]):
        episodes: List[EpisodeType] = []
        # It's possible that individual refs are invalid due to the EnvRunner
        # that produced the ref has crashed or had its entire node go down.
        # In this case, try each ref individually and collect only valid results.
        try:
            episodes = tree.flatten(ray.get(episode_refs))
        except ray.exceptions.OwnerDiedError:
            for ref in episode_refs:
                try:
                    episodes.extend(ray.get(ref))
                except ray.exceptions.OwnerDiedError:
                    pass

        env_steps = sum(len(e) for e in episodes)

        # If we have enough episodes collected to create a single train batch, pass
        # them at once through the connector to recieve a single train batch.
        batch_on_gpu = self._learner_connector(
            episodes=episodes,
            rl_module=self._module,
            metrics=self.metrics,
        )
        # Convert to a dict into a `MultiAgentBatch`.
        # TODO (sven): Try to get rid of dependency on MultiAgentBatch (once our mini-
        #  batch iterators support splitting over a dict).
        ma_batch_on_gpu = MultiAgentBatch(
            policy_batches={
                pid: SampleBatch(batch) for pid, batch in batch_on_gpu.items()
            },
            env_steps=env_steps,
        )
        return ma_batch_on_gpu

    def get_metrics(self):
        return self.metrics.reduce()
