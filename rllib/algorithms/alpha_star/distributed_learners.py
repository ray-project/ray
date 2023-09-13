import math
from typing import Any, Dict, List, Optional, Type

import ray
from ray.actor import ActorHandle
from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.actors import create_colocated_actors
from ray.rllib.utils.tf_utils import get_tf_eager_cls_if_necessary
from ray.rllib.utils.typing import PolicyID, AlgorithmConfigDict


class DistributedLearners:
    """Container class for n learning @ray.remote-turned policies.

    The container contains n "learner shards", each one consisting of one
    multi-agent replay buffer and m policy actors that share this replay
    buffer.
    """

    def __init__(
        self,
        *,
        config,
        max_num_policies_to_train: int,
        replay_actor_class: Type[ActorHandle],
        replay_actor_args: List[Any],
        num_learner_shards: Optional[int] = None,
    ):
        """Initializes a DistributedLearners instance.

        Args:
            config: The Algorithm's config dict.
            max_num_policies_to_train: Maximum number of policies that will ever be
                trainable. For these policies, we'll have to create remote
                policy actors, distributed across n "learner shards".
            num_learner_shards: Optional number of "learner shards" to reserve.
                Each one consists of one multi-agent replay actor and
                m policy actors that share this replay buffer. If None,
                will infer this number automatically from the number of GPUs
                and the max. number of learning policies.
            replay_actor_class: The class to use to produce one multi-agent
                replay buffer on each learner shard (shared by all policy actors
                on that shard).
            replay_actor_args: The args to pass to the remote replay buffer
                actor's constructor.
        """
        self.config = config
        self.num_gpus = self.config.num_gpus
        self.max_num_policies_to_train = max_num_policies_to_train
        self.replay_actor_class = replay_actor_class
        self.replay_actor_args = replay_actor_args

        # Auto-num-learner-shard detection:
        # Examples:
        # 4 GPUs + max. 10 policies to train -> 4 shards (0.4 GPU/pol).
        # 8 GPUs + max. 3 policies to train -> 3 shards (2 GPUs/pol).
        # 8 GPUs + max. 2 policies to train -> 2 shards (4 GPUs/pol).
        # 2 GPUs + max. 5 policies to train -> 2 shards (0.4 GPUs/pol).
        if num_learner_shards is None:
            self.num_learner_shards = min(
                self.num_gpus or self.max_num_policies_to_train,
                self.max_num_policies_to_train,
            )
        #
        else:
            self.num_learner_shards = num_learner_shards

        self.num_gpus_per_shard = self.num_gpus // self.num_learner_shards
        if self.num_gpus_per_shard == 0:
            self.num_gpus_per_shard = self.num_gpus / self.num_learner_shards

        num_policies_per_shard = (
            self.max_num_policies_to_train / self.num_learner_shards
        )
        self.num_gpus_per_policy = self.num_gpus_per_shard / num_policies_per_shard
        self.num_policies_per_shard = math.ceil(num_policies_per_shard)

        self.shards = [
            _Shard(
                config=self.config,
                max_num_policies=self.num_policies_per_shard,
                num_gpus_per_policy=self.num_gpus_per_policy,
                replay_actor_class=self.replay_actor_class,
                replay_actor_args=self.replay_actor_args,
            )
            for _ in range(self.num_learner_shards)
        ]

    def add_policy(self, policy_id, policy_spec):
        # Find first empty slot.
        for shard in self.shards:
            if shard.max_num_policies > len(shard.policy_actors):
                pol_actor = shard.add_policy(policy_id, policy_spec)
                return pol_actor
        raise RuntimeError("All shards are full!")

    def remove_policy(self):
        raise NotImplementedError

    def get_policy_actor(self, policy_id):
        for shard in self.shards:
            if policy_id in shard.policy_actors:
                return shard.policy_actors[policy_id]
        raise None

    def get_replay_and_policy_actors(self, policy_id):
        for shard in self.shards:
            if policy_id in shard.policy_actors:
                return shard.replay_actor, shard.policy_actors[policy_id]
        return None, None

    def get_policy_id(self, policy_actor):
        for shard in self.shards:
            for pid, act in shard.policy_actors.items():
                if act == policy_actor:
                    return pid
        raise None

    def get_replay_actors(self):
        return [shard.replay_actor for shard in self.shards]

    def stop(self) -> None:
        """Terminates all ray actors."""
        for shard in self.shards:
            shard.stop()

    def __len__(self):
        """Returns the number of all Policy actors in all our shards."""
        return sum(len(s) for s in self.shards)

    def __iter__(self):
        def _gen():
            for shard in self.shards:
                for pid, policy_actor in shard.policy_actors.items():
                    yield pid, policy_actor, shard.replay_actor

        return _gen()


class _Shard:
    def __init__(
        self,
        config,
        max_num_policies,
        num_gpus_per_policy,
        replay_actor_class,
        replay_actor_args,
    ):
        # For now, remain in config dict-land (b/c we are dealing with Policy classes
        # here which do NOT use AlgorithmConfig yet).
        if isinstance(config, AlgorithmConfig):
            config = config.to_dict()
        self.config = config
        self.has_replay_buffer = False
        self.max_num_policies = max_num_policies
        self.num_gpus_per_policy = num_gpus_per_policy
        self.replay_actor_class = replay_actor_class
        self.replay_actor_args = replay_actor_args

        self.replay_actor: Optional[ActorHandle] = None
        self.policy_actors: Dict[str, ActorHandle] = {}

    def add_policy(self, policy_id: PolicyID, policy_spec: PolicySpec):
        # Merge the policies config overrides with the main config.
        # Also, adjust `num_gpus` (to indicate an individual policy's
        # num_gpus, not the total number of GPUs).
        cfg = Algorithm.merge_algorithm_configs(
            self.config,
            dict(policy_spec.config, **{"num_gpus": self.num_gpus_per_policy}),
        )

        # Need to create the replay actor first. Then add the first policy.
        if self.replay_actor is None:
            return self._add_replay_buffer_and_policy(policy_id, policy_spec, cfg)

        # Replay actor already exists -> Just add a new policy here.

        assert len(self.policy_actors) < self.max_num_policies

        actual_policy_class = get_tf_eager_cls_if_necessary(
            policy_spec.policy_class, cfg
        )

        colocated = create_colocated_actors(
            actor_specs=[
                (
                    ray.remote(
                        num_cpus=1,
                        num_gpus=self.num_gpus_per_policy
                        if not cfg["_fake_gpus"]
                        else 0,
                    )(actual_policy_class),
                    # Policy c'tor args.
                    (policy_spec.observation_space, policy_spec.action_space, cfg),
                    # Policy c'tor kwargs={}.
                    {},
                    # Count=1,
                    1,
                )
            ],
            # Force co-locate on the already existing replay actor's node.
            node=ray.get(self.replay_actor.get_host.remote()),
        )

        self.policy_actors[policy_id] = colocated[0][0]

        return self.policy_actors[policy_id]

    def _add_replay_buffer_and_policy(
        self,
        policy_id: PolicyID,
        policy_spec: PolicySpec,
        config: AlgorithmConfigDict,
    ):
        assert self.replay_actor is None
        assert len(self.policy_actors) == 0

        actual_policy_class = get_tf_eager_cls_if_necessary(
            policy_spec.policy_class, config
        )

        if isinstance(config, AlgorithmConfig):
            config = config.to_dict()

        colocated = create_colocated_actors(
            actor_specs=[
                (self.replay_actor_class, self.replay_actor_args, {}, 1),
            ]
            + [
                (
                    ray.remote(
                        num_cpus=1,
                        num_gpus=self.num_gpus_per_policy
                        if not config["_fake_gpus"]
                        else 0,
                    )(actual_policy_class),
                    # Policy c'tor args.
                    (policy_spec.observation_space, policy_spec.action_space, config),
                    # Policy c'tor kwargs={}.
                    {},
                    # Count=1,
                    1,
                )
            ],
            node=None,
        )  # None

        self.replay_actor = colocated[0][0]
        self.policy_actors[policy_id] = colocated[1][0]
        self.has_replay_buffer = True

        return self.policy_actors[policy_id]

    def stop(self):
        """Terminates all ray actors (replay and n policy actors)."""
        # Terminate the replay actor.
        self.replay_actor.__ray_terminate__.remote()

        # Terminate all policy actors.
        for pid, policy_actor in self.policy_actors.items():
            policy_actor.__ray_terminate__.remote()

    def __len__(self):
        """Returns the number of Policy actors in this shard."""
        return len(self.policy_actors)
