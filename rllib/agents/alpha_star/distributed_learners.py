import math
from typing import Any, List, Type, Union

import ray
from ray.actor import ActorHandle
from ray.rllib.agents.trainer import Trainer
from ray.rllib.policy.policy import PolicySpec
from ray.rllib.utils.actors import create_colocated_actors
from ray.rllib.utils.typing import PolicyID


class DistributedLearners:
    """Container class for n learning @ray.remote-turned policies."""

    def __init__(
        self,
        config,
        max_num_policies: int,
        replay_actor_class: Type[ActorHandle],
        replay_actor_args: List[Any],
        num_learner_shards: Union[str, int] = "auto",
    ):
        """Initializes a DistributedLearners instance.

        Args:
            config:
            max_num_policies:
            replay_actor_class:
            replay_actor_args:
            num_learner_shards:
        """
        self.config = config
        self.num_gpus = self.config["num_gpus"]
        self.max_num_policies = max_num_policies
        self.replay_actor_class = replay_actor_class
        self.replay_actor_args = replay_actor_args

        # Examples:
        # 4 GPUs + max. 10 policies to train -> 4 shards (0.4 GPU/pol).
        # 8 GPUs + max. 3 policies to train -> 3 shards (2.667 GPUs/pol).
        # 8 GPUs + max. 2 policies to train -> 2 shards (4 GPUs/pol).
        # 2 GPUs + max. 5 policies to train -> 2 shards (0.4 GPUs/pol).
        if num_learner_shards == "auto":
            assert self.num_gpus
            self.num_learner_shards = min(self.num_gpus, self.max_num_policies)
            self.num_gpus_per_shard = self.num_gpus / self.num_learner_shards
        else:
            self.num_learner_shards = num_learner_shards
            self.num_gpus_per_shard = 0

        num_policies_per_shard = self.max_num_policies / self.num_learner_shards
        self.num_gpus_per_policy = self.num_gpus_per_shard / num_policies_per_shard
        self.num_policies_per_shard = math.ceil(num_policies_per_shard)

        self.shards = [
            _Shard(
                self.config,
                self.max_num_policies,
                self.num_gpus_per_policy,
                self.replay_actor_class,
                self.replay_actor_args,
            )
            for _ in range(self.num_learner_shards)
        ]

    def add_policy(self, policy_id, policy_spec):
        # Find first empty slot.
        for shard in self.shards:
            if shard.max_num_policies > len(shard.policy_actors):
                if shard.has_replay_buffer is False:
                    _, pol_actor = shard.add_replay_buffer_and_policy(
                        policy_id, policy_spec
                    )
                else:
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
        self.config = config
        self.has_replay_buffer = False
        self.max_num_policies = max_num_policies
        self.num_gpus_per_policy = num_gpus_per_policy
        self.replay_actor_class = replay_actor_class
        self.replay_actor_args = replay_actor_args

        self.replay_actor = None
        self.policy_actors = {}

    def add_replay_buffer_and_policy(
        self, policy_id: PolicyID, policy_spec: PolicySpec
    ):
        assert self.replay_actor is None
        assert len(self.policy_actors) == 0

        # Merge the policies config overrides with the main config.
        # Also, adjust `num_gpus` (to indicate an individual policy's
        # num_gpus, not the total number of GPUs).
        cfg = Trainer.merge_trainer_configs(
            self.config,
            dict(policy_spec.config, **{"num_gpus": self.num_gpus_per_policy}),
        )

        colocated = create_colocated_actors(
            actor_specs=[
                (self.replay_actor_class, self.replay_actor_args, {}, 1),
            ]
            + [
                (
                    ray.remote(
                        num_cpus=1,
                        num_gpus=self.num_gpus_per_policy
                        if not cfg["_fake_gpus"]
                        else 0,
                    )(policy_spec.policy_class),
                    # Policy c'tor args.
                    (policy_spec.observation_space, policy_spec.action_space, cfg),
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

        return self.replay_actor, self.policy_actors[policy_id]

    def add_policy(self, policy_id: PolicyID, policy_spec: PolicySpec):
        assert len(self.policy_actors) < self.max_num_policies

        # Merge the policies config overrides with the main config.
        # Also, adjust `num_gpus` (to indicate an individual policy's
        # num_gpus, not the total number of GPUs).
        cfg = Trainer.merge_trainer_configs(
            self.config,
            dict(policy_spec.config, **{"num_gpus": self.num_gpus_per_policy}),
        )

        colocated = create_colocated_actors(
            actor_specs=[
                (
                    ray.remote(
                        num_cpus=1,
                        num_gpus=self.num_gpus_per_policy
                        if not cfg["_fake_gpus"]
                        else 0,
                    )(policy_spec.policy_class),
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
