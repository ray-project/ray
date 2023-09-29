import gymnasium as gym
import numpy as np
import tree

from functools import partial
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from ray.experimental.tqdm_ray import tqdm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.core.models.base import STATE_IN, STATE_OUT
from ray.rllib.core.rl_module.marl_module import MultiAgentRLModule
from ray.rllib.evaluation.metrics import RolloutMetrics
from ray.rllib.env.env_runner import EnvRunner
from ray.rllib.env.utils import _gym_env_creator
from ray.rllib.evaluation.postprocessing_v2 import compute_gae_for_episode
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.replay_buffers.episode_replay_buffer import _Episode as Episode
from ray.tune.registry import ENV_CREATOR, _global_registry

_, tf, _ = try_import_tf()


# TODO (simon): MultiAgent Version.
# TODO (sven): Connectors.
# TODO (simon): Sample should return tuple or not. Batch postprocessing
# on local worker in training step (TIMING) or as callback
# `postprocess_trajectory`.
# TODO (simon): Include callbacks.
# TODO (simon): Framework-agnostic.
class PPOEnvRunner(EnvRunner):
    """An environment runner to collect data from vectorized gymnasium environments."""

    def __init__(self, config: AlgorithmConfig, **kwargs):
        """Initializes a PPOEnvRunner.

        Args:
            config: The config to use for setup of this EnvRunner.
        """

        super().__init__(config=config)

        # Get the worker index on which this instance is running.
        self.worker_index: int = kwargs.get("worker_index")

        # Register env for the local context here.
        gym.register(
            "ppo-custom-env-v0",
            partial(
                _global_registry.get(ENV_CREATOR, self.config.env),
                self.config.env_config,
            )
            if _global_registry.contains(ENV_CREATOR, self.config.env)
            else partial(
                _gym_env_creator,
                env_context=self.config.env_config,
                env_descriptor=self.config.env,
            ),
        )
        # Create the vectorized gymnasium env.
        # Wrap into VectorListInfo wrapper to get infos as lists.
        self.env = gym.wrappers.VectorListInfo(
            gym.vector.make(
                "ppo-custom-env-v0",
                num_envs=self.config.num_envs_per_worker,
                asynchronous=self.config.remote_worker_envs,
            )
        )

        self.num_envs: int = self.env.num_envs
        assert self.num_envs == self.config.num_envs_per_worker

        # Create our won instance of a PPORLModule (which then needs
        # to be weight-synched each iteration).
        policy_dict, _ = self.config.get_multi_agent_setup(env=self.env)
        module_spec = self.config.get_marl_module_spec(policy_dict=policy_dict)
        # TODO (simon): This here is only for single agent. Later introduce MA.
        # This is a MARL.
        self.marl_module: MultiAgentRLModule = module_spec.build()

        # Let us set this as default for PPO.
        self._needs_initial_reset: bool = True
        self._episodes: List[Optional[Episode]] = [None for _ in range(self.num_envs)]

        self._done_episodes_for_metrics: List[Episode] = []
        self._ongoing_episodes_for_metrics: Dict[List] = defaultdict(list)
        self._ts_since_last_metrics: int = 0

    @override(EnvRunner)
    def sample(
        self,
        *,
        num_timesteps: int = None,
        num_episodes: int = None,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> Tuple[List[Episode], List[Episode]]:
        """Runs and returns a sample (n timesteps or m episodes) on the env(s)."""

        # If not execution details are provided, use self.config.
        if num_timesteps is None and num_episodes is None:
            if self.config.batch_mode == "truncate_episodes":
                num_timesteps = (
                    self.config.get_rollout_fragment_length(
                        worker_index=self.worker_index
                    )
                    * self.num_envs
                )
            else:
                num_episodes = self.num_envs

        # Sample n timesteps.
        if num_timesteps is not None:
            return self._sample_timesteps(
                num_timesteps=num_timesteps,
                explore=explore,
                random_actions=random_actions,
                force_reset=False,
            )
        # Sample m episodes.
        else:
            # `_sample_episodes` returns only a single list (with completed episodes)
            # therefore add the empty list for the truncated episodes.
            return self._sample_episodes(
                num_episodes=num_episodes,
                explore=explore,
                random_actions=random_actions,
                with_render_data=with_render_data,
            )

    def _sample_timesteps(
        self,
        num_timesteps: int,
        explore: bool = True,
        random_actions: bool = False,
        force_reset: bool = False,
    ) -> Tuple[List[Episode], List[Episode]]:
        """Helper method to sample n timesteps."""

        done_episodes_to_return = []

        # Get initial states for all 'batch_size_B` rows in the forward batch,
        # i.e. for all vector sub_envs.
        if hasattr(self.marl_module, "get_initial_state"):
            initial_states = tree.map_structure(
                lambda s: np.repeat(s, self.num_envs, axis=0),
                self.marl_module[DEFAULT_POLICY_ID].get_initial_state(),
            )
        else:
            initial_states = {}

        # Have to reset the env (on all vector sub_envs).
        if force_reset or self._needs_initial_reset:
            obs, infos = self.env.reset()

            self._episodes = [Episode() for _ in range(self.num_envs)]
            states = initial_states

            # Set initial obs and states in the episodes.
            for i in range(self.num_envs):
                # Extract info for the vector sub_env.
                self._episodes[i].add_initial_observation(
                    initial_observation=obs[i],
                    initial_info=infos[i],
                    # TODO (simon): Check, if this works for the default
                    # stateful encoders.
                    initial_state={k: s[i] for k, s in states.items()},
                )
        # Do not reset envs, but instead continue in already started episodes.
        else:
            # Pick up stored observations and states from previous timesteps.
            obs = np.stack([eps.observations[-1] for eps in self._episodes])
            # Compile the initial state for each batch row (vector sub_env):
            # If episode just started, use the model's initial state, in the
            # other case use the state stored last in the Episode.
            states = {
                k: np.stack(
                    [
                        initial_states[k][i] if eps.states is None else eps.states[k]
                        for i, eps in enumerate(self._episodes)
                    ]
                )
                for k in initial_states.keys()
            }

        # Loop through env in enumerate.(self._episodes):
        ts = 0
        pbar = tqdm(total=num_timesteps, desc=f"Sampling {num_timesteps} timesteps ...")
        import ray

        while ts < num_timesteps:
            # Act randomly.
            if random_actions:
                actions = self.env.action_space.sample()
            # Compute an action using the RLModule.
            else:
                # Note, RLModule `forward()` methods expect `NestedDict`s.
                # TODO (simon): Framework-agnostic.
                batch = {
                    STATE_IN: tree.map_structure(
                        lambda s: tf.convert_to_tensor(s),
                        states,
                    ),
                    SampleBatch.OBS: tf.convert_to_tensor(obs),
                }

                breakpoint()
                # Explore or not.
                if explore:
                    # TODO (simon) Implement this for MARL.
                    fwd_out = self.marl_module[DEFAULT_POLICY_ID].forward_exploration(
                        batch
                    )
                    # `self.module` is a MARL module.
                    action_dist_cls = self.marl_module.foreach_module(
                        lambda m, mid: (mid, m.get_exploration_action_dist_cls()),
                    )

                else:
                    fwd_out = self.marl_module[DEFAULT_POLICY_ID].forward_inference(
                        batch
                    )
                    # `self.module` is a MARL module.
                    action_dist_cls = self.marl_module.foreach_module(
                        lambda m, mid: (mid, m.get_inference_action_dist_cls()),
                    )

                breakpoint()
                action_dist_cls = {mid: dist_cls for mid, dist_cls in action_dist_cls}
                action_dist = action_dist_cls[DEFAULT_POLICY_ID].from_logits(
                    fwd_out[SampleBatch.ACTION_DIST_INPUTS]
                )
                actions = action_dist.sample()
                action_logp = convert_to_numpy(action_dist.logp(actions))
                actions = convert_to_numpy(actions)
                fwd_out = convert_to_numpy(fwd_out)

                if STATE_OUT in fwd_out:
                    states = convert_to_numpy(fwd_out[STATE_OUT])
                    # states = tree.map_structure(lambda s: s.numpy(),
                    # fwd_out[STATE_OUT])

            breakpoint()
            obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
            ts += self.num_envs
            pbar.update(self.num_envs)

            for i in range(self.num_envs):
                # Extract state for vector sub_env.
                s = {k: s[i] for k, s in states.items()}
                # The last entry in self.observations[i] is already the reset
                # obs of the new episode.
                # TODO (simon): This might be unfortunate if a user needs to set a
                # certain env parameter during different episodes (for example for
                # benchmarking).
                # TODO (simon): Check, if there is more efficient conversion. Maybe
                # converting once before the loop for all vector sub_envs.
                if explore:
                    extra_model_output = {
                        SampleBatch.ACTION_DIST_INPUTS: fwd_out[
                            SampleBatch.ACTION_DIST_INPUTS
                        ][i],
                        SampleBatch.ACTION_LOGP: action_logp[i],
                        SampleBatch.VF_PREDS: fwd_out[SampleBatch.VF_PREDS][i],
                    }
                    # In inference we have only the action logits.
                else:
                    extra_model_output = {
                        SampleBatch.ACTION_DIST_INPUTS: fwd_out[
                            SampleBatch.ACTION_DIST_INPUTS
                        ][i],
                        SampleBatch.ACTION_LOGP: action_logp[i],
                    }
                if terminateds[i] or truncateds[i]:
                    # Finish the episode with the actual terminal observation stored in
                    # the info dict.
                    self._episodes[i].add_timestep(
                        # Gym vector env provides the `"final_observation"`.
                        infos[i]["final_observation"],
                        actions[i],
                        rewards[i],
                        infos[i]["final_info"],
                        state=s,
                        is_terminated=terminateds[i],
                        is_truncated=truncateds[i],
                        extra_model_output=extra_model_output,
                    )
                    # Make postprocessing here. Calculate advantages and value targets.
                    self._episodes[i] = compute_gae_for_episode(
                        self._episodes[i],
                        self.config,
                        self.marl_module,
                    )
                    # Reset h-states to nthe model's intiial ones b/c we are starting a
                    # new episode.
                    for k, v in (
                        self.marl_module[DEFAULT_POLICY_ID].get_initial_state().items()
                    ):
                        states[k][i] = v.numpy()

                    done_episodes_to_return.append(self._episodes[i])
                    # Create a new episode object.
                    self._episodes[i] = Episode(observations=[obs[i]], states=s)
                else:
                    self._episodes[i].add_timestep(
                        obs[i],
                        actions[i],
                        rewards[i],
                        infos[i],
                        state=s,
                        extra_model_output=extra_model_output,
                    )

        # Return done episodes ...
        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        # ... and all ongoing episode chunks. Also, make sure, we return
        # a copy and start new chunks so that callers of this function
        # do not alter the ongoing and returned Episode objects.
        ongoing_episodes = self._episodes
        self._episodes = [eps.create_successor() for eps in self._episodes]
        for eps in ongoing_episodes:
            self._ongoing_episodes_for_metrics[eps.id_].append(eps)
        # Make postprocessing here for ongoing episodes. Compute
        # advantages and value targets.
        ongoing_episodes = [
            compute_gae_for_episode(eps, self.config, self.marl_module)
            for eps in ongoing_episodes
        ]
        self._ts_since_last_metrics += ts

        return done_episodes_to_return + ongoing_episodes

    def _sample_episodes(
        self,
        num_episodes: int,
        explore: bool = True,
        random_actions: bool = False,
        with_render_data: bool = False,
    ) -> List[Episode]:
        """Helper method to run n episodes.

        See docstring of `self.sample()` for more details.
        """
        done_episodes_to_return = []

        obs, infos = self.env.reset()
        episodes = [Episode() for _ in range(self.num_envs)]

        # Multiply states n times according to our vector env batch size (num_envs).
        states = tree.map_structure(
            lambda s: np.repeat(s, self.num_envs, axis=0),
            self.marl_module[DEFAULT_POLICY_ID].get_initial_state(),
        )

        render_images = [None] * self.num_envs
        if with_render_data:
            render_images = [e.render() for e in self.env.envs]

        for i in range(self.num_envs):
            # Extract info for vector sub_env.
            # info = {k: v[i] for k, v in infos.items()}
            episodes[i].add_initial_observation(
                initial_info=infos[i],
                initial_state={k: s[i] for k, s in states.items()},
                initial_render_image=render_images[i],
            )

        eps = 0
        with tqdm(
            total=num_episodes, desc=f"Sampling {num_episodes} episodes ..."
        ) as pbar:
            while eps < num_episodes:
                if random_actions:
                    actions = self.env.action_space.sample()
                else:
                    batch = {
                        STATE_IN: tree.map_structure(
                            lambda s: tf.convert_to_tensor(s), states
                        ),
                        SampleBatch.OBS: tf.convert_to_tensor(obs),
                    }

                    if explore:
                        # TODO (simon) Implement this for MARL.
                        fwd_out = self.marl_module[
                            DEFAULT_POLICY_ID
                        ].forward_exploration(batch)
                        # `self.module` is a MARL module.
                        action_dist_cls = self.marl_module.foreach_module(
                            lambda m, mid: (mid, m.get_exploration_action_dist_cls()),
                        )
                    else:
                        # TODO (simon) Implement this for MARL.
                        fwd_out = self.marl_module[DEFAULT_POLICY_ID].forward_inference(
                            batch
                        )
                        # `self.module` is a MARL module.
                        action_dist_cls = self.marl_module.foreach_module(
                            lambda m, mid: (mid, m.get_inference_action_dist_cls()),
                        )

                    action_dist_cls = {
                        mid: dist_cls for mid, dist_cls in action_dist_cls
                    }
                    action_dist = action_dist_cls[DEFAULT_POLICY_ID].from_logits(
                        fwd_out[SampleBatch.ACTION_DIST_INPUTS]
                    )
                    actions = action_dist.sample()
                    action_logp = convert_to_numpy(action_dist.logp(actions))
                    actions = convert_to_numpy(actions)
                    fwd_out = convert_to_numpy(fwd_out)

                    if STATE_OUT in fwd_out:
                        states = fwd_out[STATE_OUT]
                        # states = tree.map_structure(
                        #     lambda s: s.numpy(), fwd_out[STATE_OUT]
                        # )

                obs, rewards, terminateds, truncateds, infos = self.env.step(actions)
                if with_render_data:
                    render_images = [e.render() for e in self.env.envs]

                for i in range(self.num_envs):
                    # Extract info and state for vector sub_env.
                    # info = {k: v[i] for k, v in infos.items()}
                    s = {k: s[i] for k, s in states.items()}
                    # The last entry in self.observations[i] is already the reset
                    # obs of the new episode.
                    if explore:
                        extra_model_output = {
                            SampleBatch.ACTION_DIST_INPUTS: fwd_out[
                                SampleBatch.ACTION_DIST_INPUTS
                            ][i],
                            SampleBatch.ACTION_LOGP: action_logp[i],
                            SampleBatch.VF_PREDS: fwd_out[SampleBatch.VF_PREDS][i],
                        }
                        # In inference we have only the action logits.
                    else:
                        extra_model_output = {
                            SampleBatch.ACTION_DIST_INPUTS: fwd_out[
                                SampleBatch.ACTION_DIST_INPUTS
                            ][i],
                            SampleBatch.ACTION_LOGP: action_logp[i],
                        }
                    if terminateds[i] or truncateds[i]:
                        eps += 1
                        pbar.update(1)

                        episodes[i].add_timestep(
                            infos["final_observation"][i],
                            actions[i],
                            rewards[i],
                            infos[i],
                            state=s,
                            is_terminated=terminateds[i],
                            is_truncated=truncateds[i],
                            extra_model_output=extra_model_output,
                        )
                        done_episodes_to_return.append(episodes[i])

                        # Also early-out if we reach the number of episodes within this
                        # for-loop.
                        if eps == num_episodes:
                            break

                        # Reset h-states to the model's initial ones b/c we are starting
                        # a new episode.
                        for k, v in (
                            self.marl_module[DEFAULT_POLICY_ID]
                            .get_initial_state()
                            .items()
                        ):
                            states[k][i] = v.numpy()

                        episodes[i] = Episode(
                            observations=[obs[i]],
                            infos=[infos[i]],
                            states=s,
                            render_images=[render_images[i]],
                        )
                    else:
                        episodes[i].add_timestep(
                            obs[i],
                            actions[i],
                            rewards[i],
                            infos[i],
                            state=s,
                            render_image=render_images[i],
                            extra_model_output=extra_model_output,
                        )

        self._done_episodes_for_metrics.extend(done_episodes_to_return)
        self._ts_since_last_metrics += sum(len(eps) for eps in done_episodes_to_return)

        # If user calls sample(num_timesteps=..) after this, we must reset again
        # at the beginning.
        self._needs_initial_reset = True

        return done_episodes_to_return

    # TODO (sven): Remove the requirement for EnvRunners/RolloutWorkers to have this
    #  API. Instead Algorithm should compile episode metrics itself via its local
    #  buffer.
    def get_metrics(self) -> List[RolloutMetrics]:
        # Compute per-episode metrics (only on already completed episodes).
        metrics = []
        for eps in self._done_episodes_for_metrics:
            episode_length = len(eps)
            episode_reward = eps.get_return()
            # Don't forget about the already returned chunks of this episode.
            if eps.id_ in self._ongoing_episodes_for_metrics:
                for eps2 in self._ongoing_episodes_for_metrics[eps.id_]:
                    episode_length += len(eps2)
                    episode_reward += eps2.get_return()
                del self._ongoing_episodes_for_metrics[eps.id_]

            metrics.append(
                RolloutMetrics(
                    episode_length=episode_length,
                    episode_reward=episode_reward,
                )
            )

        self._done_episodes_for_metrics.clear()
        self._ts_since_last_metrics = 0

        return metrics

    # TODO (sven): Remove the requirement for EnvRunners/RolloutWorkers to have this
    #  API. Replace by proper state overriding via `EnvRunner.set_state()`
    def set_weights(self, weights, global_vars=None):
        """Writes the weights of our (single-agent) RLModule."""
        self.marl_module.set_state(weights)
        # self.module[DEFAULT_POLICY_ID].set_state(weights[DEFAULT_POLICY_ID])

    def get_weights(self, modules=None):
        """Returns the weights of our (single-agent) RLModule."""
        return self.marl_module.get_state(modules)

    @override(EnvRunner)
    def assert_healthy(self):
        # Make sure, we have built our gym.vector.Env and RLModule properly.
        assert self.env and self.marl_module

    @override(EnvRunner)
    def stop(self):
        # Close our env object via gymnasium's API.
        self.env.close()
