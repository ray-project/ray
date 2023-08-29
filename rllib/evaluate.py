#!/usr/bin/env python

import collections
import copy
import gymnasium as gym
import json
import os
from pathlib import Path
import shelve
import typer

import ray
import ray.cloudpickle as cloudpickle
from ray.rllib.env import MultiAgentEnv
from ray.rllib.env.base_env import _DUMMY_AGENT_ID
from ray.rllib.env.env_context import EnvContext
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
from ray.rllib.utils.spaces.space_utils import flatten_to_single_ndarray
from ray.rllib.common import CLIArguments as cli
from ray.train._checkpoint import Checkpoint
from ray.train._internal.session import _TrainingResult
from ray.tune.utils import merge_dicts
from ray.tune.registry import get_trainable_cls, _global_registry, ENV_CREATOR

# create the "evaluate" Typer app
eval_app = typer.Typer()


class RolloutSaver:
    """Utility class for storing rollouts.

    Currently supports two behaviours: the original, which
    simply dumps everything to a pickle file once complete,
    and a mode which stores each rollout as an entry in a Python
    shelf db file. The latter mode is more robust to memory problems
    or crashes part-way through the rollout generation. Each rollout
    is stored with a key based on the episode number (0-indexed),
    and the number of episodes is stored with the key "num_episodes",
    so to load the shelf file, use something like:

    with shelve.open('rollouts.pkl') as rollouts:
       for episode_index in range(rollouts["num_episodes"]):
          rollout = rollouts[str(episode_index)]

    If outfile is None, this class does nothing.
    """

    def __init__(
        self,
        outfile=None,
        use_shelve=False,
        write_update_file=False,
        target_steps=None,
        target_episodes=None,
        save_info=False,
    ):
        self._outfile = outfile
        self._update_file = None
        self._use_shelve = use_shelve
        self._write_update_file = write_update_file
        self._shelf = None
        self._num_episodes = 0
        self._rollouts = []
        self._current_rollout = []
        self._total_steps = 0
        self._target_episodes = target_episodes
        self._target_steps = target_steps
        self._save_info = save_info

    def _get_tmp_progress_filename(self):
        outpath = Path(self._outfile)
        return outpath.parent / ("__progress_" + outpath.name)

    @property
    def outfile(self):
        return self._outfile

    def __enter__(self):
        if self._outfile:
            if self._use_shelve:
                # Open a shelf file to store each rollout as they come in
                self._shelf = shelve.open(self._outfile)
            else:
                # Original behaviour - keep all rollouts in memory and save
                # them all at the end.
                # But check we can actually write to the outfile before going
                # through the effort of generating the rollouts:
                try:
                    with open(self._outfile, "wb") as _:
                        pass
                except IOError as x:
                    print(
                        "Can not open {} for writing - cancelling rollouts.".format(
                            self._outfile
                        )
                    )
                    raise x
            if self._write_update_file:
                # Open a file to track rollout progress:
                self._update_file = self._get_tmp_progress_filename().open(mode="w")
        return self

    def __exit__(self, type, value, traceback):
        if self._shelf:
            # Close the shelf file, and store the number of episodes for ease
            self._shelf["num_episodes"] = self._num_episodes
            self._shelf.close()
        elif self._outfile and not self._use_shelve:
            # Dump everything as one big pickle:
            cloudpickle.dump(self._rollouts, open(self._outfile, "wb"))
        if self._update_file:
            # Remove the temp progress file:
            self._get_tmp_progress_filename().unlink()
            self._update_file = None

    def _get_progress(self):
        if self._target_episodes:
            return "{} / {} episodes completed".format(
                self._num_episodes, self._target_episodes
            )
        elif self._target_steps:
            return "{} / {} steps completed".format(
                self._total_steps, self._target_steps
            )
        else:
            return "{} episodes completed".format(self._num_episodes)

    def begin_rollout(self):
        self._current_rollout = []

    def end_rollout(self):
        if self._outfile:
            if self._use_shelve:
                # Save this episode as a new entry in the shelf database,
                # using the episode number as the key.
                self._shelf[str(self._num_episodes)] = self._current_rollout
            else:
                # Append this rollout to our list, to save laer.
                self._rollouts.append(self._current_rollout)
        self._num_episodes += 1
        if self._update_file:
            self._update_file.seek(0)
            self._update_file.write(self._get_progress() + "\n")
            self._update_file.flush()

    def append_step(self, obs, action, next_obs, reward, terminated, truncated, info):
        """Add a step to the current rollout, if we are saving them"""
        if self._outfile:
            if self._save_info:
                self._current_rollout.append(
                    [obs, action, next_obs, reward, terminated, truncated, info]
                )
            else:
                self._current_rollout.append(
                    [obs, action, next_obs, reward, terminated, truncated]
                )
        self._total_steps += 1


@eval_app.command()
def run(
    checkpoint: str = cli.Checkpoint,
    algo: str = cli.Algo,
    env: str = cli.Env,
    local_mode: bool = cli.LocalMode,
    render: bool = cli.Render,
    steps: int = cli.Steps,
    episodes: int = cli.Episodes,
    out: str = cli.Out,
    config: str = cli.Config,
    save_info: bool = cli.SaveInfo,
    use_shelve: bool = cli.UseShelve,
    track_progress: bool = cli.TrackProgress,
):

    if use_shelve and not out:
        raise ValueError(
            "If you set --use-shelve, you must provide an output file via "
            "--out as well!"
        )
    if track_progress and not out:
        raise ValueError(
            "If you set --track-progress, you must provide an output file via "
            "--out as well!"
        )
    # Load configuration from checkpoint file.
    config_args = json.loads(config)
    config_path = ""
    if checkpoint:
        config_dir = os.path.dirname(checkpoint)
        config_path = os.path.join(config_dir, "params.pkl")
        # Try parent directory.
        if not os.path.exists(config_path):
            config_path = os.path.join(config_dir, "../params.pkl")

    # Load the config from pickled.
    if os.path.exists(config_path):
        with open(config_path, "rb") as f:
            config = cloudpickle.load(f)
    # If no pkl file found, require command line `--config`.
    else:
        # If no config in given checkpoint -> Error.
        if checkpoint:
            raise ValueError(
                "Could not find params.pkl in either the checkpoint dir or "
                "its parent directory AND no `--config` given on command "
                "line!"
            )
        # Use default config for given agent.
        if not algo:
            raise ValueError("Please provide an algorithm via `--algo`.")
        algo_cls = get_trainable_cls(algo)
        config = algo_cls.get_default_config()

    # Make sure worker 0 has an Env.
    config["create_env_on_driver"] = True

    # Merge with `evaluation_config` (first try from command line, then from
    # pkl file).
    evaluation_config = copy.deepcopy(
        config_args.get("evaluation_config", config.get("evaluation_config", {}))
    )
    config = merge_dicts(config, evaluation_config)
    # Merge with command line `--config` settings (if not already the same anyways).
    config = merge_dicts(config, config_args)
    if not env:
        if not config.get("env"):
            raise ValueError(
                "You either need to provide an --env argument or pass"
                "an `env` key with a valid environment to your `config`"
                "argument."
            )
        env = config.get("env")

    # Make sure we have evaluation workers.
    if not config.get("evaluation_num_workers"):
        config["evaluation_num_workers"] = config.get("num_workers", 0)
    if not config.get("evaluation_duration"):
        config["evaluation_duration"] = 1

    # Hard-override this as it raises a warning by Algorithm otherwise.
    # Makes no sense anyways, to have it set to None as we don't call
    # `Algorithm.train()` here.
    config["evaluation_interval"] = 1

    # Rendering settings.
    config["render_env"] = render

    ray.init(local_mode=local_mode)

    # Create the Algorithm from config.
    cls = get_trainable_cls(algo)
    algorithm = cls(env=env, config=config)

    # Load state from checkpoint, if provided.
    if checkpoint:
        if os.path.isdir(checkpoint):
            checkpoint_dir = checkpoint
        else:
            checkpoint_dir = str(Path(checkpoint).parent)
        print(f"Restoring algorithm from {checkpoint_dir}")
        restore_result = _TrainingResult(
            checkpoint=Checkpoint.from_directory(checkpoint_dir), metrics={}
        )
        algorithm.restore(restore_result)

    # Do the actual rollout.
    with RolloutSaver(
        outfile=out,
        use_shelve=use_shelve,
        write_update_file=track_progress,
        target_steps=steps,
        target_episodes=episodes,
        save_info=save_info,
    ) as saver:
        rollout(algorithm, env, steps, episodes, saver, not render)
    algorithm.stop()


class DefaultMapping(collections.defaultdict):
    """default_factory now takes as an argument the missing key."""

    def __missing__(self, key):
        self[key] = value = self.default_factory(key)
        return value


def default_policy_agent_mapping(unused_agent_id) -> str:
    return DEFAULT_POLICY_ID


def keep_going(steps: int, num_steps: int, episodes: int, num_episodes: int) -> bool:
    """Determine whether we've run enough steps or episodes."""
    episode_limit_reached = num_episodes and episodes >= num_episodes
    step_limit_reached = num_steps and steps >= num_steps

    return False if episode_limit_reached or step_limit_reached else True


def rollout(
    agent,
    env_name,  # keep me, used in tests
    num_steps,
    num_episodes=0,
    saver=None,
    no_render=True,
):
    policy_agent_mapping = default_policy_agent_mapping

    if saver is None:
        saver = RolloutSaver()

    # Normal case: Agent was setup correctly with an evaluation WorkerSet,
    # which we will now use to rollout.
    if hasattr(agent, "evaluation_workers") and isinstance(
        agent.evaluation_workers, WorkerSet
    ):
        steps = 0
        episodes = 0
        while keep_going(steps, num_steps, episodes, num_episodes):
            saver.begin_rollout()
            eval_result = agent.evaluate()["evaluation"]
            # Increase time-step and episode counters.
            eps = agent.config["evaluation_duration"]
            episodes += eps
            steps += eps * eval_result["episode_len_mean"]
            # Print out results and continue.
            print(
                "Episode #{}: reward: {}".format(
                    episodes, eval_result["episode_reward_mean"]
                )
            )
            saver.end_rollout()
        return

    # Agent has no evaluation workers, but RolloutWorkers.
    elif hasattr(agent, "workers") and isinstance(agent.workers, WorkerSet):
        env = agent.workers.local_worker().env
        multiagent = isinstance(env, MultiAgentEnv)
        if agent.workers.local_worker().multiagent:
            policy_agent_mapping = agent.config.policy_mapping_fn
        policy_map = agent.workers.local_worker().policy_map
        state_init = {p: m.get_initial_state() for p, m in policy_map.items()}
        use_lstm = {p: len(s) > 0 for p, s in state_init.items()}

    # Agent has neither evaluation- nor rollout workers.
    else:
        from gymnasium import envs

        if envs.registry.env_specs.get(agent.config["env"]):
            # if environment is gym environment, load from gym
            env = gym.make(agent.config["env"])
        else:
            # if environment registered ray environment, load from ray
            env_creator = _global_registry.get(ENV_CREATOR, agent.config["env"])
            env_context = EnvContext(agent.config["env_config"] or {}, worker_index=0)
            env = env_creator(env_context)
        multiagent = False
        try:
            policy_map = {DEFAULT_POLICY_ID: agent.policy}
        except AttributeError:
            raise AttributeError(
                "Agent ({}) does not have a `policy` property! This is needed "
                "for performing (trained) agent rollouts.".format(agent)
            )
        use_lstm = {DEFAULT_POLICY_ID: False}

    action_init = {
        p: flatten_to_single_ndarray(m.action_space.sample())
        for p, m in policy_map.items()
    }

    steps = 0
    episodes = 0
    while keep_going(steps, num_steps, episodes, num_episodes):
        mapping_cache = {}  # in case policy_agent_mapping is stochastic
        saver.begin_rollout()
        obs, info = env.reset()
        agent_states = DefaultMapping(
            lambda agent_id: state_init[mapping_cache[agent_id]]
        )
        prev_actions = DefaultMapping(
            lambda agent_id: action_init[mapping_cache[agent_id]]
        )
        prev_rewards = collections.defaultdict(lambda: 0.0)
        terminated = truncated = False
        reward_total = 0.0
        while (
            not terminated
            and not truncated
            and keep_going(steps, num_steps, episodes, num_episodes)
        ):
            multi_obs = obs if multiagent else {_DUMMY_AGENT_ID: obs}
            action_dict = {}
            for agent_id, a_obs in multi_obs.items():
                if a_obs is not None:
                    policy_id = mapping_cache.setdefault(
                        agent_id, policy_agent_mapping(agent_id)
                    )
                    p_use_lstm = use_lstm[policy_id]
                    if p_use_lstm:
                        a_action, p_state, _ = agent.compute_single_action(
                            a_obs,
                            state=agent_states[agent_id],
                            prev_action=prev_actions[agent_id],
                            prev_reward=prev_rewards[agent_id],
                            policy_id=policy_id,
                        )
                        agent_states[agent_id] = p_state
                    else:
                        a_action = agent.compute_single_action(
                            a_obs,
                            prev_action=prev_actions[agent_id],
                            prev_reward=prev_rewards[agent_id],
                            policy_id=policy_id,
                        )
                    a_action = flatten_to_single_ndarray(a_action)
                    action_dict[agent_id] = a_action
                    prev_actions[agent_id] = a_action
            action = action_dict

            action = action if multiagent else action[_DUMMY_AGENT_ID]
            next_obs, reward, terminated, truncated, info = env.step(action)
            if multiagent:
                for agent_id, r in reward.items():
                    prev_rewards[agent_id] = r
            else:
                prev_rewards[_DUMMY_AGENT_ID] = reward

            if multiagent:
                terminated = terminated["__all__"]
                truncated = truncated["__all__"]
                reward_total += sum(r for r in reward.values() if r is not None)
            else:
                reward_total += reward
            if not no_render:
                env.render()
            saver.append_step(
                obs, action, next_obs, reward, terminated, truncated, info
            )
            steps += 1
            obs = next_obs
        saver.end_rollout()
        print("Episode #{}: reward: {}".format(episodes, reward_total))
        if terminated or truncated:
            episodes += 1


def main():
    """Run the CLI."""
    eval_app()


if __name__ == "__main__":
    main()
