from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging

import ray
from ray.rllib.env.base_env import BaseEnv, _DUMMY_AGENT_ID

logger = logging.getLogger(__name__)


class RemoteVectorEnv(BaseEnv):
    """Vector env that executes envs in remote workers.

    This provides dynamic batching of inference as observations are returned
    from the remote simulator actors. Both single and multi-agent child envs
    are supported, and envs can be stepped synchronously or async.
    """

    def __init__(self, make_env, num_envs, multiagent, sync):
        self.make_local_env = make_env
        if sync:
            self.timeout = 9999999.0  # wait for all envs
        else:
            self.timeout = 0.0  # wait for only ready envs

        def make_remote_env(i):
            logger.info("Launching env {} in remote actor".format(i))
            if multiagent:
                return _RemoteMultiAgentEnv.remote(self.make_local_env, i)
            else:
                return _RemoteSingleAgentEnv.remote(self.make_local_env, i)

        self.actors = [make_remote_env(i) for i in range(num_envs)]
        self.pending = None  # lazy init

    def poll(self):
        if self.pending is None:
            self.pending = {a.reset.remote(): a for a in self.actors}

        # each keyed by env_id in [0, num_remote_envs)
        obs, rewards, dones, infos = {}, {}, {}, {}
        ready = []

        # Wait for at least 1 env to be ready here
        while not ready:
            ready, _ = ray.wait(
                list(self.pending),
                num_returns=len(self.pending),
                timeout=self.timeout)

        # Get and return observations for each of the ready envs
        env_ids = set()
        for obj_id in ready:
            actor = self.pending.pop(obj_id)
            env_id = self.actors.index(actor)
            env_ids.add(env_id)
            ob, rew, done, info = ray.get(obj_id)
            obs[env_id] = ob
            rewards[env_id] = rew
            dones[env_id] = done
            infos[env_id] = info

        logger.debug("Got obs batch for actors {}".format(env_ids))
        return obs, rewards, dones, infos, {}

    def send_actions(self, action_dict):
        for env_id, actions in action_dict.items():
            actor = self.actors[env_id]
            obj_id = actor.step.remote(actions)
            self.pending[obj_id] = actor

    def try_reset(self, env_id):
        obs, _, _, _ = ray.get(self.actors[env_id].reset.remote())
        return obs


@ray.remote(num_cpus=0)
class _RemoteMultiAgentEnv(object):
    """Wrapper class for making a multi-agent env a remote actor."""

    def __init__(self, make_env, i):
        self.env = make_env(i)

    def reset(self):
        obs = self.env.reset()
        # each keyed by agent_id in the env
        rew = {agent_id: 0 for agent_id in obs.keys()}
        info = {agent_id: {} for agent_id in obs.keys()}
        done = {"__all__": False}
        return obs, rew, done, info

    def step(self, action_dict):
        return self.env.step(action_dict)


@ray.remote(num_cpus=0)
class _RemoteSingleAgentEnv(object):
    """Wrapper class for making a gym env a remote actor."""

    def __init__(self, make_env, i):
        self.env = make_env(i)

    def reset(self):
        obs = {_DUMMY_AGENT_ID: self.env.reset()}
        rew = {agent_id: 0 for agent_id in obs.keys()}
        info = {agent_id: {} for agent_id in obs.keys()}
        done = {"__all__": False}
        return obs, rew, done, info

    def step(self, action):
        obs, rew, done, info = self.env.step(action[_DUMMY_AGENT_ID])
        obs, rew, done, info = [{
            _DUMMY_AGENT_ID: x
        } for x in [obs, rew, done, info]]
        done["__all__"] = done[_DUMMY_AGENT_ID]
        return obs, rew, done, info
