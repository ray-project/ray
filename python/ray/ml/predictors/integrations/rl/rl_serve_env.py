import gym
import requests

from ray.rllib import ExternalEnv
from ray.rllib.utils.typing import EnvObsType


class RLServeEnv(ExternalEnv):
    def __init__(self, endpoint_uri: str, env: gym.Env):
        ExternalEnv.__init__(self, env.action_space, env.observation_space)
        self.env = env
        self.endpoint_uri = endpoint_uri

    def _get_action(self, obs: EnvObsType):
        action_dict = requests.post(
            self.endpoint_uri, json={"array": obs.tolist()}
        ).json()
        return action_dict

    def run(self):
        eid = self.start_episode()
        obs = self.env.reset()
        while True:
            action = self._get_action(obs)
            self.log_action(eid, obs, action)
            obs, reward, done, info = self.env.step(action)
            self.log_returns(eid, reward, info=info)
            if done:
                self.end_episode(eid, obs)
                obs = self.env.reset()
                eid = self.start_episode()
