"""This test is based on https://github.com/anyscale/cuj-rl-in-production
"""

import argparse
from datetime import datetime
import json
import os
import requests
import time

import gym
from gym.spaces import Discrete, Box
import numpy as np
import random

import ray
from ray import serve
from ray import tune
from ray.rllib.agents.ppo import APPOTrainer, appo
from ray.rllib.env.env_context import EnvContext
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.models.torch.fcnet import FullyConnectedNetwork as TorchFC
from ray.rllib.utils.framework import try_import_torch

torch, nn = try_import_torch()


class MysteriousCorridor(gym.Env):
    def __init__(self, config: EnvContext):
        self.seed(config.worker_index * config.num_workers)

        self.action_space = Discrete(2)
        self.observation_space = Box(0.0, 4.0, shape=(1, ), dtype=np.float32)
        self.reward = config["reward"]

        self.reset()

    def reset(self):
        # cur_pos is the actual postion of the player. not the state a player
        # sees from outside of the environemtn.
        # E.g., when cur_pos is 1, the returned state is 3.
        # Start from either side of the corridor, 0 or 4.
        self.cur_pos = random.choice([0, 4])
        return [self.cur_pos]

    def _pos_to_state(self, pos):
        ptos = [1, 3, 4, 3, 2]
        return ptos[pos]

    def step(self, action):
        assert action in [0, 1], action

        if action == 0:
            self.cur_pos = max(0, self.cur_pos - 1)
        if action == 1:
            self.cur_pos = min(4, self.cur_pos + 1)

        done = (self.cur_pos == 2)
        reward = self.reward if done else -0.1

        return [self._pos_to_state(self.cur_pos)], reward, done, {}

    def seed(self, seed=None):
        random.seed(seed)


class TorchCustomModel(TorchModelV2, nn.Module):
    """Example of a PyTorch custom model that just delegates to a fc-net."""

    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        TorchModelV2.__init__(self, obs_space, action_space, num_outputs,
                              model_config, name)
        nn.Module.__init__(self)

        self.torch_sub_model = TorchFC(obs_space, action_space, num_outputs,
                                       model_config, name)

    def forward(self, input_dict, state, seq_lens):
        input_dict["obs"] = input_dict["obs"].float()
        fc_out, _ = self.torch_sub_model(input_dict, state, seq_lens)
        return fc_out, []

    def value_function(self):
        return torch.reshape(self.torch_sub_model.value_function(), [-1])


TRAINER_CFG = {
    "env": MysteriousCorridor,
    "env_config": {
        "reward": 1.0,
    },
    "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
    "model": {
        "custom_model": TorchCustomModel,
        "vf_share_layers": True,
    },
    "num_workers": 1,  # parallelism
    "framework": "torch",
    "lr": 1e-3,
}


def train(smoke_test: bool) -> str:
    print("Training automatically with Ray Tune")
    epoch = datetime.now().strftime("%Y%m%d-%H%M")
    results = tune.run(
        tune.durable(APPOTrainer),
        name=f"CUJ-RL-{epoch}",
        config=TRAINER_CFG,
        checkpoint_freq=5,
        checkpoint_at_end=True,
        # TODO(jungong) : figure out which GCS upload_dir should be used here.
        sync_config=tune.SyncConfig(
            sync_to_driver=False,
            sync_on_checkpoint=False,
            upload_dir="gs://jun-riot-test/cuj-rl-ci/"),
        resume=None,
        stop={
            "training_iteration": 50,
            "timesteps_total": 100000,
            "episode_reward_mean": 0.2,
        },
        num_samples=1 if smoke_test else 10,
        metric="episode_reward_mean",
        mode="max")

    print("Best checkpoint: ", results.best_checkpoint)
    return results.best_checkpoint


@serve.deployment(name="corridor", num_replicas=2)
class Corridor(object):
    def __init__(self, ckpt_path):
        config = appo.DEFAULT_CONFIG.copy()
        config.update(TRAINER_CFG)
        config["num_workers"] = 0

        agent = appo.APPOTrainer(config=config, env=MysteriousCorridor)
        agent.restore(ckpt_path)

        self._policy = agent.workers.local_worker().get_policy()
        self._count = 0

    def __action(self, state):
        action = self._policy.compute_single_action(state)
        # JSON can't handle int64. Convert to int32.
        return int(action[0])

    async def __call__(self, request):
        self._count += 1

        body = await request.body()
        try:
            data = json.loads(body.decode("utf-8"))
        except ValueError:
            # Can't parse body as json data.
            return "can't decode: " + body.decode("utf-8")

        try:
            return {
                "count": self._count,
                "action": self.__action(data["state"]),
            }
        except Exception as e:
            return str(e)


# Remote function for getting serve URL from the remote head node.
@ray.remote
def get_serve_url():
    if "ANYSCALE_SESSION_DOMAIN" not in os.environ:
        return "http://127.0.0.1:8000/corridor"
    from anyscale import AnyscaleSDK
    sdk = AnyscaleSDK(auth_token=os.environ.get("ANYSCALE_CLI_TOKEN"))
    return sdk.get_session(
        os.environ.get("ANYSCALE_SESSION_ID")).result.ray_dashboard_url


def deploy(ckpt_path: str):
    serve.start(detached=True)
    Corridor.deploy(ckpt_path)
    print("Corridor service deployed!")

    serve_url = ray.get(get_serve_url.remote())
    print(f"You can query the model at: {serve_url}")

    return serve_url


def test_serve_endpoint(serve_url):
    sess = requests.Session()
    # This should redirect and give us the token cookie.
    resp = sess.get(serve_url)

    data = {"state": 1}
    resp = sess.post(resp.url, json=data)
    print("111: ", str(resp))  # DEBUG
    print(resp.json())

    data = {"state": 3}
    resp = sess.post(resp.url, json=data)
    print("222: ", str(resp))  # DEBUG
    print(resp.json())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing.")
    args = parser.parse_args()

    start = time.time()

    addr = os.environ.get("RAY_ADDRESS")
    job_name = os.environ.get("RAY_JOB_NAME", "torch_tune_serve_test")
    if addr is not None and addr.startswith("anyscale://"):
        client = ray.init(address=addr, job_name=job_name)
    else:
        client = ray.init(address="auto")

    print("Training model.")
    best_checkpoint = train(args.smoke_test)

    print("Setting up Serve.")
    serve_url = deploy(best_checkpoint)

    print("Testing Prediction Service.")
    test_serve_endpoint(serve_url)

    taken = time.time() - start
    result = {
        "time_taken": taken,
    }
    test_output_json = os.environ.get("TEST_OUTPUT_JSON",
                                      "/tmp/rllib_tune_serve_test.json")
    with open(test_output_json, "wt") as f:
        json.dump(result, f)

    print("Test Successful!")
