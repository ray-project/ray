# __doc_import_begin__
import gym
from starlette.requests import Request
import requests

import ray
import ray.rllib.agents.ppo as ppo
from ray import serve

# __doc_import_end__


# __doc_define_servable_begin__
class ServePPOModel:
    def __init__(self, checkpoint_path) -> None:
        self.trainer = ppo.PPOTrainer(
            config={
                "framework": "torch",
                # only 1 "local" worker with an env (not really used here).
                "num_workers": 0,
            },
            env="CartPole-v0")
        self.trainer.restore(checkpoint_path)

    async def __call__(self, request: Request):
        json_input = await request.json()
        obs = json_input["observation"]

        action = self.trainer.compute_action(obs)
        return {"action": int(action)}


# __doc_define_servable_end__


# __doc_train_model_begin__
def train_ppo_model():
    trainer = ppo.PPOTrainer(
        config={
            "framework": "torch",
            "num_workers": 0
        },
        env="CartPole-v0",
    )
    # Train for one iteration
    trainer.train()
    trainer.save("/tmp/rllib_checkpoint")
    return "/tmp/rllib_checkpoint/checkpoint_000001/checkpoint-1"


checkpoint_path = train_ppo_model()
# __doc_train_model_end__

ray.init(num_cpus=8)
# __doc_deploy_begin__
serve.start()
serve.create_backend("ppo", ServePPOModel, checkpoint_path)
serve.create_endpoint("ppo-endpoint", backend="ppo", route="/cartpole-ppo")
# __doc_deploy_end__

# __doc_query_begin__
# That's it! Let's test it
for _ in range(10):
    env = gym.make("CartPole-v0")
    obs = env.reset()

    print(f"-> Sending observation {obs}")
    resp = requests.get(
        "http://localhost:8000/cartpole-ppo",
        json={"observation": obs.tolist()})
    print(f"<- Received response {resp.json()}")
# Output:
# <- Received response {'action': 1}
# -> Sending observation [0.04228249 0.02289503 0.00690076 0.03095441]
# <- Received response {'action': 0}
# -> Sending observation [ 0.04819471 -0.04702759 -0.00477937 -0.00735569]
# <- Received response {'action': 0}
# ...
# __doc_query_end__
