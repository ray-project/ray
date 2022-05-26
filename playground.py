from ray.rllib.algorithms.crr import CRR, CRRConfig
from ray.rllib.utils.replay_buffers import MultiAgentReplayBuffer
import ray

# data_file = "rllib/tests/data/pendulum/small.json"
data_file = "rllib/tests/data/cartpole/small.json"

config = CRRConfig()
config.framework("torch")
# config.environment(env="Pendulum-v1", clip_actions=True)
config.environment(env="CartPole-v0", clip_actions=True)
config.offline_data(
    input_=[data_file],
    actions_in_input_normalized=True,
)
config.training(
    twin_q=True,
    train_batch_size=512,
    replay_buffer_config={"type": MultiAgentReplayBuffer, "learning_starts": 0},
)
config.evaluation(
    evaluation_interval=1,
    evaluation_num_workers=2,
    evaluation_duration=10,
    evaluation_duration_unit="episodes",
    evaluation_parallel_to_training=True,
    evaluation_config={"input": "sampler", "explore": False},
)
config.rollouts(num_rollout_workers=2)


ray.init(local_mode=True)
algo = CRR(config=config)
NITER = 100
for i in range(100):
    print('-'*30 + f'// {i}')
    result = algo.train()
    print(result)
