from ray.air.config import RunConfig, ScalingConfig
from ray.train.rl import RLTrainer

trainer = RLTrainer(
    run_config=RunConfig(stop={"training_iteration": 5}),
    scaling_config=ScalingConfig(num_workers=2, use_gpu=False),
    algorithm="PPO",
    config={
        "env": "CartPole-v0",
        "framework": "tf",
        "evaluation_num_workers": 1,
        "evaluation_interval": 1,
        "evaluation_config": {"input": "sampler"},
    },
)
result = trainer.fit()
