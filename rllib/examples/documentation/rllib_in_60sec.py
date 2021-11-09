from ray.rllib.agents.ppo import PPOTrainer

config = {
    "env": "MsPacman-v0",
    "lr": 0.0005,
    "num_workers": 2,
    "framework": "tf",
    "create_env_on_driver": True,
}

trainer = PPOTrainer(confg=config)

for _ in range(10):
    trainer.train()

trainer.evaluate()
