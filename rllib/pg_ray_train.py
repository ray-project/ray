import ray
from ray import tune
import rayportal

# settings used for both stable baselines and rllib
env_name = "CartPole-v1"
train_steps = 1000000
learning_rate = 1e-3
save_dir = "saved_models"

# ray.init(local_mode=True)
ray.init()
# training and saving
with rayportal.auto_attach():
    analysis = tune.run(
        "PG",
        stop={"timesteps_total": train_steps},
        config={
            "env": env_name,
            "lr": learning_rate,
            "framework": "torch",
            "num_training_workers": 2,
        },
        # verbose=0,
    )
