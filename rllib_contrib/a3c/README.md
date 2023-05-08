# A3C (Asynchronous Advantage Actor-Critic)

[A3C](https://arxiv.org/abs/1602.01783) is the asynchronous version of A2C, where gradients are computed on the workers directly after trajectory rollouts, and only then shipped to a central learner to accumulate these gradients on the central model. After the central model update, parameters are broadcast back to all workers. Similar to A2C, A3C scales to 16-32+ worker processes depending on the environment.


## Installation

```
conda create -n rllib-a3c python=3.10
conda activate rllib-a3c
python -m pip install ray==2.3.1
python -m pip install -e '.[development]'
pre-commit install
```

## Usage

```python
import ray
from ray import air
from ray import tune
from rllib_a3c.a3c import A3C, A3CConfig


if __name__ == "__main__":
    ray.init()

    config = (
        A3CConfig()
        .rollouts(num_rollout_workers=1)
        .framework("torch")
        .environment("CartPole-v1")
        .training(
            gamma=0.95,
        )
    )

    num_iterations = 100

    tuner = tune.Tuner(
        A3C,
        param_space=config.to_dict(),
        run_config=air.RunConfig(
            stop={"episode_reward_mean": 150, "timesteps_total": 200000},
            failure_config=air.FailureConfig(fail_fast="raise"),
        ),
    )
    results = tuner.fit()

```
