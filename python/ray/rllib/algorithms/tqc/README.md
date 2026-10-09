# TQC (Truncated Quantile Critics)

## Overview

TQC is an extension of SAC (Soft Actor-Critic) that uses distributional reinforcement learning with quantile regression to control overestimation bias in the Q-function.

**Paper**: [Controlling Overestimation Bias with Truncated Mixture of Continuous Distributional Quantile Critics](https://arxiv.org/abs/2005.04269)

## Key Features

- **Distributional Critics**: Each critic network outputs multiple quantiles instead of a single Q-value
- **Multiple Critics**: Uses `n_critics` independent critic networks (default: 2)
- **Truncated Targets**: Drops the top quantiles when computing target Q-values to reduce overestimation
- **Quantile Huber Loss**: Uses quantile regression with Huber loss for critic training

## Usage

```python
from ray.rllib.algorithms.tqc import TQCConfig

config = (
    TQCConfig()
    .environment("Pendulum-v1")
    .training(
        n_quantiles=25,        # Number of quantiles per critic
        n_critics=2,           # Number of critic networks
        top_quantiles_to_drop_per_net=2,  # Quantiles to drop for bias control
    )
)

algo = config.build()
for _ in range(100):
    result = algo.train()
    print(f"Episode reward mean: {result['env_runners']['episode_reward_mean']}")
```

## Configuration

### TQC-Specific Parameters

| Parameter                       | Default | Description                                                        |
| ------------------------------- | ------- | ------------------------------------------------------------------ |
| `n_quantiles`                   | 25      | Number of quantiles for each critic network                        |
| `n_critics`                     | 2       | Number of critic networks                                          |
| `top_quantiles_to_drop_per_net` | 2       | Number of top quantiles to drop per network when computing targets |

### Inherited from SAC

TQC inherits all SAC parameters including:

- `actor_lr`, `critic_lr`, `alpha_lr`: Learning rates
- `tau`: Target network update coefficient
- `initial_alpha`: Initial entropy coefficient
- `target_entropy`: Target entropy for automatic alpha tuning

## Algorithm Details

### Critic Update

1. Each critic outputs `n_quantiles` quantile estimates
2. For target computation:
   - Collect all quantiles from all critics: `n_critics * n_quantiles` values
   - Sort all quantiles
   - Drop the top `top_quantiles_to_drop_per_net * n_critics` quantiles
   - Use remaining quantiles as targets
3. Train critics using quantile Huber loss

### Actor Update

- Maximize expected Q-value (mean of all quantiles) minus entropy bonus
- Same as SAC but using mean of quantile estimates

### Entropy Tuning

- Same as SAC: automatically adjusts temperature parameter α

## Differences from SAC

| Aspect            | SAC            | TQC                           |
| ----------------- | -------------- | ----------------------------- |
| Critic Output     | Single Q-value | `n_quantiles` quantile values |
| Number of Critics | 2 (twin_q)     | `n_critics` (configurable)    |
| Loss Function     | Huber/MSE      | Quantile Huber Loss           |
| Target Q          | min(Q1, Q2)    | Truncated sorted quantiles    |

## References

```bibtex
@article{kuznetsov2020controlling,
  title={Controlling Overestimation Bias with Truncated Mixture of Continuous Distributional Quantile Critics},
  author={Kuznetsov, Arsenii and Shvechikov, Pavel and Grishin, Alexander and Vetrov, Dmitry},
  journal={arXiv preprint arXiv:2005.04269},
  year={2020}
}
```
