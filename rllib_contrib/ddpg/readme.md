# DDPG (Deep Deterministic Policy Gradient)

[DDPG](https://arxiv.org/abs/1509.02971) is an actor-critic, model-free algorithm based on the deterministic policy gradient that can operate over continuous action spaces.


## Installation

```
conda create -n rllib-ddpg python=3.10
conda activate rllib-ddpg
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[DDPG Example](examples/ddpg_pendulum_v1.py)