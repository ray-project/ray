# DDPPO (Decentralized Distributed Proximal Policy Optimization)

[DDPPO](https://arxiv.org/abs/1911.00357) is a method for distributed reinforcement learning in resource-intensive simulated environments based on PPO. DD-PPO is distributed (uses multiple machines), decentralized (lacks a centralized server), and synchronous (no computation is ever stale), making it conceptually simple and easy to implement.


## Installation

```
conda create -n rllib-ddppo python=3.10
conda activate rllib-ddppo
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[DDPPO Example]()