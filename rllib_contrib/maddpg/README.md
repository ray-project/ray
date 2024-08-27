# MADDPG (Multi-Agent Deep Deterministic Policy Gradient)

[MADDPG](https://arxiv.org/abs/1706.02275) is a DDPG centralized/shared critic algorithm. Code here is adapted from https://github.com/openai/maddpg to integrate with RLlib multi-agent APIs. Please check justinkterry/maddpg-rllib for examples and more information. Note that the implementation here is based on OpenAI’s, and is intended for use with the discrete MPE environments. Please also note that people typically find this method difficult to get to work, even with all applicable optimizations for their environment applied. This method should be viewed as for research purposes, and for reproducing the results of the paper introducing it.


## Installation

```
conda create -n rllib-maddpg python=3.10
conda activate rllib-maddpg
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[MADDPG Example]()