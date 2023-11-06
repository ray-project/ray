# ES (Evolution Strategies)

[ES](https://arxiv.org/abs/1703.03864) is a class of black box optimization algorithms, as an alternative to popular MDP-based RL techniques such as Q-learning and Policy Gradients. It is invariant to action frequency and delayed rewards, tolerant of extremely long horizons, and does not need temporal discounting or value function approximation.


## Installation

```
conda create -n rllib-es python=3.10
conda activate rllib-es
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[ES Example]()