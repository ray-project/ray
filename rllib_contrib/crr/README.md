# CRR (Critic Regularized Regression)


[CRR](https://arxiv.org/abs/2006.15134)  is another offline RL algorithm based on Q-learning that can learn from an offline experience replay. The challenge in applying existing Q-learning algorithms to offline RL lies in the overestimation of the Q-function, as well as, the lack of exploration beyond the observed data. The latter becomes increasingly important during bootstrapping in the bellman equation, where the Q-function queried for the next state’s Q-value(s) does not have support in the observed data. To mitigate these issues, CRR implements a simple and yet powerful idea of “value-filtered regression”. The key idea is to use a learned critic to filter-out the non-promising transitions from the replay dataset.


## Installation

```
conda create -n rllib-crr python=3.10
conda activate rllib-crr
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[CRR Example]()