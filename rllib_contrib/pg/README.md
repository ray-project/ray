# PG (Vanilla Policy Gradient)

[PG](https://papers.nips.cc/paper/1713-policy-gradient-methods-for-reinforcement-learning-with-function-approximation.pdf) is the most basic reinforcement learning algorithm that learns a policy by taking a gradient of action log probabilities and
weighting them by the return. This algorithm is also known as REINFORCE.


## Installation

```
conda create -n rllib-pg python=3.10
conda activate rllib-pg
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[PG Example]()