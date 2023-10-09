# Contextual Bandits

The Multi-armed bandit (MAB) problem provides a simplified RL setting that
involves learning to act under one situation only, i.e. the context (observation/state) and arms (actions/items-to-select) are both fixed.
Contextual bandit is an extension of the MAB problem, where at each
round the agent has access not only to a set of bandit arms/actions but also
to a context (state) associated with this iteration. The context changes
with each iteration, but, is not affected by the action that the agent takes.
The objective of the agent is to maximize the cumulative rewards, by
collecting  enough information about how the context and the rewards of the
arms are related to each other. The agent does this by balancing the
trade-off between exploration and exploitation.

Contextual bandit algorithms typically consist of an action-value model (Q
model) and an exploration strategy (epsilon-greedy, LinUCB, Thompson Sampling etc.)

RLlib supports the following online contextual bandit algorithms,
named after the exploration strategies that they employ:


## Linear Upper Confidence Bound (BanditLinUCB)
[paper](http://rob.schapire.net/papers/www10.pdf)

LinUCB assumes a linear dependency between the expected reward of an action and
its context. It estimates the Q value of each action using ridge regression.
It constructs a confidence region around the weights of the linear
regression model and uses this confidence ellipsoid to estimate the
uncertainty of action values.


## Linear Thompson Sampling (BanditLinTS)
[paper](http://proceedings.mlr.press/v28/agrawal13.pdf)

Like LinUCB, LinTS also assumes a linear dependency between the expected
reward of an action and its context and uses online ridge regression to
estimate the Q values of actions given the context. It assumes a Gaussian
prior on the weights and a Gaussian likelihood function. For deciding which
action to take, the agent samples weights for each arm, using
the posterior distributions, and plays the arm that produces the highest reward.

## Installation

```
conda create -n rllib-bandit python=3.10
conda activate rllib-bandit
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage
