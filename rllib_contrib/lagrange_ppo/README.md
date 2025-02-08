# Lagrange Proximal Policy Optimization (L-PPO) for Safe Reinforcement Learning

## Overview

Safe Reinforcement Learning considers a sequential decision-making problem with a 
safety constraint for an episode. The standard problem 
formulation constrains an average of the discounted sum of instanteneous costs
(as opposed to instantaneous rewards in the maximization objective).

The basic baseline in Safe RL is an extension of [PPO](https://arxiv.org/abs/1707.06347)
to the safety setting. The extension is achieved using the Lagrangian approach to 
constrained optimization which balances reward maximization and constraint satisfaction
by optimizing a Lagrangian multiplier. This multiplier trades off the two objectives and 
 makes sure that the balance between the two is optimal. 
 
 Our implementation generally follows 
the Open AI library [Safety Starter Agents](https://github.com/openai/safety-starter-agents)
and described in [Benchmarking Safe Reinforcemnet Learning](https://cdn.openai.com/safexp-short.pdf). We also implemented the PID Lagrangian approach for scheduling the Lagrangian multiplier 
from [Responsive Safety in Reinforcement Learning by PID Lagrangian Methods](https://arxiv.org/abs/2007.03964)
implemnted [here](https://github.com/astooke/rlpyt/tree/master/rlpyt/projects/safe). Note that our implementation differs slightly from both algorithms, therefore, the behavior of the algorithms may differ.

## Installation

```
conda create -n rllib-l-ppo python=3.9
conda activate rllib-l-ppo
pip install -r requirements.txt
pip install -e '.[development]'
```
