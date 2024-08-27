# MBMPO (Model-Based Meta-Policy-Optimization)

[MBMPO](https://arxiv.org/pdf/1809.05214.pdf) is a Dyna-styled model-based RL method that learns based on the predictions of an ensemble of transition-dynamics models. Similar to MAML, MBMPO metalearns an optimal policy by treating each dynamics model as a different task. Similar to the original paper, MBMPO is evaluated on MuJoCo, with the horizon set to 200 instead of the default 1000.

Additional statistics are logged in MBMPO. Each MBMPO iteration corresponds to multiple MAML iterations, and `MAMLIter_i_DynaTrajInner_j_episode_reward_mean` measures the agent’s returns across the dynamics models at iteration i of MAML and step j of inner adaptation. 

## Installation

```
conda create -n rllib-mbmpo python=3.10
conda activate rllib-mbmpo
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[MBMPO Example]()