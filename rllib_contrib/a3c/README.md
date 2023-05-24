# A3C (Asynchronous Advantage Actor-Critic)

[A3C](https://arxiv.org/abs/1602.01783) is the asynchronous version of A2C, where gradients are computed on the workers directly after trajectory rollouts, and only then shipped to a central learner to accumulate these gradients on the central model. After the central model update, parameters are broadcast back to all workers. Similar to A2C, A3C scales to 16-32+ worker processes depending on the environment.


## Installation

```
conda create -n rllib-a3c python=3.10
conda activate rllib-a3c
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[A3C Example](examples/a3c_cartpole_v1.py)