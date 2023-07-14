# A2C (Advantage Actor-Critic)

[A2C](https://arxiv.org/abs/1602.01783) is a synchronous, deterministic version of A3C; that’s why it is named as “A2C” with the first “A” (“asynchronous”) removed. In A3C each agent talks to the global parameters independently, so it is possible sometimes the thread-specific agents would be playing with policies of different versions and therefore the aggregated update would not be optimal. To resolve the inconsistency, a coordinator in A2C waits for all the parallel actors to finish their work before updating the global parameters and then in the next iteration parallel actors starts from the same policy. The synchronized gradient update keeps the training more cohesive and potentially to make convergence faster.


## Installation

```
conda create -n rllib-a2c python=3.10
conda activate rllib-a2c
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[A3C Example](examples/a2c_cartpole_v1.py)