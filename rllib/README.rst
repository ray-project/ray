RLlib: Industry-Grade Reinforcement Learning with TF and Torch
==============================================================

RLlib is an open-source library for reinforcement learning that offers both high scalability and a unified API for a variety of applications.

For an overview of RLlib, see the [documentation](http://docs.ray.io/en/master/rllib.html).



Quick 30sec Setup
-----------------

```
$ conda create -n rllib python=3.8
$ conda activate rllib
$ pip install "ray[rllib]"
$ pip install tensorflow
$ rllib train --run APPO --env CartPole-v0
```


Citation
--------

If you've found RLlib useful for your research, please cite our [paper](https://arxiv.org/abs/1712.09381) as follows:

```
@inproceedings{liang2018rllib,
    Author = {Eric Liang and
              Richard Liaw and
              Robert Nishihara and
              Philipp Moritz and
              Roy Fox and
              Ken Goldberg and
              Joseph E. Gonzalez and
              Michael I. Jordan and
              Ion Stoica},
    Title = {{RLlib}: Abstractions for Distributed Reinforcement Learning},
    Booktitle = {International Conference on Machine Learning ({ICML})},
    Year = {2018}
}
```

