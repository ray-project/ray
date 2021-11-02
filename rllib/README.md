RLlib: Scalable Reinforcement Learning
======================================

RLlib is an open-source library for reinforcement learning that offers both high scalability and a unified API for a variety of applications.

For an overview of RLlib, see the [documentation](http://docs.ray.io/en/master/rllib.html).

If you've found RLlib useful for your research, you can cite the [paper](https://arxiv.org/abs/1712.09381) as follows:

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

Development Install
-------------------

You can develop RLlib locally without needing to compile Ray by using the [setup-dev.py](https://github.com/ray-project/ray/blob/master/python/ray/setup-dev.py) script. This sets up links between the ``rllib`` dir in your git repo and the one bundled with the ``ray`` package. When using this script, make sure that your git branch is in sync with the installed Ray binaries (i.e., you are up-to-date on [master](https://github.com/ray-project/ray) and have the latest [wheel](https://docs.ray.io/en/master/installation.html) installed.)
