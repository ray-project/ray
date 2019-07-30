RLlib: Scalable Reinforcement Learning
======================================

RLlib is an open-source library for reinforcement learning that offers both high scalability and a unified API for a variety of applications.

For an overview of RLlib, see the [documentation](http://ray.readthedocs.io/en/latest/rllib.html).

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

To get started with RLlib development, run `python setup.py develop` in the ray/rllib directory. This installs the development package of RLlib, which overrides the built-in RLlib package from Ray
("ray.rllib_builtin"), allowing you to easily develop RLlib without needing to recompile Ray.

How this works is that the `ray.rllib` package resolves to `rllib` with higher priority, falling back to `ray.rllib_builtin` if that is not found.
