---
myst:
  html_meta:
    description: "API reference for RLlib's Distribution base class and its sampling, log-probability, and KL-divergence methods."
---

(rllib-distributions-reference-docs)=

# Distribution API

```{eval-rst}
.. currentmodule:: ray.rllib.models.distributions
```

## Base Distribution class

```{eval-rst}
.. autosummary::
   :nosignatures:
   :toctree: doc/

    ~Distribution
    ~Distribution.from_logits
    ~Distribution.sample
    ~Distribution.rsample
    ~Distribution.logp
    ~Distribution.kl
```
