---
myst:
  html_meta:
    description: "API reference for RLlib's EnvRunner class, covering construction, sampling, cleanup, and the single-agent and multi-agent EnvRunner subclasses."
---

(env-runner-reference-docs)=

# EnvRunner API

## rllib.env.env_runner.EnvRunner

```{eval-rst}
.. currentmodule:: ray.rllib.env.env_runner
```

### Construction and setup

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    EnvRunner
    EnvRunner.make_env
    EnvRunner.make_module
    EnvRunner.get_spaces
    EnvRunner.assert_healthy
```

### Sampling

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    EnvRunner.sample
    EnvRunner.get_metrics
```

### Cleanup

```{eval-rst}
.. autosummary::
    :nosignatures:
    :toctree: doc/

    EnvRunner.stop
```

## rllib.env.env_errors.StepFailedRecreateEnvError

```{eval-rst}
.. currentmodule:: ray.rllib.env.env_errors
```

```{eval-rst}
.. autoclass:: StepFailedRecreateEnvError
```

## Single-agent and multi-agent EnvRunners

By default, RLlib uses two built-in subclasses of EnvRunner, one for {ref}`single-agent <single-agent-env-runner-reference-docs>`, one
for {ref}`multi-agent <multi-agent-env-runner-reference-docs>` setups. It determines based on your config, which one to use.

Check your `config.is_multi_agent` property to find out, which of these setups you have configured
and see {ref}`the docs on setting up RLlib multi-agent <rllib-multi-agent-environments-doc>` for more details.
