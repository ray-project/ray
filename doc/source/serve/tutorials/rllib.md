---
jupytext:
    text_representation:
        extension: .md
        format_name: myst
kernelspec:
    display_name: Python 3
    language: python
    name: python3
---

(serve-rllib-tutorial)=

# Serving RLlib Models

In this guide, we will train and deploy a simple Ray RLlib model.
In particular, we show:

- How to train and store an RLlib model.
- How to load this model from a checkpoint.
- How to parse the JSON request and evaluate the payload in RLlib.

```{margin}
Check out the [Key Concepts](serve-key-concepts) page to learn more general information about Ray Serve.
```

We will train and checkpoint a simple PPO model with the `CartPole-v0` environment from `gym`.
In this tutorial we simply write to local disk, but in production you might want to consider using a cloud
storage solution like S3 or a shared file system.

Let's get started by defining a `PPO` instance, training it for one iteration and then creating a checkpoint:

```{code-cell} python3
:tags: [remove-output]

import ray
import ray.rllib.algorithms.ppo as ppo
from ray import serve

def train_ppo_model():
    # Configure our PPO algorithm.
    config = (
        ppo.PPOConfig()
        .environment("CartPole-v1")
        .framework("torch")
        .rollouts(num_rollout_workers=0)
    )
    # Create a `PPO` instance from the config.
    algo = config.build()
    # Train for one iteration.
    algo.train()
    # Save state of the trained Algorithm in a checkpoint.
    checkpoint_dir = algo.save("/tmp/rllib_checkpoint")
    return checkpoint_dir


checkpoint_path = train_ppo_model()
```

You create deployments with Ray Serve by using the `@serve.deployment` on a class that implements two methods:

- The `__init__` call creates the deployment instance and loads your data once.
  In the below example we restore our `PPO` Algorithm from the checkpoint we just created.
- The `__call__` method will be invoked every request.
  For each incoming request, this method has access to a `request` object,
  which is a [Starlette Request](https://www.starlette.io/requests/).

We can load the request body as a JSON object and, assuming there is a key called `observation`,
in your deployment you can use `request.json()["observation"]` to retrieve observations (`obs`) and
pass them into the restored `Algorithm` using the `compute_single_action` method.


```{code-cell} python3
:tags: [hide-output]
from starlette.requests import Request


@serve.deployment
class ServePPOModel:
    def __init__(self, checkpoint_path) -> None:
        # Re-create the originally used config.
        config = ppo.PPOConfig()\
            .framework("torch")\
            .rollouts(num_rollout_workers=0)
        # Build the Algorithm instance using the config.
        self.algorithm = config.build(env="CartPole-v0")
        # Restore the algo's state from the checkpoint.
        self.algorithm.restore(checkpoint_path)

    async def __call__(self, request: Request):
        json_input = await request.json()
        obs = json_input["observation"]

        action = self.algorithm.compute_single_action(obs)
        return {"action": int(action)}
```

:::{tip}
Although we used a single input and `Algorithm.compute_single_action(...)` here, you
can process a batch of input using Ray Serve's [batching](serve-performance-batching-requests) feature
and use `Algorithm.compute_actions(...)` to process a batch of inputs.
:::

Now that we've defined our `ServePPOModel` service, let's deploy it to Ray Serve.

```{code-cell} python3
:tags: [hide-output]
ppo_model = ServePPOModel.bind(checkpoint_path)
serve.run(ppo_model)
```

Note that the `checkpoint_path` that we passed to the `bind()` method will be passed to
the `__init__` method of the `ServePPOModel` class that we defined above.

Now that the model is deployed, let's query it!

```{code-cell} python3
# Note: `gymnasium` (not `gym`) will be **the** API supported by RLlib from Ray 2.3 on.
try:
    import gymnasium as gym
    gymnasium = True
except Exception:
    import gym
    gymnasium = False

import requests


env = gym.make("CartPole-v1")

for _ in range(5):
    if gymnasium:
        obs, infos = env.reset()
    else:
        obs = env.reset()

    print(f"-> Sending observation {obs}")
    resp = requests.get(
        "http://localhost:8000/", json={"observation": obs.tolist()}
    )
    print(f"<- Received response {resp.json()}")
```

You should see output like this (`observation` values will differ):

```text
<- Received response {'action': 1}
-> Sending observation [0.04228249 0.02289503 0.00690076 0.03095441]
<- Received response {'action': 0}
-> Sending observation [ 0.04819471 -0.04702759 -0.00477937 -0.00735569]
<- Received response {'action': 0}
```


:::{note}
In this example the client used the `requests` library to send a request to the server.
We defined a `json` object with an `observation` key and a Python list of observations (`obs.tolist()`).
Since `obs = env.reset()` is a `numpy.ndarray`, we used `tolist()` for conversion.
On the server side, we used `obs = json_input["observation"]` to retrieve the observations again, which has `list` type.
In the simple case of an RLlib algorithm with a simple observation space, it's possible to pass this
`obs` list to the `Algorithm.compute_single_action(...)` method.
We could also have created a `numpy` array from it first and then passed it into the `Algorithm`.

In more complex cases with tuple or dict observation spaces, you will have to do some preprocessing of
your `json_input` before passing it to your `Algorithm` instance.
The exact way to process your input depends on how you serialize your observations on the client.
:::

```{code-cell} python3
:tags: [remove-cell]
ray.shutdown()
```
