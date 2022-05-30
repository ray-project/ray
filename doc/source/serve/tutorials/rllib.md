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
Check out the [Key Concepts](key-concepts) page to learn more general information about Ray Serve.
```

We will train and checkpoint a simple PPO model with the `CartPole-v0` environment from `gym`.
In this tutorial we simply write to local disk, but in production you might want to consider using a cloud
storage solution like S3 or a shared file system.

Let's get started by defining a `PPOTrainer` instance, training it for one iteration and then creating a checkpoint:

```{code-cell} python3
:tags: [remove-output]

import ray
import ray.rllib.agents.ppo as ppo
from ray import serve

def train_ppo_model():
    trainer = ppo.PPOTrainer(
        config={"framework": "torch", "num_workers": 0},
        env="CartPole-v0",
    )
    # Train for one iteration
    trainer.train()
    trainer.save("/tmp/rllib_checkpoint")
    return "/tmp/rllib_checkpoint/checkpoint_000001/checkpoint-1"


checkpoint_path = train_ppo_model()
```

You create deployments with Ray Serve by using the `@serve.deployment` on a class that implements two methods:

- The `__init__` call creates the deployment instance and loads your data once.
  In the below example we restore our `PPOTrainer` from the checkpoint we just created.
- The `__call__` method will be invoked every request.
  For each incoming request, this method has access to a `request` object,
  which is a [Starlette Request](https://www.starlette.io/requests/).

We can load the request body as a JSON object and, assuming there is a key called `observation`,
in your deployment you can use `request.json()["observation"]` to retrieve observations (`obs`) and
pass them into the restored `trainer` using the `compute_single_action` method.


```{code-cell} python3
:tags: [hide-output]
from starlette.requests import Request


@serve.deployment(route_prefix="/cartpole-ppo")
class ServePPOModel:
    def __init__(self, checkpoint_path) -> None:
        self.trainer = ppo.PPOTrainer(
            config={
                "framework": "torch",
                "num_workers": 0,
            },
            env="CartPole-v0",
        )
        self.trainer.restore(checkpoint_path)

    async def __call__(self, request: Request):
        json_input = await request.json()
        obs = json_input["observation"]

        action = self.trainer.compute_single_action(obs)
        return {"action": int(action)}
```

:::{tip}
Although we used a single input and `trainer.compute_single_action(...)` here, you
can process a batch of input using Ray Serve's [batching](serve-batching) feature
and use `trainer.compute_actions(...)` to process a batch of inputs.
:::

Now that we've defined our `ServePPOModel` service, let's deploy it to Ray Serve.
The deployment will be exposed through the `/cartpole-ppo` route.

```{code-cell} python3
:tags: [hide-output]
serve.start()
ServePPOModel.deploy(checkpoint_path)
```

Note that the `checkpoint_path` that we passed to the `deploy()` method will be passed to
the `__init__` method of the `ServePPOModel` class that we defined above.

Now that the model is deployed, let's query it!

```{code-cell} python3
import gym
import requests


for _ in range(5):
    env = gym.make("CartPole-v0")
    obs = env.reset()

    print(f"-> Sending observation {obs}")
    resp = requests.get(
        "http://localhost:8000/cartpole-ppo", json={"observation": obs.tolist()}
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
In the simple case of an RLlib trainer with a simple observation space, it's possible to pass this
`obs` list to the `trainer.compute_single_action(...)` method.
We could also have created a `numpy` array from it first and then passed it into the `trainer`.

In more complex cases with tuple or dict observation spaces, you will have to do some preprocessing of
your `json_input` before passing it to your `trainer` instance.
The exact way to process your input depends on how you serialize your observations on the client.
:::

```{code-cell} python3
:tags: [remove-cell]
ray.shutdown()
```