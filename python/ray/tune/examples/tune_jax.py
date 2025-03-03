import os
# Prevent preeallocating entire GPU memory, locking us out of the device
os.environ["XLA_PYTHON_CLIENT_PREALLOCATE"] = "false"
import tempfile
import copy
import json

import jax
import jax.numpy as jnp
import jax.random as jr
import equinox as eqx
import diffrax
import optax
from orbax.checkpoint import PyTreeCheckpointer

import matplotlib.pyplot as plt

from ray import tune
from ray.tune import Checkpoint
from ray.tune.schedulers import ASHAScheduler

class Func(eqx.Module):
    mlp: eqx.nn.MLP
    scale: float

    def __init__(self, ode_size, width_size, depth, *, key, scale=1.0, **kwargs):
        super().__init__(**kwargs)
        self.scale = scale
        self.mlp = eqx.nn.MLP(
            in_size=ode_size,
            out_size=ode_size,
            width_size=width_size,
            depth=depth,
            activation=jax.nn.tanh,
            key=key,
        )

    def __call__(self, t, y, args):
        return self.scale * self.mlp(y)


class NeuralODE(eqx.Module):
    func: eqx.Module

    def __init__(self, ode_size, width_size, depth, *, key, scale=1.0):
        self.func = Func(ode_size, width_size, depth, key=key, scale=scale)

    def __call__(self, ts, ys):
        y0 = ys[0, :]
        solution = diffrax.diffeqsolve(
            diffrax.ODETerm(self.func),
            diffrax.Tsit5(),
            t0=ts[0],
            t1=ts[-1],
            dt0=ts[1] - ts[0],
            y0=y0,
            stepsize_controller=diffrax.PIDController(rtol=1e-3, atol=1e-5),
            saveat=diffrax.SaveAt(ts=ts),
            max_steps=8192,
        )
        return solution.ys

def _get_data(ts, *, key):
    y0 = jr.uniform(key, (2,), minval=-0.6, maxval=1)

    def f(t, y, args):
        x = y / (1 + y)
        return jnp.stack([x[1], -x[0]], axis=-1)

    solver = diffrax.Tsit5()
    dt0 = 0.1
    saveat = diffrax.SaveAt(ts=ts)
    sol = diffrax.diffeqsolve(
        diffrax.ODETerm(f), solver, ts[0], ts[-1], dt0, y0, saveat=saveat
    )
    ys = sol.ys
    return ys


def get_data(dataset_size, *, key):
    ts = jnp.linspace(0, 10, 100)
    key = jr.split(key, dataset_size)
    ys = jax.vmap(lambda key: _get_data(ts, key=key))(key)
    return ts, ys

def dataloader(arrays, batch_size, *, key):
    dataset_size = arrays[0].shape[0]
    indices = jnp.arange(dataset_size)
    while True:
        perm = jr.permutation(key, indices)
        (key,) = jr.split(key, 1)
        start = 0
        end = batch_size
        while start < dataset_size:
            batch_perm = perm[start:end]
            yield tuple(array[batch_perm] for array in arrays)
            start = end
            end = start + batch_size

steps = 1000
checkpoint_every = 500
seed = 5678
dataset_size = 256
key = jr.PRNGKey(seed)
data_key, model_key, loader_key = jr.split(key, 3)

ts, ys = get_data(dataset_size, key=data_key)

train_size = 0.6
train_ts, test_ts = ts[:int(jnp.ceil(train_size * len(ts)))], ts[int(jnp.ceil(train_size * len(ts))):]
train_ys, test_ys = ys[:, :int(jnp.ceil(train_size * len(ts)))], ys[:, int(jnp.ceil(train_size * len(ts))):]
_, length_size, data_size = train_ys.shape

config = {
    "batch_size": tune.choice([32, 128, 512]),
    "lr": tune.loguniform(1e-7, 1e-2),
    "width_size": tune.choice([32, 128, 512]),
    "depth": tune.choice([1, 3, 5]),
}

def train_fn(config):
    batch_size = config["batch_size"]
    width_size = config["width_size"]
    depth = config["depth"]
    lr = config["lr"]

    model = NeuralODE(data_size, width_size, depth, key=model_key)

    @eqx.filter_value_and_grad
    def grad_loss(model, ti, yi):
        y_pred = jax.vmap(model, in_axes=(None, 0))(ti, yi)
        return jnp.mean((yi - y_pred) ** 2)

    @eqx.filter_jit
    def make_step(ti, yi, model, opt_state):
        loss, grads = grad_loss(model, ti, yi)
        updates, opt_state = optim.update(grads, opt_state)
        model = eqx.apply_updates(model, updates)
        return loss, model, opt_state

    optim = optax.adabelief(lr)

    checkpoint = tune.get_checkpoint()
    if checkpoint:
        with checkpoint.as_directory() as checkpoint_dir:
            opt_state_loc = os.path.join(checkpoint_dir, "opt_state.eqx")
            model_loc = os.path.join(checkpoint_dir, "model.eqx")

            base_model = NeuralODE(
                ode_size=data_size,
                width_size=width_size,
                depth=depth,
                key=model_key
            )

            model = eqx.tree_deserialise_leaves(model_loc, copy.deepcopy(base_model))
            opt_state = optim.init(eqx.filter(model, eqx.is_array))
            opt_state = eqx.tree_deserialise_leaves(opt_state_loc, opt_state)

    else:
        opt_state = optim.init(eqx.filter(model, eqx.is_array))

    for step, (yi,) in zip(
        range(steps), dataloader((train_ys,), batch_size, key=loader_key)
    ):
        loss, model, opt_state = make_step(train_ts, yi, model, opt_state)

        test_model_ys = jax.vmap(model, in_axes=(None, 0))(test_ts, test_ys)
        test_loss = jnp.mean((test_model_ys - test_ys)**2)
        checkpoint_data = {
            "train_loss": float(loss),
            "test_loss": float(test_loss),
            "step": int(step),
        }
        if (step % checkpoint_every) == 0:
            with tempfile.TemporaryDirectory() as temp_dir:
                ckpt_loc = os.path.join(temp_dir, "checkpoint")
                PyTreeCheckpointer().save(ckpt_loc, checkpoint_data)
                opt_state_loc = os.path.join(ckpt_loc, "opt_state.eqx")
                model_loc = os.path.join(ckpt_loc, "model.eqx")

                print(f"Creating checkpoint at {ckpt_loc}")

                eqx.tree_serialise_leaves(model_loc, model)
                eqx.tree_serialise_leaves(opt_state_loc, opt_state)

                tune.report(
                    checkpoint_data,
                    checkpoint=Checkpoint.from_directory(ckpt_loc)
                )
        else:
            tune.report(
                checkpoint_data
            )

    return ts, ys, model


def main(gpus_per_trial=0.2, SMOKE_TEST=True):
    num_samples = 5 if SMOKE_TEST else 30
    scheduler = ASHAScheduler(max_t=steps, grace_period=200, reduction_factor=2)
    tuner = tune.Tuner(
        tune.with_resources(
            tune.with_parameters(train_fn),
            resources={"cpu": 1, "gpu": gpus_per_trial}
        ),
        run_config = tune.RunConfig(
            name="node_asha",
            stop={"training_iteration": steps},
        ),
        tune_config=tune.TuneConfig(
            scheduler=scheduler,
            metric="test_loss",
            mode="min",
            num_samples=num_samples,
            reuse_actors=True,
        ),
        param_space=config,
    )
    results = tuner.fit()
    return results


results = main()


best_result = results.get_best_result().get_best_checkpoint(metric="test_loss", mode="min").path
base_best_result = best_result.split("/")[:-1]

base = ""
for path_elem in base_best_result:
    base += path_elem + "/"

config_loc = base + "params.json"
with open(config_loc, "rb") as f:
    config = json.load(f)

model = NeuralODE(
    ode_size=data_size,
    depth=config["depth"],
    width_size=config["width_size"],
    key=jax.random.PRNGKey(42)
)

model = eqx.tree_deserialise_leaves(os.path.join(best_result, "model.eqx"), model)

plt.plot(ts, ys[0, :, 0], c="dodgerblue", label="Real")
plt.plot(ts, ys[0, :, 1], c="dodgerblue")
model_y = model(ts, ys[0, :, :])
plt.plot(ts, model_y[:, 0], c="crimson", label="Model")
plt.plot(ts, model_y[:, 1], c="crimson")
plt.vlines(x=[train_ts[-1]], ymin=-0.6, ymax=1, linestyles="--", color="k")
plt.legend()
plt.tight_layout()
plt.show()
