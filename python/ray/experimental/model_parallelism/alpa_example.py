
from flax.training.train_state import TrainState
import jax
import jax.numpy as jnp
import numpy as np
from jax import random
import optax
from flax import linen as nn

GB = 1024 ** 3

import ray
ray.init(log_to_driver=True)

@ray.remote(num_gpus=4)
class TrainerActor:
    def __init__(self):
        def train_step(state, batch):
            def loss_func(params):
                out = state.apply_fn(params, batch["x"])
                loss = jnp.mean((out - batch["y"])**2)
                return loss

            grads = jax.grad(loss_func)(state.params)
            new_state = state.apply_gradients(grads=grads)
            return new_state

        self.jax_train_step = train_step

    def init_model(self):
        class MLPModel(nn.Module):
            hidden_dim: int
            num_layers: int

            @nn.compact
            def __call__(self, x):
                for i in range(self.num_layers):
                    if i % 2 == 0:
                        x = nn.Dense(features=self.hidden_dim * 4)(x)
                    else:
                        x = nn.Dense(features=self.hidden_dim)(x)
                    x = nn.relu(x)
                return x

        dim = 2048
        batch_size = 2048
        num_layers = 10

        # Generate ground truth W and b
        rngkey = jax.random.PRNGKey(0)
        k1, k2 = random.split(rngkey)
        W = random.normal(k1, (dim, dim))
        b = random.normal(k2, (dim,))

        # Generate the training data
        ksample, knoise = random.split(k1)
        self.x = random.normal(ksample, (batch_size, dim))
        self.y = (self.x @ W + b) + 0.1 * random.normal(knoise, (batch_size, dim))

        # Initialize a train state, which includes the model paramter and optimizer state.
        model = MLPModel(hidden_dim=dim, num_layers=num_layers)
        params = model.init(rngkey, self.x)
        tx = optax.adam(learning_rate=1e-3)

        self.state = TrainState.create(apply_fn=model.apply, params=params, tx=tx)

    # Define the training function and execute one step
    def train_one_step(self):
        return self.jax_train_step(self.state, {"x": self.x, "y": self.y})

    def alpa_train_one_step(self):
        import alpa

        alpa_train_step = alpa.parallelize(self.jax_train_step)

        return alpa_train_step(self.state, {"x": self.x, "y": self.y})

    def assert_allclose(self, a, b):
        from alpa.testing import assert_allclose
        assert_allclose(a, b, atol=5e-3)

    def benchmark_jit_train_step(self):
        from alpa.util import benchmark_func

        # We need this assignment because the original `state` is "donated" and freed.
        state = self.state
        jit_train_step = jax.jit(self.jax_train_step, donate_argnums=(0,))
        def sync_func():
            jax.local_devices()[0].synchronize_all_activity()

        def serial_execution():
            nonlocal state
            state = jit_train_step(state, {"x": self.x, "y": self.y})

        costs = benchmark_func(serial_execution, sync_func, warmup=5, number=10, repeat=5) * 1e3
        print(f"Serial execution time. Mean: {np.mean(costs):.2f} ms, Std: {np.std(costs):.2f} ms")

        executable = jit_train_step.lower(state, {"x": self.x, "y": self.y}).compile().runtime_executable()
        print(f"Serial execution per GPU memory usage: {executable.total_allocation_size() / GB:.2f} GB")

    def benchmark_alpa_train_step(self):
        import alpa
        from alpa.util import benchmark_func

        # We need this assignment because the original `state_copy` is "donated" and freed.
        state = self.state
        alpa_train_step = alpa.parallelize(self.jax_train_step)

        # We distribute arguments in advance for the benchmarking purpose.
        state, batch = alpa_train_step.preshard_dynamic_args(state, {"x": self.x, "y": self.y})

        def sync_func():
            jax.local_devices()[0].synchronize_all_activity()

        def alpa_execution():
            nonlocal state, batch
            state = alpa_train_step(state, batch)

        alpa_costs = benchmark_func(alpa_execution, sync_func, warmup=5, number=10, repeat=5) * 1e3
        print(f"Alpa execution time.   Mean: {np.mean(alpa_costs):.2f} ms, Std: {np.std(alpa_costs):.2f} ms")

        alpa_executable = alpa_train_step.get_executable(state, batch)
        print(f"Alpa execution per GPU memory usage:   {alpa_executable.get_total_allocation_size() / GB:.2f} GB")

trainer_actor = TrainerActor.remote()
ray.get(trainer_actor.init_model.remote())

# ===== uncomment for assert allclose =====
# expected_state = ray.get(trainer_actor.train_one_step.remote())
# actual_state = ray.get(trainer_actor.alpa_train_one_step.remote())

# print(ray.get(trainer_actor.assert_allclose.remote(expected_state.params, actual_state.params)))

# ===== uncomment for benchmarking =====
ray.get(trainer_actor.benchmark_alpa_train_step.remote())

# Avoid buffer donation issue by re-using the same state on same actor
del trainer_actor
new_trainer_actor = TrainerActor.remote()
ray.get(new_trainer_actor.init_model.remote())
ray.get(new_trainer_actor.benchmark_jit_train_step.remote())

# On single g4dn.12xlarge, the result is:

# Alpa execution time.   Mean: 144.74 ms, Std: 0.37 ms
# Alpa execution per GPU memory usage:   0.81 GB
# Serial execution time. Mean: 437.60 ms, Std: 3.19 ms
# Serial execution per GPU memory usage: 2.78 GB
