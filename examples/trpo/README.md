# Parallelizing TRPO

In this example, we show how TRPO can be parallelized using Ray. We will be
working with John Schulman's
[modular_rl code](https://github.com/joschu/modular_rl).

For this tutorial I'll assume that you have Anaconda with Python 2.7 installed.

## Setting up the single core implementation of TRPO

First, we will run the original TRPO code.

Install these dependencies:

- [Gym](https://gym.openai.com/)
- The following Python packages:

    ```
    pip install theano
    pip install keras
    pip install tabulate
    ```

Run this code to start an experiment.
```
git clone https://github.com/joschu/modular_rl
cd modular_rl
export KERAS_BACKEND=theano && ./run_pg.py --env Pong-ram-v0 --agent modular_rl.agentzoo.TrpoAgent --video 0 --n_iter 500 --filter 1
```

**Note: On some versions of Mac OS X, this produces NaNs.**

On a m4.4xlarge EC2 instance, the first 10 iterations take 106s.


Each iteration consists of two phases. In the first phase, the rollouts are
computed (on one core). In the second phase, the objective is optimized, which
makes use of the parallel BLAS library. The code for all of this is in
`modular_rl/modular_rl/core.py`.

```python
for _ in xrange(cfg["n_iter"]):
  # Rollouts ========
  paths = get_paths(env, agent, cfg, seed_iter)
  compute_advantage(agent.baseline, paths, gamma=cfg["gamma"], lam=cfg["lam"])
  # VF Update ========
  vf_stats = agent.baseline.fit(paths)
  # Pol Update ========
  pol_stats = agent.updater(paths)
```

We will now see how this code can be parallelized.

## Parallelizing TRPO rollouts using Ray

As a first step, we will parallelize the rollouts. This is done by implementing
a function `do_rollouts_remote` similar to
[do_rollouts_serial](https://github.com/joschu/modular_rl/blob/46a6f9a0d363a7bc1c7325ff17e2eb684612a388/modular_rl/core.py#L137),
which will be called by
[get_paths](https://github.com/joschu/modular_rl/blob/46a6f9a0d363a7bc1c7325ff17e2eb684612a388/modular_rl/core.py#L102)
(called in the above code snippet).

Check out the parallel version of the TRPO code.

```
git clone https://github.com/pcmoritz/modular_rl modular_rl_ray
cd modular_rl_ray
git checkout remote
```

You can run the code using
```
export KERAS_BACKEND=theano && ./run_pg.py --env Pong-ram-v0 --agent modular_rl.agentzoo.TrpoAgent --video 0 --n_iter 500 --filter 1 --remote 1 --n_rollouts 8
```

There are few [changes](https://github.com/joschu/modular_rl/compare/master...pcmoritz:23d3ebc).
As in the [learning to play Pong example](https://github.com/ray-project/ray/tree/master/examples/rl_pong),
we use reusable variables to store the gym environment and the neural network policy. These are
then used in the remote `do_rollout` function to do a remote rollout:

```python
@ray.remote
def do_rollout(policy, timestep_limit, seed):
  # Retrieve the game environment.
  env = ray.reusables.env
  # Set the environment seed.
  env.seed(seed)
  # Set the numpy seed.
  np.random.seed(seed)
  # Retrieve the neural network agent.
  agent = ray.reusables.agent
  # Set the network weights.
  agent.set_from_flat(policy)
  return rollout(env, agent, timestep_limit)
```

All that is left is to invoke the remote function and collect the paths.

```python
def do_rollouts_remote(agent, timestep_limit, n_timesteps, n_parallel, seed_iter):
  # Put the neural network weights into the object store.
  policy = ray.put(agent.get_flat())
  paths = []
  timesteps_sofar = 0
  # Run parallel rollouts until we have enough.
  while timesteps_sofar < n_timesteps:
    # Launch rollout tasks in parallel.
    rollout_ids = [do_rollout.remote(policy, timestep_limit, seed_iter.next()) for i in range(n_parallel)]
    for rollout_id in rollout_ids:
      # Retrieve the task output from the object store.
      path = ray.get(rollout_id)
      paths.append(path)
      timesteps_sofar += pathlength(path)
  return paths
```

On the same m4.4xlarge EC2 instance, the first 10 iterations now take 42s instead of
106s.
