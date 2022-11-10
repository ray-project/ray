import ray


@ray.remote(num_cpus=1)
class Trainer:
    # Train the model on the given training data.
    def fit(self, data):
        return data * 2


@ray.remote(num_cpus=1)
class Supervisor:
    def __init__(self):
        self.data = [1, 2, 3]
        self.trainers = [Trainer.remote() for _ in range(len(self.data))]

    def fit(self):
        # Train the same model with different training data in parallel.
        return ray.get([t.fit.remote(d) for d, t in zip(self.data, self.trainers)])


supervisor = Supervisor.remote()
assert ray.get(supervisor.fit.remote()) == [2, 4, 6]
