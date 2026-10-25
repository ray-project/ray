import ray


@ray.remote(num_cpus=1)
class Trainer:
    def __init__(self, hyperparameter, data):
        self.hyperparameter = hyperparameter
        self.data = data

    # Train the model on the given training data shard.
    def fit(self):
        return self.data * self.hyperparameter


@ray.remote(num_cpus=1)
class Supervisor:
    def __init__(self, hyperparameter, data):
        self.trainers = [Trainer.remote(hyperparameter, d) for d in data]

    def fit(self):
        # Train with different data shard in parallel.
        return ray.get([trainer.fit.remote() for trainer in self.trainers])


data = [1, 2, 3]
supervisor1 = Supervisor.remote(1, data)
supervisor2 = Supervisor.remote(2, data)
# Train with different hyperparameters in parallel.
model1 = supervisor1.fit.remote()
model2 = supervisor2.fit.remote()
assert ray.get(model1) == [1, 2, 3]
assert ray.get(model2) == [2, 4, 6]
