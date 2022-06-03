# import threading

# Import the RL algorithm (Trainer) we would like to use.
import ray
from ray.rllib.agents.ppo import PPOTrainer
# ray.util.register_serializer(threading.RLock, serializer=lambda _:None, deserializer=lambda _:threading.RLock())
# @ray.remote(max_calls=1)
# def task():
#     print("Overloading current worker with spammy tasks to trigger restart ...")
#     time.sleep(2)

# tasks = [task.remote() for _ in range(100)]
# ray.get(tasks)

# ray.init(address="auto", log_to_driver=True)
# Configure the algorithm.
config = {
    # Environment (RLlib understands openAI gym registered strings).
    "env": "CartPole-v0",
    # Use 2 environment workers (aka "rollout workers") that parallelly
    # collect samples from their own environment clone(s).
    "num_workers": 2,
    # Change this to "framework: torch", if you are using PyTorch.
    # Also, use "framework: tf2" for tf2.x eager execution.
    "framework": "torch",
    # Tweak the default model provided automatically by RLlib,
    # given the environment's observation- and action spaces.
    "model": {
        "fcnet_hiddens": [500, 500],
        "fcnet_activation": "relu",
    },
    # # Set up a separate evaluation worker set for the
    # # `trainer.evaluate()` call after training (see below).
    # "evaluation_num_workers": 2,
    # Only for evaluation runs, render the env.
    "evaluation_config": {
        "render_env": True,
    },
    "num_gpus_per_worker": 1,
}

# Create our RLlib Trainer.
trainer = PPOTrainer(config=config)
# first_remote_worker = trainer.workers.remote_workers()[0]
# torch_model = trainer.get_policy().model.base_model
# torch_model.summary()
# Create a test-sample to see its size.
# sample_batch = ray.get(first_remote_worker.sample.remote())
# print(f"Batch size of a single(!) worker (have to multiply roughly by {config.num_workers}): {sample_batch.size_bytes()} bytes")

# Run it for n training iterations. A training iteration includes
# parallel sample collection by the environment workers as well as
# loss calculation on the collected batch and a model update.
for _ in range(3):
    print(trainer.train())

# # Evaluate the trained Trainer (and render each timestep to the shell's
# # output).
# trainer.evaluate()