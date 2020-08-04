import ray
import numpy as np
import time
import json
import os
from ray.tune import run, Trainable
from ray.tune.suggest.hyperopt import HyperOptSearch
from hyperopt import hp


class MyTrainableClass(Trainable):
    def _setup(self, config):
        self.timestep = 0
    def _train(self):
        self.timestep += 1
        v = np.tanh(float(self.timestep) / self.config.get("width", 1))
        v *= self.config.get("height", 1)
        return {"mean_loss": v}
    def _save(self, checkpoint_dir):
        path = os.path.join(checkpoint_dir, "checkpoint")
        with open(path, "w") as f:
            f.write(self.timestep)
        return path
    def _restore(self, checkpoint_path):
        with open(checkpoint_path) as f:
            self.timestep = json.loads(f.read())["timestep"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PyTorch Example (FOR TEST ONLY)")
    parser.add_argument(
        "--resume", action="store_true", help="Finish quickly for testing")
    parser.add_argument(
        "--ray-address",
        help="Address of Ray cluster for seamless distributed execution.")

	space = {
	    "width": hp.uniform("width", 0, 20),
	    "height": hp.uniform("height", -100, 100),
	    "activation": hp.choice("activation", ["relu", "tanh"])
	}
	current_best_params = [
	    {
	        "width": 1,
	        "height": 2,
	        "activation": 0  # Activation will be relu
	    },
	    {
	        "width": 4,
	        "height": 2,
	        "activation": 1  # Activation will be tanh
	    }
	]
	config = {
	    "num_samples": 20,
	    "stop": {
	        "training_iteration": 2
	    },
	    "local_dir": "{checkpoint_dir}",
	    "name": "experiment",
	}
	algo = HyperOptSearch(
	        space,
	        max_concurrent=1,
	        metric="mean_loss",
	        mode="min",
	        random_state_seed=5,
	        points_to_evaluate=current_best_params)
	run(MyTrainableClass, search_alg=algo, global_checkpoint_period=0,
	    resume=args.resume, **config)