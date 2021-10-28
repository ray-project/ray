import ray
from ray.train import Trainer
from ray.train.examples.tensorflow_mnist_example import train_func

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help=("Finish quickly for testing."))
    args = parser.parse_args()

    ray.init(address="auto")

    trainer = Trainer(backend="tensorflow", num_workers=4, use_gpu=True)
    trainer.start()
    results = trainer.run(
        train_func=train_func,
        config={
            "lr": 1e-3,
            "batch_size": 64,
            "epochs": 4
        })
    trainer.shutdown()
