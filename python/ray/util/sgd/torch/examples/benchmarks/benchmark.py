from __future__ import print_function

import argparse
import torch.nn.functional as F
import torch.optim as optim
import torch.utils.data.distributed
from torchvision import models
import timeit
import numpy as np

import ray
from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator

# Benchmark settings
parser = argparse.ArgumentParser(
    description="PyTorch Synthetic Benchmark",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)
parser.add_argument(
    "--smoke-test", action="store_true", default=False, help="finish quickly."
)
parser.add_argument(
    "--fp16", action="store_true", default=False, help="use fp16 training"
)

parser.add_argument("--model", type=str, default="resnet50", help="model to benchmark")
parser.add_argument("--batch-size", type=int, default=32, help="input batch size")

parser.add_argument(
    "--num-warmup-batches",
    type=int,
    default=10,
    help="number of warm-up batches that don't count towards benchmark",
)
parser.add_argument(
    "--num-batches-per-iter",
    type=int,
    default=10,
    help="number of batches per benchmark iteration",
)
parser.add_argument(
    "--num-iters", type=int, default=10, help="number of benchmark iterations"
)

parser.add_argument(
    "--no-cuda", action="store_true", default=False, help="Disables CUDA training"
)
parser.add_argument(
    "--local", action="store_true", default=False, help="Disables cluster training"
)

args = parser.parse_args()

if args.smoke_test:
    args.model = "resnet18"
    args.batch_size = 1
    args.num_iters = 1
    args.num_batches_per_iter = 2
    args.num_warmup_batches = 2
    args.local = True
    args.no_cuda = True

args.cuda = not args.no_cuda and torch.cuda.is_available()
device = "GPU" if args.cuda else "CPU"


def init_hook():
    import torch.backends.cudnn as cudnn

    cudnn.benchmark = True


class Training(TrainingOperator):
    def setup(self, config):
        model = getattr(models, config.get("model"))()
        optimizer = optim.SGD(model.parameters(), lr=0.01 * config["lr_scaler"])
        train_data = LinearDataset(4, 2)  # Have to use dummy data for training.

        self.model, self.optimizer = self.register(
            models=model,
            optimizers=optimizer,
        )
        self.register_data(train_loader=train_data, validation_loader=None)
        data = torch.randn(args.batch_size, 3, 224, 224)
        target = torch.LongTensor(args.batch_size).random_() % 1000
        if args.cuda:
            data, target = data.cuda(), target.cuda()

        self.data, self.target = data, target

    def train_epoch(self, *pargs, **kwargs):
        def benchmark():
            self.optimizer.zero_grad()
            output = self.model(self.data)
            loss = F.cross_entropy(output, self.target)
            loss.backward()
            self.optimizer.step()

        print("Running warmup...")
        if self.global_step == 0:
            timeit.timeit(benchmark, number=args.num_warmup_batches)
            self.global_step += 1
        print("Running benchmark...")
        time = timeit.timeit(benchmark, number=args.num_batches_per_iter)
        img_sec = args.batch_size * args.num_batches_per_iter / time
        return {"img_sec": img_sec}


if __name__ == "__main__":
    if args.local:
        ray.init(num_cpus=2)
    else:
        ray.init(address="auto")
    num_workers = 2 if args.local else int(ray.cluster_resources().get(device))
    from ray.util.sgd.torch.examples.train_example import LinearDataset

    print(f"Model: {args.model}")
    print("Batch size: %d" % args.batch_size)
    print("Number of %ss: %d" % (device, num_workers))

    trainer = TorchTrainer(
        training_operator_cls=Training,
        initialization_hook=init_hook,
        config={"lr_scaler": num_workers, "model": args.model},
        num_workers=num_workers,
        use_gpu=args.cuda,
        use_fp16=args.fp16,
    )

    img_secs = []
    for x in range(args.num_iters):
        result = trainer.train()
        # print(result)
        img_sec = result["img_sec"]
        print("Iter #%d: %.1f img/sec per %s" % (x, img_sec, device))
        img_secs.append(img_sec)

    # Results
    img_sec_mean = np.mean(img_secs)
    img_sec_conf = 1.96 * np.std(img_secs)
    print(f"Img/sec per {device}: {img_sec_mean:.1f} +-{img_sec_conf:.1f}")
    print(
        "Total img/sec on %d %s(s): %.1f +-%.1f"
        % (num_workers, device, num_workers * img_sec_mean, num_workers * img_sec_conf)
    )
