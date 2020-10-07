from __future__ import print_function

import argparse
import timeit
import torch.backends.cudnn as cudnn
import torch.nn.functional as F
import torch.optim as optim
import torch.utils.data.distributed
from torch.nn import DataParallel
from torchvision import models
import numpy as np
import os
# Apex
from apex import amp

# Benchmark settings
parser = argparse.ArgumentParser(
    description="PyTorch DP Synthetic Benchmark",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument(
    "--fp16-allreduce",
    action="store_true",
    default=False,
    help="use fp16 compression during allreduce")

parser.add_argument(
    "--model", type=str, default="resnet50", help="model to benchmark")
parser.add_argument(
    "--batch-size", type=int, default=32, help="input batch size")
parser.add_argument("--num-gpus", type=int, default=1, help="number of gpus")

parser.add_argument(
    "--num-warmup-batches",
    type=int,
    default=10,
    help="number of warm-up batches that don\"t count towards benchmark")
parser.add_argument(
    "--num-batches-per-iter",
    type=int,
    default=10,
    help="number of batches per benchmark iteration")
parser.add_argument(
    "--num-iters", type=int, default=10, help="number of benchmark iterations")
parser.add_argument(
    "--amp-fp16",
    action="store_true",
    default=False,
    help="Enables FP16 training with Apex.")

args = parser.parse_args()
os.environ["CUDA_VISIBLE_DEVICES"] = ",".join(
    str(i) for i in range(args.num_gpus))

cudnn.benchmark = True

# Set up standard model.
model = getattr(models, args.model)().cuda()
model = DataParallel(model)

optimizer = optim.SGD(model.parameters(), lr=0.01)

# Apex
if args.amp_fp16:
    model, optimizer = amp.initialize(model, optimizer, opt_level="O1")

# Set up fixed fake data
data = torch.randn(args.batch_size, 3, 224, 224)
target = torch.LongTensor(args.batch_size).random_() % 1000
data, target = data.cuda(), target.cuda()


def benchmark_step():
    optimizer.zero_grad()
    output = model(data)
    loss = F.cross_entropy(output, target)
    loss.backward()
    optimizer.step()


print(f"Model: {args.model}")
print("Batch size: %d" % args.batch_size)
device = "GPU"
print("Number of %ss: %d" % (device, args.num_gpus))

# Warm-up
print("Running warmup...")
timeit.timeit(benchmark_step, number=args.num_warmup_batches)

# Benchmark
print("Running benchmark...")
img_secs = []
for x in range(args.num_iters):
    time = timeit.timeit(benchmark_step, number=args.num_batches_per_iter)
    img_sec = args.batch_size * args.num_batches_per_iter / time
    print("Iter #%d: %.1f img/sec per %s" % (x, img_sec, device))
    img_secs.append(img_sec)

# Results
img_sec_mean = np.mean(img_secs)
img_sec_conf = 1.96 * np.std(img_secs)
print(f"Img/sec per {device}: {img_sec_mean:.1f} +-{img_sec_conf:.1f}")
print("Total img/sec on %d %s(s): %.1f +-%.1f" % (
    args.num_gpus,
    device,
    img_sec_mean,  # we do NOT scale this by number workers
    args.num_gpus * img_sec_conf))
