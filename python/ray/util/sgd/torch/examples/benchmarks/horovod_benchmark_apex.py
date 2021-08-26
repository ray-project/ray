from __future__ import print_function

import argparse
import torch.backends.cudnn as cudnn
import torch.nn.functional as F
import torch.optim as optim
import torch.utils.data.distributed
from torchvision import models
import horovod.torch as hvd
import timeit
import numpy as np
# Apex
from apex import amp

# Benchmark settings
parser = argparse.ArgumentParser(
    description="PyTorch Synthetic Benchmark",
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
    "--no-cuda",
    action="store_true",
    default=False,
    help="disables CUDA training")
parser.add_argument(
    "--amp-fp16",
    action="store_true",
    default=False,
    help="Enables FP16 training with Apex.")

args = parser.parse_args()
args.cuda = not args.no_cuda and torch.cuda.is_available()

hvd.init()

if args.cuda:
    # Horovod: pin GPU to local rank.
    torch.cuda.set_device(hvd.local_rank())

cudnn.benchmark = True

# Set up standard model.
model = getattr(models, args.model)()

if args.cuda:
    # Move model to GPU.
    model.cuda()

optimizer = optim.SGD(model.parameters(), lr=0.01)

# Horovod: (optional) compression algorithm.
compression = (hvd.Compression.fp16
               if args.fp16_allreduce else hvd.Compression.none)

# Horovod: wrap optimizer with DistributedOptimizer.
optimizer = hvd.DistributedOptimizer(
    optimizer,
    named_parameters=model.named_parameters(),
    compression=compression)

# Horovod: broadcast parameters & optimizer state.
hvd.broadcast_parameters(model.state_dict(), root_rank=0)
hvd.broadcast_optimizer_state(optimizer, root_rank=0)

# Apex
if args.amp_fp16:
    model, optimizer = amp.initialize(model, optimizer, opt_level="O1")

# Set up fixed fake data
data = torch.randn(args.batch_size, 3, 224, 224)
target = torch.LongTensor(args.batch_size).random_() % 1000
if args.cuda:
    data, target = data.cuda(), target.cuda()


def benchmark_step():
    optimizer.zero_grad()
    output = model(data)
    loss = F.cross_entropy(output, target)
    # Apex
    if args.amp_fp16:
        with amp.scale_loss(loss, optimizer) as scaled_loss:
            scaled_loss.backward()
            optimizer.synchronize()
        with optimizer.skip_synchronize():
            optimizer.step()
    else:
        loss.backward()
        optimizer.step()


def log(s, nl=True):
    if hvd.rank() != 0:
        return
    print(s, end="\n" if nl else "")


log(f"Model: {args.model}")
log("Batch size: %d" % args.batch_size)
device = "GPU" if args.cuda else "CPU"
log("Number of %ss: %d" % (device, hvd.size()))

# Warm-up
log("Running warmup...")
timeit.timeit(benchmark_step, number=args.num_warmup_batches)

# Benchmark
log("Running benchmark...")
img_secs = []
for x in range(args.num_iters):
    time = timeit.timeit(benchmark_step, number=args.num_batches_per_iter)
    img_sec = args.batch_size * args.num_batches_per_iter / time
    log("Iter #%d: %.1f img/sec per %s" % (x, img_sec, device))
    img_secs.append(img_sec)

# Results
img_sec_mean = np.mean(img_secs)
img_sec_conf = 1.96 * np.std(img_secs)
log(f"Img/sec per {device}: {img_sec_mean:.1f} +-{img_sec_conf:.1f}")
log("Total img/sec on %d %s(s): %.1f +-%.1f" %
    (hvd.size(), device, hvd.size() * img_sec_mean, hvd.size() * img_sec_conf))
