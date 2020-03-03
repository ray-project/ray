from __future__ import print_function

import argparse
import torch.nn.functional as F
import torch.optim as optim
import torch.utils.data.distributed
from torchvision import models
# import horovod.torch as hvd
import timeit
import numpy as np

# Benchmark settings
parser = argparse.ArgumentParser(description='PyTorch Synthetic Benchmark',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('--fp16-allreduce', action='store_true', default=False,
                    help='use fp16 compression during allreduce')

parser.add_argument('--model', type=str, default='resnet50',
                    help='model to benchmark')
parser.add_argument('--batch-size', type=int, default=32,
                    help='input batch size')

parser.add_argument('--num-warmup-batches', type=int, default=10,
                    help='number of warm-up batches that don\'t count towards benchmark')
parser.add_argument('--num-batches-per-iter', type=int, default=10,
                    help='number of batches per benchmark iteration')
parser.add_argument('--num-iters', type=int, default=10,
                    help='number of benchmark iterations')

parser.add_argument('--cuda', action='store_true', default=False,
                    help='Enables CUDA training')

args = parser.parse_args()
args.cuda = not args.no_cuda and torch.cuda.is_available()

print('Model: %s' % args.model)
print('Batch size: %d' % args.batch_size)
device = 'GPU' if args.cuda else 'CPU'
print('Number of %ss: %d' % (device, num_workers))
ray.init(address="auto")
num_workers = int(ray.cluster_resources().get(device))



from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator


def init_hook():
    import torch.backends.cudnn as cudnn
    cudnn.benchmark = True


def benchmark_evaluate(operator):
    data = torch.randn(operator.args.batch_size, 3, 224, 224)
    target = torch.LongTensor(operator.args.batch_size).random_() % 1000
    if args.cuda:
        data, target = data.cuda(), target.cuda()

    def benchmark():
        operator.optimizer.zero_grad()
        output = operator.model(data)
        loss = F.cross_entropy(output, target)
        loss.backward()
        operator.optimizer.step()

    print('Running warmup...')
    timeit.timeit(benchmark, number=operator.args.num_warmup_batches)
    print('Running benchmark...')
    time = timeit.timeit(benchmark, number=num_batches)
    img_sec = operator.args.batch_size * num_batches / time
    return {"img_sec": img_sec}

class Training(TrainingOperator):
    def setup(self, config):
        import json
        from types import SimpleNamespace
        self.args = json.loads(
            config, object_hook=lambda d: SimpleNamespace(**d))

trainer = TorchTrainer(
    model_creator=getattr(models, args.model),
    optimizer_creator=lambda model, cfg: optim.SGD(
        model.parameters(), lr=0.01 * config.get("lr_scaler")),
    initialization_hook=init_hook,
    config=dict(
        lr_scaler=hvd.local_size(),
        batch_size=args.batch_size),
    training_operator_cls=Training,
    use_gpu=args.cuda
)

img_secs = []
for x in range(args.num_iters):
    print("trainer has been setup")
    result = trainer.apply_all_operators(benchmark_evaluate)
    print(result)
    img_sec = result["img_sec"]
    print('Iter #%d: %.1f img/sec per %s' % (x, img_sec, device))
    img_secs.append(img_sec)

# Results
img_sec_mean = np.mean(img_secs)
img_sec_conf = 1.96 * np.std(img_secs)
log('Img/sec per %s: %.1f +-%.1f' % (device, img_sec_mean, img_sec_conf))
log('Total img/sec on %d %s(s): %.1f +-%.1f' %
    (num_workers, device, num_workers * img_sec_mean, num_workers * img_sec_conf))
