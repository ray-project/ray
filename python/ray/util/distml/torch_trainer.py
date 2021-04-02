from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse
import time

import ray
from ray.util.distml.scratch.core.ps import PSStrategy


parser = argparse.ArgumentParser(description='parameter server')
parser.add_argument('-a', '--num-iterations', type=int, default=1000,
                    help='enable asynchronous training')
parser.add_argument('-n', '--num-workers', type=int, required=True, default=1,
                    help='number of parameter server workers')
parser.add_argument('-m', '--model', type=str, default="resnet50",
                    help='neural network model type')
parser.add_argument('-p', '--num-ps', type=int, default=1, help='number of ps shards')
parser.add_argument('-b', '--batch-size', type=int, default=128, help='batch size per replica')
args = parser.parse_args()


class AverageMeter(object):
    """Computes and stores the average and current value"""
    def __init__(self, name, fmt=':f'):
        self.name = name
        self.fmt = fmt
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count

    def __str__(self):
        fmtstr = '{name} {val' + self.fmt + '} ({avg' + self.fmt + '})'
        return fmtstr.format(**self.__dict__)


class ProgressMeter(object):
    def __init__(self, num_batches, meters, prefix=""):
        self.batch_fmtstr = self._get_batch_fmtstr(num_batches)
        self.meters = meters
        self.prefix = prefix

    def display(self, batch):
        entries = [self.prefix + self.batch_fmtstr.format(batch)]
        entries += [str(meter) for meter in self.meters]
        print('\t'.join(entries))

    def _get_batch_fmtstr(self, num_batches):
        num_digits = len(str(num_batches // 1))
        fmt = '{:' + str(num_digits) + 'd}'
        return '[' + fmt + '/' + fmt.format(num_batches) + ']'



import ray.util.distml.torch_operator import TrainingOperator


class MyTrainingOperator(TrainingOperator):
    def setup(self):
        pass

def main():
    """Setup Ray instances."""
    # ray.init(resources={"machine": 2})
    ray.init(address='auto')

    strategy = PSStrategy(
        num_worker=args.num_workers,
        num_ps=args.num_ps,
        model=MyTrainingOperator
        batch_size=args.batch_size)

    # strategy.initialize()
    print("Parameter server running...")

    batch_time = AverageMeter('Time', ':6.3f')
    loss_meters = [AverageMeter('Loss{}'.format(i), ':.4e') for i in range(args.num_workers)]
    progress = ProgressMeter(
        args.num_iterations,
        [batch_time] + loss_meters)
    for i in range(args.num_iterations):
        start_time = time.time()
        losses = strategy.step()
        batch_time.update(time.time() - start_time)
        for loss, loss_meter in zip(losses, loss_meters):
            loss_meter.update(loss)
        progress.display(i)

    ray.shutdown()


if __name__ == '__main__':
    main()
