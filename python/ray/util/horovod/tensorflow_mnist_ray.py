import argparse
import os
import numpy as np
from tqdm import tqdm

import torch
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from torchvision import datasets, transforms

import horovod.torch as hvd
from horovod.torch.elastic.sampler import ElasticSampler
from horovod.ray import ray_logger
from horovod.ray.elastic import TestDiscovery, ElasticRayExecutor

# Training settings
parser = argparse.ArgumentParser(
    description='PyTorch MNIST Example',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument(
    '--log-dir', default='./logs', help='tensorboard log directory')
parser.add_argument(
    '--checkpoint-format',
    default='./checkpoint-{epoch}.pth.tar',
    help='checkpoint file format')
parser.add_argument(
    '--data-dir', default='./new_data', help='MNIST dataset directory')

parser.add_argument(
    '--epochs', type=int, default=90, help='number of epochs to train')
parser.add_argument(
    '--lr', type=float, default=0.01, help='learning rate for a single GPU')

parser.add_argument(
    '--no-cuda',
    action='store_true',
    default=False,
    help='disables CUDA training')
parser.add_argument('--seed', type=int, default=42, help='random seed')
parser.add_argument(
    '--forceful',
    action="store_true",
    help="Removes the node upon deallocation (non-gracefully).")
parser.add_argument(
    '--change-frequency-s', type=int, default=20, help='random seed')

# Elastic Horovod settings
parser.add_argument(
    '--batches-per-commit',
    type=int,
    default=50,
    help='number of batches processed before calling `state.commit()`; '
    'commits prevent losing progress if an error occurs, but slow '
    'down training.')
parser.add_argument(
    '--batches-per-host-check',
    type=int,
    default=10,
    help=(
        'number of batches processed before calling '
        '`state.check_host_updates()`; '
        'this check is very fast compared to state.commit() (which calls this '
        'as part of the commit process) but because it '
        'still incurs some cost due to broadcast, '
        'we may not want to perform it every batch.'))
parser.add_argument(
    '--data-dir',
    help='location of the training dataset in the local filesystem (will be downloaded if needed)'
)

args = parser.parse_args()


def load_data_mnist():
    # Horovod: limit # of CPU threads to be used per worker.
    torch.set_num_threads(4)

    kwargs = {'num_workers': 0, 'pin_memory': True} if args.cuda else {}
    data_dir = args.data_dir or './data'
    from filelock import FileLock
    with FileLock(os.path.expanduser("~/.horovod_lock")):
        train_dataset = \
            datasets.MNIST(data_dir, train=True, download=True,
                           transform=transforms.Compose([
                               transforms.ToTensor(),
                               transforms.Normalize((0.1307,), (0.3081,))
                           ]))
    train_sampler = ElasticSampler(train_dataset)
    train_loader = torch.utils.data.DataLoader(
        train_dataset, batch_size=8, sampler=train_sampler, **kwargs)

    return train_loader, train_sampler


class tqdm_callback:
    def __init__(self):
        self._progress_bar = None
        self._current_epoch = None
        self._world_size = None
        self._mode = None

    def __call__(self, info):
        tqdm_mode = info["tqdm_mode"]
        assert tqdm_mode in {"val", "train"}
        reset = False
        if self._mode != tqdm_mode or \
                self._current_epoch != info["epoch"] or \
                self._world_size != info["world_size"]:
            reset = True
            self._mode = tqdm_mode
            self._current_epoch = info["epoch"]
            self._world_size = info["world_size"]

        if reset:
            if self._progress_bar is not None:
                self._progress_bar.close()
            epoch = self._current_epoch + 1
            self._progress_bar = tqdm(
                total=info["total"],
                desc=f'[mode={tqdm_mode}] Epoch     #{epoch}')

        scoped = {k: v for k, v in info.items() if k.startswith(tqdm_mode)}
        self._progress_bar.set_postfix(scoped)
        self._progress_bar.update(1)


class TensorboardCallback:
    def __init__(self, logdir):
        from torch.utils.tensorboard import SummaryWriter
        self.log_writer = SummaryWriter(logdir)

    def __call__(self, info):
        tqdm_mode = info["tqdm_mode"]
        epoch = info["epoch"]
        for k, v in info.items():
            if k.startswith(tqdm_mode):
                self.log_writer.add_scalar(k, v, epoch)


def train(state, train_loader):
    epoch = state.epoch
    batch_offset = state.batch

    state.model.train()
    state.train_sampler.set_epoch(epoch)
    train_loss = Metric('train_loss')
    train_accuracy = Metric('train_accuracy')

    for batch_idx, (data, target) in enumerate(train_loader):
        # Elastic Horovod: update the current batch index this epoch
        # and commit / check for host updates. Do not check hosts when
        # we commit as it would be redundant.
        state.batch = batch_offset + batch_idx
        if args.batches_per_commit > 0 and \
                state.batch % args.batches_per_commit == 0:
            state.commit()
        elif args.batches_per_host_check > 0 and \
                state.batch % args.batches_per_host_check == 0:
            state.check_host_updates()

        if args.cuda:
            data, target = data.cuda(), target.cuda()
        state.optimizer.zero_grad()

        output = state.model(data)
        train_accuracy.update(accuracy(output, target))

        loss = F.cross_entropy(output, target)
        train_loss.update(loss)
        loss.backward()
        state.optimizer.step()
        # Only log from the 0th rank worker.
        if hvd.rank() == 0:
            ray_logger.log({
                "tqdm_mode": 'train',
                "train/loss": train_loss.avg.item(),
                "train/accuracy": 100. * train_accuracy.avg.item(),
                "total": len(train_loader),
                "epoch": epoch,
                "world_size": hvd.size()
            })


def accuracy(output, target):
    # get the index of the max log-probability
    pred = output.max(1, keepdim=True)[1]
    return pred.eq(target.view_as(pred)).cpu().float().mean()


def save_checkpoint(state):
    if hvd.rank() == 0:
        filepath = args.checkpoint_format.format(epoch=state.epoch + 1)
        state = {
            'model': state.model.state_dict(),
            'optimizer': state.optimizer.state_dict(),
            'scheduler': state.scheduler.state_dict(),
        }
        torch.save(state, filepath)


def end_epoch(state):
    state.epoch += 1
    state.batch = 0
    state.train_sampler.set_epoch(state.epoch)
    state.commit()


# Horovod: average metrics from distributed training.
class Metric(object):
    def __init__(self, name):
        self.name = name
        self.sum = torch.tensor(0.)
        self.n = torch.tensor(0.)

    def update(self, val):
        self.sum += hvd.allreduce(val.detach().cpu(), name=self.name)
        self.n += 1

    @property
    def avg(self):
        return self.sum / self.n


class Net(nn.Module):
    def __init__(self, large=False):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 10, kernel_size=5)
        self.conv2 = nn.Conv2d(10, 20, kernel_size=5)
        self.conv2_drop = nn.Dropout2d()
        self.fc1 = nn.Linear(320, 300)
        self.hiddens = []
        if large:
            self.hiddens = nn.ModuleList(
                [nn.Linear(300, 300) for i in range(30)])
        self.fc2 = nn.Linear(300, 10)

    def forward(self, x):
        x = F.relu(F.max_pool2d(self.conv1(x), 2))
        x = F.relu(F.max_pool2d(self.conv2_drop(self.conv2(x)), 2))
        x = x.view(-1, 320)
        x = F.relu(self.fc1(x))
        if self.hiddens:
            for layer in self.hiddens:
                x = F.relu(layer(x))
        x = F.dropout(x, training=self.training)
        x = self.fc2(x)
        return F.log_softmax(x)


def run(large=False):
    hvd.init()

    torch.manual_seed(args.seed)
    args.cuda = not args.no_cuda and torch.cuda.is_available()

    if args.cuda:
        # Horovod: pin GPU to local rank.
        torch.cuda.set_device(hvd.local_rank())
        torch.cuda.manual_seed(args.seed)

    # If set > 0, will resume training from a given checkpoint.
    resume_from_epoch = 0
    for try_epoch in range(args.epochs, 0, -1):
        if os.path.exists(args.checkpoint_format.format(epoch=try_epoch)):
            resume_from_epoch = try_epoch
            break

    # Load MNIST dataset
    train_loader, train_sampler = load_data_mnist()

    model = Net(large=large)
    if args.cuda:
        model.cuda()

    # Horovod: scale learning rate by the number of GPUs.
    optimizer = optim.SGD(
        model.parameters(),
        lr=args.lr * np.sqrt(hvd.size()),
        momentum=0.9,
        weight_decay=5e-4)

    # Horovod: wrap optimizer with DistributedOptimizer.
    optimizer = hvd.DistributedOptimizer(
        optimizer, named_parameters=model.named_parameters())

    scheduler = torch.optim.lr_scheduler.CosineAnnealingLR(
        optimizer, T_max=200)

    # Restore from a previous checkpoint, if initial_epoch is specified.
    # Horovod: restore on the first worker which will broadcast
    # weights to other workers.
    if resume_from_epoch > 0 and hvd.rank() == 0:
        filepath = args.checkpoint_format.format(epoch=resume_from_epoch)
        checkpoint = torch.load(filepath)
        model.load_state_dict(checkpoint['model'])
        optimizer.load_state_dict(checkpoint['optimizer'])
        scheduler.load_state_dict(checkpoint['scheduler'])

    def on_state_reset():
        # Horovod: scale the learning rate as controlled by the LR schedule
        scheduler.base_lrs = [args.lr * hvd.size() for _ in scheduler.base_lrs]

    state = hvd.elastic.TorchState(
        model=model,
        optimizer=optimizer,
        scheduler=scheduler,
        train_sampler=train_sampler,
        epoch=resume_from_epoch,
        batch=0)
    state.register_reset_callbacks([on_state_reset])

    @hvd.elastic.run
    def full_train(state, train_loader):
        while state.epoch < args.epochs:
            train(state, train_loader)
            state.scheduler.step()
            save_checkpoint(state)
            end_epoch(state)

    full_train(state, train_loader)

def main():
    settings = ElasticRayExecutor.create_settings(verbose=2)
    settings.discovery = TestDiscovery(
        min_hosts=2,
        max_hosts=5,
        change_frequency_s=args.change_frequency_s,
        use_gpu=True,
        cpus_per_slot=1,
        _graceful=not args.forceful,
        verbose=False)
    executor = ElasticRayExecutor(
        settings, use_gpu=True, cpus_per_slot=1, override_discovery=False)
    executor.start()
    executor.run(
        lambda: run(large=True),
        callbacks=[tqdm_callback(),
                   TensorboardCallback(args.log_dir)])

if __name__ == '__main__':
    import ray
    ray.init(address="auto")
    main()

