# Imagenet Models RaySGD Example

Based on [the timm package](https://github.com/rwightman/pytorch-image-models).

# Usage

## Ray autoscaler

- `ray up cluster.yaml`
- `ray rsync-up -A cluster.yaml`
- `ray submit train.py -- data -n=4`

## Manual

- Make `train.py` and `args.py` available on the remote host.
- `pip install timm`
- Download and unpack an ImageNet-compatible dataset (has to be full size). Internally we use [Imagenette](https://github.com/fastai/imagenette) for development purposes.
- Optional: setup a ray cluster (`ray start --head` on the head node and `ray start --redis-address HEAD_ADDRESS` on each of the worker nodes).
- Run `python train.py DATA_DIRECTORY` on the head node.

## Manual (single node)

- `pip install timm`
- `ray start --head`
- `python train.py DATA_DIRECTORY`
- Use the `-n` argument to control the number of processes + GPUs used.

# Advantages

Compared to the original `timm` package, the RaySGD train script has a few advantages:

- Compatibility with Ray autoscaler (automatic simple cluster provisioning).
- Built-in fault tolerance (epochs will checkpoint and restart if a worker fails). This means you can, for example, make all your worker nodes preemtible (e.g. AWS spot requests). **Note:** the head node *must* be non-preemtible.
- Since a Ray cluster is already setup, you can run other distributed tasks.

# Limitations

Support for some command line flags from the original `timm` package has been intentionally dropped:
- `-j/--workers` - Ray can start multiple training processes with `-n/--num-workers` instead.
- `--num-gpu` - Ray can use multiple GPUs by launching multiple processes with `-n/--num-workers` instead. (`DistributedDataParallel` is faster than simple `DataParallel` in practice anyway)

Other features are still in the works:
- Logging
- Compatibility with timm checkpoints
- EMA
- Sync batch norm
- Learning rate scheduling
- Some testing
