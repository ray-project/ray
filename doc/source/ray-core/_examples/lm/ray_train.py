#!/usr/bin/env python3 -u

import math
import copy
import socket
import time

import ray

import fairseq
from fairseq import options
from fairseq_cli.train import main
from contextlib import closing

_original_save_checkpoint = fairseq.checkpoint_utils.save_checkpoint


class RayDistributedActor:
    """Actor to perform distributed training."""

    def run(self, url, world_rank, args):
        """Runs the fairseq training.

        We set args for different ray actors for communication,
        add a checkpoint hook, and call the main function of fairseq.
        """

        # Set the init_method and rank of the process for distributed training.
        print("Ray worker at {url} rank {rank}".format(
            url=url, rank=world_rank))
        self.url = url
        self.world_rank = world_rank
        args.distributed_rank = world_rank
        args.distributed_init_method = url

        # Add a checkpoint hook to make use of new resources.
        self.add_checkpoint_hook(args)

        # Call the original main function of fairseq.
        main(args, init_distributed=(args.distributed_world_size > 1))

    def add_checkpoint_hook(self, args):
        """Add a hook to the original save_checkpoint function.

        This checks if there are new computational resources available.
        If so, raise exception to restart the training process and
        make use of the new resources.
        """

        if args.cpu:
            original_n_cpus = args.distributed_world_size

            def _new_save_checkpoint(*args, **kwargs):
                _original_save_checkpoint(*args, **kwargs)
                n_cpus = int(ray.cluster_resources()["CPU"])
                if n_cpus > original_n_cpus:
                    raise Exception(
                        "New CPUs find (original %d CPUs, now %d CPUs)" %
                        (original_n_cpus, n_cpus))
        else:
            original_n_gpus = args.distributed_world_size

            def _new_save_checkpoint(*args, **kwargs):
                _original_save_checkpoint(*args, **kwargs)
                n_gpus = int(ray.cluster_resources().get("GPU", 0))
                if n_gpus > original_n_gpus:
                    raise Exception(
                        "New GPUs find (original %d GPUs, now %d GPUs)" %
                        (original_n_gpus, n_gpus))

        fairseq.checkpoint_utils.save_checkpoint = _new_save_checkpoint

    def get_node_ip(self):
        """Returns the IP address of the current node."""
        return ray.util.get_node_ip_address()

    def find_free_port(self):
        """Finds a free port on the current node."""
        with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
            s.bind(("", 0))
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            return s.getsockname()[1]


def run_fault_tolerant_loop():
    """Entrance function to the fairseq library, providing fault-tolerance."""

    # Parse the command line arguments.
    parser = options.get_training_parser()
    add_ray_args(parser)
    args = options.parse_args_and_arch(parser)
    original_args = copy.deepcopy(args)

    # Main loop for fault-tolerant training.
    retry = True
    while retry:
        args = copy.deepcopy(original_args)

        # Initialize Ray.
        ray.init(address=args.ray_address)

        set_num_resources(args)
        set_batch_size(args)

        # Set up Ray distributed actors.
        Actor = ray.remote(
            num_cpus=1, num_gpus=int(not args.cpu))(RayDistributedActor)
        workers = [Actor.remote() for i in range(args.distributed_world_size)]

        # Get the IP address and a free port of actor 0, which is used for
        # fairseq distributed training.
        ip = ray.get(workers[0].get_node_ip.remote())
        port = ray.get(workers[0].find_free_port.remote())
        address = "tcp://{ip}:{port}".format(ip=ip, port=port)

        # Start the remote processes, and check whether their are any process
        # fails. If so, restart all the processes.
        unfinished = [
            worker.run.remote(address, i, args)
            for i, worker in enumerate(workers)
        ]
        try:
            while len(unfinished) > 0:
                finished, unfinished = ray.wait(unfinished)
                finished = ray.get(finished)
            retry = False
        except Exception as inst:
            print("Ray restart because following error occurs:")
            print(inst)
            retry = True
        ray.shutdown()


def add_ray_args(parser):
    """Add ray and fault-tolerance related parser arguments to the parser."""
    group = parser.add_argument_group("Ray related arguments")
    group.add_argument(
        "--ray-address",
        default="auto",
        type=str,
        help="address for ray initialization")
    group.add_argument(
        "--fix-batch-size",
        default=None,
        metavar="B1,B2,...,B_N",
        type=lambda uf: options.eval_str_list(uf, type=int),
        help="fix the actual batch size (max_sentences * update_freq "
        "* n_GPUs) to be the fixed input values by adjusting update_freq "
        "accroding to actual n_GPUs; the batch size is fixed to B_i for "
        "epoch i; all epochs >N are fixed to B_N")
    return group


def set_num_resources(args):
    """Get the number of resources and set the corresponding fields."""
    if args.cpu:
        args.distributed_world_size = int(ray.cluster_resources()["CPU"])
    else:
        n_gpus = int(ray.cluster_resources().get("GPU", 0))
        while n_gpus == 0:
            print("No GPUs available, wait 10 seconds")
            time.sleep(10)
            n_gpus = int(ray.cluster_resources().get("GPU", 0))
        args.distributed_world_size = n_gpus


def set_batch_size(args):
    """Fixes the total batch_size to be agnostic to the GPU count."""
    if args.fix_batch_size is not None:
        args.update_freq = [
            math.ceil(batch_size /
                      (args.max_sentences * args.distributed_world_size))
            for batch_size in args.fix_batch_size
        ]
        print("Training on %d GPUs, max_sentences=%d, update_freq=%s" %
              (args.distributed_world_size, args.max_sentences,
               repr(args.update_freq)))


if __name__ == "__main__":
    run_fault_tolerant_loop()
