import argparse
import importlib
import sys

from mpi4py import MPI

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Setup MPI worker")
    parser.add_argument("worker_entry")
    parser.add_argument("main_entry")

    args, remaining_args = parser.parse_known_args()

    comm = MPI.COMM_WORLD

    rank = comm.Get_rank()

    if rank == 0:
        entry_file = args.main_entry

        sys.argv[1:] = remaining_args
        spec = importlib.util.spec_from_file_location("__main__", entry_file)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
    else:
        from ray.runtime_env import mpi_init

        mpi_init()
        module, func = args.worker_entry.rsplit(".", 1)
        m = importlib.import_module(module)
        f = getattr(m, func)
        f()
