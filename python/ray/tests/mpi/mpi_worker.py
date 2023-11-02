# copied from here
# https://github.com/alshedivat/ACM-Python-Tutorials-KAUST-2014/blob/master/mpi4py-tutorial/examples/compute_pi-montecarlo-mpi.py
from mpi4py import MPI
import numpy

def compute_pi(samples):
    count = 0
    for x, y in samples:
        if x**2 + y**2 <= 1:
            count += 1
    pi = 4*float(count)/len(samples)
    return pi



def run():
    comm = MPI.COMM_WORLD
    nprocs = comm.Get_size()
    myrank = comm.Get_rank()

    if myrank == 0:
        numpy.random.seed(1)
        N = 100000 // nprocs
        samples = numpy.random.random((nprocs, N, 2))
    else:
        samples = None

    samples = comm.scatter(samples, root=0)

    mypi = compute_pi(samples) / nprocs

    pi = comm.reduce(mypi, root=0)

    if myrank == 0:
        return pi

if __name__ == "__main__":
    run()
