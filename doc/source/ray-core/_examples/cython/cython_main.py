import ray
import click
import inspect

import numpy as np
import cython_examples as cyth


def run_func(func, *args, **kwargs):
    """Helper function for running examples"""
    ray.init()

    func = ray.remote(func)

    # NOTE: kwargs not allowed for now
    result = ray.get(func.remote(*args))

    # Inspect the stack to get calling example
    caller = inspect.stack()[1][3]
    print("%s: %s" % (caller, str(result)))

    return result


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def cli():
    """Working with Cython actors and functions in Ray"""


@cli.command()
def example1():
    """Cython def function"""

    run_func(cyth.simple_func, 1, 2, 3)


@cli.command()
def example2():
    """Cython def function, recursive"""

    run_func(cyth.fib, 10)


@cli.command()
def example3():
    """Cython def function, built-in typed parameter"""

    # NOTE: Cython will attempt to cast argument to correct type
    # NOTE: Floats will be cast to int, but string, for example will error
    run_func(cyth.fib_int, 10)


@cli.command()
def example4():
    """Cython cpdef function"""

    run_func(cyth.fib_cpdef, 10)


@cli.command()
def example5():
    """Cython wrapped cdef function"""

    # NOTE: cdef functions are not exposed to Python
    run_func(cyth.fib_cdef, 10)


@cli.command()
def example6():
    """Cython simple class"""

    ray.init()

    cls = ray.remote(cyth.simple_class)
    a1 = cls.remote()
    a2 = cls.remote()

    result1 = ray.get(a1.increment.remote())
    result2 = ray.get(a2.increment.remote())

    print(result1, result2)


@cli.command()
def example7():
    """Cython with function from BrainIAK (masked log)"""

    run_func(cyth.masked_log, np.array([-1.0, 0.0, 1.0, 2.0]))


@cli.command()
def example8():
    """Cython with blas. NOTE: requires scipy"""

    # See cython_blas.pyx for argument documentation
    mat = np.array(
        [[[2.0, 2.0], [2.0, 2.0]], [[2.0, 2.0], [2.0, 2.0]]], dtype=np.float32)
    result = np.zeros((2, 2), np.float32, order="C")

    run_func(cyth.compute_kernel_matrix, "L", "T", 2, 2, 1.0, mat, 0, 2, 1.0,
             result, 2)


if __name__ == "__main__":
    cli()
