import os
import random
import time

import ray


@ray.remote
class Actor:
    def __init__(self, init_value, fail_after=None, sys_exit=False):
        self.i = init_value
        self.fail_after = fail_after
        self.sys_exit = sys_exit

        self.count = 0

    def _fail_if_needed(self):
        if self.fail_after and self.count > self.fail_after:
            # Randomize the failures to better cover multi actor scenarios.
            if random.random() > 0.5:
                if self.sys_exit:
                    os._exit(1)
                else:
                    raise RuntimeError("injected fault")

    def inc(self, x):
        self.i += x
        self.count += 1
        self._fail_if_needed()
        return self.i

    def double_and_inc(self, x):
        self.i *= 2
        self.i += x
        return self.i

    def echo(self, x):
        self.count += 1
        self._fail_if_needed()
        return x

    def append_to(self, lst):
        lst.append(self.i)
        return lst

    def inc_two(self, x, y):
        self.i += x
        self.i += y
        return self.i

    def sleep(self, x):
        time.sleep(x)
        return x

    @ray.method(num_returns=2)
    def return_two(self, x):
        return x, x + 1

    def read_input(self, x):
        return x

    @ray.method(num_returns=2)
    def inc_and_return_two(self, x):
        self.i += x
        return self.i, self.i + 1

    @ray.method(num_returns=1)
    def return_two_as_one(self, x):
        return x, x + 1

    @ray.method(num_returns=2)
    def return_two_from_three(self, x):
        return x, x + 1, x + 2

    @ray.method(num_returns=2)
    def return_two_but_raise_exception(self, x):
        raise RuntimeError
        return 1, 2

    def get_events(self):
        return getattr(self, "__ray_cgraph_events", [])


@ray.remote
class Collector:
    def __init__(self):
        self.results = []

    def collect(self, x):
        self.results.append(x)
        return self.results

    def collect_two(self, x, y):
        self.results.append(x)
        self.results.append(y)
        return self.results

    def collect_three(self, x, y, z):
        self.results.append(x)
        self.results.append(y)
        self.results.append(z)
        return self.results
