import ray

from typing import Callable, List

"""
Chain the function to make a sequential pipeline:
   step1 -> step2 -> step3 -> ...
"""


def function_chain(steps: List[Callable]) -> Callable:
    assert len(steps) != 0

    def chain_func(*args, **kw_argv):
        remote_tasks = list(map(ray.remote, steps))
        # Get the first function as a start
        wf_step = remote_tasks[0].bind(*args, **kw_argv)
        for i in range(1, len(steps)):
            # Convert each function inside steps into workflow step
            # function and then use the previous output as the input
            # for them.
            wf_step = remote_tasks[i].bind(wf_step)
        return wf_step

    return chain_func


"""
Multiply semantics of each steps:
  [[s_1_1, s_1_2],
   [s_2_1, s_2_2]]

      /-> s_1_1 ->  s_2_1 - \
entry           \-> s_2_2 ---\
      \-> s_1_2 ->  s_2_1 ----> end
                \-> s_2_2 --/

Each step will only be executed one time.

Basically, given a list of list [L1, L2, ...], we'd like to have
  L1 x L2 x L3
"""


def function_compose(steps: List[List[Callable]]) -> Callable:
    assert len(steps) != 0

    @ray.remote
    def finish(*args):
        return args

    def entry(*args, **kw_args):
        layer_0 = steps[0]
        wf = [ray.remote(f).bind(*args, **kw_args) for f in layer_0]
        for layer_i in steps[1:]:
            new_wf = [ray.remote(f).bind(w) for f in layer_i for w in wf]
            wf = new_wf
        return finish.bind(*wf)

    return entry


if __name__ == "__main__":

    def add(i: int, v: int):
        return i + v

    pipeline = function_chain(
        [
            lambda v: add(v, 1),
            lambda v: add(v, 2),
            lambda v: add(v, 3),
            lambda v: add(v, 4),
        ]
    )

    workflow_id = "__function_chain_test"
    try:
        ray.workflow.delete(workflow_id)
    except Exception:
        pass
    assert ray.workflow.run(pipeline(10), workflow_id=workflow_id) == 20

    pipeline = function_compose(
        [
            [
                lambda v: add(v, 1),
                lambda v: add(v, 2),
            ],
            [
                lambda v: add(v, 3),
                lambda v: add(v, 4),
            ],
        ]
    )

    workflow_id = "__function_compose_test"
    try:
        ray.workflow.delete(workflow_id)
    except Exception:
        pass
    assert ray.workflow.run(pipeline(10), workflow_id=workflow_id) == (14, 15, 15, 16)
