import ray

from typing import Callable, List
ray.workflow.init()


def function_chain(steps: List[Callable]) -> Callable:
    assert len(steps) != 0

    @ray.workflow.step
    def chain_func(*args, **kw_argv):
        wf_step = ray.workflow.step(steps[0]).step(*args, **kw_argv)
        for i in range(1, len(steps)):
            wf_step = ray.workflow.step(steps[i]).step(wf_step)
        return wf_step

    return chain_func


if __name__ == "__main__":

    def add(i: int):
        return i + 1

    pipeline = function_chain([add, add, add, add])
    assert pipeline.step(10).run(workflow_id="test") == 14
