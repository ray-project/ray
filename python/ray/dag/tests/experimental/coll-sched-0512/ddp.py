import logging
import torch
import fire

import ray
import ray.experimental.collective as collective
from ray.dag import InputNode, MultiOutputNode


logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s %(filename)s:%(lineno)d %(funcName)s] %(message)s",
)


@ray.remote(num_gpus=1)
class Actor:
    def __init__(self):
        return

    def compute(self, _):
        return torch.ones(1)

    def update(self, _):
        return


def run_ddp():
    ray.init()

    actors = [Actor.remote() for _ in range(2)]
    upds = []
    with InputNode() as inp:
        grads = [actor.compute.bind(inp) for actor in actors]
        grads_reduced = collective.allreduce.bind(grads)
        upds.extend(
            [actor.update.bind(grad) for actor, grad in zip(actors, grads_reduced)]
        )
        grads = [actor.compute.bind(grad) for actor, grad in zip(actors, grads)]
        grads_reduced = collective.allreduce.bind(grads)
        upds.extend(
            [actor.update.bind(grad) for actor, grad in zip(actors, grads_reduced)]
        )
        dag = MultiOutputNode(upds)

    compiled_dag = dag.experimental_compile()
    logger.info(compiled_dag.actor_to_execution_schedule)

    ray.shutdown()


def main(name: str):
    if name == "ddp":
        run_ddp()
    else:
        logger.error(f"Unknown name: {name}")


if __name__ == "__main__":
    fire.Fire(main)
