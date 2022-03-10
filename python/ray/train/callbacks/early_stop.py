from typing import List, Dict

import operator

from ray.train import TrainingCallback


OPS = {
    ">": operator.gt,
    "<": operator.lt,
    ""
}


class EarlyStoppingCallback(TrainingCallback):
    """Callback to terminate training after a certain threshold is reached.

    If the value for the provided metric of any worker meets the provided
    stopping criteria, training execution will immediately be halted and not
    run to completion.

    If an ``EarlyStoppingCallback`` is provided to ``Trainer.run``,
    and training is stopped, then the return values of the training function
    will not be returned by the Trainer.

    Args:
        key (str): The name of the key in ``train.report()`` who's value to
            compare against threshold.
        threshold (Any): The value to compare the metric against.
        greater (bool): If ``True``, then stop training if the value in
            ``key`` for any worker is greater than threshold. If ``False``,
            then stop training if value in ``key`` is less than or equal to
            the threshold. Ignored if a ``comparator_fn`` is passed in.


    """

    def __init__(self, key: str, threshold, greater, comparator_fn: Callable):
        self.results_preprocessor = [ValuePreprocessor(...)]
        self.threshold = threshold
        self.key = key

        if greater:
            self.operator = operator.gt
        else:
            self.operator = operator.le

        self.early_stop = False

    def handle_result(self, results: List[Dict], **info):
        assert len(results) == 1


        if self.operator(results[self.key], self.threshold):
            self.early_stop = True

    def should_terminate(self) -> bool:
        return self.early_stop

