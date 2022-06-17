from collections import Counter
from typing import Dict, List, Optional, Union

from tensorflow.keras.callbacks import Callback as KerasCallback

from ray.air import session
from ray.air.checkpoint import Checkpoint
from ray.util.annotations import PublicAPI


class _Callback(KerasCallback):
    """Base class for Air's Keras callbacks."""

    _allowed = [
        "batch_begin",
        "batch_end",
        "epoch_begin",
        "epoch_end",
        "train_batch_begin",
        "train_batch_end",
        "test_batch_begin",
        "test_batch_end",
        "predict_batch_begin",
        "predict_batch_end",
        "train_begin",
        "train_end",
        "test_begin",
        "test_end",
        "predict_begin",
        "predict_end",
    ]

    def __init__(self, on: Union[str, List[str]] = "validation_end"):
        super(_Callback, self).__init__()

        if not isinstance(on, list):
            on = [on]
        if any(w not in self._allowed for w in on):
            raise ValueError(
                "Invalid trigger time selected: {}. Must be one of {}".format(
                    on, self._allowed
                )
            )
        self._on = on

    def _handle(self, logs: Dict, when: str):
        raise NotImplementedError

    def on_batch_begin(self, batch, logs=None):
        if "batch_begin" in self._on:
            self._handle(logs, "batch_begin")

    def on_batch_end(self, batch, logs=None):
        if "batch_end" in self._on:
            self._handle(logs, "batch_end")

    def on_epoch_begin(self, epoch, logs=None):
        if "epoch_begin" in self._on:
            self._handle(logs, "epoch_begin")

    def on_epoch_end(self, epoch, logs=None):
        if "epoch_end" in self._on:
            self._handle(logs, "epoch_end")

    def on_train_batch_begin(self, batch, logs=None):
        if "train_batch_begin" in self._on:
            self._handle(logs, "train_batch_begin")

    def on_train_batch_end(self, batch, logs=None):
        if "train_batch_end" in self._on:
            self._handle(logs, "train_batch_end")

    def on_test_batch_begin(self, batch, logs=None):
        if "test_batch_begin" in self._on:
            self._handle(logs, "test_batch_begin")

    def on_test_batch_end(self, batch, logs=None):
        if "test_batch_end" in self._on:
            self._handle(logs, "test_batch_end")

    def on_predict_batch_begin(self, batch, logs=None):
        if "predict_batch_begin" in self._on:
            self._handle(logs, "predict_batch_begin")

    def on_predict_batch_end(self, batch, logs=None):
        if "predict_batch_end" in self._on:
            self._handle(logs, "predict_batch_end")

    def on_train_begin(self, logs=None):
        if "train_begin" in self._on:
            self._handle(logs, "train_begin")

    def on_train_end(self, logs=None):
        if "train_end" in self._on:
            self._handle(logs, "train_end")

    def on_test_begin(self, logs=None):
        if "test_begin" in self._on:
            self._handle(logs, "test_begin")

    def on_test_end(self, logs=None):
        if "test_end" in self._on:
            self._handle(logs, "test_end")

    def on_predict_begin(self, logs=None):
        if "predict_begin" in self._on:
            self._handle(logs, "predict_begin")

    def on_predict_end(self, logs=None):
        if "predict_end" in self._on:
            self._handle(logs, "predict_end")


@PublicAPI(stability="beta")
class Callback(_Callback):
    def __init__(
        self,
        metrics: Optional[Union[str, List[str], Dict[str, str]]] = None,
        on: Union[str, List[str]] = "epoch_end",
        frequency: Union[int, List[int]] = 1,
    ):
        """
        Args:
            metrics: Metrics to report. If this is a list, each item describes
            the metric key reported to Keras, and it will reported under the
            same name. If this is a dict, each key will be the name reported
            and the respective value will be the metric key reported to Keras.
            If this is None, all Keras logs will be reported.
        on: When to report metrics. Must be one of
            the Keras event hooks (less the ``on_``), e.g.
            "train_start", or "predict_end". Defaults to "epoch_end".
        frequency: Checkpoint frequency. If this is an integer `n`,
            checkpoints are saved every `n` times each hook was called. If
            this is a list, it specifies the checkpoint frequencies for each
            hook individually.

        You can use this in both TuneSession and TrainSession.

        Example:
            .. code-block: python

            ############# Using it in TrainSession ###############
            from ray.air.callbacks.keras import Callback
            def train_loop_per_worker():
                strategy = tf.distribute.MultiWorkerMirroredStrategy()
                with strategy.scope():
                    model = build_model()
                    #model.compile(...)
                model.fit(dataset_shard, callbacks=[Callback()])
        """
        if isinstance(frequency, list):
            if not isinstance(on, list) or len(frequency) != len(on):
                raise ValueError(
                    "If you pass a list for checkpoint frequencies, the `on` "
                    "parameter has to be a list with the same length."
                )

        self._frequency = frequency
        super(Callback, self).__init__(on)
        self._metrics = metrics
        self._counter = Counter()

    def _handle(self, logs: Dict, when: str = None):
        self._counter[when] += 1

        if isinstance(self._frequency, list):
            index = self._on.index(when)
            freq = self._frequency[index]
        else:
            freq = self._frequency

        checkpoint = None
        if freq > 0 and self._counter[when] % freq == 0:
            self.model.save("my_model", overwrite=True)
            checkpoint = Checkpoint.from_directory("my_model")

        if not self._metrics:
            report_dict = logs
        else:
            report_dict = {}
            for key in self._metrics:
                if isinstance(self._metrics, dict):
                    metric = self._metrics[key]
                else:
                    metric = key
                report_dict[key] = logs[metric]

        session.report(report_dict, checkpoint=checkpoint)
