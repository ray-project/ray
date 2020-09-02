"""Sub-module introducing keras Callback for reporting the model metrics."""
from typing import Dict
from tensorflow.keras.callbacks import Callback
from ...reporter import Reporter


class TuneKerasReporterCallback(Callback):
    """Tune Callback for Keras.

    The pourpose of the class is to make easy to execute schedulers such as
    ASHA on Keras models for any of the considered metrics.

    Keep in mind that, if you desire to track metrics such as the validation
    metrics (the ones beginning with 'val_') you will need to use 'epoch'
    as value for the frequency.

    Usage example
    ---------------------------------
    Suppose you have a model object called `model` and you want to report the
    metrics, including for instance the AUROC. 
    You can achieve your goals as follows:

    ```python
    from tensorflow.keras.metrics import AUC
    from ray.tune import 

    reporter = build_my_reporter(...)
    model = build_my_model(...)
    model.compile(
        optimizer="nadam",
        loss="binary_crossentropy",
        metrics=[
            "accuracy",
            AUC(curve="ROC", name="AUROC")
        ]
    )

    model.fit(
        X, y,
        callbacks=[
            TuneKerasReporterCallback(reporter)
        ]
    )
    ```
    
    """

    FREQUENCIES = {"batch", "epoch"}

    def __init__(self, reporter: Reporter, frequency: str = "epoch"):
        """Initialize new TuneKerasReporterCallback object.

        Args:
            reporter (Reporter): reporter object to use to send the results.
            frequency (str): frequency of reporting intermediate results.
                The frequency can either be epoch (default) or batch.
                Epoch is preferred over batch to avoid adding significant
                overhead to the training loop.

        Raises:
            ValueError: when an unsupported frequency is given.
        """
        if frequency not in TuneKerasReporterCallback.FREQUENCIES:
            raise ValueError(
                "{} not supported as a frequency.".format(frequency)
            )
        self._frequency = frequency
        self._reporter = reporter
        super(TuneKerasReporterCallback, self).__init__()

    def on_batch_end(self, batch: int, logs: Dict = None):
        """Report metrics on the end of each batch if requested.

        Args:
            batch: int,
                Number of the current batch.
            logs: Dict = None,
                Available informations for the current batch, tipically
                including the required metrics in the 'metrics' keyword
                argument of the model.
        """
        if self.freq == "batch" and logs is not None:
            self._reporter.report(**logs)

    def on_epoch_end(self, epoch: int, logs: Dict = None):
        """Report metrics on the end of each epoch if requested.

        Args:
            epoch: int,
                Number of the current epoch.
            logs: Dict = None,
                Available informations for the current epoch, tipically
                including the required metrics in the 'metrics' keyword
                argument of the model.
        """
        if self.freq == "epoch" and logs is not None:
            self._reporter.report(**logs)
