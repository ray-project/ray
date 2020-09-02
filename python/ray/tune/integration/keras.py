"""Sub-module introducing keras Callback for reporting the model metrics."""
from typing import Dict
from tensorflow.keras.callbacks import Callback
from ...reporter import Reporter


class TuneReporterCallback(Callback):
    """Tune Callback for Keras.

    The pourpose of the class is to make easy to execute schedulers such as
    ASHA on Keras models for any of the considered metrics.

    Keep in mind that, if you desire to track metrics such as the validation
    metrics (the ones beginning with 'val_') you will need to use 'epoch'
    as value for the frequency.
    """

    FREQUENCIES = {"batch", "epoch"}

    def __init__(self, reporter: Reporter, frequency: str = "epoch"):
        """Initialize new TuneReporterCallback object.

        Args:
            reporter (Reporter): reporter object to use to send the results.
            frequency (str): frequency of reporting intermediate results.
                The frequency can either be epoch (default) or batch.
                Epoch is preferred over batch to avoid adding significant
                overhead to the training loop.

        Raises:
            ValueError: when an unsupported frequency is given.
        """
        if frequency not in TuneReporterCallback.FREQUENCIES:
            raise ValueError(
                "{} not supported as a frequency.".format(frequency)
            )
        self._frequency = frequency
        self._reporter = reporter
        super(TuneReporterCallback, self).__init__()

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
