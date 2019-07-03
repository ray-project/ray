from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import keras
from ray.tune import track


class TuneReporterCallback(keras.callbacks.Callback):
    def __init__(self, reporter=None, freq="batch", logs={}):
        self.reporter = reporter or track.log
        self.iteration = 0
        if freq not in ["batch", "epoch"]:
            raise ValueError("{} not supported as a frequency.".format(freq))
        self.freq = freq
        super(TuneReporterCallback, self).__init__()

    def on_batch_end(self, batch, logs={}):
        if not self.freq == "batch":
            return
        self.iteration += 1
        for metric in list(logs):
            if "loss" in metric and "neg_" not in metric:
                logs["neg_" + metric] = -logs[metric]
        self.reporter(keras_info=logs, mean_accuracy=logs["acc"])

    def on_epoch_end(self, batch, logs={}):
        if not self.freq == "epoch":
            return
        self.iteration += 1
        for metric in list(logs):
            if "loss" in metric and "neg_" not in metric:
                logs["neg_" + metric] = -logs[metric]
        self.reporter(keras_info=logs, mean_accuracy=logs["acc"])
