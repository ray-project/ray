import os
import random
import tensorflow as tf
import ray

from ray.tune import schedulers
from ray.tune import Trainable
from ray import tune
from ray.tune.logger import Logger
from ray.tune.result import TRAINING_ITERATION, TIME_TOTAL_S, TIMESTEPS_TOTAL

ray.init(local_mode=False)

config = {
    'seed': 1,
    'verbose': False,
    'model': {
        'h_size': 256,
        'nb_h_layer': 3,
        'activation': 'leakyrelu',
    },
    'training': {
        'batch_size': 64,
        'lr': tune.sample_from(lambda spec: random.uniform(5e-4, 5e-2)),
        'optimizer': 'adam',
        'reg_param': tune.sample_from(lambda spec: random.uniform(1e-2, 5e1)),
    },
    'eval': {
        'batch_size': 256,
    },
    'tune': {
        'resources_per_trial': {
            'cpu': 1,
            'gpu': 0,
        },
        "stop": {
            TRAINING_ITERATION: 32,
        },
        'local_dir': os.path.dirname(os.path.realpath(__file__)) + '/pbt',
        'num_samples': 16,
        'checkpoint_freq': 0,
        'checkpoint_at_end': True,
        'verbose': 1,
    },
}

tune_scheduler_config = {
    'time_attr': TRAINING_ITERATION,
    'metric': 'loss',
    'mode': 'min',
    'perturbation_interval': 4,
    'quantile_fraction': .5,
    'resample_probability': .25,  # par param: 25% new, 75% (50% *1.2, 50% *.8)
    'hyperparam_mutations': {
        'training': {
            'lr': lambda: random.uniform(5e-4, 1e-1),
            'reg_param': lambda: random.uniform(1e-2, 5e1),
        }
    }
}


class PBTTrain(Trainable):
    def __getitem__(self, item):
        return getattr(self, item)

    def _setup(self, config):
        self.model = tf.keras.models.Sequential([
            tf.keras.layers.Dense(
                16, batch_input_shape=[256, 1], activation='relu'),
            tf.keras.layers.Dense(16, activation='tanh'),
            tf.keras.layers.Dense(1),
        ])

        self.lr = tf.Variable(config['training']['lr'])
        self.reg_param = tf.Variable(config['training']['reg_param'])
        self.optim = tf.keras.optimizers.SGD(self.lr)

        self.ckpt = tf.train.Checkpoint(
            model=self.model,
            lr=self.lr,
            reg_param=self.reg_param,
        )

        self.new_config = None

    def _train(self):
        for i in range(100):
            input = tf.random.normal([256, 1])
            y_true = input**2

            with tf.GradientTape() as tape:
                output = self.model(input)
                reg = 0.
                for w in self.model.trainable_weights:
                    reg += tf.reduce_mean(w)
                reg /= len(self.model.trainable_weights)
                loss = tf.reduce_mean(
                    tf.square(output - y_true)) + self.reg_param * reg

            grads = tape.gradient(loss, self.model.trainable_weights)
            self.optim.apply_gradients(
                zip(grads, self.model.trainable_weights))

        # Eval
        loss = 0.
        for i in range(100):
            input = tf.random.normal([256, 1])
            y_true = input**2

            output = self.model(input)

            loss += tf.reduce_mean(tf.square(output - y_true))
        loss /= 10

        tune_dict = {
            'loss': loss.numpy(),
            'lr': self.lr.numpy(),
            'reg_param': self.reg_param.numpy(),
        }

        return tune_dict

    def _save(self, checkpoint_dir):
        save_path_prefix = self.ckpt.write(
            os.path.join(checkpoint_dir, 'ckpt'))

        # This is needed to comply with tune framework
        open(save_path_prefix, 'a').close()

        return save_path_prefix

    def _restore(self, save_path_prefix):
        self.ckpt.restore(save_path_prefix)

        if self.new_config is not None:
            for key, val in self.new_config.items():
                self[key].assign(val)

            self.new_config = None

        return

    def reset_config(self, new_config):
        self.new_config = {
            'lr': new_config['training']['lr'],
            'reg_param': new_config['training']['reg_param'],
        }

        return True


class TF2Logger(Logger):
    def _init(self):
        self._file_writer = tf.summary.create_file_writer(self.logdir)

    def on_result(self, result):
        with tf.device('/CPU:0'):
            with self._file_writer.as_default():
                step = result.get(
                    TIMESTEPS_TOTAL) or result[TRAINING_ITERATION]

                tmp = result.copy()
                for k in [
                        "config", "pid", "timestamp", TIME_TOTAL_S,
                        TRAINING_ITERATION
                ]:
                    if k in tmp:
                        del tmp[k]  # not useful to log these

                for attr, value in tmp.items():
                    if type(value) in [int, float]:
                        tf.summary.scalar(attr, value, step=step)
        self._file_writer.flush()

    def flush(self):
        self._file_writer.flush()

    def close(self):
        self._file_writer.close()


scheduler = schedulers.PopulationBasedTraining(**tune_scheduler_config)

trial_list = tune.run(
    PBTTrain,
    config=config,
    scheduler=scheduler,
    loggers=[TF2Logger],
    **config['tune'])
