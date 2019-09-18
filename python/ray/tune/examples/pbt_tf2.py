import os
import random
import ray
import numpy as np

from ray.tune import schedulers
from ray.tune import Trainable
from ray import tune
from ray.tune.logger import Logger
from ray.tune.result import TRAINING_ITERATION, TIME_TOTAL_S, TIMESTEPS_TOTAL


class PBTTrain(Trainable):
    tf = __import__('tensorflow')

    def __getitem__(self, item):
        return getattr(self, item)

    def _setup(self, config):
        self.model = self.tf.keras.models.Sequential([
            self.tf.keras.layers.Dense(
                16, batch_input_shape=[256, 1], activation='relu'),
            self.tf.keras.layers.Dense(16, activation='tanh'),
            self.tf.keras.layers.Dense(1),
        ])

        self.lr = self.tf.Variable(config['training']['lr'])
        self.reg_param = self.tf.Variable(config['training']['reg_param'])
        self.optim = self.tf.keras.optimizers.SGD(self.lr)

        self.ckpt = self.tf.train.Checkpoint(
            model=self.model,
            lr=self.lr,
            reg_param=self.reg_param,
        )

        self.new_config = None

    def _train(self):
        for i in range(100):
            x = self.tf.random.normal([256, 1])
            y_true = x**2

            with self.tf.GradientTape() as tape:
                output = self.model(x)
                reg = 0.
                for w in self.model.trainable_weights:
                    reg += self.tf.reduce_mean(w)
                reg /= len(self.model.trainable_weights)
                loss = self.tf.reduce_mean(
                    self.tf.square(output - y_true)) + self.reg_param * reg

            grads = tape.gradient(loss, self.model.trainable_weights)
            self.optim.apply_gradients(
                zip(grads, self.model.trainable_weights))

        # Eval
        loss = 0.
        for i in range(100):
            x = self.tf.random.normal([256, 1])
            y_true = x**2

            output = self.model(x)

            loss += self.tf.reduce_mean(self.tf.square(output - y_true))
        loss /= 10

        loss_val = loss.numpy()
        tune_dict = {
            'loss': loss_val,
            'lr': self.lr.numpy(),
            'reg_param': self.reg_param.numpy(),
            'done': np.isnan(loss_val)
        }

        return tune_dict

    def _save(self, checkpoint_dir):
        save_path_prefix = self.ckpt.write(
            os.path.join(checkpoint_dir, 'ckpt'))

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


if __name__ == "__main__":
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
            'reg_param': tune.sample_from(lambda spec: random.uniform(1e-2, 2e1)),
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
                TRAINING_ITERATION: 50,
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
        'perturbation_interval': 10,
        'quantile_fraction': .5,
        'resample_probability': .25,  # par param: 25% new, 75% (50% *1.2, 50% *.8)
        'hyperparam_mutations': {
            'training': {
                'lr': lambda: random.uniform(5e-4, 1e-1),
                'reg_param': lambda: random.uniform(1e-2, 5e1),
            }
        }
    }
    scheduler = schedulers.PopulationBasedTraining(**tune_scheduler_config)

    trial_list = tune.run(
        PBTTrain,
        config=config,
        scheduler=scheduler, 
        **config['tune']
    )
