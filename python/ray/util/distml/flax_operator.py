import numpy as np
import cupy as cp
from jax import grad, value_and_grad
import jax.numpy as jnp
from jax.tree_util import tree_flatten, tree_unflatten, tree_map

import flax
import flax.traverse_util as traverse_util
from flax.core import FrozenDict, unfreeze, freeze
from flax import serialization

from ray.util.distml.jax_operator import JAXTrainingOperator
from ray.util.sgd.utils import TimerCollection, AverageMeterCollection

tqdm = None
try:
    from tqdm import tqdm
except ImportError:
    pass


class FLAXTrainingOperator(JAXTrainingOperator):

    def __init__(self, operator_config):
        super(FLAXTrainingOperator, self).__init__(operator_config)

    def register(self, 
                 *,
                 model, 
                 optimizer, 
                 criterion, 
                 lr_schedulers=None, 
                 jit_mode=False):
        """Register a few critical information about the model to operator."""
        self.criterion = criterion
        if lr_schedulers:
            self.lr_schedulers = lr_schedulers
            print("WARNING: jax not support learning rate scheduler." 
                  "This will not work.")
        
        self._register_model(model)
        self._register_optimizer(optimizer)
            
    def _register_model(self, model):
        self.model = model

    def _register_optimizer(self, optimizer):
        self.optimizer = optimizer

    def loss_func(self, params, batch):
        inputs, targets = batch
        logits = self.model.apply(params, inputs)
        return self.criterion(logits, targets)

    def derive_updates(self, batch):
        loss_val, gradient = self._calculate_gradient(self.optimizer, batch)
        return loss_val.item(), tree_flatten(gradient)[0]

    def apply_updates(self, gradient, num_workers):
        if not hasattr(self, "tree"):
            self.tree = tree_flatten(self.optimizer.target)[1]
    
        if isinstance(gradient, dict):
            gradient = freeze(traverse_util.unflatten_dict(gradient))
        else:
            gradient = tree_unflatten(self.tree, gradient)
        gradient = tree_map(lambda x: x/num_workers, gradient)

        self.optimizer = self.optimizer.apply_gradient(gradient)
        self.train_step_num += 1

    def _calculate_gradient(self, optimizer, batch):
        params = optimizer.target
        loss_val, gradient = value_and_grad(self.loss_func)(params, batch)
        return loss_val, gradient

    def validate(self, info={}):
        if not hasattr(self, "model"):
            raise RuntimeError("model has not registered.")
        if not hasattr(self, "criterion"):
            raise RuntimeError("criterion unset. Please register criterion in setup.")
        params = self.optimizer.target
        validation_loader = self.validation_loader
        metric_meters = AverageMeterCollection()
        
        for batch_idx, batch in enumerate(validation_loader):
            batch_info = {"batch_idx": batch_idx}
            batch_info.update(info)
            metrics = self.validate_step(params, batch, batch_info)
            metric_meters.update(metrics, n=metrics.pop("samples_num", 1))
        return metric_meters.summary()

    def validate_step(self, params, batch, batch_info):
        criterion = self.criterion
        predict_fun = self.model.apply
        # unpack features into list to support multiple inputs model
        inputs, targets = batch

        with self.timers.record("eval_fwd"):
            outputs = predict_fun(params, inputs)
            loss = criterion(outputs, targets)
            prediction_class = jnp.argmax(outputs, axis=1)
            targets_class = jnp.argmax(targets, axis=1)

        acc = jnp.mean(prediction_class == targets_class)
        samples_num = targets.shape[0]
        return {
            "val_loss": loss.item(),
            "val_accuracy": acc.item(),
            "samples_num": samples_num
        }

    def save_parameters(self, checkpoint):
        # Use flax.serialization package to turn target to bytes.
        # We write these bytes to checkpoint.
        bytes_output = serialization.to_bytes(self.optimizer.target)
        with open(checkpoint, "wb") as f:
            f.write(bytes_output)

    def load_parameters(self, checkpoint):
        with open(checkpoint, "rb") as f:
            bytes_output = f.read()

        optimizer = self.optimizer
        states = serialization.to_state_dict(optimizer)
        target = serialization.from_bytes(states["target"], bytes_output)
        states["target"] = target
        optimizer = serialization.from_state_dict(optimizer, states)
        self.optimizer = optimizer

    def get_parameters(self, cpu):
        params = self.optimizer.target
        flatten_params, tree = tree_flatten(params)

        if cpu:
            flatten_params = list(map(np.asarray, flatten_params))
        return flatten_params

    def get_named_parameters(self, cpu):
        params = self.optimizer.target
        if cpu:
            params = tree_map(lambda x: np.asarray(x), params)
        params_flat_dict = traverse_util.flatten_dict(unfreeze(params))
        return params_flat_dict

    def set_parameters(self, new_params):
        assert isinstance(new_params, dict)
        optimizer = self.optimizer
        new_params = traverse_util.unflatten_dict(new_params)

        states = serialization.to_state_dict(optimizer)
        states["target"] = new_params

        self.optimizer = serialization.from_state_dict(optimizer, states)

    def reset_optimizer_for_params(self, params):
        self.tree = tree_flatten(params)[1]
        self.optimizer = self.optimizer.init_param_state(params)
