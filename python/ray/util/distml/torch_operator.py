import logging

from ray.util.distml.base_operator import TrainingOperator

import torch
from torch.nn.modules.loss import _Loss

logger = logging.getLogger(__name__)


class TorchTrainingOperator(TrainingOperator):
    """Class to define the training logic of a PyTorch Model."""
    def __init__(self,
                 operator_config,
                 *args,
                 **kwargs):
        # Initialize the PyTorch training operator
        super(TorchTrainingOperator, self).__init__()

        # Should be set by users in the `register` function.
        self.model = None
        self.optimizer = None
        self.criterion = None

        # Models, optimizers, and criterion registered by users.
        self._model = None
        self._optimizer = None
        self._criterion = None
        self._lr_scheduler = None

        # Data loaders for training and validation, registered by users.
        self._train_loader = None
        self._validation_loader = None

    def register(self,
                 model,
                 optimizer,
                 *args,
                 criterion=None,
                 lr_scheduler=None,
                 **kwargs):
        # TODO(Hao): support custom training loop by allowing multiple model, optimizer,
        # e.g. GAN case.
        if not isinstance(model, torch.nn.Module):
            raise RuntimeError("`model` must be torch.nn.Modules. "
                               "Got: {}".format(model))
        self._model = model
        if not isinstance(optimizer, torch.optim.Optimizer):
            raise RuntimeError("`optimizer` must be torch.optim.Optimizer. "
                               "Got: {}".format(optimizer))
        self._optimizer = optimizer

        if criterion:
            if not isinstance(self._criterion, _Loss):
                raise RuntimeError("`criterion` must be torch.nn.module._Loss. "
                                   "Got: {}".format(self._criterion))
            self._criterion = criterion

        if lr_scheduler:
            # TODO(Hao): register schedulers
            self._lr_schedulers = lr_scheduler
        return self._model, self._optimizer, self._criterion

    def register_data(self, *, train_loader=None, validation_loader=None):
        self._train_loader = train_loader
        self._validation_loader = validation_loader
        # TODO(Hao): convert each data loader to be distributed

    def derive_updates(self, batch):
        """Compute the parameter updates on a given batch of data.

        The `derive_updates` function should be called in conjunction with
        the next `apply_updates` function in order to finish one iterator
        of training.
        """

        # TODO(Hao): 1. Add metric meters
        #             2. add lr_scheduler later.
        # TODO(Hao): make this work
        if not self.model:
            raise RuntimeError("Please set self.model at setup or override "
                               "this function for deriving gradient updates.")
        model = self.model
        if not self.optimizer:
            raise RuntimeError("Please set self.optimizer at setup or override "
                               "this function for deriving gradient updates.")
        optimizer = self.optimizer
        if not self.criterion:
            raise RuntimeError("Please set self.criterion at setup or override "
                               "this function for deriving gradient updates.")
        criterion = self.criterion

        # unroll the batch
        *features, target = batch
        model.train()
        # TODO(Hao): scope the code below using a timer?
        # calculate the loss
        output = model(*features)
        loss = criterion(output, target)
        optimizer.zero_grad()
        loss.backward()

        # TODO(Hao): make the following work and unified with abstract class.
        grads = self._get_gradients(model)
        training_info = {"train_loss": loss.item()}
        return grads, training_info

    def apply_updates(self, updates):
        """Apply the updates using the optimizer.step() in Torch."""
        # TODO(Hao): set the gradients in the optimizer state using updates.
        self._set_gradients(updates)
        self.optimizer.step()

    def validate(self, validation_iterator):
        """Perform validation over validation dataset, represented as an iterator."""
        metric = {}
        for i, batch in enumerate(validation_iterator):
            metric_per_batch = self.validate_step(batch)
            metric.update(metric_per_batch)
        return metric

    def validate_step(self, batch):
        """Perform validation over a data batch."""
        # TODO(Hao): implement this method, referring to RaySGD.
        if not self.model:
            raise RuntimeError("Please set self.model at setup or override "
                               "this function for validation.")
        model = self.model
        if not self.criterion:
            raise RuntimeError("Please set self.criterion at setup or override "
                               "this function for validation.")
        criterion = self.criterion
        *feature, target = batch
        output = model(*feature)
        loss = criterion(output, target)
        batch_metric = {"val_loss": loss.item()}
        return batch_metric

    def get_states(self):
        """Return the states of this training operator."""
        states = {
            "model": self._model.state_dict(),
            "optimizer": self._optimizer.state_dict(),
            "custom": self.get_custom_states()
        }
        if self._lr_scheduler:
            states.update({
                "lr_scheduler": self._lr_scheduler.state_dict()}
            )
        return states

    def load_states(self, states=None, checkpoint=None):
        """Load the states into the operator."""
        if not states and not checkpoint:
            raise RuntimeError("One of `states` and `checkpoint` should be provided. "
                               "Got states: {}, checkpoint: {}.".format(states, checkpoint))
        if not states and checkpoint :
            states = self._load_from_checkpoint(checkpoint)
        self.model.load_state_dict(states["model"])
        self.optimizer.load_state_dict(states["optimizer"])
        if self._lr_scheduler:
            self._lr_scheduler.load_state_dict(states["lr_scheduler"])
        self.load_custom_states(states["custom"])

    def _load_from_checkpoint(self, checkpoint=None):
        """Load state_dict from file path."""
        pass
