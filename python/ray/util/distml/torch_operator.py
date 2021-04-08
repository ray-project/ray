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

        # Models, optimizers, and criterion registered by users.
        self._model = None
        self._optimizer = None
        self._criterion = None
        self._lr_schedulers = None

        # Data loaders for training and validation, registered by users.
        self._train_loader = None
        self._validation_loader = None

    def register(self,
                 model,
                 optimizer,
                 *args,
                 criterion=None,
                 lr_schedulers=None,
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

        if lr_schedulers:
            # TODO(Hao): register schedulers
            pass
        return self._model, self._optimizer, self._criterion

    def register_data(self, *, train_loader=None, validation_loader=None):
        self._train_loader = train_loader
        self._validation_loader = validation_loader
        # TODO(Hao): convert each data loader to be distributed

    def derive_updates(self, batch, batch_info):

        # TODO(Hao): 1. Add metric meters
        #             2. add lr_scheduler later.
        model = self.model
        optimizer = self.optimizer
        loss_func = self.loss_func

        *features, target = batch
        output = model(*features)
        loss = loss_func(output, target)

        # check it is a torch optimizer
        optimizer.zero_grad()
        loss.backward()
        grads = self._get_gradients(model)
        return grads

    def apply_updates(self, updates):
        self._set_updates(updates)
        self.optimizer.step()
        return

    def validate(self, validation_iterator):
        for i, batch in enumerate(validation_iterator):
            self.validate_step(batch)

    def validate_step(self, batch):
        """Perform validation over a data batch."""
        # TODO(Hao): implement this method, referring to RaySGD.
        pass

    def save_parameters(self, checkpoint):
        # TODO(Hao)
        pass

    def load_parameters(self, checkpoint):
        # TODO(Hao)
        pass