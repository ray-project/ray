import inspect
import logging

import torch
from ray.util.sgd.torch import TrainingOperator
from ray.util.sgd.utils import RayFileLock

logger = logging.getLogger(__name__)


class CreatorOperator(TrainingOperator):
    """A subclass of TrainingOperator specifically for defining training
    state using creator functions.
    """
    @classmethod
    def set_creators(cls, model_creator, optimizer_creator,
                     data_creator=None, loss_creator=None,
                     scheduler_creator=None, serialize_data_creation=True):
        cls.model_creator = model_creator
        cls.optimizer_creator = optimizer_creator
        cls.data_creator = data_creator
        cls.loss_creator = loss_creator
        cls.scheduler_creator = scheduler_creator
        cls.serialize_data_creation = serialize_data_creation

    def _validate_loaders(self, loaders):
        assert loaders, "Loaders need to be returned in data_creator."
        if isinstance(loaders, (tuple, list)):
            if len(loaders) == 1:
                return loaders, None
            elif len(loaders) == 2:
                return loaders
            else:
                raise ValueError(
                    f"Number of loaders must be <= 2. Got {loaders}")
        # No great way of checking type otherwise
        return loaders, None

    def _initialize_dataloaders(self, config):
        logger.debug("Instantiating dataloaders.")
        loaders = None
        if self.serialize_data_creation:
            logger.debug("Serializing the dataloading process.")
            with RayFileLock():
                loaders = self.data_creator(config)
        else:
            loaders = self.data_creator(config)
        train_loader, val_loader = self._validate_loaders(loaders)

        return train_loader, val_loader

    def setup(self, config):
        kwargs = {}
        logger.debug("Loading data.")
        train_loader = None
        validation_loader = None


        kwargs["train_loader"] = train_loader
        kwargs["validation_loader"] = validation_loader

        logger.debug("Creating model")
        models = self.model_creator(config)

        kwargs["models"] = models

        logger.debug("Creating optimizer.")
        optimizers = self.optimizer_creator(models, config)

        kwargs["optimizers"] = optimizers

        if self.scheduler_creator:
            logger.debug("Creating scheduler.")
            schedulers = self.scheduler_creator(optimizers, config)
            kwargs["schedulers"] = schedulers

        if self.loss_creator:
            logger.debug("Creating loss.")
            if inspect.isclass(self.loss_creator) and issubclass(
                self.loss_creator, torch.nn.modules.loss._Loss):
                criterion = self.loss_creator()
            else:
                criterion = self.loss_creator(config)
            kwargs["criterion"] = criterion

        state = self.register(**kwargs)
        self.models, self.optimizers = state[:2]
        if isinstance(self.models, tuple):
            self.model = self.models[0]
        else:
            self.model = self.models

        if isinstance(self.optimizers, tuple):
            self.optimizer = self.optimizers[0]
        else:
            self.optimizer = self.optimizers

        if len(state) >= 3:
            self.criterion = state[2]
        if len(state) == 4:
            self.schedulers = state[3]
            if isinstance(self.schedulers, tuple):
                self.scheduler = self.schedulers[0]
            else:
                self.scheduler = self.schedulers