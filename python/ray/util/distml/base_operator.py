class TrainingOperator:
    """Abstract class to define the training loop of a model.

    Note that we must separately expose the two functions for (1) gradient derivation
    and (1) gradient application in order for Ray collective backend to take over.
    The specific training logic bundled with frameworks (JAX, PyTorch) should be implemented
    in the subclasses of this class.
    """
    def __init__(self):
        self.setup()

    def register(self,
                 *,
                 models,
                 optimizers,
                 loss=None):
        """Register the model, optimizer, and loss with ray.distml.

        This function needs to be called in self.setup().
        """
        self._models = models
        self._optimizers = optimizers
        self._loss = loss

    def setup(self):
        """Instantiated by users."""
        raise NotImplementedError()

    def train_batch(self, batch):
        model = self._model
        optimizer = self._optimizer
        loss = self._loss
        *features, target = batch
        # by default we use GPUs
        loss, updates = self.derive_updates(features, target,
                                      model, optimizer, loss)
        self.apply_updates(updates)
        # some metrics to return
        metrics = {"training_loss": loss}
        return metrics

    def derive_updates(self, *args, **kwargs):
        """Should be instantiated by subclass operators."""
        raise NotImplementedError()

    def apply_updates(self, updates):
        raise NotImplementedError()
