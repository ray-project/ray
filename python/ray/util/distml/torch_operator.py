from ray.util.distml.base_operator import TrainingOperator

class PyTorchTrainingOperator(TrainingOperator):

    def __init__(self, *args, **kwargs):
        # do something.
        super(PyTorchTrainingOperator, self).__init__()


    def derive_updates(self, batch, batch_info):
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

    def set_parameters(self, params):
        pass

    def _set_updates(self, updates):
        """Set the gradients for the model."""
        pass

    def _get_updates(self):
        """Get the gradients after backward"""
        pass