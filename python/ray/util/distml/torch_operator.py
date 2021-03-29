class TrainingOperator:

    def __init__(self):
        pass

    def setup():
        """Function that needs to be override by users."""
        pass

    def register(self, *, model, optimizer, critierion):
        """Register a few critical information about the model to operator."""
        model = ...
        optimizer = ...
        self._register_model(model)
        self._register_optimizer(optimizer)

    def _register_model(self, model):
        pass

    def _register_optimizer(self, optimizer):
        pass
