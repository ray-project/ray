import abc


class RLTrainer(abc.ABC):
    # A base framework agnostic interface class for training RLModules
    # module: RLModule: A reference to the RLModule that we are training

    @abc.abstractmethod
    def update(self, batch, **kwargs):
        # Performs an update and returns anything that might be needed inside the
        # algorithm.
        raise NotImplementedError

    @abc.abstractmethod
    def make_module(self, module_config):
        # a protected abstract method that creates the RLModule
        raise NotImplementedError

    @abc.abstractmethod
    def compile_results(self, batch, fwd_out, update_out, **kwargs):
        # a protected abstract method that computes the loss
        raise NotImplementedError


class SARLTrainer(RLTrainer, abc.ABC):
    # A base framework agnostic class for training single-agent RLModules
    # module: RLModule
    def __init__(self, config):
        # config: a dictionary of parameters
        self.config = config

    def update(
        self, batch, fwd_kwargs=None, loss_kwargs=None, grad_kwargs=None, **kwargs
    ):
        # uses module.forward_train(), compute_loss, and
        # compute_grads_and_apply_if_needed() to perform updates, override if
        #  needed (e.g. DQN)
        batch = self._prepare_sample_batch(batch)
        fwd_out = self.module.forward_train(batch, **fwd_kwargs)
        print(fwd_out)
        # loss_out = self.compute_loss(batch, fwd_out, **loss_kwargs)
        # update_out = self.compute_grads_and_apply_if_needed(batch,
        #           fwd_out, loss_out, **grad_kwargs)
        # compiled_results = self.compile_results(batch,
        #           fwd_out, update_out, **kwargs)
        # return compiled_results
        return "here"

    @abc.abstractmethod
    def compute_loss(self, batch, fwd_out, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def compute_grads_and_apply_if_needed(self, batch, fwd_out, loss_out, **kwargs):
        raise NotImplementedError

    @abc.abstractmethod
    def _prepare_sample_batch(self, batch):
        raise NotImplementedError
