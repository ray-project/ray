"""Abstract class for framework-specific training operators."""
from abc import ABCMeta
from abc import abstractmethod


class TrainingOperator(metaclass=ABCMeta):
    """Abstract class to define the training loop of a model.

    This class is subclassed by the framework-specific operator implementations.
    Note that we must separately expose the two functions for (1) gradient derivation
    and (2) gradient application in order for Ray collective backend to take over.
    The specific training logic bundled with frameworks (JAX, PyTorch) should be implemented
    in the subclasses of this class.
    """
    def __init__(self, operator_config, *args, **kwargs):
        self._config = operator_config

    @abstractmethod
    def register(self,
                 models,
                 optimizers,
                 *args,
                 criterion=None,
                 lr_schedulers=None,
                 **kwargs):
        """Register the model, optimizer, and loss with ray.distml.

        The function is instantiated in the framework-specific subclass. It
        is expected to be called by the user in self.setup().
        """
        raise NotImplementedError()

    @abstractmethod
    def register_data(self, *, train_loader=None, validation_loader=None):
        """Register batch-emitting data loaders."""
        raise NotImplementedError()

    @abstractmethod
    def setup(self, operator_config):
        """Instantiated by users

        In this method, the user should register the model, optimizer, criterion,
        and data loaders to the operator class.
        """
        raise NotImplementedError()

    @abstractmethod
    def derive_updates(self, *args, **kwargs):
        """The first substep in train_step that derives the updates.

        This method should be instantiated by subclass operators.
        """
        raise NotImplementedError()

    @abstractmethod
    def apply_updates(self, updates):
        """The second sub-step in train_step that derives the updates.

        This method should be instantiated by subclass operators.
        """
        raise NotImplementedError()

    @abstractmethod
    def validate(self, *args, **kwargs):
        raise NotImplementedError()

    @abstractmethod
    def validate_step(self, *args, **kwargs):
        raise NotImplementedError()


    def get_custom_states(self, *args, **kwargs):
        """Functions to be optionally override by users to represent any custom states.

        See ``save_parameters`` for more details.
        """
        pass

    def load_custom_states(self, states, *args, **kwargs):
        """Functions to be optionally override by users to load any custom states.

        See ``load_parameters`` for more details.
        """
        pass


    @abstractmethod
    def save_states(self, states):
        """Save the states to a file path.

         This function shall be instantiated in framework-specific operator
         implementations.
         """
        raise NotImplementedError()

    @abstractmethod
    def get_states(self, states):
        """Return the states for the operator in a dict."""
        raise NotImplementedError()

    @abstractmethod
    def load_states(self, checkpoint):
        """Load the states from a file path.

        This functions shall be instantiated in framework-specific operators
        implementations.
        """
        raise NotImplementedError()
