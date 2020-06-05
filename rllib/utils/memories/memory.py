from abc import abstractmethod, ABCMeta

from ray.rllib.utils.annotations import DeveloperAPI


@DeveloperAPI
class Memory(metaclass=ABCMeta):
    @DeveloperAPI
    def __init__(self, record_space, capacity=100000):
        """Initializes a Memory object.

        Args:
            record_space (gym.spaces.Space): The record Space to
            capacity (int): Max number of records to store in the buffer.
                When the buffer overflows the old memories are dropped.
        """
        self.record_space = record_space
        self.capacity = capacity

    @abstractmethod
    def __len__(self):
        """
        Returns:
             int: The number of records currently in the buffer.
        """
        raise NotImplementedError

    @DeveloperAPI
    @abstractmethod
    def add(self, records, **kwargs):
        """Adds the given records to this Memory.

        Args:
            records (dict): Flat record dict with column name keys (str)
                and data as values.
            **kwargs: For forward compatibility.
        """
        raise NotImplementedError

    @DeveloperAPI
    @abstractmethod
    def sample(self, batch_size):
        """Samples a batch of `batch_size` records from the Memory.

        Args:
            batch_size (int): How many transitions to sample.

        Returns:

        """
        raise NotImplementedError

    @DeveloperAPI
    @abstractmethod
    def sample_idxes(self, batch_size):
        """Returns batch_size indices according to underlying retrieval logic.

        Args:
            batch_size (int): The number of indices to return.

        Returns:
            List[int]: The list of sampled indices.
        """
        raise NotImplementedError

    @DeveloperAPI
    @abstractmethod
    def sample_with_idxes(self, idxes):
        """Returns the records from the memory at the given indices.

        Args:
            idxes (List[int]): The list of indices, for which to return the
                records.

        Returns:
            Dict[str,any]: The dict of (batched) column records.
        """
        raise NotImplementedError

    @DeveloperAPI
    @abstractmethod
    def stats(self, debug=False):
        """Returns a dict with stats about this Memory object.

        Args:
            debug (bool): Whether to include extra debugging keys in the
                returned stats dict.

        Returns:
            dict: The dict with stats information in it.
        """
        raise NotImplementedError
