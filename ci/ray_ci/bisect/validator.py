import abc


class Validator(abc.ABC):
    @abc.abstractmethod
    def run(self, test: str) -> bool:
        """
        Validate whether the test is passing or failing on the given revision
        """
        pass
