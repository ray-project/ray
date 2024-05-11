import abc

from ray_release.test import Test


class Validator(abc.ABC):
    @abc.abstractmethod
    def run(self, test: Test) -> bool:
        """
        Validate whether the test is passing or failing on the given revision
        """
        pass
