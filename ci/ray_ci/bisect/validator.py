import abc

from ray_release.test import Test


class Validator(abc.ABC):
    @abc.abstractmethod
    def run(self, test: Test, revision: str) -> bool:
        """
        Validate whether the test is passing or failing on the given revision
        """
        pass
