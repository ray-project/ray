import abc
from typing import List, Optional, Dict


class SGDCallback(metaclass=abc.ABCMeta):
    def handle_result(self, results: Optional[List[Dict]]):
        pass

    def start(self):
        pass

    def shutdown(self):
        pass
