from abc import ABC, abstractmethod
from typing import List, Optional


class SubProgressBarMixin(ABC):
    """Abstract class for operators that support sub-progress bars"""

    @abstractmethod
    def get_sub_progress_bar_names(self) -> Optional[List[str]]:
        """
        Returns list of sub-progress bar names

        This is used to create the sub-progress bars in the progress manager.
        Note that sub-progress bars will be created in the order returned by
        this method.
        """
        ...

    @abstractmethod
    def set_sub_progress_bar(self, name, pg):
        """
        Sets sub-progress bars

        name: name of sub-progress bar
        pg: SubProgressBar instance (progress_manager.py)
        """
        # Skipping type-checking for circular imports
        ...
