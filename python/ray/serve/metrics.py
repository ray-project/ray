from ray.util import metrics
from typing import Tuple, Optional, Dict

REPLICA_TAG = "N/A"
DEPLOYMENT = "N/A"


class Counter(metrics.Counter):
    def __init__(
        self, name: str, description: str = "", tag_keys: Optional[Tuple[str]] = None
    ):
        # Make sure customer tags are not using the serve default tags.
        assert "deployment" not in tag_keys
        assert "replica" not in tag_keys
        # Get serve tag inserted:
        if tag_keys:
            tag_keys = ("deployment", "replica") + tag_keys
        else:
            tag_keys = ("deployment", "replica")
        super().__init__(name, description, tag_keys)
        global REPLICA_TAG, DEPLOYMENT
        self._set_default_tags({"deployment": REPLICA_TAG, "replica": DEPLOYMENT})

    def _set_default_tags(self, default_tags: Dict[str, str]):
        super().set_default_tags(default_tags)

    def set_default_tags(self, default_tags: Dict[str, str]):
        # Make sure customer tags are not using the serve default tags.
        assert "deployment" not in default_tags
        assert "replica" not in default_tags
        global REPLICA_TAG, DEPLOYMENT
        default_tags["deployment"] = REPLICA_TAG
        default_tags["replica"] = DEPLOYMENT
        self._set_default_tags(default_tags)
