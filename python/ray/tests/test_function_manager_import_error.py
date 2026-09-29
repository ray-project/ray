import logging
import sys
from unittest.mock import MagicMock, patch

import pytest

import ray
from ray._private.function_manager import FunctionActorManager


def test_actor_import_error_is_logged(caplog):
    worker = MagicMock()
    worker.gcs_client.internal_kv_get.return_value = b"actor entry"
    manager = FunctionActorManager(worker)
    descriptor = MagicMock()
    descriptor.function_id.binary.return_value = b"actor-id"
    actor_entry = {
        "job_id": ray.JobID.from_int(1).binary(),
        "class_name": b"BrokenActor",
        "module": b"missing_module",
        "class": b"pickled actor",
        "actor_method_names": b'["run"]',
    }

    with patch("ray._private.function_manager.pickle.loads") as loads:
        loads.side_effect = [
            actor_entry,
            ModuleNotFoundError("No module named 'missing_module'"),
        ]
        with caplog.at_level(logging.ERROR, logger="ray._private.function_manager"):
            actor_class = manager._load_actor_class_from_gcs(
                ray.JobID.from_int(1), descriptor
            )

    assert "Failed to load actor class BrokenActor" in caplog.text
    assert "No module named 'missing_module'" in caplog.text
    with pytest.raises(RuntimeError, match="failed to import on the worker"):
        actor_class().run()


if __name__ == "__main__":
    sys.exit(pytest.main(["-sv", __file__]))
