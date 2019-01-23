# This is the strangest workaround for Python 2.7. Somehow all the unique
# ids end up in this namespace (try "import ray; ray.ObjectID") and we need
# this so pickle can find them. Hopefully there is another way.

from ray.raylet import (UniqueID, ObjectID, DriverID, ClientID, ActorID,
                        ActorHandleID, FunctionID, ActorClassID, TaskID,
                        _config)  # noqa: E402
