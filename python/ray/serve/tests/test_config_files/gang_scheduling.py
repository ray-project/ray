import json
import os

from ray import serve


@serve.deployment
class GangApp:
    def __call__(self, *args):
        ctx = serve.context._get_internal_replica_context()
        gc = ctx.gang_context
        return json.dumps({"pid": os.getpid(), "gang_id": gc.gang_id if gc else None})


app = GangApp.bind()
