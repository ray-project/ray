import os
import subprocess
import tempfile

import ray
from ray import serve


def test_new_driver(serve_instance):
    script = """
import ray
ray.init(address="auto")

from ray import serve
serve.init()

@serve.route("/driver")
def driver(flask_request):
    return "OK!"
"""

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as f:
        path = f.name
        f.write(script)

    proc = subprocess.Popen(["python", path])
    return_code = proc.wait(timeout=10)
    assert return_code == 0

    handle = serve.get_handle("driver")
    assert ray.get(handle.remote()) == "OK!"

    os.remove(path)
