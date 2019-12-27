import os
import subprocess
import tempfile

import ray
from ray.experimental import serve


def test_new_driver(serve_instance):
    script = """
import ray
ray.init(address="auto")

from ray.experimental import serve
serve.init()


def function(flask_request):
    return "OK!"

serve.create_endpoint("driver", "/driver")
serve.create_backend(function, "driver:v1")
serve.link("driver", "driver:v1")
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
