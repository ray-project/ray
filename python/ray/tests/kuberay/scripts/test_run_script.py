import test_script
import inspect
import subprocess

import pdb; pdb.set_trace()
script_string = inspect.getsource(test_script)
out = subprocess.check_output(["python", "-c", script_string]).decode().strip()
print(out)
assert out == "Hello world."
print("ok")
