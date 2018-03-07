from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import subprocess
import tempfile


def run_and_get_output(command):
    with tempfile.NamedTemporaryFile() as tmp:
        p = subprocess.Popen(command, stdout=tmp)
        if p.wait() != 0:
            raise RuntimeError("ray start did not terminate properly")
        with open(tmp.name, 'r') as f:
            result = f.readlines()
            return "\n".join(result)
        tmp.close()
