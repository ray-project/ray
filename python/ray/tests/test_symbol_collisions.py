"""
This script ensures that various libraries do not conflict with ray by
trying to import both libraries in both orders.
A specific example is that importing ray after pyarrow causes a Segfault.
"""
import subprocess

TESTED_LIBRARIES = ["pyarrow"]


def test_imports():
    for library in TESTED_LIBRARIES:
        try:
            subprocess.check_output([
                "python", "-c", "import {}; import {}".format(library, "ray")
            ])
            subprocess.check_output([
                "python", "-c", "import {}; import {}".format("ray", library)
            ])
        except Exception as e:
            print(
                subprocess.run([
                    "python", "-c",
                    "import {0}; print(Error with importing: '{0}',"
                    "{0}.__version__)".format(library)
                ]))
            raise e


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
