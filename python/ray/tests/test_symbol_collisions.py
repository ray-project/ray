"""
This script ensures that various libraries do not conflict with ray by
trying to import both libraries in both orders.
A specific example is that importing ray after pyarrow causes a Segfault.
"""
import subprocess

TESTED_LIBRARIES = ["pyarrow"]


def test_imports():
    def try_imports(library1, library2):
        return_info = subprocess.run([
            "python", "-c", "import {}; import {}".format(library1, library2)
        ])
        if return_info.returncode != 0:
            return "Importing {} before {} caused an error".format(
                library1, library2)
        return ""

    for library in TESTED_LIBRARIES:
        assert try_imports("ray", library) == ""
        assert try_imports(library, "ray") == ""


if __name__ == "__main__":
    import sys
    import pytest
    sys.exit(pytest.main(["-v", __file__]))
