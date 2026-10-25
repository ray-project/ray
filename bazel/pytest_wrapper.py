import sys

import pytest

if __name__ == "__main__":
    exit_code = pytest.main(sys.argv[1:])
    if exit_code is pytest.ExitCode.NO_TESTS_COLLECTED:
        exit_code = pytest.ExitCode.OK

    sys.exit(exit_code)
