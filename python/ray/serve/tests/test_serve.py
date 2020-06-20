import pytest
from pathlib import Path
import sys

if __name__ == "__main__":
    curr_dir = Path(__file__).parent
    test_paths = curr_dir.rglob("test_*.py")
    sorted_path = sorted(map(lambda path: str(path.absolute()), test_paths))
    serve_tests_files = list(sorted_path)

    print("Testing the following files")
    for test_file in serve_tests_files:
        print("->", test_file.split("/")[-1])

    sys.exit(pytest.main(["-v", "-s"] + serve_tests_files))
