"""
This script ensures that various libraries do not conflict with ray by 
trying to import both libraries in both orders.
A specific example is that importing ray after pyarrow causes a Segfault.
"""
import sys, subprocess, importlib

TESTED_LIBRARIES = ["pyarrow"]


def try_imports(library1, library2):
    program_name = sys.argv[0]
    return_info = subprocess.run(["python", program_name, library1, library2])
    if return_info.returncode != 0:
        print("Importing {} before {} caused an error".format(
            library1, library2))
    return return_info.returncode


if len(sys.argv) == 1:
    final_exit_code = 0
    for library in TESTED_LIBRARIES:
        final_exit_code += try_imports("ray", library)
        final_exit_code += try_imports(library, "ray")
    sys.exit(final_exit_code)

if len(sys.argv) == 3:
    importlib.import_module(sys.argv[1])
    importlib.import_module(sys.argv[2])
    sys.exit(0)