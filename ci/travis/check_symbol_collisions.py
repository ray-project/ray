"""
This script ensures that various libraries do not conflict with ray by 
trying to import both libraries in both orders.
A specific example is that importing ray after pyarrow causes a Segfault.
"""
import sys, subprocess, importlib

TESTED_LIBRARIES = ["pyarrow"]

if len(sys.argv) == 1:
    final_exit_code = 0
    program_name = sys.argv[0]
    for library in TESTED_LIBRARIES:
        order_one = subprocess.run(["python", program_name, library, "ray"])
        order_two = subprocess.run(["python", program_name, "ray", library])
        final_exit_code += order_one.returncode + order_two.returncode
    sys.exit(final_exit_code)

if len(sys.argv) == 3:
    importlib.import_module(sys.argv[1])
    importlib.import_module(sys.argv[2])
    sys.exit(0)