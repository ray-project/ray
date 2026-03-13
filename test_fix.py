#!/usr/bin/env python3
"""Test script to verify the smart_open compatibility fix."""

import builtins
import contextlib
import io
import sys

# Mock the original print function as it would be saved
_original_print = builtins.print
_original_stdout = sys.stdout

def redirected_print(*objects, sep=" ", end="\n", file=None, flush=False):
    """Simplified version of the fixed redirected_print function."""
    # If file is specified and not default streams, use original print
    if file not in [sys.stdout, sys.stderr, None]:
        return _original_print(*objects, sep=sep, end=end, file=file, flush=flush)
    
    # If stdout has been redirected, honor that redirection
    if sys.stdout is not _original_stdout:
        return _original_print(*objects, sep=sep, end=end, file=file, flush=flush)

    # Otherwise, this would normally go to logger (simulate with prefix)
    _original_print(f"[LOGGED] {' '.join(map(str, objects))}", end=end)

# Apply the patch
builtins.print = redirected_print

# Test 1: Normal print should be "logged" 
print("Test 1: Normal print")

# Test 2: Redirected stdout should be captured, not logged
print("\nTest 2: Testing stdout redirection...")
captured_output = io.StringIO()
with contextlib.redirect_stdout(captured_output):
    print("This should be captured")

captured = captured_output.getvalue()
print(f"Captured: {repr(captured)}")
print("Success!" if captured == "This should be captured\n" else "Failed!")

# Test 3: Custom file should work
print("\nTest 3: Testing custom file...")
with io.StringIO() as custom_file:
    print("Custom file content", file=custom_file)
    custom_content = custom_file.getvalue()

print(f"Custom file captured: {repr(custom_content)}")
print("Success!" if custom_content == "Custom file content\n" else "Failed!")

# Restore original print
builtins.print = _original_print