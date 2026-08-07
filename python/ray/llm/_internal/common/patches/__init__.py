"""Temporary runtime patches of third-party libraries (currently vLLM).

Each module here is a stopgap for behavior Ray Serve LLM needs before it exists
upstream. The goal is for this package to trend toward empty: every patch states
its removal condition in its module docstring and tracks the upstream work that
will retire it. Patches are deleted as soon as that upstream lands.
"""
