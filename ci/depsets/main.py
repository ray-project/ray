#!/usr/bin/env python3

import sys
import os

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src"))
from src.dependencies.cli import cli


if __name__ == "__main__":
    cli()
