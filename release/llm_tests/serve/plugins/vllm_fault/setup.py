# SPDX-License-Identifier: Apache-2.0
"""
Setup script for vLLM fault injection plugin.

Install with:
    pip install -e .
"""

from setuptools import setup

setup(
    name="vllm_fault",
    version="0.1.0",
    description=(
        "vLLM plugin that crashes a worker process on signal, "
        "for testing DP fault tolerance."
    ),
    packages=["vllm_fault"],
    entry_points={
        "vllm.general_plugins": [
            "fault = vllm_fault:register"
        ]
    },
    python_requires=">=3.8",
)
