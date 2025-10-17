"""
XGBoost Trainer with External Memory Support

This module provides the XGBoostTrainer for distributed XGBoost training
with optional external memory optimization for large datasets.

The only public API is the XGBoostTrainer class. All other functions
are internal utilities and should not be imported directly.
"""

from .xgboost_trainer import XGBoostTrainer

__all__ = ["XGBoostTrainer"]
