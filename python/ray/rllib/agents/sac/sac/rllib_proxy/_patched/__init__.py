""" This module contains code that is present in both RLLib v0.6.6 and RLLib v0.8.0 but
has minor differences between the two versions and thus needs to be patched.
"""
# NOTE(ADI): As I compare classes between RLLib v0.6.6 and RLLib v0.8.0, I will add the
#  patches required to make RLLib v0.6.6 compatible with RLLib v0.8.0 in this module.
from ._trainer import Trainer
