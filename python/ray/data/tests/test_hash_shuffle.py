from dataclasses import dataclass
from typing import Dict, Optional, Any
from unittest.mock import MagicMock, patch

import pytest

from ray.data import DataContext
from ray.data._internal.execution.interfaces import PhysicalOperator
from ray.data._internal.execution.operators.join import JoinOperator
from ray.data._internal.logical.interfaces import LogicalOperator
from ray.data._internal.logical.operators.join_operator import JoinType
from ray.data._internal.util import GiB
from ray.data.block import BlockMetadata

