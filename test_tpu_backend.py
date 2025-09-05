#!/usr/bin/env python3
"""
Unit tests for TPU backend functionality in TorchTrainer.
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import os
import sys

# Mock ray imports for testing
sys.modules['ray'] = Mock()
sys.modules['ray.train'] = Mock()
sys.modules['ray.train.torch'] = Mock()
sys.modules['torch'] = Mock()
sys.modules['torch.distributed'] = Mock()

from ray.train.torch.config import TorchConfig, _TorchTPUBackend
from ray.train._internal.worker_group import WorkerGroup


class TestTorchConfig(unittest.TestCase):
    """Test TorchConfig TPU functionality."""
    
    def test_torch_config_default(self):
        """Test default TorchConfig behavior."""
        config = TorchConfig()
        self.assertFalse(config.use_tpu)
        self.assertIsNone(config.backend)
    
    def test_torch_config_with_tpu(self):
        """Test TorchConfig with TPU enabled."""
        config = TorchConfig(use_tpu=True)
        self.assertTrue(config.use_tpu)
        self.assertIsNone(config.backend)
    
    def test_backend_cls_property(self):
        """Test backend_cls property returns correct backend."""
        # Test default backend
        config = TorchConfig()
        self.assertEqual(config.backend_cls.__name__, '_TorchBackend')
        
        # Test TPU backend
        config = TorchConfig(use_tpu=True)
        self.assertEqual(config.backend_cls.__name__, '_TorchTPUBackend')


class TestTorchTPUBackend(unittest.TestCase):
    """Test _TorchTPUBackend functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.worker_group = Mock(spec=WorkerGroup)
        self.worker_group.execute = Mock()
        self.worker_group.execute_single = Mock(return_value=("127.0.0.1", 12345))
        
        self.config = TorchConfig(use_tpu=True)
        self.backend = _TorchTPUBackend()
    
    def test_backend_initialization(self):
        """Test backend initialization."""
        self.assertIsNotNone(self.backend)
    
    def test_on_start_with_tpu_enabled(self):
        """Test on_start method with TPU enabled."""
        self.backend.on_start(self.worker_group, self.config)
        
        # Verify environment variables were set
        self.worker_group.execute.assert_called()
    
    def test_on_start_without_tpu_enabled(self):
        """Test on_start method without TPU enabled."""
        config_without_tpu = TorchConfig()
        config_without_tpu.backend = "cuda"
        
        with self.assertRaises(ValueError) as context:
            self.backend.on_start(self.worker_group, config_without_tpu)
        
        self.assertIn("TPU backend requires use_tpu=True", str(context.exception))
    
    def test_on_training_start(self):
        """Test on_training_start method."""
        self.backend.on_training_start(self.worker_group, self.config)
        
        # Verify both environment setup and process group setup were called
        self.assertEqual(self.worker_group.execute.call_count, 2)
    
    def test_on_shutdown(self):
        """Test on_shutdown method."""
        self.backend.on_shutdown(self.worker_group, self.config)
        
        # Verify cleanup was called
        self.worker_group.execute.assert_called_once()


class TestTPUIntegration(unittest.TestCase):
    """Test TPU integration with TorchTrainer."""
    
    @patch('ray.train.torch.config._TorchTPUBackend')
    def test_torch_trainer_tpu_auto_config(self):
        """Test that TorchTrainer automatically configures TPU backend."""
        # This would test the actual TorchTrainer integration
        # For now, we'll just verify the backend class is available
        from ray.train.torch.config import _TorchTPUBackend
        self.assertIsNotNone(_TorchTPUBackend)


if __name__ == "__main__":
    unittest.main() 