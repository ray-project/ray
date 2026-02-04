"""Direct validation tests for ingress.py source code.

These tests validate the change by reading and parsing the source file directly,
avoiding dependency resolution issues with vLLM and torch.
"""

import re
import pytest
from pathlib import Path


class TestIngressSourceCode:
    """Tests that validate the ingress.py source code directly."""

    @pytest.fixture
    def ingress_source(self):
        """Load the ingress.py source code."""
        # Path from ray/python/ray/llm/tests/serve/ to ray/python/ray/llm/_internal/serve/core/ingress/
        ingress_path = Path(__file__).parent.parent.parent / "_internal" / "serve" / "core" / "ingress" / "ingress.py"
        with open(ingress_path, 'r') as f:
            return f.read()

    def test_min_replicas_present_in_default_options(self, ingress_source):
        """Test that min_replicas is present in DEFAULT_INGRESS_OPTIONS."""
        # Search for the DEFAULT_INGRESS_OPTIONS definition
        pattern = r'DEFAULT_INGRESS_OPTIONS\s*=\s*\{[^}]*"autoscaling_config"[^}]*\}'
        match = re.search(pattern, ingress_source, re.DOTALL)
        assert match is not None, "DEFAULT_INGRESS_OPTIONS not found"
        options_block = match.group()
        assert '"min_replicas"' in options_block, "min_replicas not found in autoscaling_config"

    def test_min_replicas_equals_zero(self, ingress_source):
        """Test that min_replicas is set to 0."""
        # Find the specific assignment
        pattern = r'"min_replicas":\s*0'
        match = re.search(pattern, ingress_source)
        assert match is not None, "min_replicas is not set to 0"

    def test_target_ongoing_requests_present(self, ingress_source):
        """Test that target_ongoing_requests is present in autoscaling config."""
        pattern = r'"target_ongoing_requests"'
        match = re.search(pattern, ingress_source)
        assert match is not None, "target_ongoing_requests not found"

    def test_call_method_enum_exists(self, ingress_source):
        """Test that CallMethod enum is defined."""
        pattern = r'class\s+CallMethod\s*\(\s*Enum\s*\)'
        match = re.search(pattern, ingress_source)
        assert match is not None, "CallMethod enum class not found"

    def test_call_method_has_chat(self, ingress_source):
        """Test that CallMethod has CHAT member."""
        pattern = r'CHAT\s*=\s*"chat"'
        match = re.search(pattern, ingress_source)
        assert match is not None, "CHAT member not found in CallMethod"

    def test_call_method_has_completions(self, ingress_source):
        """Test that CallMethod has COMPLETIONS member."""
        pattern = r'COMPLETIONS\s*=\s*"completions"'
        match = re.search(pattern, ingress_source)
        assert match is not None, "COMPLETIONS member not found in CallMethod"

    def test_call_method_has_transcriptions(self, ingress_source):
        """Test that CallMethod has TRANSCRIPTIONS member."""
        pattern = r'TRANSCRIPTIONS\s*=\s*"transcriptions"'
        match = re.search(pattern, ingress_source)
        assert match is not None, "TRANSCRIPTIONS member not found in CallMethod"

    def test_default_endpoints_defined(self, ingress_source):
        """Test that DEFAULT_ENDPOINTS is defined."""
        pattern = r'DEFAULT_ENDPOINTS\s*=\s*\{'
        match = re.search(pattern, ingress_source)
        assert match is not None, "DEFAULT_ENDPOINTS not found"

    def test_endpoints_include_models(self, ingress_source):
        """Test that DEFAULT_ENDPOINTS includes models endpoint."""
        pattern = r'"models".*?lambda\s+app:'
        match = re.search(pattern, ingress_source, re.DOTALL)
        assert match is not None, "models endpoint not found"

    def test_endpoints_include_chat(self, ingress_source):
        """Test that DEFAULT_ENDPOINTS includes chat endpoint."""
        pattern = r'"chat".*?lambda\s+app:'
        match = re.search(pattern, ingress_source, re.DOTALL)
        assert match is not None, "chat endpoint not found"

    def test_endpoints_include_completions(self, ingress_source):
        """Test that DEFAULT_ENDPOINTS includes completions endpoint."""
        pattern = r'"completions".*?lambda\s+app:'
        match = re.search(pattern, ingress_source, re.DOTALL)
        assert match is not None, "completions endpoint not found"

    def test_autoscaling_config_structure(self, ingress_source):
        """Test the complete autoscaling_config structure."""
        # Extract the autoscaling_config block
        pattern = r'"autoscaling_config":\s*\{([^}]+)\}'
        match = re.search(pattern, ingress_source, re.DOTALL)
        assert match is not None, "autoscaling_config block not found"
        config_block = match.group(1)
        
        # Check both required keys are present
        assert '"min_replicas"' in config_block, "min_replicas not in autoscaling_config"
        assert '"target_ongoing_requests"' in config_block, "target_ongoing_requests not in autoscaling_config"

    def test_openai_ingress_class_exists(self, ingress_source):
        """Test that OpenAiIngress class is defined."""
        pattern = r'class\s+OpenAiIngress\s*\(\s*DeploymentProtocol\s*\)'
        match = re.search(pattern, ingress_source)
        assert match is not None, "OpenAiIngress class not found"


class TestMinReplicasIntegration:
    """Integration tests for the min_replicas change."""

    @pytest.fixture
    def ingress_source(self):
        """Load the ingress.py source code."""
        # Path from ray/python/ray/llm/tests/serve/ to ray/python/ray/llm/_internal/serve/core/ingress/
        ingress_path = Path(__file__).parent.parent.parent / "_internal" / "serve" / "core" / "ingress" / "ingress.py"
        with open(ingress_path, 'r') as f:
            return f.read()

    def test_min_replicas_allows_scale_to_zero(self, ingress_source):
        """Test that min_replicas: 0 configuration allows scaling to zero."""
        # Verify the setting exists
        assert '"min_replicas": 0' in ingress_source, \
            "min_replicas not set to 0 - cannot scale to zero"

    def test_autoscaling_config_consistency(self, ingress_source):
        """Test that autoscaling config is internally consistent."""
        # Extract DEFAULT_INGRESS_OPTIONS
        pattern = r'DEFAULT_INGRESS_OPTIONS\s*=\s*\{(.*?)\n\}'
        match = re.search(pattern, ingress_source, re.DOTALL)
        assert match is not None, "DEFAULT_INGRESS_OPTIONS block not found"
        
        options_block = match.group(1)
        # Check indentation and structure
        assert 'autoscaling_config' in options_block
        assert '"min_replicas"' in options_block
        assert '"target_ongoing_requests"' in options_block


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
