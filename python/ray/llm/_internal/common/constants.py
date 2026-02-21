"""
Generic constants for common utilities.

These constants are used by generic utilities and should not contain
serve-specific or batch-specific values.
"""

# Cloud object caching timeouts (in seconds)
CLOUD_OBJECT_EXISTS_EXPIRE_S = 300  # 5 minutes
CLOUD_OBJECT_MISSING_EXPIRE_S = 30  # 30 seconds

# LoRA adapter configuration file name
LORA_ADAPTER_CONFIG_NAME = "adapter_config.json"
