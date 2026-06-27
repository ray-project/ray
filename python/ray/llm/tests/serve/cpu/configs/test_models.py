import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pydantic
import pytest

from ray.llm._internal.common.utils.download_utils import NodeModelDownloadable
from ray.llm._internal.serve.core.configs.accelerators import (
    CPUAccelerator,
    CPUConfig,
    GPUAccelerator,
    GPUConfig,
    NPUAccelerator,
    NPUConfig,
    TPUAccelerator,
    TPUConfig,
)
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    LoraConfig,
    ModelLoadingConfig,
)

CONFIG_DIRS_PATH = str(Path(__file__).parent / "configs")


class TestModelConfig:
    def test_construction(self):
        """Test construct an LLMConfig doesn't error out and has correct attributes."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            accelerator_type="A100-40G",  # Dash instead of underscore when specifying accelerator type
            deployment_config={
                "autoscaling_config": {
                    "min_replicas": 3,
                    "max_replicas": 7,
                }
            },
        )
        assert llm_config.deployment_config["autoscaling_config"]["min_replicas"] == 3
        assert llm_config.deployment_config["autoscaling_config"]["max_replicas"] == 7
        assert llm_config.model_loading_config.model_id == "llm_model_id"
        assert llm_config.accelerator_type == "A100-40G"

    def test_construction_requires_model_loading_config(self):
        """Test that constructing an LLMConfig without model_loading_config errors out"""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                accelerator_type="L4",
            )

    def test_accelerator_type_optional(self):
        """Test that accelerator_type is optional when initializing LLMConfig."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model")
        )
        assert llm_config.model_loading_config.model_id == "test_model"
        assert llm_config.accelerator_type is None

    def test_invalid_accelerator_type(self):
        """Test that invalid accelerator types raise validation errors."""
        with pytest.raises(pydantic.ValidationError):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="INVALID_GPU",  # Invalid string value
            )

        # Test invalid numeric value
        with pytest.raises(pydantic.ValidationError):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type=123,  # Must be a string
            )

        # Test that underscore is not supported in accelerator type
        with pytest.raises(pydantic.ValidationError):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="A100_40G",  # Should use A100-40G instead
            )

    def test_model_loading_config_forbids_extra_fields(self):
        """Test that ModelLoadingConfig rejects extra fields."""

        with pytest.raises(pydantic.ValidationError, match="engine_kwargs"):
            ModelLoadingConfig(
                model_id="test_model",
                model_source="test_source",
                engine_kwargs={"max_model_len": 8000},  # This should be rejected
            )

        valid_config = ModelLoadingConfig(
            model_id="test_model", model_source="test_source"
        )
        assert valid_config.model_id == "test_model"
        assert valid_config.model_source == "test_source"

    def test_invalid_generation_config(self, disable_placement_bundles):
        """Test that passing an invalid generation_config raises an error."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="L4",
                generation_config="invalid_config",  # Should be a dictionary, not a string
            )

    def test_deployment_type_checking(self, disable_placement_bundles):
        """Test that deployment config type checking works."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                deployment_config={
                    "max_ongoing_requests": -1,
                },
                accelerator_type="L4",
            )

    def test_autoscaling_type_checking(self, disable_placement_bundles):
        """Test that autoscaling config type checking works."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                deployment_config={
                    "autoscaling_config": {
                        "min_replicas": -1,
                    },
                },
                accelerator_type="L4",
            )

    def test_deployment_unset_fields_are_not_included(self, disable_placement_bundles):
        """Test that unset fields are not included in the deployment config."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="L4",
        )
        assert "max_ongoing_requests" not in llm_config.deployment_config
        assert "graceful_shutdown_timeout_s" not in llm_config.deployment_config

    def test_autoscaling_unset_fields_are_not_included(self, disable_placement_bundles):
        """Test that unset fields are not included in the autoscaling config."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            deployment_config={
                "autoscaling_config": {
                    "min_replicas": 3,
                    "max_replicas": 7,
                },
            },
            accelerator_type="L4",
        )
        assert (
            "metrics_interval_s"
            not in llm_config.deployment_config["autoscaling_config"]
        )
        assert (
            "upscaling_factor" not in llm_config.deployment_config["autoscaling_config"]
        )

    def test_engine_config_cached(self):
        """Test that the engine config is cached and not recreated when calling
        get_engine_config so the attributes on the engine will be persisted."""

        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
        )
        old_engine_config = llm_config.get_engine_config()
        old_engine_config.hf_model_id = "fake_hf_model_id"
        new_engine_config = llm_config.get_engine_config()
        assert new_engine_config is old_engine_config

    def test_experimental_configs(self):
        """Test that `experimental_configs` can be used."""
        # Test with a valid dictionary can be used.
        experimental_configs = {
            "experimental_feature1": "value1",
            "experimental_feature2": "value2",
        }
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
            experimental_configs=experimental_configs,
        )
        assert llm_config.experimental_configs == experimental_configs

        # test with invalid dictionary will raise a validation error.
        with pytest.raises(
            pydantic.ValidationError,
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    model_id="llm_model_id",
                ),
                experimental_configs={123: "value1"},
            )

    def test_log_engine_metrics_disable_log_stats_validation(self):
        """Test that log_engine_metrics=True prevents disable_log_stats=True."""
        with pytest.raises(
            pydantic.ValidationError,
            match="disable_log_stats cannot be set to True when log_engine_metrics is enabled",
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                log_engine_metrics=True,
                engine_kwargs={"disable_log_stats": True},
            )

    @pytest.mark.parametrize(
        "load_format,expected_download_model",
        [
            ("runai_streamer", NodeModelDownloadable.NONE),
            ("runai_streamer_sharded", NodeModelDownloadable.NONE),
            ("tensorizer", NodeModelDownloadable.NONE),
            (None, NodeModelDownloadable.MODEL_AND_TOKENIZER),
        ],
    )
    def test_load_format_callback_context(self, load_format, expected_download_model):
        """Test that different load_format values set correct worker_node_download_model in callback context."""
        engine_kwargs = {"load_format": load_format} if load_format is not None else {}

        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            engine_kwargs=engine_kwargs,
        )

        # Get the callback instance which should trigger the context setup
        callback = llm_config.get_or_create_callback()

        # Check that the callback context has the correct worker_node_download_model value
        assert hasattr(callback, "ctx"), "Callback should have ctx attribute"
        assert callback.ctx.worker_node_download_model == expected_download_model


class TestFieldValidators:
    """Test the field validators for dict validation."""

    def test_model_loading_config_dict_validation(self):
        """Test that model_loading_config accepts and validates dict input."""
        config_dict = {"model_id": "microsoft/DialoGPT-medium"}

        llm_config = LLMConfig(model_loading_config=config_dict, llm_engine="vLLM")

        assert isinstance(llm_config.model_loading_config, ModelLoadingConfig)
        assert llm_config.model_loading_config.model_id == "microsoft/DialoGPT-medium"

    def test_model_loading_config_validation_error(self):
        """Test that invalid dict raises proper validation error."""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            LLMConfig(
                model_loading_config={"invalid_field": "value"}, llm_engine="vLLM"
            )

        assert "Invalid model_loading_config" in str(exc_info.value)

    def test_lora_config_dict_validation(self):
        """Test that lora_config accepts and validates dict input."""
        llm_config = LLMConfig(
            model_loading_config={"model_id": "test"},
            lora_config=None,
            llm_engine="vLLM",
        )

        assert llm_config.lora_config is None

        lora_dict = {
            "dynamic_lora_loading_path": "s3://bucket/lora",
            "max_num_adapters_per_replica": 8,
        }

        llm_config2 = LLMConfig(
            model_loading_config={"model_id": "test"},
            lora_config=lora_dict,
            llm_engine="vLLM",
        )

        assert isinstance(llm_config2.lora_config, LoraConfig)
        assert llm_config2.lora_config.max_num_adapters_per_replica == 8
        assert llm_config2.lora_config.dynamic_lora_loading_path == "s3://bucket/lora"

    def test_lora_config_validation_error(self):
        """Test that invalid lora config dict raises proper validation error."""
        with pytest.raises(pydantic.ValidationError) as exc_info:
            LLMConfig(
                model_loading_config={"model_id": "test"},
                lora_config={"max_num_adapters_per_replica": "invalid_string"},
                llm_engine="vLLM",
            )

        assert "Invalid lora_config" in str(exc_info.value)


class TestAcceleratorConfigLogic:
    """Test the accelerator_config logic and its interaction with accelerator_type."""

    def test_accelerator_config_field_basic(self):
        """Test that accelerator_config field works with basic values."""
        # Test CPU config
        llm_config_cpu = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_config={"kind": "cpu"},
        )
        assert llm_config_cpu.accelerator_config.kind == "cpu"
        engine_config = llm_config_cpu.get_engine_config()
        assert engine_config.accelerator_config.kind == "cpu"

        # Test GPU config
        llm_config_gpu = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_config={"kind": "gpu"},
        )
        assert llm_config_gpu.accelerator_config.kind == "gpu"
        engine_config_gpu = llm_config_gpu.get_engine_config()
        assert engine_config_gpu.accelerator_config.kind == "gpu"

    def test_accelerator_type_with_cpu_config_raises_error(self):
        """Test that accelerator_type with CPU config raises a validation error."""
        with pytest.raises(
            pydantic.ValidationError,
            match="accelerator_type='L4' cannot be used with CPU-only configurations",
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_config={"kind": "cpu"},
                accelerator_type="L4",
            )

    def test_accelerator_type_with_cpu_only_placement_group_raises_error(self):
        """Test that accelerator_type with CPU-only placement_group_config raises error."""
        with pytest.raises(
            pydantic.ValidationError,
            match="accelerator_type='L4' cannot be used with CPU-only configurations",
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="L4",
                placement_group_config={"bundles": [{"CPU": 4}]},
            )

    def test_accelerator_type_with_empty_bundles_raises_error(self):
        """Test that accelerator_type with empty bundles list raises error."""
        with pytest.raises(
            pydantic.ValidationError,
            match="accelerator_type='L4' cannot be used with CPU-only configurations",
        ):
            LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_type="L4",
                placement_group_config={"bundles": []},
            )

    def test_accelerator_type_with_gpu_placement_group_succeeds(self):
        """Test that accelerator_type with GPU-containing placement_group_config succeeds."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="L4",
            placement_group_config={"bundles": [{"GPU": 1, "CPU": 4}]},
        )
        assert llm_config.accelerator_type == "L4"

    def test_accelerator_type_with_gpu_config_succeeds(self):
        """Test that accelerator_type with GPU config succeeds."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="L4",
            accelerator_config={"kind": "gpu"},
        )
        assert llm_config.accelerator_type == "L4"
        engine_config = llm_config.get_engine_config()
        assert engine_config.accelerator_type == "L4"

    def test_llm_config_accelerator_type_hardware_mismatch(self):
        """Test that passing a GPU accelerator_type with a TPU config raises an error."""
        with pytest.raises(
            pydantic.ValidationError,
            match="Hardware mismatch",
        ):
            LLMConfig(
                model_loading_config={"model_id": "test_model"},
                accelerator_type="L4",
                accelerator_config={"kind": "tpu", "topology": "4x4"},
            )

    def test_engine_config_infers_tpu_from_accelerator_type_string(self):
        """Test that the engine config infers a TPU backend directly from the accelerator_type string."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="TPU-V6E",
        )

        # Validate engine correctly inferred the TPU backend
        engine_config = llm_config.get_engine_config()

        assert isinstance(engine_config.accelerator, TPUAccelerator)
        assert engine_config.accelerator_type == "TPU-V6E"

    def test_requires_deferred_placement_group(self):
        """Test that requires_deferred_placement_group correctly identifies deferred PG requirements."""
        cpu_accel = CPUAccelerator()
        assert cpu_accel.requires_deferred_placement_group is False

        gpu_accel = GPUAccelerator()
        assert gpu_accel.requires_deferred_placement_group is False

        tpu_accel_no_topo = TPUAccelerator(TPUConfig(kind="tpu"))
        assert tpu_accel_no_topo.requires_deferred_placement_group is False

        tpu_accel_with_topo = TPUAccelerator(TPUConfig(kind="tpu", topology="4x4"))
        assert tpu_accel_with_topo.requires_deferred_placement_group is True

    @pytest.mark.parametrize(
        "topology,num_devices,accelerator_type_str,expected_bundles_count,expected_chips_per_host",
        [
            ("1x1", 1, "TPU-V6E", 1, 1),
            ("1x1", 1, "TPU-V7X", 1, 1),
            ("4x4", 16, "TPU-V6E", 4, 4),
            ("2x2x2", 8, "TPU-V5P", 2, 4),
            ("2x2", 4, "TPU-V5LITEPOD", 1, 4),
            ("2x2x1", 4, "TPU-V4", 1, 4),
            ("2x4", 8, "TPU-V6E", 1, 8),
        ],
    )
    def test_default_bundles_topology(
        self,
        topology,
        num_devices,
        accelerator_type_str,
        expected_bundles_count,
        expected_chips_per_host,
    ):
        """Test that different topologies return correct per-host bundles."""
        tpu_accel = TPUAccelerator(TPUConfig(kind="tpu", topology=topology))
        bundles = tpu_accel.default_bundles(
            num_devices=num_devices, accelerator_type_str=accelerator_type_str
        )

        assert len(bundles) == expected_bundles_count
        for bundle in bundles:
            assert bundle["TPU"] == expected_chips_per_host
            assert f"accelerator_type:{accelerator_type_str}" in bundle

    def test_default_bundles_topology_missing_accelerator_type_raises(self):
        """Test that ValueError is raised when topology is present but accelerator type is missing."""
        tpu_accel = TPUAccelerator(TPUConfig(kind="tpu", topology="4x4"))
        with pytest.raises(
            ValueError,
            match="`accelerator_type` must be specified when `topology` is present",
        ):
            tpu_accel.default_bundles(num_devices=16, accelerator_type_str=None)

    def test_default_bundles_topology_non_multiple_num_devices_raises(self):
        """Test that ValueError is raised when num_devices is not a multiple of chips_per_host."""
        tpu_accel = TPUAccelerator(TPUConfig(kind="tpu", topology="4x4"))
        with pytest.raises(ValueError, match="must be a multiple of chips_per_host"):
            tpu_accel.default_bundles(num_devices=6, accelerator_type_str="TPU-V6E")

    def test_accelerator_config_npu_kind(self):
        """Test that accelerator_config with kind='npu' works."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_config={"kind": "npu"},
        )
        assert llm_config.accelerator_config.kind == "npu"
        engine_config = llm_config.get_engine_config()
        assert engine_config.accelerator_config.kind == "npu"
        assert isinstance(engine_config.accelerator, NPUAccelerator)

    def test_engine_config_infers_npu_from_accelerator_type_string(self):
        """Test that the engine config infers an NPU backend from the accelerator_type string."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="Ascend910B",
        )
        engine_config = llm_config.get_engine_config()
        assert isinstance(engine_config.accelerator, NPUAccelerator)
        assert engine_config.accelerator_type == "Ascend910B"

    def test_npu_placement_bundles_use_npu_resource_key(self):
        """Test that NPU accelerator generates NPU bundles, not GPU bundles."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="Ascend910B",
            engine_kwargs={"tensor_parallel_size": 4},
        )
        engine_config = llm_config.get_engine_config()
        bundles = engine_config.placement_bundles
        assert len(bundles) == 4
        for bundle in bundles:
            assert "NPU" in bundle
            assert bundle["NPU"] == 1
            assert "GPU" not in bundle

    def test_npu_uses_ray_executor(self):
        """Test that NPU config uses Ray executor, not MP executor."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_config={"kind": "npu"},
        )
        engine_config = llm_config.get_engine_config()
        kwargs = engine_config.get_initialization_kwargs()
        assert kwargs["distributed_executor_backend"] == "ray"

    def test_npu_remote_options(self):
        """Test that NPUAccelerator returns NPU resources in remote options."""
        accel = NPUAccelerator()
        options = accel.get_remote_options("Ascend910B")
        assert "resources" in options
        assert "NPU" in options["resources"]
        assert options["accelerator_type"] == "Ascend910B"

    def test_accelerator_type_with_npu_bundles_succeeds(self):
        """Test that accelerator_type with NPU-containing placement_group_config succeeds."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model"),
            accelerator_type="Ascend910B",
            placement_group_config={"bundles": [{"NPU": 1, "CPU": 4}]},
        )
        assert llm_config.accelerator_type == "Ascend910B"
        assert llm_config.accelerator_config.kind == "npu"

    def test_detect_cluster_accelerator_kind_returns_none_when_ray_not_initialized(
        self,
    ):
        """Test that _detect_cluster_accelerator_kind returns None when Ray is not initialized."""
        with patch("ray.is_initialized", return_value=False):
            result = LLMConfig._detect_cluster_accelerator_kind()
            assert result is None

    def test_detect_cluster_accelerator_kind_detects_npu(self):
        """Test that _detect_cluster_accelerator_kind detects NPU from cluster resources."""
        with (
            patch("ray.is_initialized", return_value=True),
            patch(
                "ray.cluster_resources",
                return_value={"CPU": 4, "NPU": 8, "memory": 100},
            ),
        ):
            result = LLMConfig._detect_cluster_accelerator_kind()
            assert result == "npu"

    def test_detect_cluster_accelerator_kind_detects_gpu(self):
        """Test that _detect_cluster_accelerator_kind detects GPU from cluster resources."""
        with (
            patch("ray.is_initialized", return_value=True),
            patch(
                "ray.cluster_resources",
                return_value={"CPU": 4, "GPU": 8, "memory": 100},
            ),
        ):
            result = LLMConfig._detect_cluster_accelerator_kind()
            assert result == "gpu"

    def test_detect_cluster_accelerator_kind_detects_tpu(self):
        """Test that _detect_cluster_accelerator_kind detects TPU from cluster resources."""
        with (
            patch("ray.is_initialized", return_value=True),
            patch(
                "ray.cluster_resources",
                return_value={"CPU": 4, "TPU": 4, "memory": 100},
            ),
        ):
            result = LLMConfig._detect_cluster_accelerator_kind()
            assert result == "tpu"

    def test_detect_cluster_accelerator_kind_npu_priority_over_gpu(self):
        """Test that NPU is detected before GPU when both are in cluster (NPU-only cluster edge case)."""
        with (
            patch("ray.is_initialized", return_value=True),
            patch(
                "ray.cluster_resources",
                return_value={"CPU": 4, "GPU": 2, "NPU": 8, "memory": 100},
            ),
        ):
            result = LLMConfig._detect_cluster_accelerator_kind()
            assert result == "npu"

    def test_detect_cluster_accelerator_kind_returns_none_on_exception(self):
        """Test that _detect_cluster_accelerator_kind returns None when ray.cluster_resources() raises."""
        with (
            patch("ray.is_initialized", return_value=True),
            patch("ray.cluster_resources", side_effect=Exception("connection error")),
        ):
            result = LLMConfig._detect_cluster_accelerator_kind()
            assert result is None

    def test_kind_to_config_maps_correctly(self):
        """Test that _kind_to_config maps kind strings to the correct AcceleratorConfig."""
        configs = {
            "npu": NPUConfig,
            "gpu": GPUConfig,
            "tpu": TPUConfig,
            "cpu": CPUConfig,
        }
        for kind, expected_cls in configs.items():
            config = LLMConfig._kind_to_config(kind)
            assert isinstance(config, expected_cls)
            assert config.kind == kind

    def test_auto_detect_npu_accelerator_config_when_no_config_specified(self):
        """Test that accelerator_config is auto-resolved to NPUConfig when cluster has NPU resources."""
        with (
            patch("ray.is_initialized", return_value=True),
            patch(
                "ray.cluster_resources",
                return_value={"CPU": 4, "NPU": 8, "memory": 100},
            ),
        ):
            llm_config = LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
            )
            assert llm_config.accelerator_config.kind == "npu"
            assert isinstance(llm_config.accelerator_config, NPUConfig)

    def test_explicit_config_overrides_auto_detect(self):
        """Test that explicit accelerator_config overrides auto-detection."""
        with (
            patch("ray.is_initialized", return_value=True),
            patch(
                "ray.cluster_resources",
                return_value={"CPU": 4, "NPU": 8, "memory": 100},
            ),
        ):
            llm_config = LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
                accelerator_config={"kind": "gpu"},
            )
            # Explicit config should take priority, not overridden by auto-detect
            assert llm_config.accelerator_config.kind == "gpu"

    def test_gpu_fallback_when_no_accelerator_detected(self):
        """Test that accelerator_config falls back to GPUConfig when no accelerator is in cluster."""
        with (
            patch("ray.is_initialized", return_value=True),
            patch(
                "ray.cluster_resources",
                return_value={"CPU": 4, "memory": 100},
            ),
        ):
            llm_config = LLMConfig(
                model_loading_config=ModelLoadingConfig(model_id="test_model"),
            )
            assert llm_config.accelerator_config.kind == "gpu"


class TestCheckpointInfo:
    def test_apply_checkpoint_info_uses_autoconfig_and_threads_trust_remote_code(self):
        """apply_checkpoint_info uses AutoConfig (not PretrainedConfig) and forwards
        trust_remote_code to every HF config load call."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(model_id="test_model")
        )
        mock_hf_config = MagicMock(spec=["architectures", "vision_config"])
        mock_hf_config.architectures = ["LlavaForCausalLM"]

        with patch(
            "transformers.AutoConfig.from_pretrained", return_value=mock_hf_config
        ) as mock_auto:
            llm_config.apply_checkpoint_info("vision/model", trust_remote_code=True)

        assert all(
            call.kwargs["trust_remote_code"] is True
            for call in mock_auto.call_args_list
        )
        assert llm_config._supports_vision is True
        assert llm_config._model_architecture == "LlavaForCausalLM"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
