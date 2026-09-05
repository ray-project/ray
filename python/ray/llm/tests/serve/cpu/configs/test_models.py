import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pydantic
import pytest

from ray.llm._internal.common.utils.download_utils import (
    STREAMING_LOAD_FORMATS,
    STREAMING_URI_SCHEMES,
    NodeModelDownloadable,
)
from ray.llm._internal.serve.core.configs.accelerators import (
    CPUAccelerator,
    CPUConfig,
    GPUAccelerator,
    GPUConfig,
    TPUAccelerator,
    TPUConfig,
)
from ray.llm._internal.serve.core.configs.llm_config import (
    LLMConfig,
    LoraConfig,
    ModelLoadingConfig,
)
from ray.llm._internal.serve.engines.vllm.vllm_models import VLLMEngineConfig

CONFIG_DIRS_PATH = str(Path(__file__).parent / "configs")

_RESOLVE_TARGET = (
    "ray.llm._internal.serve.engines.vllm.vllm_models.get_model_location_on_disk"
)


def _nothing_downloaded():
    """Patch the disk lookup to behave as it does when nothing is cached.

    `get_model_location_on_disk` returns its argument unchanged when it finds
    no snapshot, which is what every streaming deployment sees.
    """
    return patch(_RESOLVE_TARGET, side_effect=lambda model_id: model_id)


def _downloaded_at(local_path: str):
    """Patch the disk lookup to behave as it does after a completed download."""
    return patch(_RESOLVE_TARGET, return_value=local_path)


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
        old_engine_config.model_source = "fake_model_source"
        new_engine_config = llm_config.get_engine_config()
        assert new_engine_config is old_engine_config
        assert new_engine_config.model_source == "fake_model_source"

    def test_remote_model_source_is_mirrored_but_cached_under_model_id(self):
        """A remote model_source is kept verbatim and cached under model_id.

        The URI must not name the cache directory -- it leaks its scheme and
        slashes into it (``models--s3:----bucket--...``, #63363) -- but it must
        stay reachable, because a streaming load format downloads nothing and
        has no other address to give the engine.
        """
        bucket_uri = "s3://my-bucket/my-model"
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source=bucket_uri,
            ),
        )
        engine_config = llm_config.get_engine_config()
        assert engine_config.model_source == bucket_uri
        assert engine_config.cache_id == "llm_model_id"
        assert engine_config.mirror_config is not None
        assert engine_config.mirror_config.bucket_uri == bucket_uri

    @pytest.mark.parametrize(
        "bucket_uri",
        [
            "s3://my-bucket/my-model",
            "gs://my-bucket/my-model",
            "abfss://container@account.dfs.core.windows.net/my-model",
        ],
    )
    def test_cache_id_never_contains_uri_separators(self, bucket_uri):
        """Regression guard for #63363, for every scheme we accept."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source=bucket_uri,
            ),
        )
        assert llm_config.get_engine_config().cache_id == "llm_model_id"

    def test_hf_model_source_used_directly(self):
        """A plain HuggingFace model_source is not mirrored."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source="facebook/opt-1.3b",
            ),
        )
        engine_config = llm_config.get_engine_config()
        assert engine_config.model_source == "facebook/opt-1.3b"
        assert engine_config.cache_id == "facebook/opt-1.3b"
        assert engine_config.mirror_config is None
        assert engine_config.resolve_model_path() == "facebook/opt-1.3b"

    def test_local_path_model_source_used_directly(self):
        """A local path is handed to the engine unchanged."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source="/mnt/models/opt-1.3b",
            ),
        )
        engine_config = llm_config.get_engine_config()
        assert engine_config.mirror_config is None
        assert engine_config.resolve_model_path() == "/mnt/models/opt-1.3b"

    def test_no_model_source_falls_back_to_model_id(self):
        """With no model_source, the model_id is the HuggingFace repo id."""
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
            ),
        )
        engine_config = llm_config.get_engine_config()
        assert engine_config.model_source == "llm_model_id"
        assert engine_config.cache_id == "llm_model_id"
        assert engine_config.mirror_config is None
        assert engine_config.resolve_model_path() == "llm_model_id"

    @pytest.mark.parametrize("load_format", STREAMING_LOAD_FORMATS)
    def test_streaming_load_format_streams_from_uri(self, load_format):
        """Streaming formats skip the download, so the engine must get the URI.

        This is the regression reported in #64978 and #65477: with nothing on
        disk the engine was handed the ``model_id`` alias, which vLLM reports
        as a missing HuggingFace repo.
        """
        bucket_uri = "s3://my-bucket/my-model"
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source=bucket_uri,
            ),
            engine_kwargs=dict(load_format=load_format),
        )
        engine_config = llm_config.get_engine_config()
        with _nothing_downloaded():
            assert engine_config.resolve_model_path() == bucket_uri
            # The value that actually reaches vLLM.
            assert engine_config.get_initialization_kwargs()["model"] == bucket_uri

    @pytest.mark.parametrize("load_format", STREAMING_LOAD_FORMATS)
    def test_streaming_load_format_prefers_a_downloaded_copy(self, load_format):
        """A snapshot already on disk still wins over the URI."""
        local_path = "/root/.cache/huggingface/hub/models--llm_model_id/snapshots/abc"
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source="s3://my-bucket/my-model",
            ),
            engine_kwargs=dict(load_format=load_format),
        )
        engine_config = llm_config.get_engine_config()
        with _downloaded_at(local_path):
            assert engine_config.resolve_model_path() == local_path
            assert engine_config.get_initialization_kwargs()["model"] == local_path

    def test_downloaded_mirror_resolves_to_local_path(self):
        """The ordinary download path is unchanged: local snapshot wins."""
        local_path = "/root/.cache/huggingface/hub/models--llm_model_id/snapshots/abc"
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source="s3://my-bucket/my-model",
            ),
        )
        engine_config = llm_config.get_engine_config()
        with _downloaded_at(local_path):
            assert engine_config.get_initialization_kwargs()["model"] == local_path

    def test_cloud_mirror_config_model_source_resolves_to_bucket_uri(self):
        """An explicit CloudMirrorConfig behaves like a remote string source."""
        bucket_uri = "s3://my-bucket/my-model"
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source=dict(bucket_uri=bucket_uri),
            ),
        )
        engine_config = llm_config.get_engine_config()
        assert engine_config.cache_id == "llm_model_id"
        with _nothing_downloaded():
            assert engine_config.resolve_model_path() == bucket_uri

    def test_served_model_name_is_always_the_model_id(self):
        """`model_id` names the model, `model_source` locates it.

        Keeping those two mappings unconditional is the point of the split; see
        the discussion on #64978.
        """
        bucket_uri = "s3://my-bucket/my-model"
        for engine_kwargs in ({}, dict(load_format="runai_streamer")):
            llm_config = LLMConfig(
                model_loading_config=ModelLoadingConfig(
                    model_id="llm_model_id",
                    model_source=bucket_uri,
                ),
                engine_kwargs=engine_kwargs,
            )
            engine_config = llm_config.get_engine_config()
            with _nothing_downloaded():
                kwargs = engine_config.get_initialization_kwargs()
            assert kwargs["served_model_name"] == ["llm_model_id"]
            assert kwargs["model"] == bucket_uri

    def test_streaming_load_format_rejects_unstreamable_scheme(self):
        """abfss:// can be downloaded but not streamed, so say so up front.

        Without this the URI reaches vLLM, which reports it as a missing
        HuggingFace repo -- an error that names the URI but not the cause.
        """
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source="abfss://container@account.dfs.core.windows.net/m",
            ),
            engine_kwargs=dict(load_format="runai_streamer"),
        )
        with pytest.raises(pydantic.ValidationError, match="do not support"):
            llm_config.get_engine_config()

    def test_non_streaming_load_format_allows_any_downloadable_scheme(self):
        """The scheme check is scoped to streaming; downloads are unaffected."""
        uri = "abfss://container@account.dfs.core.windows.net/m"
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source=uri,
            ),
        )
        engine_config = llm_config.get_engine_config()
        assert engine_config.mirror_config.bucket_uri == uri

    @pytest.mark.parametrize("scheme", STREAMING_URI_SCHEMES)
    def test_streaming_load_format_accepts_streamable_schemes(self, scheme):
        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id="llm_model_id",
                model_source=f"{scheme}my-bucket/my-model",
            ),
            engine_kwargs=dict(load_format="runai_streamer"),
        )
        engine_config = llm_config.get_engine_config()
        with _nothing_downloaded():
            assert engine_config.resolve_model_path() == f"{scheme}my-bucket/my-model"

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

    def test_vllm_engine_config_accelerator_type_with_cpu_config_raises_error(self):
        """Test that VLLMEngineConfig rejects accelerator_type with CPU config."""
        with pytest.raises(
            pydantic.ValidationError,
            match="accelerator_type='L4' cannot be used with CPU-only configurations",
        ):
            VLLMEngineConfig(
                model_id="test-model",
                accelerator_type="L4",
                accelerator_config=CPUConfig(kind="cpu"),
            )

    def test_vllm_engine_config_accelerator_type_with_gpu_config_succeeds(self):
        """Test that VLLMEngineConfig accepts accelerator_type with GPU config."""
        engine_config = VLLMEngineConfig(
            model_id="test-model",
            accelerator_type="L4",
            accelerator_config=GPUConfig(kind="gpu"),
        )

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


class TestApplyCheckpointInfo:
    """Test that apply_checkpoint_info derives capabilities from the HF config."""

    @pytest.fixture
    def mock_hf_config(self):
        hf_config = MagicMock()
        hf_config.architectures = ["Qwen2ForCausalLM"]
        del hf_config.vision_config
        return hf_config

    def _make_llm_config(self, model_id="org/model"):
        return LLMConfig(model_loading_config=ModelLoadingConfig(model_id=model_id))

    @patch("transformers.AutoConfig.from_pretrained")
    def test_uses_provided_hf_config_without_reloading(
        self, mock_from_pretrained, mock_hf_config
    ):
        config = self._make_llm_config()

        config.apply_checkpoint_info("org/repo:Q5_K_M", hf_config=mock_hf_config)

        mock_from_pretrained.assert_not_called()
        assert config._model_architecture == "Qwen2ForCausalLM"
        assert config._supports_vision is False

    def test_detects_vision_from_provided_hf_config(self, mock_hf_config):
        mock_hf_config.vision_config = MagicMock()
        config = self._make_llm_config()

        config.apply_checkpoint_info("org/repo", hf_config=mock_hf_config)

        assert config._supports_vision is True

    @patch("transformers.AutoConfig.from_pretrained")
    def test_falls_back_to_loading_from_path(
        self, mock_from_pretrained, mock_hf_config
    ):
        mock_from_pretrained.return_value = mock_hf_config
        config = self._make_llm_config()

        config.apply_checkpoint_info("org/plain-model")

        mock_from_pretrained.assert_called_once()
        assert mock_from_pretrained.call_args.args[0] == "org/plain-model"
        assert config._model_architecture == "Qwen2ForCausalLM"

    @patch("transformers.AutoConfig.from_pretrained")
    def test_load_failure_raises_value_error(self, mock_from_pretrained):
        mock_from_pretrained.side_effect = OSError("no config.json")
        config = self._make_llm_config()

        with pytest.raises(ValueError, match="Failed to load Hugging Face config"):
            config.apply_checkpoint_info("org/missing-model")


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
