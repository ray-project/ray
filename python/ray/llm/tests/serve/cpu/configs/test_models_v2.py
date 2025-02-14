import json
from typing import Dict, List

import pydantic
import pytest

from ray.llm._internal.serve.configs.models import LLMConfig

from pathlib import Path

CONFIG_DIRS_PATH = str(Path(__file__).parent / "configs")


class TestModelConfig:
    def get_app_objs(self, llm_app_data_paths: List[str]) -> List[Dict]:
        llm_app_objs = []

        for path in llm_app_data_paths:
            with open(path, "r") as f:
                llm_app_data = json.load(f)
                llm_app_objs.append(llm_app_data)

        return llm_app_objs

    def test_hf_prompt_format(self):
        """Check that the HF prompt format is correctly parsed."""
        with open(
            f"{CONFIG_DIRS_PATH}/matching_configs/hf_prompt_format_v2.yaml", "r"
        ) as f:
            LLMConfig.parse_yaml(f)

    def test_hf_prompt_format_error_use_hf_chat_template_false(self):
        """Check that the HF prompt format throw validation error when
        `use_hugging_face_chat_template` is set to false."""
        with pytest.raises(
            pydantic.ValidationError,
        ):
            with open(
                f"{CONFIG_DIRS_PATH}/matching_configs/hf_prompt_format_v2_error.yaml",
                "r",
            ) as f:
                LLMConfig.parse_yaml(f)
