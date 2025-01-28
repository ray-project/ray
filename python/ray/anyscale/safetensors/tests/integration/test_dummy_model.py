import filecmp
import gc
import os
import subprocess
import sys
import tempfile
import time
from contextlib import contextmanager
from typing import Dict, Generator, Optional, Tuple

import pytest
import requests
import torch
from safetensors.torch import save_file

from ray.anyscale.safetensors import set_local_cache_dir
from ray.anyscale.safetensors._private.http_downloader import (  # noqa: E402
    _flush_pending_operations,
    get_safetensor_metadata_len,
)
from ray.anyscale.safetensors._private.uri import parse_uri_info
from ray.anyscale.safetensors.exceptions import NotFoundError
from ray.anyscale.safetensors.torch import load_file

DEVICE = "cuda" if os.environ.get("TEST_ON_CUDA", "0") == "1" else "cpu"


def _gen_dummy_state_dict(*, num_tensors: int) -> Dict:
    generator = torch.Generator()
    generator.manual_seed(0)
    return {
        f"dummy_key_{i}": torch.rand(
            2048, 2048, dtype=torch.float16, generator=generator
        )
        for i in range(num_tensors, num_tensors + 16)
    }


@contextmanager
def _get_local_http_server_with_dummy_model(
    error_chance: float = 0, error_in_metadata: bool = False
) -> Generator[Tuple[str, str, str, Dict, subprocess.Popen], None, None]:
    state_dict = _gen_dummy_state_dict(num_tensors=16)
    with tempfile.TemporaryDirectory() as tmpdir:
        file_name = "dummy_model.safetensors"
        file_path = os.path.join(tmpdir, file_name)
        save_file(state_dict, file_path)

        with open(file_path, "rb") as f:
            metadata_len = get_safetensor_metadata_len(f.read(8))
        process = subprocess.Popen(
            [
                sys.executable,
                os.path.join(os.path.dirname(__file__), "range_http_server.py"),
                "8010",
                str(error_chance),
                str(metadata_len) if not error_in_metadata else "0",
            ],
            cwd=tmpdir,
        )
        assert process.poll() is None
        file_url = f"http://localhost:8010/{file_name}"
        while process.poll() is None:
            try:
                req = requests.head(file_url)
                req.raise_for_status()
                break
            except Exception:
                time.sleep(0.1)
        assert process.poll() is None
        try:
            yield file_url, file_path, file_name, state_dict, process
        finally:
            process.terminate()


@pytest.fixture()
def cleanup():
    try:
        yield
    finally:
        gc.collect()
        torch.cuda.empty_cache()


@pytest.fixture
def local_http_server_with_dummy_model(
    error_chance: float, error_in_metadata: bool
) -> Generator[Tuple[str, str, str, Dict, subprocess.Popen], None, None]:
    with _get_local_http_server_with_dummy_model(
        error_chance, error_in_metadata
    ) as info:
        yield info


def _assert_state_dicts_equal(state_dict: Dict, expected_state_dict: Dict):
    for k, v in expected_state_dict.items():
        assert torch.equal(v, state_dict[k].to("cpu")), k


def _check_cached_file(
    url: str, file_name: str, source_file_path: str, local_cache_dir: Optional[str]
):
    _flush_pending_operations(timeout_s=10)
    if local_cache_dir is None:
        return

    cached_file_path = os.path.join(
        local_cache_dir, parse_uri_info(url).cache_prefix, file_name
    )
    print(f"Checking that {cached_file_path} matches {source_file_path}...")
    assert filecmp.cmp(cached_file_path, source_file_path, shallow=False)


def _gen_matching_empty_state_dict(s: Dict, *, device: str = "cpu") -> Dict:
    return {k: v.to(device).fill_(0) for k, v in s.items()}


def _load_and_assert_equal(
    url: str,
    source_state_dict: Dict,
    *,
    device: str = "cpu",
    populate_existing_state_dict: bool = False,
    strict: bool = True,
):
    if populate_existing_state_dict:
        state_dict = _gen_matching_empty_state_dict(source_state_dict, device=device)
        load_file(url, _existing_state_dict=state_dict, _strict=strict)
    else:
        state_dict = load_file(url, device=device)

    _assert_state_dicts_equal(state_dict, source_state_dict)


@pytest.mark.parametrize("populate_existing_state_dict", [True, False])
@pytest.mark.parametrize("error_chance", [0.0, 0.2])
@pytest.mark.parametrize("error_in_metadata", [True, False])
@pytest.mark.parametrize("enable_local_cache", [True, False])
@pytest.mark.parametrize("device", [DEVICE])
def test_basic_load(
    error_chance,
    error_in_metadata,
    enable_local_cache: bool,
    local_http_server_with_dummy_model,
    populate_existing_state_dict,
    cleanup,
    tmpdir,
    device,
):
    local_cache_dir = tmpdir + "/cache" if enable_local_cache else None
    set_local_cache_dir(local_cache_dir)

    (
        url,
        source_file_path,
        file_name,
        source_state_dict,
        _,
    ) = local_http_server_with_dummy_model

    _load_and_assert_equal(
        url,
        source_state_dict,
        populate_existing_state_dict=populate_existing_state_dict,
        device=device,
    )
    _check_cached_file(url, file_name, source_file_path, local_cache_dir)


@pytest.mark.parametrize("error_chance", [1.0])
@pytest.mark.parametrize("error_in_metadata", [True, False])
@pytest.mark.parametrize("populate_existing_state_dict", [True, False])
def test_error_raised_after_retries(
    error_chance,
    error_in_metadata,
    local_http_server_with_dummy_model,
    populate_existing_state_dict,
):
    (
        url,
        source_file_path,
        file_name,
        source_state_dict,
        _,
    ) = local_http_server_with_dummy_model

    with pytest.raises(Exception):  # noqa
        if populate_existing_state_dict:
            assert False, "Not supported."
        else:
            load_file(url)


@pytest.mark.parametrize("populate_existing_state_dict", [True, False])
@pytest.mark.parametrize("error_chance", [0.0])
@pytest.mark.parametrize("error_in_metadata", [False])
@pytest.mark.parametrize("device", [DEVICE])
def test_restore_from_local_cache(
    error_chance,
    error_in_metadata,
    local_http_server_with_dummy_model,
    populate_existing_state_dict,
    device: str,
    cleanup,
    tmpdir,
):
    local_cache_dir = tmpdir + "/cache"
    set_local_cache_dir(local_cache_dir)

    (
        url,
        source_file_path,
        file_name,
        source_state_dict,
        http_server_process,
    ) = local_http_server_with_dummy_model

    # First run, should load from HTTP and populate the local cache dir.
    _load_and_assert_equal(
        url,
        source_state_dict,
        populate_existing_state_dict=populate_existing_state_dict,
        device=device,
    )
    _check_cached_file(url, file_name, source_file_path, local_cache_dir)

    # Kill the HTTP server to force loading from the local cache dir.
    http_server_process.terminate()
    http_server_process.wait(timeout=10)
    _flush_pending_operations(timeout_s=10)
    _load_and_assert_equal(
        url,
        source_state_dict,
        populate_existing_state_dict=populate_existing_state_dict,
        device=device,
    )


@pytest.mark.parametrize("error_chance", [0.0])
@pytest.mark.parametrize("error_in_metadata", [False])
@pytest.mark.parametrize("device", [DEVICE])
@pytest.mark.parametrize("load_from_cache", [True, False])
@pytest.mark.parametrize("strict", [True, False])
def test_strict_mode(
    error_chance,
    error_in_metadata,
    device: str,
    local_http_server_with_dummy_model,
    load_from_cache: bool,
    strict: bool,
    cleanup,
    tmpdir,
):
    if load_from_cache:
        local_cache_dir = tmpdir + "/cache"
    else:
        local_cache_dir = None
    set_local_cache_dir(local_cache_dir)

    (
        url,
        source_file_path,
        file_name,
        source_state_dict,
        http_server_process,
    ) = local_http_server_with_dummy_model

    state_dict = _gen_matching_empty_state_dict(source_state_dict)

    if load_from_cache:
        # Load normally to populate local cache dir.
        load_file(url, _existing_state_dict=state_dict)

        # Terminate HTTP server to ensure reads are from the local cache.
        http_server_process.terminate()
        http_server_process.wait(timeout=10)
        _flush_pending_operations(timeout_s=10)

    # Test extra key.
    state_dict_with_extra_key = state_dict.copy()
    state_dict_with_extra_key["extra_key"] = torch.empty(4, device=device)

    if strict:
        with pytest.raises(
            ValueError,
            match="Mismatch between remote and local state dict tensor names",
        ):
            load_file(
                url, _existing_state_dict=state_dict_with_extra_key, _strict=strict
            )
    else:
        load_file(url, _existing_state_dict=state_dict_with_extra_key, _strict=strict)

    # Test missing key.
    state_dict_with_missing_key = state_dict.copy()
    state_dict_with_missing_key.pop(next(iter(state_dict)))

    if strict:
        with pytest.raises(
            ValueError,
            match="Mismatch between remote and local state dict tensor names",
        ):
            load_file(
                url, _existing_state_dict=state_dict_with_missing_key, _strict=strict
            )
    else:
        load_file(url, _existing_state_dict=state_dict_with_missing_key, _strict=strict)


@pytest.mark.parametrize("error_chance", [0.0])
@pytest.mark.parametrize("error_in_metadata", [False])
def test_404_raises_not_found(
    local_http_server_with_dummy_model,
):
    """Verify that trying to download a nonexistent file raises a NotFoundError."""
    url, _, _, _, _ = local_http_server_with_dummy_model
    missing_url = url + ".fake_suffix"
    with pytest.raises(NotFoundError, match=missing_url):
        load_file(missing_url)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
