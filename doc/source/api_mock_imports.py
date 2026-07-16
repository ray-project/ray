"""Single source of truth for modules mocked during API doc generation.

Three consumers import from here so they agree on what "not installed in the
docbuild image" means:

- ``conf.py`` -- Sphinx ``autodoc_mock_imports`` for the full ``make html``
  build.
- ``api_autogen.py`` -- the standalone autosummary stub generator.
- ``ci/ray_ci/doc`` -- the API/doc consistency check, whose ``resolve()`` walk
  imports documented names directly (bypassing Sphinx), so it must apply the
  same mocks or an optional-dependency API (for example ``ray.data.llm.*``,
  which pulls in the vLLM/SGLang batch stack) reads as "does not resolve".

Keep this module free of imports and side effects: it is imported both by the
Sphinx config and by a plain-``python`` CI script.
"""

# Third-party libraries absent from the docbuild image. Safe for any consumer to
# mock, including the consistency check (which imports Ray for real). Do NOT add
# ``ray.*`` here: the check resolves Ray's own symbols against real objects, and
# a mock answers any getattr -- mocking a ``ray.*`` path would blind the
# resolve/dedup policy to deleted or renamed Ray APIs under it.
THIRD_PARTY_MOCK_MODULES = [
    "aiohttp",
    "async_timeout",
    "backoff",
    "cachetools",
    "comet_ml",
    "composer",
    "cupy",
    "dask",
    "datasets",
    "fastapi",
    "filelock",
    "fsspec",
    "google",
    "grpc",
    "gymnasium",
    "horovod",
    "huggingface",
    "httpx",
    "joblib",
    "lightgbm",
    "lightgbm_ray",
    "mlflow",
    "nevergrad",
    "pandas",
    "pytorch_lightning",
    "scipy",
    "setproctitle",
    "skimage",
    "sklearn",
    "starlette",
    "tensorflow",
    "torch",
    "torchvision",
    "transformers",
    "tree",
    "typer",
    "uvicorn",
    "wandb",
    "watchfiles",
    "openai",
    "xgboost",
    "xgboost_ray",
    "psutil",
    "colorama",
    "vllm",
]

# Compiled/generated Ray modules that are absent only when docs build against a
# source checkout without a built Ray. The consistency check runs against an
# installed Ray wheel where these are present, so it must NOT mock them; only the
# Sphinx build adds these.
BUILD_ONLY_MOCK_MODULES = [
    "ray._raylet",
    "ray.core.generated",
    "ray.serve.generated",
]
