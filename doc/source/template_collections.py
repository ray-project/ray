"""Build-time fetch and Sphinx wiring for external Anyscale example templates.

Everything here exists because the docs build pulls example templates from
``templates.ci.ray.io`` at build time (via sphinx-collections) and then renders
the fetched ``_collections/`` content. It is kept out of ``conf.py`` so that
changes to template fetching/publishing are scoped for CI and ownership, and
``conf.py`` stays focused on Sphinx behavior.

``conf.py`` consumes this module as::

    from template_collections import (
        collections,
        collections_clean,
        collections_final_clean,
    )
    import template_collections

    exclude_patterns += template_collections.exclude_patterns()
    ipython3_lexer_patterns = [*template_collections.IPYTHON3_LEXER_PATTERNS, ...]

    def setup(app):
        template_collections.register(app)
"""
import io
import json
import logging
import os
import pathlib
import random
import re
import time
import zipfile
from urllib.error import HTTPError, URLError
from urllib.request import urlopen

logger = logging.getLogger(__name__)

# -- sphinx-collections: pull external template files at build time -----------

_TEMPLATES_CI_BASE = "https://templates.ci.ray.io"
_TEMPLATE_CHANNEL_API = _TEMPLATES_CI_BASE + "/templates/{name}/latest/channel.json"

# Hard timeouts on the templates.ci.ray.io HTTP calls. The doc build previously
# stalled when the templates host was slow or unresponsive (#63112 revert);
# explicit timeouts let urlopen surface a TimeoutError instead of hanging
# indefinitely so `_fetch_and_extract_zip` fails fast with a clear error
# instead of blocking the build.
_TEMPLATE_CHANNEL_TIMEOUT_S = 30
_TEMPLATE_DOWNLOAD_TIMEOUT_S = 90

# Retry policy for the templates.ci.ray.io HTTP calls. Many doc builds can run
# concurrently against the same endpoint (PR previews + branch builds); under
# that load the host returns transient errors (HTTP 5xx/429, connection resets,
# read timeouts). A single urlopen with no retry would drop the template, and
# since every template is wired into a toctree that broken ref fails the whole
# build under `fail_on_warning`. Retry with exponential backoff plus full random
# jitter so the retry bursts from many concurrent builds don't re-synchronize
# and hammer the endpoint in lockstep.
_TEMPLATE_FETCH_ATTEMPTS = 3
_TEMPLATE_RETRY_BASE_S = 1.0


def _urlopen_read_with_retries(url, timeout):
    """Fetch `url` and return its body bytes, retrying transient failures.

    Retries cover both the connection and the body read (a slow `read()` under
    load can itself time out). Only transient conditions are retried — a 4xx
    other than 429 won't fix itself, so it's raised immediately. After the
    final attempt the last exception propagates to the caller, which aborts the
    build with a clear, attributed error.
    """
    last_exc = None
    for attempt in range(_TEMPLATE_FETCH_ATTEMPTS):
        try:
            with urlopen(url, timeout=timeout) as resp:
                return resp.read()
        except HTTPError as exc:
            last_exc = exc
            # HTTPError is a subclass of URLError, so it must be caught first.
            # Don't retry deterministic client errors (4xx except 429).
            if exc.code != 429 and not (500 <= exc.code < 600):
                raise
        except (URLError, TimeoutError, OSError) as exc:
            # URLError wraps DNS/refused/connection-reset; TimeoutError is the
            # urlopen/read timeout; OSError covers lower-level socket errors.
            last_exc = exc
        if attempt < _TEMPLATE_FETCH_ATTEMPTS - 1:
            base = _TEMPLATE_RETRY_BASE_S * (2 ** attempt)
            delay = base + random.uniform(0, base)  # full jitter on the delay
            logger.info(
                "sphinx-collections: retrying %s in %.1fs "
                "(attempt %d/%d) after: %s",
                url,
                delay,
                attempt + 1,
                _TEMPLATE_FETCH_ATTEMPTS,
                last_exc,
            )
            time.sleep(delay)
    raise last_exc

_TEMPLATE_COLLECTIONS = {
    "asynchronous_inference": {
        "target": "serve/tutorials/asynchronous-inference",
    },
    "audio-dataset-curation-llm-judge": {
        "target": "ray-overview/examples/e2e-audio",
    },
    "deepspeed_finetune": {
        "target": "train/examples/pytorch/deepspeed_finetune",
    },
    "deployment-serve-llm": {
        "target": "serve/tutorials/deployment-serve-llm",
    },
    "distributing-pytorch": {
        "target": "train/examples/pytorch/distributing-pytorch",
    },
    "e2e-rag-deepdive": {
        "target": "ray-overview/examples/e2e-rag",
    },
    "e2e-timeseries-forecasting": {
        "target": "ray-overview/examples/e2e-timeseries",
    },
    "entity-recognition-with-llms": {
        "target": "ray-overview/examples/entity-recognition-with-llms",
    },
    "image-search-and-classification": {
        "target": "ray-overview/examples/e2e-multimodal-ai-workloads",
    },
    "llm_batch_inference_text": {
        "target": "data/examples/llm_batch_inference_text",
    },
    "llm_batch_inference_vision": {
        "target": "data/examples/llm_batch_inference_vision",
    },
    "langchain-agent-ray-serve": {
        "target": "ray-overview/examples/langchain_agent_ray_serve/content",
    },
    "llm_finetuning": {
        "target": "ray-overview/examples/llamafactory-llm-fine-tune",
    },
    "multi_agent_a2a": {
        "target": "ray-overview/examples/multi_agent_a2a",
    },
    "mcp-ray-serve": {
        "target": "ray-overview/examples/mcp-ray-serve",
    },
    "model-composition-recsys": {
        "target": "serve/tutorials/model-composition-recsys",
    },
    "model-multiplexing": {
        "target": "serve/tutorials/model_multiplexing_forecast",
    },
    "object-detection-video-processing": {
        "target": "ray-overview/examples/object-detection",
    },
    "ray_train_workloads": {
        "target": "train/tutorials",
    },
    "pytorch-fsdp": {
        "target": "train/examples/pytorch/pytorch-fsdp",
    },
    "pytorch-profiling": {
        "target": "train/examples/pytorch/pytorch-profiling",
    },
    "tensor_parallel_autotp": {
        "target": "train/examples/pytorch/tensor_parallel_autotp",
    },
    "tensor_parallel_dtensor": {
        "target": "train/examples/pytorch/tensor_parallel_dtensor",
    },
    "tune_pytorch_asha": {
        "target": "tune/examples/tune_pytorch_asha",
    },
    "unstructured_data_ingestion": {
        "target": "data/examples/unstructured_data_ingestion",
    },
    "xgboost-training-and-serving": {
        "target": "ray-overview/examples/e2e-xgboost",
    },
}


# Pinned build id per template, loaded from template_pins.json. The docs build
# fetches these exact builds instead of `latest`, so a docs build is
# reproducible: a template rebuilt on templates.ci.ray.io can no longer
# retroactively change what a previously green docs build fetched. (An unpinned
# `latest` let a template's notebook rename silently break an otherwise-unchanged
# docs build the moment the rebuilt artifact was promoted.) Pins are bumped by
# the auto-bump workflow in anyscale/docs from each template's
# latest/channel.json `tmpl_build_id`; prefer that PR over hand-editing the JSON.
#
# Pinning assumes templates.ci.ray.io retains per-build artifacts. If an old
# build is removed, its stale pin no longer fetches.
_PINS_PATH = pathlib.Path(__file__).parent / "template_pins.json"
_TEMPLATE_PINS = json.loads(_PINS_PATH.read_text())["pins"]


def _resolve_template_url(name):
    """Return the build.zip URL for a template, honoring its pinned build id.

    A pinned template resolves directly to its immutable
    ``/{build_id}/build.zip``. templates.ci.ray.io serves no per-build
    channel.json, so the URL is constructed rather than resolved through the
    channel API. A template not yet in ``_TEMPLATE_PINS`` -- e.g. one just added
    to ``_TEMPLATE_COLLECTIONS`` -- falls back to ``latest`` with a warning so
    it still builds until a pin is added.
    """
    build_id = _TEMPLATE_PINS.get(name)
    if build_id is not None:
        url = f"{_TEMPLATES_CI_BASE}/templates/{name}/{build_id}/build.zip"
        logger.info("sphinx-collections: resolved pinned URL %s", url)
        return url

    logger.warning(
        "sphinx-collections: template %r has no entry in _TEMPLATE_PINS; "
        "falling back to 'latest'. Add a pin to make the docs build "
        "reproducible.",
        name,
    )
    api_url = _TEMPLATE_CHANNEL_API.format(name=name)
    logger.info("sphinx-collections: resolving template URL from %s", api_url)
    data = json.loads(
        _urlopen_read_with_retries(api_url, _TEMPLATE_CHANNEL_TIMEOUT_S)
    )
    # Replace the ascommon:/// protocol with the templates.ci.ray.io base URL,
    # then append /build.zip to get the docs build archive.
    url = data["url"].replace("ascommon:///", _TEMPLATES_CI_BASE + "/")
    url = url.rstrip("/") + "/build.zip"
    logger.info("sphinx-collections: resolved URL %s", url)
    return url


def _fetch_and_extract_zip(config):
    """Download a zip archive and extract it into the collection target directory.

    A failure fetching, downloading, or extracting a template aborts the build
    immediately with an error naming the template. Every template is wired into
    a toctree, so a missing one fails the build anyway under `fail_on_warning`,
    but as a "nonexisting document" warning emitted far downstream of the real
    cause. Failing here, at the fetch, points straight at the actual problem
    (the templates host or the archive) instead of a dangling toctree ref.
    Transient host errors are already absorbed by the retry/backoff in
    `_urlopen_read_with_retries`; this fires only once those are exhausted.
    """
    import shutil

    name = config["name"]
    target = pathlib.Path(config["target"])
    try:
        url = _resolve_template_url(name)
        if target.is_dir():
            shutil.rmtree(target)
        target.mkdir(parents=True, exist_ok=True)
        logger.info("sphinx-collections: downloading %s -> %s", url, target)
        zip_bytes = io.BytesIO(
            _urlopen_read_with_retries(url, _TEMPLATE_DOWNLOAD_TIMEOUT_S)
        )
        with zipfile.ZipFile(zip_bytes) as zf:
            # Guard against Zip Slip: a member name like `../../foo` would make
            # extractall write outside `target`. These archives come from the
            # first-party templates.ci.ray.io host, so this is defense in depth,
            # not a live threat, but the check is cheap and fails the build
            # clearly if a malformed archive ever ships. Normalize member paths
            # lexically (os.path.normpath, no filesystem I/O per member);
            # extractall writes symlink entries as regular files, so there's no
            # symlink-traversal risk that would need physical resolution.
            target_resolved = target.resolve()
            for member in zf.namelist():
                dest = pathlib.Path(os.path.normpath(target_resolved / member))
                if not dest.is_relative_to(target_resolved):
                    raise RuntimeError(
                        f"sphinx-collections: refusing to extract {member!r} from "
                        f"template {name!r}: path escapes {target_resolved}."
                    )
            zf.extractall(target)
        logger.info(
            "sphinx-collections: extracted %d files to %s",
            len(zf.namelist()),
            target,
        )
    except Exception as exc:
        # Don't leave a half-extracted archive in the tree, then abort with a
        # clear, attributed error. sphinx-collections runs in `safe` mode by
        # default, so re-raising here surfaces as a build-halting
        # CollectionsDriverError rather than a swallowed skip.
        if target.is_dir():
            shutil.rmtree(target, ignore_errors=True)
        raise RuntimeError(
            f"sphinx-collections: template {name!r} failed to fetch/extract into "
            f"{target} after {_TEMPLATE_FETCH_ATTEMPTS} attempt(s): {exc}. This is "
            f"a template fetch failure (templates.ci.ray.io or the archive), not a "
            f"broken toctree reference — fix the fetch and rebuild; don't chase the "
            f"downstream 'nonexisting document' warnings it would otherwise cause."
        ) from exc


collections = {
    name: {
        "driver": "function",
        "source": _fetch_and_extract_zip,
        "target": coll["target"],
        "clean": False,
        "final_clean": False,
        "write_result": False,
    }
    for name, coll in _TEMPLATE_COLLECTIONS.items()
}

# Don't wipe the target before build — other docs may co-exist in parent dirs.
collections_clean = True
# Clean up collected files after build so they don't get committed.
collections_final_clean = True

# External templates fetched by sphinx_collections (see #62179) land here at
# build time; their notebook JSON has no language_info, so Sphinx defaults to
# the python3 lexer and chokes on !pip / %magic cells. conf.py splices these
# into its ipython3_lexer_patterns list alongside the in-tree content notebooks.
IPYTHON3_LEXER_PATTERNS = [
    "_collections/**/*.ipynb",
]


def exclude_patterns():
    """exclude_patterns entries for the fetched ``_collections/`` content."""
    return [
        "_collections/serve/tutorials/deployment-serve-llm/README.*",
        "_collections/serve/tutorials/deployment-serve-llm/*.ipynb",
        "_collections/serve/tutorials/deployment-serve-llm/**/*.ipynb",
        # Each template ships README.md + README.ipynb at the same docname; keep
        # only the .md and exclude the duplicate .ipynb at the template root and
        # in any sub-template directories. The template root README.md is the
        # actual content page that toctrees / examples.yml refer to.
        *[
            pattern
            for coll in _TEMPLATE_COLLECTIONS.values()
            for pattern in (
                f"_collections/{coll['target']}/README.ipynb",
                f"_collections/{coll['target']}/**/README.ipynb",
            )
        ],
        # ray_train_workloads bundles sub-folder READMEs that aren't part of any
        # toctree (only the notebooks are). Exclude them to avoid orphan warnings.
        # Keep the root README.* — train.rst's toctree and tutorials button both
        # link to /_collections/train/tutorials/README.
        "_collections/train/tutorials/*/README.*",  # one-level sidecars (getting-started, workload-patterns)
        "_collections/train/tutorials/*/**/README.*",  # deeper sidecars, if any
        # asynchronous-inference has no docs landing page (nothing links to it), so
        # exclude its README.md to avoid an orphan warning. Do NOT list a template
        # that IS linked from a toctree here: README.ipynb is already globally
        # excluded above, so README.md is that template's canonical page — excluding
        # it deletes the page and breaks the reference (this happened to
        # tune_pytorch_asha when its notebook was renamed to README.ipynb).
        "_collections/serve/tutorials/asynchronous-inference/README.md",
        # llamafactory: master excludes the in-tree paths only, but this branch
        # also pulls a copy via sphinx-collections (see _TEMPLATE_COLLECTIONS).
        # Mirror the in-tree patterns under _collections/ so the fetched copy
        # is suppressed too. The template has no landing page on docs.ray.io.
        "_collections/ray-overview/examples/llamafactory-llm-fine-tune/README.*",
        "_collections/ray-overview/examples/llamafactory-llm-fine-tune/**/*.ipynb",
    ]


def register(app):
    """Connect the Sphinx hooks that handle fetched ``_collections/`` content."""

    class CollectionsFootnoteFilter(logging.Filter):
        # Example notebooks fetched into _collections (from templates.ci.ray.io) contain
        # prose that myst-parser 5.x parses as reST footnote refs/targets, which docutils
        # then flags as errors. That content is external (not in this repo), so it can't
        # be fixed here -- the fix belongs upstream. Drop these specific errors for
        # _collections paths so the fail_on_warning build is not blocked by fetched content.
        # TODO: fix the offending footnote-like text upstream and remove this filter.
        def filter(self, record):
            # INFO/DEBUG records (the bulk of build output) can't be the
            # footnote/target warnings below, so skip getMessage() for them.
            if record.levelno < logging.WARNING:
                return True
            msg = record.getMessage()
            if (
                "autonumbered footnote references" in msg
                or "Unknown target name" in msg
            ):
                location = str(getattr(record, "location", "") or "")
                if "_collections" in location:
                    return False
            return True

    logging.getLogger("sphinx").addFilter(CollectionsFootnoteFilter())

    # Fix code-block language tags in _collections markdown files.
    # Notebooks converted to markdown tag cells that contain a Jupyter magic or
    # shell escape (e.g. ``!uv pip install ...`` / ``%matplotlib``) as
    # ``python`` code blocks, which Pygments can't lex as Python and which fail
    # the build under ``-W``.  Re-tag any such block as ``ipython3`` so the
    # python parts stay highlighted as python and ``!``/``%`` lines render as
    # shell.  The magic can appear anywhere in the cell (a cell often runs some
    # Python and then shells out), not only on the first line.
    _PY_CODE_FENCE_RE = re.compile(r"```python\n(.*?)```", re.DOTALL)
    _MAGIC_LINE_RE = re.compile(r"^[ \t]*[!%]\S", re.MULTILINE)

    def fix_collections_code_blocks(app, docname, source):
        if not docname.startswith("_collections/"):
            return

        def _retag(match):
            body = match.group(1)
            if _MAGIC_LINE_RE.search(body):
                return "```ipython3\n" + body + "```"
            return match.group(0)

        source[0] = _PY_CODE_FENCE_RE.sub(_retag, source[0])

    app.connect('source-read', fix_collections_code_blocks)
