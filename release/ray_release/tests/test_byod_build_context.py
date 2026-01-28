import os
import sys
import tempfile

import pytest

from ray_release.byod.build_context import (
    _INSTALL_PYTHON_DEPS_SCRIPT,
    build_context_digest,
    decode_build_context,
    encode_build_context,
    fill_build_context_dir,
    make_build_context,
)


def test_make_build_context() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = os.path.join(tmpdir, "post_build.sh")
        with open(script_path, "w") as f:
            f.write("echo hello")

        depset_path = os.path.join(tmpdir, "deps.lock")
        with open(depset_path, "w") as f:
            f.write("numpy==1.0.0")

        ctx = make_build_context(
            base_dir=tmpdir,
            envs={"FOO": "bar"},
            post_build_script="post_build.sh",
            python_depset="deps.lock",
        )

        assert ctx == {
            "envs": {"FOO": "bar"},
            "post_build_script": "post_build.sh",
            "post_build_script_digest": ctx["post_build_script_digest"],
            "python_depset": "deps.lock",
            "python_depset_digest": ctx["python_depset_digest"],
            "install_python_deps_script_digest": ctx[
                "install_python_deps_script_digest"
            ],
        }
        assert ctx["post_build_script_digest"].startswith("sha256:")
        assert ctx["python_depset_digest"].startswith("sha256:")
        assert ctx["install_python_deps_script_digest"].startswith("sha256:")


def test_make_build_context_partial() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        script_path = os.path.join(tmpdir, "post_build.sh")
        with open(script_path, "w") as f:
            f.write("echo hello")

        ctx = make_build_context(
            base_dir=tmpdir,
            post_build_script="post_build.sh",
        )

        assert ctx == {
            "post_build_script": "post_build.sh",
            "post_build_script_digest": ctx["post_build_script_digest"],
        }
        assert ctx["post_build_script_digest"].startswith("sha256:")


def test_make_build_context_empty() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        ctx = make_build_context(base_dir=tmpdir)
        assert ctx == {}


def test_encode_build_context() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "build.sh"), "w") as f:
            f.write("echo hello")

        ctx = make_build_context(
            base_dir=tmpdir,
            envs={"ZZZ": "last", "AAA": "first"},
            post_build_script="build.sh",
        )
        encoded = encode_build_context(ctx)

        # Verify minified (no spaces)
        assert " " not in encoded

        # Verify deterministic via digest - same content in different order produces same digest
        ctx_reordered = {
            "post_build_script_digest": ctx["post_build_script_digest"],
            "post_build_script": "build.sh",
            "envs": {"AAA": "first", "ZZZ": "last"},
        }
        assert build_context_digest(ctx) == build_context_digest(ctx_reordered)


def test_decode_build_context() -> None:
    data = '{"envs":{"FOO":"bar"},"post_build_script":"build.sh"}'
    ctx = decode_build_context(data)

    assert ctx["envs"] == {"FOO": "bar"}
    assert ctx["post_build_script"] == "build.sh"


def test_encode_decode_roundtrip() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "script.sh"), "w") as f:
            f.write("echo hello")

        with open(os.path.join(tmpdir, "deps.lock"), "w") as f:
            f.write("numpy==1.0.0")

        ctx = make_build_context(
            base_dir=tmpdir,
            envs={"KEY": "value"},
            post_build_script="script.sh",
            python_depset="deps.lock",
        )

        encoded = encode_build_context(ctx)
        decoded = decode_build_context(encoded)

        assert decoded == ctx


def test_build_context_digest() -> None:
    with tempfile.TemporaryDirectory() as tmpdir:
        with open(os.path.join(tmpdir, "build.sh"), "w") as f:
            f.write("echo hello")

        with open(os.path.join(tmpdir, "build2.sh"), "w") as f:
            f.write("echo world")

        ctx1 = make_build_context(
            base_dir=tmpdir,
            post_build_script="build.sh",
        )
        ctx2 = make_build_context(
            base_dir=tmpdir,
            post_build_script="build2.sh",
        )
        ctx3 = make_build_context(
            base_dir=tmpdir,
            envs={"FOO": "bar"},
            post_build_script="build.sh",
        )

        digest1 = build_context_digest(ctx1)
        digest2 = build_context_digest(ctx2)
        digest3 = build_context_digest(ctx3)

        # Different file contents produce different digests
        assert digest1 != digest2
        # Different envs produce different digests
        assert digest1 != digest3
        # All three are different
        assert len({digest1, digest2, digest3}) == 3


def test_fill_build_context_dir_empty() -> None:
    with (
        tempfile.TemporaryDirectory() as source_dir,
        tempfile.TemporaryDirectory() as build_dir,
    ):
        ctx = make_build_context(base_dir=source_dir)

        fill_build_context_dir(ctx, source_dir, build_dir)

        # Check Dockerfile
        with open(os.path.join(build_dir, "Dockerfile")) as f:
            dockerfile = f.read()
        expected_dockerfile = "\n".join(
            [
                "# syntax=docker/dockerfile:1.3-labs",
                "ARG BASE_IMAGE",
                "FROM ${BASE_IMAGE}",
                "",
            ]
        )
        assert dockerfile == expected_dockerfile

        # Check no other files were created
        assert os.listdir(build_dir) == ["Dockerfile"]


def test_fill_build_context_dir() -> None:
    with (
        tempfile.TemporaryDirectory() as source_dir,
        tempfile.TemporaryDirectory() as build_dir,
    ):
        # Create input files
        with open(os.path.join(source_dir, "post_build.sh"), "w") as f:
            f.write("#!/bin/bash\necho hello")

        with open(os.path.join(source_dir, "deps.lock"), "w") as f:
            f.write("numpy==1.0.0\npandas==2.0.0")

        ctx = make_build_context(
            base_dir=source_dir,
            envs={"FOO": "bar", "BAZ": "qux"},
            post_build_script="post_build.sh",
            python_depset="deps.lock",
        )

        fill_build_context_dir(ctx, source_dir, build_dir)

        # Check Dockerfile
        with open(os.path.join(build_dir, "Dockerfile")) as f:
            dockerfile = f.read()
        expected_dockerfile = "\n".join(
            [
                "# syntax=docker/dockerfile:1.3-labs",
                "ARG BASE_IMAGE",
                "FROM ${BASE_IMAGE}",
                "ENV \\",
                "  BAZ=qux \\",
                "  FOO=bar",
                "COPY install_python_deps.sh /tmp/install_python_deps.sh",
                "COPY python_depset.lock python_depset.lock",
                "RUN bash /tmp/install_python_deps.sh python_depset.lock",
                "COPY post_build_script.sh /tmp/post_build_script.sh",
                "RUN bash /tmp/post_build_script.sh",
                "",
            ]
        )
        assert dockerfile == expected_dockerfile

        # Check copied files
        with open(os.path.join(build_dir, "post_build_script.sh")) as f:
            assert f.read() == "#!/bin/bash\necho hello"

        with open(os.path.join(build_dir, "python_depset.lock")) as f:
            assert f.read() == "numpy==1.0.0\npandas==2.0.0"

        with open(os.path.join(build_dir, "install_python_deps.sh")) as f:
            assert f.read() == _INSTALL_PYTHON_DEPS_SCRIPT


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
