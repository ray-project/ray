"""Unit tests for the Modal-compatible ``Image``.

Modal's own suite asserts on the Dockerfile commands each layer generates::

    layers = get_image_layers(image.object_id, servicer)
    assert any("pip install scikit-learn" in cmd for cmd in layers[0].dockerfile_commands)

Image building is not implemented in this version, so there are no layers to
inspect. What remains is the ``ResolvedImage`` -- everything that becomes
sandbox configuration rather than a build step -- plus the refusals that stand
in for the builders. Nothing here touches runsc or a Ray cluster.
"""

import sys

import pytest

from ray.experimental.sandbox.modal.exception import InvalidError
from ray.experimental.sandbox.modal.image import Image, resolve_image

BASE = "python:3.13-slim"


def startup_files(image: Image):
    """The (local, remote) pairs an image would push once the sandbox is up."""
    return [(f.local_path, f.remote_path) for f in resolve_image(image).startup_files]


# -- constructors ----------------------------------------------------------


def test_from_registry_carries_the_tag():
    assert Image.from_registry("busybox:latest").reference == "busybox:latest"


def test_direct_construction_is_allowed():
    """Deliberate divergence: Modal's Image() raises, ours is a real constructor."""
    assert Image("busybox:latest").reference == "busybox:latest"


@pytest.mark.parametrize("reference", ["", "   ", None, 5, []])
def test_a_blank_or_non_string_reference_is_rejected(reference):
    with pytest.raises(InvalidError, match="non-empty string"):
        Image(reference)


def test_from_scratch_explains_why_it_cannot_work():
    with pytest.raises(NotImplementedError, match="sleep infinity"):
        Image.from_scratch()


@pytest.mark.parametrize("bad", [42, object(), b"bytes"])
def test_a_bad_image_type_is_rejected(bad):
    with pytest.raises(TypeError, match="str or Image"):
        resolve_image(bad)


# -- combinators -----------------------------------------------------------


def test_pipe_applies_a_function_and_composes():
    def add(image, message, *, value="default"):
        return image.workdir(f"/{message}").env({"FOO": value})

    image = Image(BASE).pipe(add, "piped", value="bar")
    resolved = resolve_image(image)
    assert resolved.workdir == "/piped"
    assert resolved.env == {"FOO": "bar"}


def test_imports_is_a_no_op_context_manager():
    with Image(BASE).imports():
        pass


# -- runtime metadata ------------------------------------------------------


def test_env_lands_in_the_resolved_image():
    assert resolve_image(Image(BASE).env({"HELLO": "world!"})).env == {
        "HELLO": "world!"
    }


def test_later_env_calls_win_on_conflicting_keys():
    image = Image(BASE).env({"A": "1", "B": "1"}).env({"B": "2"})
    assert resolve_image(image).env == {"A": "1", "B": "2"}


@pytest.mark.parametrize("vars", [{"K": 123}, {"K": []}, {5: "v"}])
def test_env_values_must_be_strings(vars):
    with pytest.raises(InvalidError, match="must be strings"):
        Image(BASE).env(vars)


def test_env_must_be_a_dict():
    with pytest.raises(InvalidError, match="must be a dict"):
        Image(BASE).env(["A=1"])


def test_workdir_lands_in_the_resolved_image():
    assert resolve_image(Image(BASE).workdir("/app")).workdir == "/app"


@pytest.mark.parametrize("path", ["relative", "./x", "app/sub"])
def test_workdir_must_be_absolute(path):
    with pytest.raises(InvalidError, match="absolute"):
        Image(BASE).workdir(path)


def test_shell_maps_a_single_interpreter():
    assert resolve_image(Image(BASE).shell(["/bin/sh"])).shell == "/bin/sh"


@pytest.mark.parametrize("argv", [["/bin/bash", "-c"], ["/bin/sh", "-c"]])
def test_shell_accepts_the_canonical_modal_argv(argv):
    """`["/bin/bash", "-c"]` is how Modal code spells this, and the backend
    appends `-c` itself, so the trailing token is dropped rather than refused."""
    assert resolve_image(Image(BASE).shell(argv)).shell == argv[0]


@pytest.mark.parametrize("argv", [[], ["/bin/bash", "-lc"], ["/bin/sh", "-c", "x"]])
def test_an_unmappable_shell_argv_is_refused(argv):
    with pytest.raises(NotImplementedError, match="single interpreter path"):
        Image(BASE).shell(argv)


@pytest.mark.parametrize("bad", ["bash", 5, {"a": 1}, ["ok", 5]])
def test_shell_rejects_anything_but_a_list_of_strings(bad):
    with pytest.raises(InvalidError, match="list of strings"):
        Image(BASE).shell(bad)


def test_cmd_and_entrypoint_land_in_the_resolved_image():
    image = Image(BASE).entrypoint(["/usr/bin/env"]).cmd(["echo", "hi"])
    resolved = resolve_image(image)
    assert resolved.argv_prefix == ("/usr/bin/env",)
    assert resolved.default_cmd == ("echo", "hi")


@pytest.mark.parametrize("method", ["cmd", "entrypoint"])
@pytest.mark.parametrize("bad", ["sh", 4711, {"a": 1}, ["ok", 5]])
def test_argv_setters_reject_anything_but_a_list_of_strings(method, bad):
    """`cmd("sh")` would otherwise become ('s', 'h') -- a string is iterable."""
    with pytest.raises(InvalidError, match="list of strings"):
        getattr(Image(BASE), method)(bad)


@pytest.mark.parametrize("method", ["cmd", "entrypoint"])
def test_an_empty_argv_is_accepted(method):
    getattr(Image(BASE), method)([])


@pytest.mark.parametrize(
    "entrypoint,cmd,override,expected",
    [
        ((), (), [], []),
        ((), ("a",), [], ["a"]),
        ((), ("a",), ["b"], ["b"]),
        (("e",), ("a",), [], ["e", "a"]),
        # Modal: create(*args) overrides CMD, ENTRYPOINT still prefixes.
        (("e",), ("a",), ["b"], ["e", "b"]),
        (("e",), (), [], ["e"]),
    ],
)
def test_main_argv_precedence(entrypoint, cmd, override, expected):
    image = Image(BASE).entrypoint(list(entrypoint)).cmd(list(cmd))
    assert resolve_image(image).main_argv(override) == expected


def test_metadata_alone_resolves_to_the_base_image():
    image = Image(BASE).env({"A": "1"}).workdir("/app").cmd(["x"]).entrypoint(["y"])
    assert image.reference == BASE
    resolved = resolve_image(image)
    assert resolved.reference == BASE
    assert resolved.force_pull is False


# -- local files -----------------------------------------------------------


def test_add_local_file_defaults_to_a_startup_push(tmp_path):
    source = tmp_path / "a.txt"
    source.write_text("hello")

    image = Image(BASE).add_local_file(source, "/app/a.txt")
    assert startup_files(image) == [(str(source), "/app/a.txt")]


def test_add_local_file_with_copy_is_refused(tmp_path):
    """copy=True bakes the file into a layer, which needs a build."""
    source = tmp_path / "a.txt"
    source.write_text("hello")

    with pytest.raises(NotImplementedError, match="copy=False"):
        Image(BASE).add_local_file(source, "/app/a.txt", copy=True)


def test_a_trailing_slash_appends_the_basename(tmp_path):
    source = tmp_path / "a.txt"
    source.write_text("x")
    image = Image(BASE).add_local_file(source, "/app/")
    assert resolve_image(image).startup_files[0].remote_path == "/app/a.txt"


def test_add_local_dir_walks_the_tree(tmp_path):
    (tmp_path / "pkg").mkdir()
    (tmp_path / "pkg" / "mod.py").write_text("x")
    (tmp_path / "top.txt").write_text("y")

    image = Image(BASE).add_local_dir(tmp_path, "/app")
    destinations = {remote for _, remote in startup_files(image)}
    assert destinations == {"/app/top.txt", "/app/pkg/mod.py"}


def test_add_local_dir_honours_an_ignore_sequence(tmp_path):
    (tmp_path / "keep.py").write_text("x")
    (tmp_path / "drop.pyc").write_text("y")

    image = Image(BASE).add_local_dir(tmp_path, "/app", ignore=["*.pyc"])
    assert {remote for _, remote in startup_files(image)} == {"/app/keep.py"}


def test_add_local_dir_honours_an_ignore_callable(tmp_path):
    (tmp_path / "keep.py").write_text("x")
    (tmp_path / "skip.py").write_text("y")

    image = Image(BASE).add_local_dir(
        tmp_path, "/app", ignore=lambda path: path.name.startswith("skip")
    )
    paths = {item.remote_path for item in resolve_image(image).startup_files}
    assert paths == {"/app/keep.py"}


def test_add_local_dir_rejects_a_file(tmp_path):
    source = tmp_path / "a.txt"
    source.write_text("x")
    with pytest.raises(NotADirectoryError):
        Image(BASE).add_local_dir(source, "/app")


def test_add_local_dir_adds_a_tree_in_one_step(tmp_path):
    """Going through add_local_file per entry rebuilt the whole tuple and a
    whole Image each time, which is quadratic in the size of the tree."""
    for i in range(50):
        (tmp_path / f"f{i}.txt").write_text("x")
    base = Image(BASE).add_local_file(tmp_path / "f0.txt", "/pre/existing.txt")

    evolutions = 0
    original = Image._evolve

    def counting_evolve(self, **changes):
        nonlocal evolutions
        evolutions += 1
        return original(self, **changes)

    Image._evolve = counting_evolve
    try:
        image = base.add_local_dir(tmp_path, "/app")
    finally:
        Image._evolve = original

    assert evolutions == 1, "one tree, one new Image"
    assert len(startup_files(image)) == 51, "the earlier addition is kept"


def test_add_local_dir_keeps_the_order_of_the_tree(tmp_path):
    for name in ("c.txt", "a.txt", "b.txt"):
        (tmp_path / name).write_text("x")
    image = Image(BASE).add_local_dir(tmp_path, "/app")
    assert [remote for _, remote in startup_files(image)] == [
        "/app/a.txt",
        "/app/b.txt",
        "/app/c.txt",
    ]


def test_add_local_dir_on_an_empty_tree_changes_nothing(tmp_path):
    (tmp_path / "empty").mkdir()
    base = Image(BASE)
    assert base.add_local_dir(tmp_path / "empty", "/app") == base


def test_add_local_dir_refuses_copy_even_for_an_empty_tree(tmp_path):
    """The refusal used to live in the loop body, so an empty tree slipped
    past it and returned an Image that had quietly ignored copy=True."""
    (tmp_path / "empty").mkdir()
    with pytest.raises(NotImplementedError, match="copy=True"):
        Image(BASE).add_local_dir(tmp_path / "empty", "/app", copy=True)


@pytest.mark.parametrize("method", ["add_local_file", "add_local_dir"])
def test_local_additions_require_an_absolute_remote_path(tmp_path, method):
    (tmp_path / "a.txt").write_text("x")
    target = tmp_path / "a.txt" if method == "add_local_file" else tmp_path
    with pytest.raises(InvalidError, match="absolute"):
        getattr(Image(BASE), method)(target, "relative/path")


def test_add_local_python_source_locates_a_package():
    image = Image(BASE).add_local_python_source("json")
    paths = {item.remote_path for item in resolve_image(image).startup_files}
    assert any(path.endswith("/json/__init__.py") for path in paths)


def test_add_local_python_source_includes_only_python_files(tmp_path, monkeypatch):
    """Modal's default ignore is NON_PYTHON_FILES -- only `.py` survives.

    Defaulting to ("*.pyc",) instead meant everything *except* compiled files
    was copied, so a package's .env, shared objects and nested .git went into
    the image, and into the recipe digest under copy=True.
    """
    import importlib

    package = tmp_path / "fixturepkg"
    package.mkdir()
    (package / "__init__.py").write_text("")
    (package / "mod.py").write_text("x = 1")
    (package / "data.json").write_text("{}")
    (package / "mod.pyc").write_bytes(b"\x00")
    (package / ".secret").write_text("token")
    (package / "__pycache__").mkdir()
    (package / "__pycache__" / "mod.cpython-311.pyc").write_bytes(b"\x00")

    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    image = Image(BASE).add_local_python_source("fixturepkg")
    names = {
        item.remote_path.rsplit("/", 1)[-1]
        for item in resolve_image(image).startup_files
    }
    assert names == {"__init__.py", "mod.py"}


def test_add_local_python_source_rejects_an_unknown_module():
    with pytest.raises(ModuleNotFoundError, match="definitely_not_a_module"):
        Image(BASE).add_local_python_source("definitely_not_a_module")


@pytest.mark.parametrize("bad", [5, None, [5]])
def test_add_local_python_source_rejects_non_string_modules(bad):
    """InvalidError, not TypeError: Modal's _flatten_str_args raises that, so
    `except modal.exception.InvalidError` has to catch it."""
    with pytest.raises(InvalidError, match="must only contain strings"):
        Image(BASE).add_local_python_source(bad)


def test_add_local_python_source_accepts_a_nested_list():
    """Modal's *args convention: strings or lists of strings."""
    image = Image(BASE).add_local_python_source(["json"])
    assert resolve_image(image).startup_files


# -- immutability and equality ---------------------------------------------


@pytest.mark.parametrize(
    "call",
    [
        lambda image: image.env({"A": "1"}),
        lambda image: image.workdir("/app"),
        lambda image: image.cmd(["x"]),
        lambda image: image.entrypoint(["y"]),
        lambda image: image.shell(["/bin/sh"]),
    ],
)
def test_every_builder_leaves_the_receiver_untouched(call):
    original = Image(BASE)
    derived = call(original)
    assert derived is not original
    assert resolve_image(original).env == {}
    assert resolve_image(original).workdir is None
    assert derived != original


def test_equal_images_compare_and_hash_alike():
    assert Image(BASE) == Image.from_registry(BASE)
    assert len({Image(BASE), Image.from_registry(BASE)}) == 1


@pytest.mark.parametrize(
    "call",
    [
        lambda image: image.env({"A": "1"}),
        lambda image: image.workdir("/app"),
        lambda image: image.cmd(["x"]),
        lambda image: image.entrypoint(["y"]),
        lambda image: image.shell(["/bin/sh"]),
        # force_pull is part of identity too: it changes what create() does.
        lambda image: Image.from_registry(image.reference, force_build=True),
    ],
)
def test_metadata_differences_make_images_unequal(call):
    assert call(Image(BASE)) != Image(BASE)


def test_images_are_usable_as_dict_keys():
    mapping = {Image(BASE): "a", Image(BASE).env({"X": "1"}): "b"}
    assert len(mapping) == 2


# -- the unimplemented surface ---------------------------------------------


@pytest.mark.parametrize(
    "call",
    [
        lambda: Image.from_dockerfile("Dockerfile"),
        lambda: Image.from_aws_ecr("x"),
        lambda: Image.from_gcp_artifact_registry("x"),
        lambda: Image.micromamba(),
        lambda: Image.from_id("im-123"),
        lambda: Image.from_name("x"),
        lambda: Image(BASE).dockerfile_commands("RUN x"),
        lambda: Image(BASE).run_function(print),
        lambda: Image(BASE).pip_install_private_repos("x", git_user="u"),
        lambda: Image(BASE).uv_pip_install("x"),
        lambda: Image(BASE).uv_sync(),
        lambda: Image(BASE).poetry_install_from_file("pyproject.toml"),
        lambda: Image(BASE).micromamba_install("x"),
        lambda: Image(BASE).build(),
        lambda: Image(BASE).publish("name"),
        lambda: Image(BASE).hydrate(),
        lambda: Image(BASE).logs,
        # Unimplemented specifically because image building is not: each of
        # these is a body away from working once the builder returns.
        lambda: Image.debian_slim(),
        lambda: Image.debian_slim("3.12"),
        lambda: Image(BASE).run_commands("echo x"),
        lambda: Image(BASE).pip_install("six"),
        lambda: Image(BASE).apt_install("git"),
        lambda: Image(BASE).pip_install_from_requirements("requirements.txt"),
        lambda: Image(BASE).pip_install_from_pyproject("pyproject.toml"),
    ],
)
def test_the_unsupported_surface_is_refused(call):
    with pytest.raises(NotImplementedError, match="Ray sandbox backend"):
        call()


@pytest.mark.parametrize(
    "call",
    [
        lambda: Image.from_registry("x", secret=object()),
        lambda: Image.from_registry("x", secrets=[object()]),
        lambda: Image.from_registry("x", setup_dockerfile_commands=["RUN y"]),
        lambda: Image.from_registry("x", add_python="3.12"),
    ],
)
def test_unsupported_build_parameters_are_refused(call):
    with pytest.raises(NotImplementedError):
        call()


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
