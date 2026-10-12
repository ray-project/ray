"""Unit tests for the Modal-compatible ``Image``.

Modal's own suite asserts on the Dockerfile commands each layer generates::

    layers = get_image_layers(image.object_id, servicer)
    assert any("pip install scikit-learn" in cmd for cmd in layers[0].dockerfile_commands)

The equivalent here is the build plan an Image compiles to: the commands
each step runs, what it inherits from the image, and how its layer is keyed.
Plus the ``ResolvedImage`` -- everything that becomes sandbox configuration
rather than a build step -- and the refusals for what remains unsupported.
Nothing here touches runsc or a Ray cluster.
"""

import sys

import pytest

from ray.experimental.sandbox.image_build import (
    CopyFromImageStep,
    CopyStep,
    RunStep,
    step_key,
)
from ray.experimental.sandbox.modal._image_build import prepare_build_plan
from ray.experimental.sandbox.modal.exception import InvalidError, NotFoundError
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


def test_an_empty_shell_argv_is_refused():
    with pytest.raises(InvalidError, match="interpreter"):
        Image(BASE).shell([])


@pytest.mark.parametrize(
    "argv", [["/bin/bash", "-lc"], ["/bin/bash", "-o", "pipefail", "-c"]]
)
def test_a_longer_shell_argv_is_the_build_shell(argv):
    """Like a Dockerfile SHELL, the whole argv prefixes later build commands."""
    image = Image(BASE).shell(argv).run_commands("echo x")
    assert resolve_image(image).shell == argv[0]
    (step,) = prepare_build_plan(resolve_image(image).build_specs).steps
    assert step.shell == tuple(argv)


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


# alpine/git's own config, the image the Modal measurements below used.
_GIT_CONFIG = {"Entrypoint": ["git"], "Cmd": ["--help"]}


@pytest.mark.parametrize(
    "build,override,expected",
    [
        # Each case's expectation was measured on Modal with alpine/git.
        (lambda image: image, [], ["git", "--help"]),
        (lambda image: image.cmd(["--version"]), [], ["git", "--version"]),
        # An empty argv clears the base's rather than falling back to it.
        (lambda image: image.entrypoint([]), ["echo", "MAIN"], ["echo", "MAIN"]),
        (lambda image: image.cmd([]), [], ["git"]),
        # A new ENTRYPOINT keeps the inherited CMD: Modal does not apply
        # Docker's rule that setting one resets the other.
        (
            lambda image: image.entrypoint(["/bin/echo", "EP"]),
            [],
            ["/bin/echo", "EP", "--help"],
        ),
        # Left with nothing to run, the sandbox starts idle.
        (lambda image: image.entrypoint([]).cmd([]), [], []),
    ],
)
def test_main_argv_layers_over_the_base_image_config(build, override, expected):
    resolved = resolve_image(build(Image(BASE)))
    assert resolved.main_argv(override, _GIT_CONFIG) == expected


def test_an_unset_argv_is_none_rather_than_empty():
    """None is what lets the base image's own value through."""
    resolved = resolve_image(Image(BASE))
    assert resolved.argv_prefix is None
    assert resolved.default_cmd is None


def test_metadata_alone_resolves_to_the_base_image():
    image = Image(BASE).env({"A": "1"}).workdir("/app").cmd(["x"]).entrypoint(["y"])
    assert image.reference == BASE
    resolved = resolve_image(image)
    assert resolved.reference == BASE
    assert resolved.force_token is None
    assert resolved.build_specs == ()


# -- local files -----------------------------------------------------------


def test_add_local_file_defaults_to_a_startup_push(tmp_path):
    source = tmp_path / "a.txt"
    source.write_text("hello")

    image = Image(BASE).add_local_file(source, "/app/a.txt")
    assert startup_files(image) == [(str(source), "/app/a.txt")]


def test_add_local_file_with_copy_is_a_build_step(tmp_path):
    """copy=True bakes the file into a layer instead of pushing it later."""
    source = tmp_path / "a.txt"
    source.write_text("hello")

    image = Image(BASE).add_local_file(source, "/app/", copy=True)
    assert startup_files(image) == []
    (spec,) = resolve_image(image).build_specs
    assert spec.files == ((str(source), "/app/a.txt"),)


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


def test_add_local_dir_with_copy_on_an_empty_tree_changes_nothing(tmp_path):
    (tmp_path / "empty").mkdir()
    base = Image(BASE)
    assert base.add_local_dir(tmp_path / "empty", "/app", copy=True) == base


def test_add_local_dir_with_copy_is_one_build_step(tmp_path):
    (tmp_path / "src" / "pkg").mkdir(parents=True)
    (tmp_path / "src" / "a.py").write_text("a")
    (tmp_path / "src" / "pkg" / "b.py").write_text("b")
    image = Image(BASE).add_local_dir(tmp_path / "src", "/app", copy=True)
    (spec,) = resolve_image(image).build_specs
    assert sorted(remote for _, remote in spec.files) == ["/app/a.py", "/app/pkg/b.py"]


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
        # Forcing is part of identity too: it changes what create() does.
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
        lambda: Image.from_id("im-123"),
        lambda: Image.from_name("x"),
        lambda: Image(BASE).dockerfile_commands("RUN x"),
        lambda: Image(BASE).run_function(print),
        lambda: Image(BASE).pip_install_private_repos("x", git_user="u"),
        lambda: Image(BASE).build(),
        lambda: Image(BASE).publish("name"),
        lambda: Image(BASE).hydrate(),
        lambda: Image(BASE).logs,
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


# -- build steps -------------------------------------------------------------


def plan_steps(image: Image):
    """The steps the image compiles to; COPY archives stay local bytes."""
    plan = prepare_build_plan(resolve_image(image).build_specs, put=lambda data: data)
    return list(plan.steps) if plan else []


def run_commands_of(image: Image):
    return [step.commands for step in plan_steps(image) if isinstance(step, RunStep)]


@pytest.mark.parametrize(
    "call, expected",
    [
        # Packages are sorted and shell-quoted, as Modal generates them.
        (lambda i: i.pip_install("b", "a"), [("python -m pip install a b",)]),
        (lambda i: i.pip_install(["a b", "c"]), [("python -m pip install 'a b' c",)]),
        # Modal's own quirk: two spaces before --pre when no flag precedes it.
        (lambda i: i.pip_install("a", pre=True), [("python -m pip install a  --pre",)]),
        (
            lambda i: i.pip_install(
                "a", index_url="https://x/simple", extra_options="--no-deps"
            ),
            [("python -m pip install a --index-url https://x/simple --no-deps",)],
        ),
        (
            lambda i: i.apt_install("git", "curl"),
            [("apt-get update", "apt-get install -y git curl")],
        ),
        (lambda i: i.run_commands("a", ["b", "c"]), [("a", "b", "c")]),
        (
            lambda i: i.micromamba_install("x", channels=["conda-forge"]),
            [("micromamba install x -c conda-forge --yes",)],
        ),
        (
            lambda i: i.uv_pip_install("b", "a", pre=True),
            [
                (
                    "/.uv/uv pip install --python $(command -v python) "
                    "--compile-bytecode --prerelease allow a b",
                )
            ],
        ),
    ],
)
def test_build_steps_generate_modals_commands(call, expected):
    assert run_commands_of(call(Image(BASE))) == expected


def test_uv_comes_from_its_image():
    steps = plan_steps(Image(BASE).uv_pip_install("a", uv_version="0.8.0"))
    assert steps[0] == CopyFromImageStep("ghcr.io/astral-sh/uv:0.8.0", "/uv", "/.uv/uv")


def test_debian_slim_is_modals_definition():
    image = Image.debian_slim("3.12")
    assert image.reference == "python:3.12.10-slim-bookworm"
    assert run_commands_of(image) == [
        (
            "apt-get update",
            "apt-get install -y gcc gfortran build-essential",
            "pip install --upgrade pip wheel uv",
            "echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections",
        )
    ]
    assert resolve_image(image).default_cmd == ("sleep", "172800")


def test_debian_slim_defaults_to_the_local_python():
    image = Image.debian_slim()
    assert image.reference.startswith("python:{}.{}.".format(*sys.version_info))


@pytest.mark.parametrize("version", ["2.7", "3.9", "3.13t", "banana"])
def test_debian_slim_rejects_unsupported_pythons(version):
    with pytest.raises(InvalidError):
        Image.debian_slim(version)


def test_debian_slim_rejects_a_float_version():
    with pytest.raises(TypeError, match="string"):
        Image.debian_slim(3.12)


def test_micromamba_runs_steps_through_its_shell():
    image = Image.micromamba("3.12").micromamba_install("numpy")
    assert image.reference == "mambaorg/micromamba:2.1.1-debian12-slim"
    steps = plan_steps(image)
    assert [s.commands for s in steps] == [
        ("micromamba install -n base -y python=3.12 pip -c conda-forge",),
        ("micromamba install numpy --yes",),
    ]
    assert all(s.shell == ("/usr/local/bin/_dockerfile_shell.sh",) for s in steps)
    assert ("MAMBA_DOCKERFILE_ACTIVATE", "1") in steps[0].env
    assert resolve_image(image).env == {"MAMBA_DOCKERFILE_ACTIVATE": "1"}


def test_pip_install_from_requirements_copies_the_file_first(tmp_path):
    reqs = tmp_path / "requirements.txt"
    reqs.write_text("six\n")
    copy, run = plan_steps(
        Image(BASE).pip_install_from_requirements(reqs, find_links="https://w")
    )
    assert isinstance(copy, CopyStep)
    assert run.commands == (
        "python -m pip install -r /.requirements.txt -f https://w --find-links https://w",
    )


def test_pyproject_dependencies_are_read_at_build_time(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    image = Image(BASE).pip_install_from_pyproject(pyproject, ["test"])
    # Defining the image reads nothing: the file does not even exist yet.
    pyproject.write_text(
        '[project]\nname = "x"\ndependencies = ["b", "a"]\n'
        '[project.optional-dependencies]\ntest = ["pytest"]\n'
    )
    assert run_commands_of(image) == [("python -m pip install a b pytest",)]


def test_a_pyproject_without_dependencies_fails_when_built(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.poetry]\nname = "x"\n')
    with pytest.raises(ValueError, match="project.dependencies"):
        plan_steps(Image(BASE).pip_install_from_pyproject(pyproject))


def test_uv_sync_copies_the_project_and_puts_its_venv_on_path(tmp_path):
    (tmp_path / "pyproject.toml").write_text('[project]\nname = "x"\n')
    (tmp_path / "uv.lock").write_text("lock")
    uv, copy, run = plan_steps(Image(BASE).uv_sync(str(tmp_path), groups=["dev"]))
    assert uv.dest == "/.uv/uv"
    assert isinstance(copy, CopyStep)
    assert run.commands == (
        "/.uv/uv sync --project=/.uv --no-install-workspace --compile-bytecode "
        "--group=dev --frozen",
    )
    assert run.image_env == (("PATH", "/.uv/.venv/bin:$PATH"),)


def test_uv_sync_refuses_a_workspace(tmp_path):
    (tmp_path / "pyproject.toml").write_text("[tool.uv.workspace]\nmembers = []\n")
    with pytest.raises(InvalidError, match="workspaces"):
        plan_steps(Image(BASE).uv_sync(str(tmp_path)))


def test_poetry_needs_its_lockfile(tmp_path):
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text('[tool.poetry]\nname = "x"\n')
    with pytest.raises(NotFoundError, match="poetry.lock"):
        plan_steps(Image(BASE).poetry_install_from_file(str(pyproject)))
    (tmp_path / "poetry.lock").write_text("lock")
    install, copy, run = plan_steps(
        Image(BASE).poetry_install_from_file(str(pyproject), with_=["dev"])
    )
    assert install.commands == ("python -m pip install poetry",)
    assert isinstance(copy, CopyStep)
    assert run.commands == (
        "cd /tmp/poetry && poetry config virtualenvs.create false && "
        "poetry install --no-root --with dev --compile",
    )


@pytest.mark.parametrize(
    "call",
    [
        lambda i: i.pip_install(),
        lambda i: i.apt_install(),
        lambda i: i.run_commands(),
        lambda i: i.uv_pip_install(),
        lambda i: i.micromamba_install(),
    ],
)
def test_a_build_step_with_nothing_to_do_returns_the_image(call):
    image = Image(BASE)
    assert call(image) is image


@pytest.mark.parametrize("method", ["pip_install", "uv_pip_install"])
def test_packages_cannot_smuggle_options(method):
    with pytest.raises(InvalidError, match="extra_options"):
        getattr(Image(BASE), method)("--index-url=evil")


@pytest.mark.parametrize("kwarg", ["secrets", "gpu"])
def test_build_steps_refuse_modal_only_parameters(kwarg):
    with pytest.raises(NotImplementedError):
        Image(BASE).pip_install(
            "six", **{kwarg: ["x"] if kwarg == "secrets" else "A10G"}
        )


def test_a_build_step_after_copy_false_files_is_refused(tmp_path):
    source = tmp_path / "a.txt"
    source.write_text("x")
    image = Image(BASE).add_local_file(source, "/a.txt")
    with pytest.raises(InvalidError, match="copy=True"):
        image.run_commands("cat /a.txt")
    # copy=True files may be followed by build steps.
    Image(BASE).add_local_file(source, "/a.txt", copy=True).run_commands("cat /a.txt")


def test_build_steps_see_the_image_metadata_set_before_them():
    image = (
        Image(BASE)
        .env({"A": "1"})
        .workdir("/app")
        .run_commands("one", env={"SECRET": "s"})
        .env({"B": "2"})
        .run_commands("two")
    )
    first, second = plan_steps(image)
    assert first.env == (("A", "1"),) and first.workdir == "/app"
    assert first.build_env == (("SECRET", "s"),)
    assert second.env == (("A", "1"), ("B", "2"))
    assert second.build_env == ()
    # Build-only variables never reach the sandbox.
    assert resolve_image(image).env == {"A": "1", "B": "2"}


def test_force_build_is_inherited_by_later_steps_only():
    image = (
        Image(BASE)
        .run_commands("a")
        .run_commands("b", force_build=True)
        .pip_install("c")
    )
    a, b, c = plan_steps(image)
    assert a.force_token is None
    assert b.force_token is not None and c.force_token == b.force_token


def test_a_forced_base_forces_every_step():
    image = Image.from_registry(BASE, force_build=True).run_commands("a")
    resolved = resolve_image(image)
    (step,) = plan_steps(image)
    assert resolved.force_token is not None and step.force_token == resolved.force_token


def test_force_tokens_are_per_image_object():
    first = Image(BASE).run_commands("a", force_build=True)
    second = Image(BASE).run_commands("a", force_build=True)
    assert plan_steps(first)[0].force_token != plan_steps(second)[0].force_token
    # ... but forcing is not content: the definitions are equal.
    assert first == second == Image(BASE).run_commands("a")


def test_modal_force_build_forces_every_step(monkeypatch):
    monkeypatch.setenv("MODAL_FORCE_BUILD", "1")
    (step,) = plan_steps(Image(BASE).run_commands("a"))
    assert step.force_token is not None


def test_build_steps_are_part_of_identity():
    assert Image(BASE).run_commands("a") == Image(BASE).run_commands("a")
    assert Image(BASE).run_commands("a") != Image(BASE).run_commands("b")
    assert len({Image(BASE).run_commands("a"), Image(BASE).run_commands("a")}) == 1


def test_shared_leading_steps_share_layer_keys():
    """Prefix-aware caching: the first k keys of two chains are equal."""

    def keys(image):
        key, out = "base", []
        for step in plan_steps(image):
            key = step_key(key, 0, step)
            out.append(key)
        return out

    common = Image(BASE).apt_install("git").pip_install("six")
    a = keys(common.run_commands("a"))
    b = keys(common.run_commands("b"))
    assert a[:2] == b[:2] and a[2] != b[2]
    # The parent's force epoch re-keys every descendant.
    step = plan_steps(common)[1]
    assert step_key("base", 0, step) != step_key("base", 1, step)


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
