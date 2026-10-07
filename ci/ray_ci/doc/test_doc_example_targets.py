import sys

import pytest

from ci.ray_ci.doc.doc_example_targets import (
    format_report,
    label_to_path,
    parse_query_xml,
    select,
)

# Trimmed `bazel query 'tests(//doc/...)' --output=xml` output, one rule per
# shape the doc tests take.
_QUERY_XML = """<?xml version="1.1" encoding="UTF-8" standalone="no"?>
<query version="2">
  <rule class="py_test" name="//doc:source/data/doc_code/loading">
    <list name="tags"><string value="exclusive"/><string value="team:data"/></list>
    <list name="srcs"><label value="//doc:source/data/doc_code/loading.py"/></list>
  </rule>
  <rule class="py_test" name="//doc:batch_prediction">
    <list name="tags"><string value="team:ml"/></list>
    <list name="args">
      <string value="--path"/>
      <string value="doc/source/core/examples/batch_prediction.ipynb"/>
    </list>
    <list name="data"><label value="//doc/source/core/examples:core_examples"/></list>
    <list name="srcs"><label value="//doc:test_myst_doc.py"/></list>
  </rule>
  <rule class="py_test" name="//doc/source/tune/examples:ax_example">
    <list name="tags"><string value="team:ml"/></list>
    <list name="args">
      <string value="--find-recursively"/>
      <string value="--path"/>
      <string value="ax_example.ipynb"/>
    </list>
    <list name="srcs"><label value="//doc:test_myst_doc.py"/></list>
  </rule>
  <rule class="py_test" name="//doc:source/data/working-with-tensors">
    <list name="tags">
      <string value="doctest"/><string value="team:data"/><string value="cpu"/>
    </list>
    <list name="data"><label value="//doc:source/data/working-with-tensors.md"/></list>
  </rule>
  <rule class="py_test" name="//doc:doctest[serve]">
    <list name="tags">
      <string value="doctest"/><string value="team:serve"/><string value="cpu"/>
    </list>
    <list name="data">
      <label value="//doc:source/serve/a.md"/>
      <label value="//doc:source/serve/b.md"/>
    </list>
  </rule>
  <rule class="py_test" name="//doc/source/llm/examples/batch:vllm">
    <list name="tags"><string value="gpu"/><string value="team:llm"/></list>
    <list name="args"><string value="--path"/><string value="vllm.ipynb"/></list>
  </rule>
</query>
"""


@pytest.fixture
def targets():
    return parse_query_xml(_QUERY_XML)


def test_label_to_path():
    assert label_to_path("//doc:source/a.py") == "doc/source/a.py"
    assert label_to_path("//doc/source/x:y.ipynb") == "doc/source/x/y.ipynb"
    assert label_to_path("@py_deps//pytest") is None


def test_selects_only_the_target_naming_the_file(targets):
    selection = select(["doc/source/data/doc_code/loading.py"], targets)
    assert selection.runnable == {"data": ["//doc:source/data/doc_code/loading"]}


def test_notebook_matches_by_path_argument_not_filegroup(targets):
    # Both notebook forms: a repo-relative --path in doc/BUILD.bazel, and a
    # package-relative one from py_test_run_all_notebooks.
    selection = select(
        [
            "doc/source/core/examples/batch_prediction.ipynb",
            "doc/source/tune/examples/ax_example.ipynb",
        ],
        targets,
    )
    assert selection.runnable == {
        "ml": ["//doc/source/tune/examples:ax_example", "//doc:batch_prediction"]
    }
    # A sibling notebook in the same filegroup runs nothing.
    selection = select(["doc/source/core/examples/other.ipynb"], targets)
    assert selection.runnable == {}
    assert selection.unmatched == ["doc/source/core/examples/other.ipynb"]


def test_per_file_doctest_runs(targets):
    selection = select(["doc/source/data/working-with-tensors.md"], targets)
    assert selection.runnable == {"data": ["//doc:source/data/working-with-tensors"]}


def test_library_wide_doctest_is_reported_not_run(targets):
    selection = select(["doc/source/serve/a.md"], targets)
    assert selection.runnable == {}
    assert selection.library_wide == {"doc/source/serve/a.md": ["//doc:doctest[serve]"]}


def test_gpu_target_is_reported_not_run(targets):
    path = "doc/source/llm/examples/batch/vllm.ipynb"
    selection = select([path], targets)
    assert selection.runnable == {}
    assert selection.gpu == {path: ["//doc/source/llm/examples/batch:vllm"]}


def test_harness_and_build_files_are_ignored(targets):
    # doc/test_myst_doc.py is in every notebook target's srcs, but it's the
    # shared runner, not an example, so it must not select them all.
    selection = select(["doc/test_myst_doc.py", "doc/source/BUILD.bazel"], targets)
    assert selection.runnable == {}
    assert selection.unmatched == []


def test_report_groups_by_team(targets):
    selection = select(
        [
            "doc/source/data/doc_code/loading.py",
            "doc/source/core/examples/batch_prediction.ipynb",
            "doc/source/serve/a.md",
        ],
        targets,
    )
    report = format_report(selection)
    assert "Running (data): //doc:source/data/doc_code/loading" in report
    assert "Running (ml): //doc:batch_prediction" in report
    assert "library-wide target //doc:doctest[serve]" in report


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
