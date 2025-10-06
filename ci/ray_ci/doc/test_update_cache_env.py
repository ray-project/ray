import os
import pickle
import sys
import tempfile

import pytest
from sphinx.project import Project

from ci.ray_ci.doc.build_cache import ENVIRONMENT_PICKLE
from ci.ray_ci.doc.cmd_update_cache_env import update_environment_pickle


class FakeBuildEnv:
    def __init__(self, srcdir: str, doctreedir: str, project: Project, all_docs: dict):
        self.srcdir = "srcdir"
        self.doctreedir = "doctreedir"
        self.project = Project(
            "srcdir",
            {".rst": "restructuredtext", ".md": "myst-nb", ".ipynb": "myst-nb"},
        )
        self.project.discover()
        self.all_docs = {}


def _generate_test_env():
    list_files = ["file1", "file2", "file3", "file4", "file5"]
    with tempfile.TemporaryDirectory() as temp_dir:
        env = FakeBuildEnv(
            os.path.join(temp_dir, "source"),
            os.path.join(temp_dir, "_build/doctrees"),
            Project(
                os.path.join(temp_dir, "source"),
                {".rst": "restructuredtext", ".md": "myst-nb", ".ipynb": "myst-nb"},
            ),
            {},
        )
        p = Project(
            env.srcdir,
            {".rst": "restructuredtext", ".md": "myst-nb", ".ipynb": "myst-nb"},
        )
        p.discover()
        env.project = p
        env.all_docs = {}
        # If you have a list of documents, you can add them like this:
        current_time = 1234567890
        for doc in list_files:
            env.all_docs[doc] = current_time
        return env


def test_update_environment_pickle():
    with tempfile.TemporaryDirectory() as temp_dir:
        env = _generate_test_env()
        os.makedirs(os.path.join(temp_dir, "doc/_build/doctrees"))
        with open(os.path.join(temp_dir, "doc", ENVIRONMENT_PICKLE), "wb+") as f:
            pickle.dump(env, f, pickle.HIGHEST_PROTOCOL)

        pending_files = ["file1", "file2", "file3"]
        update_environment_pickle(temp_dir, pending_files)
        with open(os.path.join(temp_dir, "doc", ENVIRONMENT_PICKLE), "rb+") as f:
            env = pickle.load(f)
            assert env.srcdir == os.path.join(temp_dir, "doc/source")
            assert env.doctreedir == os.path.join(temp_dir, "doc/_build/doctrees")
            assert env.project.srcdir == os.path.join(temp_dir, "doc/source")
            assert len(env.all_docs) == 5
            assert env.all_docs["file1"] == 1234567890
            assert env.all_docs["file2"] == 1234567890
            assert env.all_docs["file3"] == 1234567890
            assert env.all_docs["file4"] != 1234567890
            assert env.all_docs["file5"] != 1234567890


if __name__ == "__main__":
    sys.exit(pytest.main(["-vv", __file__]))
