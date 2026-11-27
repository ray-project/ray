import os
import stat
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from bazel.gen_extract import gen_extract


class GenExtractTest(unittest.TestCase):
    def test_clears_obsolete_readonly_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            generated = Path(tmp) / "python" / "generated"
            stale = generated / "nested" / "obsolete.py"
            stale.parent.mkdir(parents=True)
            stale.touch()
            stale.chmod(stat.S_IREAD)

            with patch.dict(os.environ, BUILD_WORKSPACE_DIRECTORY=tmp):
                gen_extract([], clear_dir_first=["missing", "generated"])

            self.assertFalse(generated.exists())


if __name__ == "__main__":
    unittest.main()
