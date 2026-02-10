import os
import subprocess
import sys
import tempfile

import pytest

import ray
from ray._common.test_utils import wait_for_condition
from ray.cross_language import java_actor_class

# Source code of MyClass.java
_MY_CLASS_JAVA = """
public class MyClass {
    public int printToLog(String line) {
        System.err.println(line);
        return 0;
    }
}
"""


@pytest.mark.skipif(
    sys.platform == "win32" or sys.platform == "darwin",
    reason="Does not work on Windows and OSX.",
)
def test_log_java_worker_logs(shutdown_only, capsys):
    with tempfile.TemporaryDirectory() as tmp_dir:
        print("using tmp_dir", tmp_dir)
        with open(os.path.join(tmp_dir, "MyClass.java"), "w") as f:
            f.write(_MY_CLASS_JAVA)
        subprocess.check_call(["javac", "MyClass.java"], cwd=tmp_dir)
        subprocess.check_call(["jar", "-cf", "myJar.jar", "MyClass.class"], cwd=tmp_dir)

        ray.init(
            job_config=ray.job_config.JobConfig(code_search_path=[tmp_dir]),
        )

        handle = java_actor_class("MyClass").remote()
        ray.get(handle.printToLog.remote("here's my random line!"))

        def check():
            out, err = capsys.readouterr()
            out += err
            with capsys.disabled():
                print(out)
            return "here's my random line!" in out

        wait_for_condition(check)

        ray.shutdown()


if __name__ == "__main__":
    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-sv", __file__]))
