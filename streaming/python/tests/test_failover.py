import subprocess
import sys
import time
from typing import List

import ray
from ray.streaming import StreamingContext


def test_word_count():
    try:
        ray.init(
            job_config=ray.job_config.JobConfig(code_search_path=sys.path))
        # time.sleep(10) # for gdb to attach
        ctx = StreamingContext.Builder() \
            .option("streaming.context-backend.type", "local_file") \
            .option(
                "streaming.context-backend.file-state.root",
                "/tmp/ray/cp_files/"
            ) \
            .option("streaming.checkpoint.timeout.secs", "3") \
            .build()

        print("-----------submit job-------------")

        ctx.read_text_file(__file__) \
            .set_parallelism(1) \
            .flat_map(lambda x: x.split()) \
            .map(lambda x: (x, 1)) \
            .key_by(lambda x: x[0]) \
            .reduce(lambda old_value, new_value:
                    (old_value[0], old_value[1] + new_value[1])) \
            .filter(lambda x: "ray" not in x) \
            .sink(lambda x: print("####result", x))
        ctx.submit("word_count")

        print("-----------checking output-------------")
        retry_count = 180 / 5  # wait for 3min
        while not has_sink_output():
            time.sleep(5)
            retry_count -= 1
            if retry_count <= 0:
                raise RuntimeError("Can not find output")

        print("-----------killing worker-------------")
        time.sleep(5)
        kill_all_worker()

        print("-----------checking checkpoint-------------")
        cp_ok_num = checkpoint_success_num()
        retry_count = 300000 / 5  # wait for 5min
        while True:
            cur_cp_num = checkpoint_success_num()
            print("-----------checking checkpoint"
                  ", cur_cp_num={}, old_cp_num={}-------------".format(
                      cur_cp_num, cp_ok_num))
            if cur_cp_num > cp_ok_num:
                print("--------------TEST OK!------------------")
                break
            time.sleep(5)
            retry_count -= 1
            if retry_count <= 0:
                raise RuntimeError(
                    "Checkpoint keeps failing after fail-over, test failed!")
    finally:
        ray.shutdown()


def run_cmd(cmd: List):
    try:
        out = subprocess.check_output(cmd).decode()
    except subprocess.CalledProcessError as e:
        out = str(e)
    return out


def grep_log(keyword: str) -> str:
    out = subprocess.check_output(
        ["grep", "-r", keyword, "/tmp/ray/session_latest/logs"])
    return out.decode()


def has_sink_output() -> bool:
    try:
        grep_log("####result")
        return True
    except Exception:
        return False


def checkpoint_success_num() -> int:
    try:
        return grep_log("Finish checkpoint").count("\n")
    except Exception:
        return 0


def kill_all_worker():
    cmd = [
        "bash", "-c", "grep -r \'Initializing job worker, exe_vert\' "
        " /tmp/ray/session_latest/logs | awk -F\'pid\' \'{print $2}\'"
        "| awk -F\'=\' \'{print $2}\'" + "| xargs kill -9"
    ]
    print(cmd)
    return subprocess.run(cmd)


if __name__ == "__main__":
    test_word_count()
