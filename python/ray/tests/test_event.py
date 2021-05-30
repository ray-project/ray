import pytest
import ray
import os
import sys
import _thread
import psutil
import json

from ray._private.test_utils import wait_for_condition

# test log reporter, first ray event API would write
# the event log file, then we search the specific event line with the label and
# message, these events located in the different files, should be sorted
# we sort them by the label(we use PIPELINE_1, PIPELINE_2... and so on)
# then we will test the correctness of this message


@ray.remote
class Counter(object):
    def __init__(self):
        self.n = 0

    def increment(self):
        ray.report_event(ray.EventSeverity.INFO, "PIPELINE_2",
                         "running increment function in the Counter Actor")
        self.n += 1

    def increment_in_private_thread(self):
        def event_log():
            ray.report_event(
                ray.EventSeverity.INFO, "PIPELINE_2",
                "running increment function in the Counter Actor in private"
                " thread")

        _thread.start_new_thread(event_log, ())
        ray.report_event(ray.EventSeverity.INFO, "PIPELINE_1",
                         "running increment function in the Counter Actor")
        self.n += 1

    def read(self):
        return self.n


# use specific label and message to search event in log file
def search_specific_event(event_dir, log_file_type, search_list):
    file_list = os.listdir(event_dir)
    target_file_list = []

    for file_name in file_list:
        if (file_name.find(log_file_type) != -1):
            target_file_list.append(os.path.join(event_dir, file_name))
    event_list = []

    for file_name in target_file_list:
        with open(file_name, "r") as f:
            for line in f.readlines():
                ok = False
                for pattern in search_list:
                    if (pattern[0] in line and pattern[1] in line):
                        ok = True
                        break
                if ok:
                    event_list.append(line)
    return event_list


# test 1. log reporter 2. task id, job_id ,node_id logic 3.message with
# multiple lines
def test_event_file(ray_start_regular):
    event_dir = ray_start_regular["event_dir"]
    search_list = []

    message = "start ray worker"
    ray.report_event(ray.EventSeverity.INFO, "PIPELINE_1",
                     "test for breakline\n\ntest for breakline again\n")
    ray.report_event(ray.EventSeverity.INFO, "PIPELINE_1", message)
    search_list.append(("PIPELINE_1", message))

    counters = [Counter.remote() for i in range(4)]
    [c.increment.remote() for c in counters]
    futures = [c.read.remote() for c in counters]

    ray.get(futures)

    message = "End ray worker"
    ray.report_event(ray.EventSeverity.INFO, "PIPELINE_3", message)
    search_list.append(("PIPELINE_2",
                        "running increment function in the Counter Actor"))
    search_list.append(("PIPELINE_3", message))

    event_list = search_specific_event(event_dir, "CORE_WORKER", search_list)

    assert len(event_list) == 6


# test private thread event context copy
def test_event_file_in_private_thread(ray_start_regular):
    event_dir = ray_start_regular["event_dir"]
    search_list = []

    counter = Counter.remote()
    counter.increment_in_private_thread.remote()
    ray.get(counter.read.remote())

    search_list.append(("PIPELINE_1",
                        "running increment function in the Counter Actor"))
    search_list.append(
        ("PIPELINE_2",
         "running increment function in the Counter Actor in private thread"))

    event_list = search_specific_event(event_dir, "CORE_WORKER", search_list)

    assert len(event_list) == 2


def test_event_file_in_worker_pool_child_process_signaled_callback(
        ray_start_regular):
    event_dir = ray_start_regular["event_dir"]
    search_list = []
    event_list = []
    child_pid = None

    raylet_pid = ray.worker.global_worker.node.all_processes[
        ray.ray_constants.PROCESS_TYPE_RAYLET][0].process.pid
    raylet_process = psutil.Process(raylet_pid)
    raylet_children = raylet_process.children(recursive=True)

    def condition_find_worker_process():
        nonlocal child_pid
        nonlocal search_list
        for pro in raylet_children:
            try:
                if "ray::" in pro.cmdline()[0]:
                    child_pid = pro.pid
            except Exception:
                ...
        if child_pid is None:
            return False
        # Killed one raylet child process
        os.kill(child_pid, 9)

        even_label = "WORKER_PROCESS_TERMINATED"
        event_message = f"Process terminated, process id: {child_pid}"
        search_list.append((even_label, event_message))
        return True

    def condition_event_length_eq_1():
        nonlocal child_pid
        nonlocal search_list
        nonlocal event_list
        event_list = search_specific_event(event_dir, "RAYLET", search_list)
        return len(event_list) == 1

    wait_for_condition(condition_find_worker_process)
    wait_for_condition(condition_event_length_eq_1)

    event_dict_custom_fields = json.loads(event_list[0])["custom_fields"]
    job_id = ray.worker.global_worker.core_worker.get_current_job_id().hex()
    node_id = ray.worker.global_worker.core_worker.get_current_node_id().hex()

    assert event_dict_custom_fields["signal_no"] == "9"
    assert "Killed" in event_dict_custom_fields["signal_name"]
    assert event_dict_custom_fields["pid"] == str(child_pid)
    assert event_dict_custom_fields["job_id"] == job_id
    assert event_dict_custom_fields["node_id"] == node_id
    assert event_dict_custom_fields["exit_code"] == "--"


def test_event_file_in_worker_pool_child_process_kill_by_raylet(
        ray_start_regular):
    event_dir = ray_start_regular["event_dir"]
    search_list = []
    event_list = []
    child_pid = None

    raylet_pid = ray.worker.global_worker.node.all_processes[
        ray.ray_constants.PROCESS_TYPE_RAYLET][0].process.pid
    raylet_process = psutil.Process(raylet_pid)
    raylet_children = raylet_process.children(recursive=True)

    def condition_find_worker_process():
        nonlocal child_pid
        nonlocal search_list
        for pro in raylet_children:
            try:
                if "ray::" in pro.cmdline()[0]:
                    child_pid = pro.pid
            except Exception:
                ...
        if child_pid is None:
            return False

        even_label = "WORKER_PROCESS_TERMINATED"
        event_message = f"Process terminated, process id: {child_pid}"
        search_list.append((even_label, event_message))
        return True

    wait_for_condition(condition_find_worker_process)
    ray.shutdown()

    def condition_event_length_none_zero():
        nonlocal child_pid
        nonlocal search_list
        nonlocal event_list
        event_list = search_specific_event(event_dir, "RAYLET", search_list)
        return len(event_list) != 0

    with pytest.raises(RuntimeError) as error:
        wait_for_condition(condition_event_length_none_zero, timeout=5)
    assert error.value.args[
        0] == "The condition wasn't met before the timeout expired."


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
