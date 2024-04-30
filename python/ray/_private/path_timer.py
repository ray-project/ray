import os
import time

PATH_TIMER_ENABLED = "RAY_PATH_TIMER_ENABLED" in os.environ

time_list = [] # list of (event, timestamp)


def tick(event) -> None:
    if PATH_TIMER_ENABLED:
        time_list.append((event, time.time()))

def summary() -> None:
    if not PATH_TIMER_ENABLED:
        return
    n = len(time_list)
    if n > 1:
        total = time_list[-1][1] - time_list[0][1]
        print(f"======= Total time: {total}s =======")
    for i in range(n - 1):
        e1, ts1 = time_list[i]
        e2, ts2 = time_list[i + 1]
        if total is not None:
            print(f"{e1} -> {e2}: {ts2 - ts1}s , {(ts2-ts1)/total*100:.2f}%")
        else:
            print(f"{e1} -> {e2}: {ts2 - ts1}s")
