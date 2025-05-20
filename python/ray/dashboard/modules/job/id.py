#!/usr/bin/env python
# coding: utf-8
import hashlib
import os
import socket
import threading
import time

__HOST_NAME = socket.gethostname()
__GLOBAL_ID_COUNT = 0
__LOCK = threading.Lock()


def generate_global_unique_id():
    process_id = os.getpid()
    thread_num = threading.current_thread().ident
    current_time_millis = int(round(time.time() * 1000))
    seed_str = "%s %s %s %s" % (
        __HOST_NAME,
        process_id,
        thread_num,
        current_time_millis,
    )

    try:
        global __GLOBAL_ID_COUNT, __LOCK
        __LOCK.acquire()
        seed_str = seed_str + str(__GLOBAL_ID_COUNT)
        __GLOBAL_ID_COUNT += 1
    finally:
        __LOCK.release()

    return hashlib.sha256(seed_str.encode("utf-8")).hexdigest()[:16]
