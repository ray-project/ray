package io.ray.runtime.util;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * some utilities for system process.
 */
public class SystemUtil {

  static final ReentrantLock pidlock = new ReentrantLock();
  static Integer pid;

  public static int pid() {
    if (pid == null) {
      pidlock.lock();
      try {
        if (pid == null) {
          RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
          String name = runtime.getName();
          int index = name.indexOf("@");
          if (index != -1) {
            pid = Integer.parseInt(name.substring(0, index));
          } else {
            throw new RuntimeException("parse pid error:" + name);
          }
        }

      } finally {
        pidlock.unlock();
      }
    }
    return pid;

  }
}
