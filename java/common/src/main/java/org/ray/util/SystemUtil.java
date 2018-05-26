package org.ray.util;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.locks.ReentrantLock;
import org.ray.util.logger.RayLog;

/**
 * some utilities for system process
 */
public class SystemUtil {

  public static String userHome() {
    return System.getProperty("user.home");
  }

  public static String userDir() {
    return System.getProperty("user.dir");
  }

  public static boolean startWithJar(Class<?> cls) {
    return cls.getResource(cls.getSimpleName() + ".class").getFile().split("!")[0].endsWith(".jar");
  }

  public static boolean startWithJar(String clsName) {
    Class<?> cls;
    try {
      cls = Class.forName(clsName);
      return cls.getResource(cls.getSimpleName() + ".class").getFile().split("!")[0]
          .endsWith(".jar");
    } catch (ClassNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      RayLog.core.error("error at SystemUtil startWithJar", e);
      return false;
    }
  }

  static Integer pid;
  static final ReentrantLock pidlock = new ReentrantLock();

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
