package io.ray.runtime.util;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * some utilities for system process.
 */
public class SystemUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(SystemUtil.class);

  static final ReentrantLock pidlock = new ReentrantLock();
  static Integer pid;

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
      LOGGER.error("error at SystemUtil startWithJar", e);
      return false;
    }
  }

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
