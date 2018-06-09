package org.ray.util.logger;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.Logger;
import org.ray.util.CommonUtil;

/**
 * Dynamic logger without properties configuration file.
 */
public class DynamicLog {

  static final ThreadLocal<String> PREFIX = new ThreadLocal<>();

  private static LogLevel logLevel = LogLevel.DEBUG;

  private static Boolean logLevelSetFlag = false;
  private static Map<String/*Samplekey*/, SampleStatis> sampleStatis = new ConcurrentHashMap<>();
  private final String key;

  private DynamicLog(String key) {
    this.key = key;
  }

  public static String getContextPrefix() {
    return PREFIX.get();
  }

  /**
   * set the context prefix for all logs.
   */
  public static void setContextPrefix(String prefix) {
    PREFIX.set(prefix);
  }

  /**
   * set the level for all logs.
   */
  public static void setLogLevel(String level) {
    if (logLevelSetFlag) { /* one shot, avoid the risk of multithreading */
      return;
    }
    logLevelSetFlag = true;
    logLevel = LogLevel.of(level);
  }

  public static DynamicLog registerName(String name) {
    return DynamicLogNameRegister.registerName(name);
  }

  public static Collection<DynamicLog> values() {
    return DynamicLogNameRegister.names.values();
  }

  @Override
  public int hashCode() {
    return this.toString().hashCode();
  }

  @Override
  public boolean equals(Object o) {
    return this.toString().equals(o.toString());
  }

  @Override
  public String toString() {
    return this.getKey();
  }

  public String getKey() {
    return this.key;
  }

  public void debug(String log) {
    if (!getenumLogLevel().needLog(LogLevel.DEBUG)) {
      return;
    }
    log = wrap("debug", log);
    Logger[] loggers = DynamicLogManager.getLogs(this);
    for (Logger logger : loggers) {
      logger.debug(log);
    }
  }

  private static LogLevel getenumLogLevel() {
    return logLevel;
  }

  private String wrap(String level, String log) {
    StackTraceElement[] stes = Thread.currentThread().getStackTrace();
    String ret = "[" + level + "]" + "[" + stes[3].getFileName() + ":" + stes[3].getLineNumber()
        + "] - " + (log == null ? "" : log);
    String prefix = PREFIX.get();
    if (prefix != null) {
      ret = "[" + prefix + "]" + ret;
    }
    return ret;
  }

  public void info(String log) {
    if (!getenumLogLevel().needLog(LogLevel.INFO)) {
      return;
    }
    log = wrap("info", log);
    Logger[] loggers = DynamicLogManager.getLogs(this);
    for (Logger logger : loggers) {
      logger.info(log);
    }
  }

  public void warn(String log) {
    if (!getenumLogLevel().needLog(LogLevel.WARN)) {
      return;
    }
    log = wrap("warn", log);
    Logger[] loggers = DynamicLogManager.getLogs(this);
    for (Logger logger : loggers) {
      logger.warn(log);
    }
  }

  public void warn(String log, Throwable e) {
    if (!getenumLogLevel().needLog(LogLevel.WARN)) {
      return;
    }
    log = wrap("warn", log);
    Logger[] loggers = DynamicLogManager.getLogs(this);
    for (Logger logger : loggers) {
      logger.warn(log, e);
    }
  }

  public void error(String log, Throwable e) {
    if (!getenumLogLevel().needLog(LogLevel.ERROR)) {
      return;
    }
    log = wrap("error", log);
    if (e == null) {
      error(log);
      return;
    }

    Logger[] loggers = DynamicLogManager.getLogs(this);
    for (Logger logger : loggers) {
      logger.error(log, e);
    }
  }

  public void error(String log) {
    if (!getenumLogLevel().needLog(LogLevel.ERROR)) {
      return;
    }
    log = wrap("error", log);
    Logger[] loggers = DynamicLogManager.getLogs(this);
    for (Logger logger : loggers) {
      logger.error(log);
    }
  }

  public void error(Throwable e) {
    if (!getenumLogLevel().needLog(LogLevel.ERROR)) {
      return;
    }
    String log = wrap("error", e == null ? null : e.getMessage());
    if (e == null) {
      error(log);
      return;
    }
    Logger[] loggers = DynamicLogManager.getLogs(this);
    for (Logger logger : loggers) {
      logger.error(log, e);
    }
  }

  /**
   * Print sample error log.
   */
  public boolean sampleError(Object sampleKeyO, String log, Throwable e) {
    String sampleKey = sampleKeyO.toString();
    try {
      SampleStatis ss = sampleStatis.computeIfAbsent(sampleKey, k -> new SampleStatis());
      if (ss.gamble()) {
        Logger[] loggers = DynamicLogManager.getLogs(this);
        for (Logger logger : loggers) {
          if (e != null) {
            logger.error("[" + sampleKey + "] - " + log, e);
          } else {
            logger.error("[" + sampleKey + "] - " + log);
          }
        }
        return true;
      } else {
        return false;
      }
    } finally {
      if (sampleStatis.size() > 100000) {
        sampleStatis = new ConcurrentHashMap<>();
      }
    }
  }

  public String getDefaultLogFileName() {
    return this.key + ".log";
  }

  //statistic for sampling
  private static class SampleStatis {

    int total;

    public boolean gamble() {
      int randomRange;

      if (total < 100) {
        randomRange = 1;
      } else if (total < 1000) {
        randomRange = 1000;
      } else if (total < 100000) {
        randomRange = 10000;
      } else if (total < 1000000) {
        randomRange = 100000;
      } else {
        total = 0;
        randomRange = 1;
      }
      if (CommonUtil.getRandom(randomRange) == 0) {
        total++;
        return true;
      } else {
        total++;
        return false;
      }
    }
  }

  public static class DynamicLogNameRegister {

    static final Map<String, DynamicLog> names = new ConcurrentHashMap<>();

    public static DynamicLog registerName(String name) {
      DynamicLog ret = names.get(name);
      if (ret != null) {
        return ret;
      }
      synchronized (names) {
        ret = names.get(name);
        if (ret != null) {
          return ret;
        }
        ret = new DynamicLog(name);
        names.put(name, ret);
        return ret;
      }
    }
  }

}
