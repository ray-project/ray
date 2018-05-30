package org.ray.util.logger;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.ray.util.SystemUtil;

/**
 * Manager for dynamic loggers
 */
public class DynamicLogManager {

  //whether to print the log on std(ie. console)
  public static boolean printOnStd = false;
  //the root directory of log files
  public static String logsDir;
  public static String logsSuffix;
  public static Level level = Level.DEBUG; //Level.INFO;
  /**  */
  private static final int LOG_CACHE_SIZE = 32 * 1024;
  protected final static String DAY_DATE_PATTERN = "'.'yyyy-MM-dd";
  //    private final static String                          HOUR_DATE_PATTERN     = "'.'yyyy-MM-dd_HH";
  //    private final static String                          GBK                   = "GBK";
  private final static String DAILY_APPENDER_NAME = "_DAILY_APPENDER_NAME";
  //    private final static String                          CONSOLE_APPENDER_NAME = "_CONSOLE_APPENDER_NAME";
  private final static String LAYOUT_PATTERN = "%d [%t]%m%n";

  private static final ConcurrentHashMap<DynamicLog, Logger> loggers = new ConcurrentHashMap<>();

  private static int MAX_FILE_NUM = 10;

  private static String MAX_FILE_SIZE = "500MB";

  private static boolean initFinished = false;


  /**
   * init from system properties
   * -DlogOutput=console/file_path
   * if file_path contains *pid*, it will be replaced with real PID of this JAVA process
   * if file_path contains *pid_suffix*, all log file will append the suffix -> xxx-pid.log
   */
  static {
    String logOutput = System.getProperty("logOutput");
    if (null == logOutput
        || logOutput.equalsIgnoreCase("console")
        || logOutput.equalsIgnoreCase("std")
        || logOutput.equals("")) {
      DynamicLogManager.printOnStd = true;
      System.out.println("config log output as std");
    } else {
      if (logOutput.contains("*pid*")) {
        logOutput = logOutput.replaceAll("\\*pid\\*", String.valueOf(SystemUtil.pid()));
      }
      if (logOutput.contains("*pid_suffix*")) {
        logOutput = logOutput.replaceAll("\\*pid_suffix\\*", "");
        if (logOutput.endsWith("/")) {
          logOutput = logOutput.substring(0, logOutput.length() - 1);
        }
        DynamicLogManager.logsSuffix = String.valueOf(SystemUtil.pid());
      }
      System.out.println("config log output as " + logOutput);
      DynamicLogManager.logsDir = logOutput;
    }
    String logLevel = System.getProperty("logLevel");
    if (logLevel != null && logLevel.equals("debug")) {
      level = Level.DEBUG;
    }
  }

  public static synchronized void init(int maxFileNum, String maxFileSize) {
    if (initFinished) {
      return;
    }
    initFinished = true;
    System.out.println(
        "DynamicLogManager init with maxFileNum:" + maxFileNum + " maxFileSize:" + maxFileSize);
    if (loggers.size() > 0) {
      System.err
          .println("already have logger be maked before init log file system, please check it");
    }
    MAX_FILE_NUM = maxFileNum;
    MAX_FILE_SIZE = maxFileSize;
  }

  public static Logger[] getLogs(DynamicLog dynLog) {
    Logger logger = loggers.get(dynLog);
    if (logger == null) {
      synchronized (loggers) {
        logger = loggers.get(dynLog);
        if (logger == null) {
          logger = initLogger(dynLog);
        }
      }
    }
    return new Logger[]{logger};
  }

  private static Logger initLogger(DynamicLog dynLog) {
    if (printOnStd) {
      Logger reallogger = Logger.getLogger(dynLog.getKey());
      ConsoleLogger logger = new ConsoleLogger(dynLog.getKey(), reallogger);
      PatternLayout layout = new PatternLayout(LAYOUT_PATTERN);
      ConsoleAppender appender = new ConsoleAppender(layout, ConsoleAppender.SYSTEM_OUT);
      reallogger.removeAllAppenders();
      reallogger.addAppender(appender);
      reallogger.setLevel(level);
      reallogger.setAdditivity(false);
      loggers.putIfAbsent(dynLog, logger);
      return logger;
    } else {
      Logger logger = makeLogger(dynLog.getKey(), dynLog.getDefaultLogFileName());
      loggers.putIfAbsent(dynLog, logger);
      return logger;
    }

  }

  protected static Logger makeLogger(String loggerName, String filename) {
    Logger logger = Logger.getLogger(loggerName);
    PatternLayout layout = new PatternLayout(LAYOUT_PATTERN);
    File dir = new File(logsDir);
    if (!dir.exists()) {
      dir.mkdirs();
    }
    String logFileName = logsDir + "/" + filename;
    if (logsSuffix != null) {
      logFileName = logFileName.substring(0, logFileName.length() - 4) + "-" + logsSuffix
          + ".log";
    }
    System.out.println("new_log_path:" + logFileName);
    RollingFileAppender appender;
    try {
      appender = new TimedFlushDailyRollingFileAppender(layout, logFileName);
      appender.setAppend(true);
      appender.setEncoding("UTF-8");
      appender.setName(DAILY_APPENDER_NAME);
      appender.setBufferSize(LOG_CACHE_SIZE);
      appender.setBufferedIO(true);
      appender.setImmediateFlush(false);
      appender.setMaxBackupIndex(MAX_FILE_NUM);
      appender.setMaxFileSize(MAX_FILE_SIZE);
      appender.activateOptions();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    logger.removeAllAppenders();
    logger.addAppender(appender);

    logger.setLevel(level);
    logger.setAdditivity(false);
    return logger;
  }

}
