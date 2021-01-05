package io.ray.runtime.util;

import com.typesafe.config.Config;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.generated.Common.WorkerType;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.WriterAppender;

public class LoggingUtil {

  private static boolean setup = false;

  public static synchronized void setupLogging(RayConfig rayConfig) {
    if (setup) {
      return;
    }
    setup = true;

    WriterAppender appender;
    Config config = rayConfig.getInternalConfig();
    if (rayConfig.workerMode == WorkerType.DRIVER) {
      // Logs of drivers are printed to console.
      appender = new ConsoleAppender();
      appender.setName("console");
    } else {
      // Logs of workers are printed to files.
      RollingFileAppender rfAppender = new RollingFileAppender();
      appender = rfAppender;

      rfAppender.setName("file");
      String logPath = rayConfig.logDir + "/java-worker-" + SystemUtil.pid() + ".log";
      rfAppender.setFile(logPath);
      rfAppender.setMaxFileSize(config.getString("ray.logging.max-file-size"));
      rfAppender.setMaxBackupIndex(config.getInt("ray.logging.max-backup-files"));
    }
    Level level = Level.toLevel(config.getString("ray.logging.level"));
    appender.setThreshold(level);
    PatternLayout patternLayout = new PatternLayout(config.getString("ray.logging.pattern"));
    appender.setLayout(patternLayout);
    appender.activateOptions();
    Logger.getLogger("io.ray").addAppender(appender);
  }
}
