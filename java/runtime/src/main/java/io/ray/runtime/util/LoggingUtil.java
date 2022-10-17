package io.ray.runtime.util;

import com.typesafe.config.Config;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.generated.Common.WorkerType;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.LoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.RootLoggerComponentBuilder;
import org.apache.logging.log4j.core.config.builder.impl.BuiltConfiguration;

public class LoggingUtil {

  private static boolean setup = false;

  public static synchronized void setupLogging(RayConfig rayConfig) {
    if (setup) {
      return;
    }
    setup = true;

    LoggerContext.getContext().reconfigure();
    Config config = rayConfig.getInternalConfig();

    if (rayConfig.workerMode == WorkerType.DRIVER) {
      // Logs of drivers are printed to console.
      ConfigurationBuilder<BuiltConfiguration> builder =
          ConfigurationBuilderFactory.newConfigurationBuilder();

      builder.setStatusLevel(Level.INFO);
      builder.setConfigurationName("DefaultLogger");

      // create a console appender
      AppenderComponentBuilder appenderBuilder =
          builder
              .newAppender("Console", "CONSOLE")
              .addAttribute("target", ConsoleAppender.Target.SYSTEM_OUT);
      appenderBuilder.add(
          builder
              .newLayout("PatternLayout")
              .addAttribute("pattern", config.getString("ray.logging.pattern")));
      RootLoggerComponentBuilder rootLogger = builder.newRootLogger(Level.INFO);
      rootLogger.add(builder.newAppenderRef("Console"));

      builder.add(appenderBuilder);
      builder.add(rootLogger);
      Configurator.reconfigure(builder.build());

    } else {
      // Logs of workers are printed to files.
      String jobIdHex = System.getenv("RAY_JOB_ID");
      String maxFileSize = System.getenv("RAY_ROTATION_MAX_BYTES");
      if (StringUtils.isEmpty(maxFileSize)) {
        maxFileSize = rayConfig.getInternalConfig().getString("ray.logging.max-file-size");
      }
      String maxBackupFiles = System.getenv("RAY_ROTATION_BACKUP_COUNT");
      if (StringUtils.isEmpty(maxBackupFiles)) {
        maxBackupFiles = rayConfig.getInternalConfig().getString("ray.logging.max-backup-files");
      }

      ConfigurationBuilder<BuiltConfiguration> globalConfigBuilder =
          ConfigurationBuilderFactory.newConfigurationBuilder();

      // TODO(qwang): We can use rayConfig.logLevel instead.
      Level level = Level.toLevel(config.getString("ray.logging.level"));

      globalConfigBuilder.setStatusLevel(Level.INFO);
      globalConfigBuilder.setConfigurationName("DefaultLogger");

      /// Setup root logger for Java worker.
      RootLoggerComponentBuilder rootLoggerBuilder = globalConfigBuilder.newAsyncRootLogger(level);
      rootLoggerBuilder.addAttribute("RingBufferSize", "1048576");
      final String javaWorkerLogName = "JavaWorkerLogToRollingFile";
      setupLogger(
          globalConfigBuilder,
          rayConfig.logDir,
          new RayConfig.LoggerConf(
              javaWorkerLogName,
              "java-worker-" + jobIdHex + "-" + SystemUtil.pid(),
              config.getString("ray.logging.pattern")),
          maxFileSize,
          maxBackupFiles);
      rootLoggerBuilder.add(globalConfigBuilder.newAppenderRef(javaWorkerLogName));
      globalConfigBuilder.add(rootLoggerBuilder);
      /// Setup user loggers.
      for (RayConfig.LoggerConf conf : rayConfig.loggers) {
        final String logPattern =
            StringUtils.isEmpty(conf.pattern)
                ? config.getString("ray.logging.pattern")
                : conf.pattern;
        setupUserLogger(
            globalConfigBuilder,
            rayConfig.logDir,
            new RayConfig.LoggerConf(conf.loggerName, conf.fileName, logPattern),
            maxFileSize,
            maxBackupFiles);
      }
      Configurator.reconfigure(globalConfigBuilder.build());
    }
  }

  private static void setupUserLogger(
      ConfigurationBuilder<BuiltConfiguration> globalConfigBuilder,
      String logDir,
      RayConfig.LoggerConf userLoggerConf,
      String maxFileSize,
      String maxBackupFiles) {
    LoggerComponentBuilder userLoggerBuilder =
        globalConfigBuilder.newAsyncLogger(userLoggerConf.loggerName);
    setupLogger(globalConfigBuilder, logDir, userLoggerConf, maxFileSize, maxBackupFiles);
    userLoggerBuilder
        .add(globalConfigBuilder.newAppenderRef(userLoggerConf.loggerName))
        .addAttribute("additivity", false);
    globalConfigBuilder.add(userLoggerBuilder);
  }

  private static void setupLogger(
      ConfigurationBuilder<BuiltConfiguration> globalConfigBuilder,
      String logDir,
      RayConfig.LoggerConf userLoggerConf,
      String maxFileSize,
      String maxBackupFiles) {
    LayoutComponentBuilder layoutBuilder =
        globalConfigBuilder
            .newLayout("PatternLayout")
            .addAttribute("pattern", userLoggerConf.pattern);
    ComponentBuilder userLoggerTriggeringPolicy =
        globalConfigBuilder
            .newComponent("Policies")
            .addComponent(
                globalConfigBuilder
                    .newComponent("SizeBasedTriggeringPolicy")
                    .addAttribute("size", maxFileSize));
    ComponentBuilder userLoggerRolloverStrategy =
        globalConfigBuilder
            .newComponent("DefaultRolloverStrategy")
            .addAttribute("max", maxBackupFiles);
    final String logFileName =
        userLoggerConf.fileName.replace("%p", String.valueOf(SystemUtil.pid()));
    final String logPath = logDir + "/" + logFileName + ".log";
    final String rotatedLogPath = logDir + "/" + logFileName + ".%i.log";
    AppenderComponentBuilder userLoggerAppenderBuilder =
        globalConfigBuilder
            .newAppender(userLoggerConf.loggerName, "RollingFile")
            .addAttribute("filePattern", rotatedLogPath)
            .add(layoutBuilder)
            .addComponent(userLoggerTriggeringPolicy)
            .addComponent(userLoggerRolloverStrategy)
            .addAttribute("fileName", logPath);
    globalConfigBuilder.add(userLoggerAppenderBuilder);
  }
}
