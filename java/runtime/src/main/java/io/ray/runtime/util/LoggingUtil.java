package io.ray.runtime.util;

import com.typesafe.config.Config;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.generated.Common.WorkerType;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.builder.api.AppenderComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ComponentBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilder;
import org.apache.logging.log4j.core.config.builder.api.ConfigurationBuilderFactory;
import org.apache.logging.log4j.core.config.builder.api.LayoutComponentBuilder;
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

      builder.setStatusLevel(Level.DEBUG);
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
      RootLoggerComponentBuilder rootLogger = builder.newRootLogger(Level.DEBUG);
      rootLogger.add(builder.newAppenderRef("Console"));

      builder.add(appenderBuilder);
      rootLogger.add(builder.newAppenderRef("LogToRollingFile"));
      builder.add(rootLogger);
      Configurator.reconfigure(builder.build());

    } else {
      // Logs of workers are printed to files.
      String jobIdHex = System.getenv("RAY_JOB_ID");
      String logPath =
          rayConfig.logDir + "/java-worker-" + jobIdHex + "-" + SystemUtil.pid() + ".log";
      String rollingLogPath =
          rayConfig.logDir + "/java-worker-" + jobIdHex + "-" + SystemUtil.pid() + ".%i.log";

      ConfigurationBuilder<BuiltConfiguration> builder =
          ConfigurationBuilderFactory.newConfigurationBuilder();
      builder.setStatusLevel(Level.DEBUG);
      builder.setConfigurationName("DefaultLogger");

      // TODO(qwang): We can use rayConfig.logLevel instead.
      Level level = Level.toLevel(config.getString("ray.logging.level"));
      RootLoggerComponentBuilder rootLogger = builder.newAsyncRootLogger(level);
      rootLogger.addAttribute("RingBufferSize", "1048576");
      // Create a rolling file appender.
      LayoutComponentBuilder layoutBuilder =
          builder
              .newLayout("PatternLayout")
              .addAttribute("pattern", config.getString("ray.logging.pattern"));
      ComponentBuilder triggeringPolicy =
          builder
              .newComponent("Policies")
              .addComponent(
                  builder
                      .newComponent("SizeBasedTriggeringPolicy")
                      .addAttribute(
                          "size",
                          rayConfig.getInternalConfig().getString("ray.logging.max-file-size")));
      AppenderComponentBuilder appenderBuilder =
          builder
              .newAppender("LogToRollingFile", "RollingFile")
              .addAttribute("fileName", logPath)
              .addAttribute("filePattern", rollingLogPath)
              .add(layoutBuilder)
              .addComponent(triggeringPolicy);
      builder.add(appenderBuilder);
      rootLogger.add(builder.newAppenderRef("LogToRollingFile"));
      builder.add(rootLogger);
      Configurator.reconfigure(builder.build());
    }
  }
}
