package org.ray.streaming.runtime.util;

import org.apache.log4j.Category;
import org.apache.log4j.RollingFileAppender;
import org.apache.log4j.spi.LoggingEvent;

public class ModuleNameAppender extends RollingFileAppender {

  private static String moduleName;
  private String pid;

  public ModuleNameAppender() {
    pid = EnvUtil.getJvmPid();
  }

  public static String getModuleName() {
    return moduleName;
  }

  public static void setModuleName(String moduleName) {
    if (null == ModuleNameAppender.moduleName) {
      ModuleNameAppender.moduleName = moduleName;
    }
  }

  @Override
  public void subAppend(LoggingEvent event) {
    String msg = formatInfo(event);

    Throwable t = null;
    if (event.getThrowableInformation() != null) {
      t = event.getThrowableInformation().getThrowable();
    }

    super.subAppend(new LoggingEvent(Category.class.getName(), event
        .getLogger(), event.getLevel(), msg, t));
  }

  private String formatInfo(LoggingEvent event) {
    StringBuilder sb = new StringBuilder();
    if (moduleName != null) {
      sb.append("[").append(moduleName).append("] ");
    }
    sb.append("[").append(pid).append("] ");
    sb.append(event.getMessage());
    return sb.toString();
  }
}