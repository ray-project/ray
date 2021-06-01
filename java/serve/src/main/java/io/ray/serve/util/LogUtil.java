package io.ray.serve.util;

import java.util.Enumeration;
import org.apache.log4j.Appender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.slf4j.helpers.MessageFormatter;

/**
 * Ray Serve common log tool.
 */
public class LogUtil {

  public static final String RAY_SERVE = "io.ray";

  @SuppressWarnings("rawtypes")
  public static void setLayout(String backendTag, String replicaTag) {
    Logger logger = Logger.getLogger(RAY_SERVE);
    Enumeration enumeration = logger.getAllAppenders();
    while (enumeration.hasMoreElements()) {
      Appender appender = (Appender) enumeration.nextElement();
      Layout layout = appender.getLayout();
      if (!(layout instanceof PatternLayout)) {
        continue;
      }
      PatternLayout patternLayout = (PatternLayout) layout;
      String pattern = patternLayout.getConversionPattern();
      patternLayout.setConversionPattern(
          format("{} component=serve backend={} replica={}", pattern, backendTag, replicaTag));

      MessageFormatter.format(pattern, pattern).toString();
    }
  }

  public static String format(String messagePattern, Object... args) {
    return MessageFormatter.arrayFormat(messagePattern, args).getMessage();
  }

}
