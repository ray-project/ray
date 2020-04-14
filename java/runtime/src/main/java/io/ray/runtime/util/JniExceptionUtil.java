package io.ray.runtime.util;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// Required by JNI macro RAY_CHECK_JAVA_EXCEPTION
public final class JniExceptionUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(JniExceptionUtil.class);

  public static String getStackTrace(String fileName, int lineNumber, String function,
      Throwable throwable) {
    LOGGER.error("An unexpected exception occurred while executing Java code from JNI ({}:{} {}).",
        fileName, lineNumber, function, throwable);
    // Return the exception in string form to JNI.
    return ExceptionUtils.getStackTrace(throwable);
  }
}
