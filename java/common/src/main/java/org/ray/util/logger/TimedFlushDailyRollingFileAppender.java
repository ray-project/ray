package org.ray.util.logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.log4j.Layout;
import org.apache.log4j.RollingFileAppender;

/**
 * Normal log appender.
 */
public class TimedFlushDailyRollingFileAppender extends RollingFileAppender {

  private static final Set<TimedFlushDailyRollingFileAppender> all = new HashSet<>();

  static {
    new TimedFlushLogThread().start();
  }

  public TimedFlushDailyRollingFileAppender() {
    super();
    synchronized (all) {
      all.add(this);
    }
  }

  public TimedFlushDailyRollingFileAppender(Layout layout, String filename) throws IOException {
    super(layout, filename);
    synchronized (all) {
      all.add(this);
    }
  }

  private void flush() {
    try {
      if (!checkEntryConditions()) {
        return;
      }
      qw.flush();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static class TimedFlushLogThread extends Thread {

    public TimedFlushLogThread() {
      super();
      setName("TimedFlushLogThread");
      setDaemon(true);
    }

    public void run() {
      while (true) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        synchronized (all) {
          for (TimedFlushDailyRollingFileAppender appender : all) {
            appender.flush();
          }
        }
      }
    }
  }
}
