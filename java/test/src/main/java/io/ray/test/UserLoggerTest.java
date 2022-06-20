package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.util.SystemUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class UserLoggerTest extends BaseTest {

  private static final Logger LOG1 = LoggerFactory.getLogger("test_user_logger1");

  private static final Logger LOG2 = LoggerFactory.getLogger("test_user_logger2");

  static final String LOG_CONTEXT = "This is an user log";

  static final String CURR_LOG_DIR = "/tmp/ray/session_latest/logs";

  @BeforeClass
  public void setupJobConfig() {
    System.setProperty("ray.job.jvm-options.0", "-Dray.logging.loggers.0.name=test_user_logger1");
    System.setProperty(
        "ray.job.jvm-options.1", "-Dray.logging.loggers.0.file-name=test_user_logger-1-%p");
    System.setProperty(
        "ray.job.jvm-options.2",
        "-Dray.logging.loggers.0.pattern=%d{yyyy-MM-dd-HH:mm:ss,SSS}%p,%c{1},[%t]:%m%n");
    System.setProperty("ray.job.jvm-options.3", "-Dray.logging.loggers.1.name=test_user_logger2");
    System.setProperty(
        "ray.job.jvm-options.4", "-Dray.logging.loggers.1.file-name=test_user_logger-2-%p");
  }

  private static class ActorWithUserLogger {
    public int getPid() {
      LOG1.info(LOG_CONTEXT + "1");
      LOG2.info(LOG_CONTEXT + "2");
      return SystemUtil.pid();
    }
  }

  public void testUserLogger() throws IOException {
    ActorHandle<ActorWithUserLogger> actor = Ray.actor(ActorWithUserLogger::new).remote();
    int actorPid = actor.task(ActorWithUserLogger::getPid).remote().get();
    testUserLogger(actorPid, "1");
    testUserLogger(actorPid, "2");
  }

  private void testUserLogger(int pid, String indexStr) throws IOException {
    File userLoggerFile =
        new File(
            CURR_LOG_DIR
                + "/test_user_logger-%i-%p.log"
                    .replace("%i", indexStr)
                    .replace("%p", String.valueOf(pid)));
    Assert.assertTrue(userLoggerFile.exists());
    BufferedReader reader = new BufferedReader(new FileReader(userLoggerFile));
    String context = reader.readLine();
    Assert.assertTrue(context.endsWith(LOG_CONTEXT + indexStr));
  }
}
