package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.JobId;
import io.ray.runtime.util.SystemUtil;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class RayJavaLoggingTest extends BaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(RayJavaLoggingTest.class);

  @BeforeClass
  public void setup() {
    System.setProperty("ray.job.logging-level", "DEBUG");
  }

  private static class HeavyLoggingActor {
    public int getPid() {
      return SystemUtil.pid();
    }

    public boolean log() {
      for (int i = 0; i < 100000; ++i) {
        LOG.info("hello world, this is a log.");
      }
      return true;
    }
  }

  @Test
  public void testJavaLoggingRotate() {
    ActorHandle<HeavyLoggingActor> loggingActor =
        Ray.actor(HeavyLoggingActor::new)
            .setJvmOptions(
                ImmutableList.of(
                    "-Dray.logging.max-file-size=1KB", "-Dray.logging.max-backup-files=3"))
            .remote();
    Assert.assertTrue(loggingActor.task(HeavyLoggingActor::log).remote().get());
    final int pid = loggingActor.task(HeavyLoggingActor::getPid).remote().get();
    final JobId jobId = Ray.getRuntimeContext().getCurrentJobId();
    String currLogDir = "/tmp/ray/session_latest/logs";
    for (int i = 1; i <= 3; ++i) {
      File rotatedFile =
          new File(String.format("%s/java-worker-%s-%d.%d.log", currLogDir, jobId, pid, i));
      Assert.assertTrue(rotatedFile.exists());
      long fileSize = rotatedFile.length();
      Assert.assertTrue(fileSize > 1024 && fileSize < 1024 * 2);
    }
  }

  @Test
  public void testDebugLogLevel() throws FileNotFoundException {
    String fileName = Ray.task(RayJavaLoggingTest::getCurrentLogFileName).remote().get();
    File logFile = new File(fileName);
    Assert.assertTrue(logFile.exists());
    FileReader fr = new FileReader(logFile);
    BufferedReader br = new BufferedReader(fr);
    Assert.assertTrue(br.lines().anyMatch(line -> line.contains("DEBUG")));
  }

  private static String getCurrentLogFileName() {
    String currLogDir = "/tmp/ray/session_latest/logs";
    final JobId jobId = Ray.getRuntimeContext().getCurrentJobId();
    return String.format("%s/java-worker-%s-%d.log", currLogDir, jobId, SystemUtil.pid());
  }
}
