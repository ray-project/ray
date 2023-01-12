package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.JobId;
import io.ray.runtime.util.SystemUtil;
import java.io.File;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class RayJavaLoggingTest extends BaseTest {

  private static final Logger LOG = LoggerFactory.getLogger(RayJavaLoggingTest.class);

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

  public void testJavaLoggingRotate() {
    ActorHandle<HeavyLoggingActor> loggingActor =
        Ray.actor(HeavyLoggingActor::new)
            .setJvmOptions(
                ImmutableList.of(
                    "-Dray.logging.max-file-size=1KB", "-Dray.logging.max-backup-files=3"))
            .remote();
    Assert.assertTrue(loggingActor.task(HeavyLoggingActor::log).remote().get());
    final int pid = loggingActor.task(HeavyLoggingActor::getPid).remote().get();
    // Note(qwang): Due to log4j2 is async, once the `log()` actor method returned, we
    // still have no confident to make sure all log messages are printed to the log files,
    // especially on slow CI machine.
    boolean rotated =
        TestUtils.waitForCondition(
            () -> {
              final JobId jobId = Ray.getRuntimeContext().getCurrentJobId();
              String currLogDir = "/tmp/ray/session_latest/logs";
              for (int i = 1; i <= 3; ++i) {
                File rotatedFile =
                    new File(
                        String.format("%s/java-worker-%s-%d.%d.log", currLogDir, jobId, pid, i));
                if (!rotatedFile.exists()) {
                  return false;
                }
                long fileSize = rotatedFile.length();
                boolean fileSizeExpected = fileSize > 1024 && fileSize < 1024 * 2;
                if (!fileSizeExpected) {
                  return false;
                }
              }
              return true;
            },
            10 * 1000);
    Assert.assertTrue(rotated);
  }
}
