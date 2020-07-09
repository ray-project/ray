package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.api.Ray;
import java.io.File;
import java.lang.reflect.Method;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

public class BaseTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(BaseTest.class);

  private List<File> filesToDelete = ImmutableList.of();

  @BeforeMethod(alwaysRun = true)
  public void setUpBase(Method method) {
    Assert.assertNull(Ray.internal());
    Ray.init();
    // These files need to be deleted after each test case.
    filesToDelete = ImmutableList.of(
        new File(Ray.getRuntimeContext().getRayletSocketName()),
        new File(Ray.getRuntimeContext().getObjectStoreSocketName()),
        // TODO(pcm): This is a workaround for the issue described
        // in the PR description of https://github.com/ray-project/ray/pull/5450
        // and should be fixed properly.
        new File("/tmp/ray/test/raylet_socket")
    );
    // Make sure the files will be deleted even if the test doesn't exit gracefully.
    filesToDelete.forEach(File::deleteOnExit);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDownBase() {
    Ray.shutdown();

    for (File file : filesToDelete) {
      file.delete();
    }
  }

}
