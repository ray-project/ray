package org.ray.api.test;

import java.io.File;
import org.junit.After;
import org.junit.Before;
import org.ray.api.Ray;

public class BaseTest {

  @Before
  public void setUp() {
    System.setProperty("ray.home", "../..");
    System.setProperty("ray.resources", "CPU:4,RES-A:4");
    Ray.init();
  }

  @After
  public void tearDown() {
    // TODO(qwang): This is double check to check that the socket file is removed actually.
    // We could not enable this until `systemInfo` enabled.
    //File rayletSocketFIle = new File(Ray.systemInfo().rayletSocketName());
    Ray.shutdown();

    //remove raylet socket file
    //rayletSocketFIle.delete();

    // unset system properties
    System.clearProperty("ray.home");
    System.clearProperty("ray.resources");
  }

}
