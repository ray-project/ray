package io.ray.test;

import org.testng.IClassListener;
import org.testng.ITestClass;

public class SystemPropertyListener implements IClassListener {

  @Override
  public void onAfterClass(ITestClass testClass) {
    for (String key : System.getProperties().stringPropertyNames()) {
      if (key.startsWith("ray.")) {
        System.clearProperty(key);
      }
    }
  }
}
