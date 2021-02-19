package io.ray.test;

import org.testng.IClassListener;
import org.testng.ITestClass;

public class ConfigListener implements IClassListener {

  @Override
  public void onAfterClass(ITestClass testClass) {
    TestUtils.clearConfig();
  }
}
