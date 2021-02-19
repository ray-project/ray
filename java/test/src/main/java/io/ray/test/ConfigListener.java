package io.ray.test;

import org.testng.IClassListener;
import org.testng.ITestClass;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class ConfigListener implements IClassListener, ITestListener {

  @Override
  public void onTestStart(ITestResult result) {
    TestUtils.setConfigForMethod();
  }

  @Override
  public void onAfterClass(ITestClass testClass) {
    TestUtils.setConfigForClass();
  }
}
