package io.ray.api;

import java.time.LocalDateTime;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class TestProgressListener implements IInvokedMethodListener, ITestListener {

  private String getFullTestName(ITestResult testResult) {
    return testResult.getTestClass().getName() + "."
        + testResult.getMethod().getMethodName();
  }

  private void printInfo(String tag, String content) {
    System.out.println(
        "============ [" + LocalDateTime.now().toString() + "] [" + tag + "] " + content
            + " ============");
  }

  @Override
  public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {
    printInfo("INVOKE METHOD", getFullTestName(testResult));
  }

  @Override
  public void afterInvocation(IInvokedMethod method, ITestResult testResult) {
  }

  @Override
  public void onTestStart(ITestResult result) {
    printInfo("TEST START", getFullTestName(result));
  }

  @Override
  public void onTestSuccess(ITestResult result) {
    printInfo("TEST SUCCESS", getFullTestName(result));
  }

  @Override
  public void onTestFailure(ITestResult result) {
    printInfo("TEST FAILURE", getFullTestName(result));
    Throwable throwable = result.getThrowable();
    if (throwable != null) {
      throwable.printStackTrace();
    }
  }

  @Override
  public void onTestSkipped(ITestResult result) {
    printInfo("TEST SKIPPED", getFullTestName(result));
  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    printInfo("TEST FAILED BUT WITHIN SUCCESS PERCENTAGE", getFullTestName(result));
  }

  @Override
  public void onStart(ITestContext context) {
  }

  @Override
  public void onFinish(ITestContext context) {
  }
}
