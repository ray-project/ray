package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.runtime.runner.RunManager;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.LinkedList;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;

public class TestProgressListener implements IInvokedMethodListener, ITestListener {

  // Travis aborts CI if no outputs for 10 minutes. So threshold needs to be smaller than 10m.
  private static final long hangDetectionThresholdMillis = 5 * 60 * 1000;
  private static final int TAIL_NO_OF_LINES = 500;
  private Thread testMainThread;
  private long testStartTimeMillis;

  private String getFullTestName(ITestResult testResult) {
    return testResult.getTestClass().getName() + "." + testResult.getMethod().getMethodName();
  }

  private void printSection(String sectionName) {
    System.out.println(
        "============ [" + LocalDateTime.now().toString() + "] " + sectionName + " ============");
  }

  private void printTestStage(String tag, String content) {
    printSection("[" + tag + "] " + content);
  }

  @Override
  public void beforeInvocation(IInvokedMethod method, ITestResult testResult) {}

  @Override
  public void afterInvocation(IInvokedMethod method, ITestResult testResult) {}

  @Override
  public void onTestStart(ITestResult result) {
    printTestStage("TEST START", getFullTestName(result));
    testStartTimeMillis = System.currentTimeMillis();
    // TODO(kfstorm): Add a timer to detect hang
    if (testMainThread == null) {
      testMainThread = Thread.currentThread();
      Thread hangDetectionThread =
          new Thread(
              () -> {
                try {
                  // If current task case has ran for more than 5 minutes.
                  while (System.currentTimeMillis() - testStartTimeMillis
                      < hangDetectionThresholdMillis) {
                    Thread.sleep(1000);
                  }
                  printSection("TEST CASE HANGED");
                  // TODO(kfstorm): Print stack of other threads (and other Ray processes).
                  printSection("STACK TRACE OF TEST THREAD");
                  for (StackTraceElement element : testMainThread.getStackTrace()) {
                    System.out.println(element.toString());
                  }
                  printJavaProcesses();
                  printLogFiles();
                  printSection("ABORT TEST");
                  System.exit(1);
                } catch (InterruptedException e) {
                  // ignored
                }
              });
      hangDetectionThread.setDaemon(true);
      hangDetectionThread.start();
    }
  }

  @Override
  public void onTestSuccess(ITestResult result) {
    printTestStage("TEST SUCCESS", getFullTestName(result));
  }

  @Override
  public void onTestFailure(ITestResult result) {
    printTestStage("TEST FAILURE", getFullTestName(result));
    Throwable throwable = result.getThrowable();
    if (throwable != null) {
      throwable.printStackTrace();
    }
    printLogFiles();
  }

  @Override
  public void onTestSkipped(ITestResult result) {
    printTestStage("TEST SKIPPED", getFullTestName(result));
  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    printTestStage("TEST FAILED BUT WITHIN SUCCESS PERCENTAGE", getFullTestName(result));
  }

  @Override
  public void onStart(ITestContext context) {}

  @Override
  public void onFinish(ITestContext context) {}

  private void printJavaProcesses() {
    printSection("JPS RESULT");
    try {
      System.out.println(RunManager.runCommand(ImmutableList.of("jps")));
    } catch (Exception e) {
      System.out.println("Failed to get jps result.");
      e.printStackTrace();
    }
    printSection("JPS RESULT END");

    printSection("PGREP JAVA RESULT");
    try {
      System.out.println(RunManager.runCommand(ImmutableList.of("pgrep", "java")));
    } catch (Exception e) {
      System.out.println("Failed to get pgrep java result.");
      e.printStackTrace();
    }
    printSection("PGREP JAVA RESULT END");
  }

  private void printLogFiles() {
    Collection<File> logFiles =
        FileUtils.listFiles(new File("/tmp/ray/session_latest/logs"), null, false);
    for (File file : logFiles) {
      tailFile(file.toPath());
    }
  }

  private void tailFile(Path filePath) {
    printSection(
        String.format("LAST %d LINES OF LOG FILE %s", TAIL_NO_OF_LINES, filePath.getFileName()));
    LinkedList<String> lastLines = new LinkedList<>();
    try (Stream<String> stream = Files.lines(filePath)) {
      stream.forEachOrdered(
          line -> {
            lastLines.push(line);
            if (lastLines.size() > TAIL_NO_OF_LINES) {
              lastLines.pop();
            }
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    while (lastLines.size() > 0) {
      System.out.println(lastLines.pop());
    }
    printSection(String.format("END OF LOG FILE %s", filePath.getFileName()));
  }
}
