package io.ray.test;

import com.google.common.collect.ImmutableList;
import io.ray.runtime.runner.RunManager;
import java.io.File;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.testng.IInvokedMethod;
import org.testng.IInvokedMethodListener;
import org.testng.ITestContext;
import org.testng.ITestListener;
import org.testng.ITestResult;
import org.testng.SkipException;

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
                  printDebugInfo(null, /*testHanged=*/ true);
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
    printDebugInfo(result, /*testHanged=*/ false);
  }

  @Override
  public void onTestSkipped(ITestResult result) {
    printTestStage("TEST SKIPPED", getFullTestName(result));
    printDebugInfo(result, /*testHanged=*/ false);
  }

  @Override
  public void onTestFailedButWithinSuccessPercentage(ITestResult result) {
    printTestStage("TEST FAILED BUT WITHIN SUCCESS PERCENTAGE", getFullTestName(result));
  }

  @Override
  public void onStart(ITestContext context) {}

  @Override
  public void onFinish(ITestContext context) {}

  private void printDebugInfo(ITestResult result, boolean testHanged) {
    boolean testFailed = false;
    if (result != null) {
      Throwable throwable = result.getThrowable();
      if (throwable != null && !(throwable instanceof SkipException)) {
        testFailed = true;
        throwable.printStackTrace();
      }
    }
    if (!testFailed && !testHanged) {
      return;
    }

    if (testHanged) {
      if (result != null) {
        printSection("TEST CASE HANGED: " + getFullTestName(result));
      } else {
        printSection("TEST CASE HANGED");
      }
      printSection("TEST CASE HANGED");
      printSection("STACK TRACE OF TEST THREAD");
      for (StackTraceElement element : testMainThread.getStackTrace()) {
        System.out.println(element.toString());
      }
      Set<Integer> javaPids = getJavaPids();
      for (Integer pid : javaPids) {
        runCommandSafely(ImmutableList.of("jstack", pid.toString()));
        // TODO(kfstorm): Check lldb or gdb exists rather than detecting OS type.
        if (SystemUtils.IS_OS_MAC) {
          runCommandSafely(
              ImmutableList.of("lldb", "--batch", "-o", "bt all", "-p", pid.toString()));
        } else {
          runCommandSafely(
              ImmutableList.of(
                  "sudo", "gdb", "-batch", "-ex", "thread apply all bt", "-p", pid.toString()));
        }
      }
    }

    printLogFiles();

    if (testHanged) {
      printSection("ABORT TEST");
      System.exit(1);
    }
  }

  private String runCommandSafely(List<String> command) {
    String output;
    String commandString = String.join(" ", command);
    printSection(commandString);
    try {
      output = RunManager.runCommand(command);
      System.out.println(output);
    } catch (Exception e) {
      System.out.println("Failed to execute command: " + commandString);
      e.printStackTrace();
      output = "";
    }
    return output;
  }

  private Set<Integer> getJavaPids() {
    Set<Integer> javaPids = new HashSet<>();
    String jpsOutput = runCommandSafely(ImmutableList.of("jps", "-v"));
    try {
      for (String line : StringUtils.split(jpsOutput, "\n")) {
        String[] parts = StringUtils.split(line);
        if (parts.length > 1 && parts[1].toLowerCase().equals("jps")) {
          // Skip jps.
          continue;
        }
        Integer pid = Integer.valueOf(parts[0]);
        javaPids.add(pid);
      }
    } catch (Exception e) {
      System.out.println("Failed to parse jps output.");
      e.printStackTrace();
    }

    String pgrepJavaResult = runCommandSafely(ImmutableList.of("pgrep", "java"));
    try {
      for (String line : StringUtils.split(pgrepJavaResult, "\n")) {
        Integer pid = Integer.valueOf(line);
        javaPids.add(pid);
      }
    } catch (Exception e) {
      System.out.println("Failed to parse pgrep java output.");
      e.printStackTrace();
    }

    return javaPids;
  }

  private void printLogFiles() {
    Collection<File> logFiles =
        FileUtils.listFiles(new File("/tmp/ray/session_latest/logs"), null, false);
    for (File file : logFiles) {
      runCommandSafely(
          ImmutableList.of("tail", "-n", String.valueOf(TAIL_NO_OF_LINES), file.getAbsolutePath()));
    }
  }
}
