package io.ray.runtime.runner;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RunManagerTest {
  private static List<String> command(String mode) {
    String java =
        System.getProperty("java.home") + File.separator + "bin" + File.separator + "java";
    return Arrays.asList(
        java, "-cp", System.getProperty("java.class.path"), Child.class.getName(), mode);
  }

  @Test(timeOut = 15000)
  public void testLargeOutput() throws Exception {
    String output = RunManager.runCommand(command("large"), 10, TimeUnit.SECONDS);
    Assert.assertTrue(output.length() >= 2 * 1024 * 1024);
    Assert.assertTrue(output.contains("stdout-end"));
    Assert.assertTrue(output.contains("stderr-end"));
  }

  @Test(timeOut = 15000)
  public void testNonZeroExit() {
    RuntimeException error =
        Assert.expectThrows(
            RuntimeException.class,
            () -> RunManager.runCommand(command("failure"), 10, TimeUnit.SECONDS));
    Assert.assertTrue(error.getMessage().contains("exit value of the process is 17"));
    Assert.assertTrue(error.getMessage().contains("failure-output"));
  }

  @Test(timeOut = 15000)
  public void testTimeout() {
    long start = System.nanoTime();
    RuntimeException error =
        Assert.expectThrows(
            RuntimeException.class,
            () -> RunManager.runCommand(command("timeout"), 1, TimeUnit.SECONDS));
    Assert.assertTrue(error.getMessage().contains("not exited in time"));
    Assert.assertTrue(error.getMessage().contains("child-started"));
    // Include the bounded termination wait, but don't wait for the child to finish sleeping.
    Assert.assertTrue(TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - start) < 8);
  }

  public static class Child {
    public static void main(String[] args) throws Exception {
      switch (args[0]) {
        case "large":
          System.out.write(new byte[2 * 1024 * 1024]);
          System.out.println("stdout-end");
          System.err.println("stderr-end");
          break;
        case "failure":
          System.err.println("failure-output");
          System.exit(17);
          break;
        case "timeout":
          System.out.println("child-started");
          System.out.flush();
          Thread.sleep(10000);
          break;
        default:
          throw new IllegalArgumentException(args[0]);
      }
    }
  }
}
