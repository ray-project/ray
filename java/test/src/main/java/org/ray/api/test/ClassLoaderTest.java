package org.ray.api.test;

import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

import org.apache.commons.io.FileUtils;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.options.ActorCreationOptions;
import org.ray.runtime.AbstractRayRuntime;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ClassLoaderTest extends BaseTest {

  private final String resourcePath = FileUtils.getTempDirectoryPath()
      + "/ray_test/ClassLoaderTest";

  @BeforeClass
  public void setUp() {
    // The potential issue of multiple `ClassLoader` instances for the same job on multi-threading
    // scenario only occurs if the classes are loaded from the job resource path.
    System.setProperty("ray.job.resource-path", resourcePath);
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.job.resource-path");
  }

  @Test
  public void testClassLoaderInMultiThreading() throws Exception {
    TestUtils.skipTestUnderSingleProcess();
    Assert.assertTrue(TestUtils.getRuntime().getRayConfig().numWorkersPerProcess > 1);

    final String jobResourcePath = resourcePath + "/" + Ray.getRuntimeContext().getCurrentJobId();
    File jobResourceDir = new File(jobResourcePath);
    FileUtils.deleteQuietly(jobResourceDir);
    jobResourceDir.mkdirs();
    jobResourceDir.deleteOnExit();

    // In this test case the class is expected to be loaded from the job resource path, so we need
    // to put the compiled class file into the job resource path and load it later.
    String testJavaFile = ""
        + "import java.lang.management.ManagementFactory;\n"
        + "import java.lang.management.RuntimeMXBean;\n"
        + "\n"
        + "public class ClassLoaderTester {\n"
        + "\n"
        + "  static volatile int value;\n"
        + "\n"
        + "  public int getPid() {\n"
        + "    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();\n"
        + "    String name = runtime.getName();\n"
        + "    int index = name.indexOf(\"@\");\n"
        + "    if (index != -1) {\n"
        + "      return Integer.parseInt(name.substring(0, index));\n"
        + "    } else {\n"
        + "      throw new RuntimeException(\"parse pid error:\" + name);\n"
        + "    }\n"
        + "  }\n"
        + "\n"
        + "  public int increase() throws InterruptedException {\n"
        + "    return increaseInternal();\n"
        + "  }\n"
        + "\n"
        + "  public static synchronized int increaseInternal() throws InterruptedException {\n"
        + "    int oldValue = value;\n"
        + "    Thread.sleep(10 * 1000);\n"
        + "    value = oldValue + 1;\n"
        + "    return value;\n"
        + "  }\n"
        + "\n"
        + "  public int getClassLoaderHashCode() {\n"
        + "    return this.getClass().getClassLoader().hashCode();\n"
        + "  }\n"
        + "}";

    // Write the demo java file to the job resource path.
    String javaFilePath = jobResourcePath + "/ClassLoaderTester.java";
    Files.write(Paths.get(javaFilePath), testJavaFile.getBytes());

    // Compile the java file.
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    int result = compiler.run(null, null, null, "-d", jobResourcePath, javaFilePath);
    if (result != 0) {
      throw new RuntimeException("Couldn't compile ClassLoaderTester.java.");
    }

    FunctionDescriptor constructor = new JavaFunctionDescriptor("ClassLoaderTester", "<init>",
        "()V");
    RayActor<?> actor1 = createActor(constructor);
    FunctionDescriptor getPid = new JavaFunctionDescriptor("ClassLoaderTester", "getPid", "()I");
    int pid = this.<Integer>callActorFunction(actor1, getPid, new Object[0], 1).get();
    RayActor<?> actor2;
    while (true) {
      // Create another actor which share the same process of actor 1.
      actor2 = createActor(constructor);
      int actor2Pid = this.<Integer>callActorFunction(actor2, getPid, new Object[0], 1).get();
      if (actor2Pid == pid) {
        break;
      }
    }

    FunctionDescriptor getClassLoaderHashCode = new JavaFunctionDescriptor("ClassLoaderTester",
        "getClassLoaderHashCode",
        "()I");
    RayObject<Integer> hashCode1 = callActorFunction(actor1, getClassLoaderHashCode, new Object[0],
        1);
    RayObject<Integer> hashCode2 = callActorFunction(actor2, getClassLoaderHashCode, new Object[0],
        1);
    Assert.assertEquals(hashCode1.get(), hashCode2.get());

    FunctionDescriptor increase = new JavaFunctionDescriptor("ClassLoaderTester", "increase",
        "()I");
    RayObject<Integer> value1 = callActorFunction(actor1, increase, new Object[0], 1);
    RayObject<Integer> value2 = callActorFunction(actor2, increase, new Object[0], 1);
    Assert.assertNotEquals(value1.get(), value2.get());
  }

  private RayActor<?> createActor(FunctionDescriptor functionDescriptor)
      throws Exception {
    Method createActorMethod = AbstractRayRuntime.class.getDeclaredMethod("createActorImpl",
        FunctionDescriptor.class, Object[].class, ActorCreationOptions.class);
    createActorMethod.setAccessible(true);
    return (RayActor<?>) createActorMethod
        .invoke(TestUtils.getRuntime(), functionDescriptor, new Object[0], null);
  }

  private <T> RayObject<T> callActorFunction(RayActor<?> rayActor,
      FunctionDescriptor functionDescriptor, Object[] args, int numReturns) throws Exception {
    Method callActorFunctionMethod = AbstractRayRuntime.class.getDeclaredMethod("callActorFunction",
        RayActor.class, FunctionDescriptor.class, Object[].class, int.class);
    callActorFunctionMethod.setAccessible(true);
    return (RayObject<T>) callActorFunctionMethod
        .invoke(TestUtils.getRuntime(), rayActor, functionDescriptor, args, numReturns);
  }
}
