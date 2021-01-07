package io.ray.test;

import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.options.ActorCreationOptions;
import io.ray.runtime.AbstractRayRuntime;
import io.ray.runtime.functionmanager.FunctionDescriptor;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import java.io.File;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Optional;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class ClassLoaderTest extends BaseTest {

  private final String codeSearchPath =
      FileUtils.getTempDirectoryPath() + "/ray_test/ClassLoaderTest";

  @BeforeClass
  public void setUp() {
    // The potential issue of multiple `ClassLoader` instances for the same job on multi-threading
    // scenario only occurs if the classes are loaded from the job code search path.
    System.setProperty("ray.job.code-search-path", codeSearchPath);
  }

  @AfterClass
  public void tearDown() {
    System.clearProperty("ray.job.code-search-path");
  }

  @Test(groups = {"cluster"})
  public void testClassLoaderInMultiThreading() throws Exception {
    File jobResourceDir = new File(codeSearchPath);
    FileUtils.deleteQuietly(jobResourceDir);
    jobResourceDir.mkdirs();
    jobResourceDir.deleteOnExit();

    // In this test case the class is expected to be loaded from the job code search path,
    // so we need to put the compiled class file into the job code search path and load it
    // later.
    String testJavaFile =
        ""
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

    // Write the demo java file to the job code search path.
    String javaFilePath = codeSearchPath + "/ClassLoaderTester.java";
    Files.write(Paths.get(javaFilePath), testJavaFile.getBytes());

    // Compile the java file.
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    int result = compiler.run(null, null, null, "-d", codeSearchPath, javaFilePath);
    if (result != 0) {
      throw new RuntimeException("Couldn't compile ClassLoaderTester.java.");
    }

    FunctionDescriptor constructor =
        new JavaFunctionDescriptor("ClassLoaderTester", "<init>", "()V");
    ActorHandle<?> actor1 = createActor(constructor);
    FunctionDescriptor getPid = new JavaFunctionDescriptor("ClassLoaderTester", "getPid", "()I");
    int pid =
        this.<Integer>callActorFunction(actor1, getPid, new Object[0], Optional.of(Integer.class))
            .get();
    ActorHandle<?> actor2;
    while (true) {
      // Create another actor which share the same process of actor 1.
      actor2 = createActor(constructor);
      int actor2Pid =
          this.<Integer>callActorFunction(actor2, getPid, new Object[0], Optional.of(Integer.class))
              .get();
      if (actor2Pid == pid) {
        break;
      }
    }

    FunctionDescriptor getClassLoaderHashCode =
        new JavaFunctionDescriptor("ClassLoaderTester", "getClassLoaderHashCode", "()I");
    ObjectRef<Integer> hashCode1 =
        callActorFunction(
            actor1, getClassLoaderHashCode, new Object[0], Optional.of(Integer.class));
    ObjectRef<Integer> hashCode2 =
        callActorFunction(
            actor2, getClassLoaderHashCode, new Object[0], Optional.of(Integer.class));
    Assert.assertEquals(hashCode1.get(), hashCode2.get());

    FunctionDescriptor increase =
        new JavaFunctionDescriptor("ClassLoaderTester", "increase", "()I");
    ObjectRef<Integer> value1 =
        callActorFunction(actor1, increase, new Object[0], Optional.of(Integer.class));
    ObjectRef<Integer> value2 =
        callActorFunction(actor2, increase, new Object[0], Optional.of(Integer.class));
    Assert.assertNotEquals(value1.get(), value2.get());
  }

  private ActorHandle<?> createActor(FunctionDescriptor functionDescriptor) throws Exception {
    Method createActorMethod =
        AbstractRayRuntime.class.getDeclaredMethod(
            "createActorImpl",
            FunctionDescriptor.class,
            Object[].class,
            ActorCreationOptions.class);
    createActorMethod.setAccessible(true);
    return (ActorHandle<?>)
        createActorMethod.invoke(
            TestUtils.getUnderlyingRuntime(), functionDescriptor, new Object[0], null);
  }

  private <T> ObjectRef<T> callActorFunction(
      ActorHandle<?> rayActor,
      FunctionDescriptor functionDescriptor,
      Object[] args,
      Optional<Class<?>> returnType)
      throws Exception {
    Method callActorFunctionMethod =
        AbstractRayRuntime.class.getDeclaredMethod(
            "callActorFunction",
            BaseActorHandle.class,
            FunctionDescriptor.class,
            Object[].class,
            Optional.class);
    callActorFunctionMethod.setAccessible(true);
    return (ObjectRef<T>)
        callActorFunctionMethod.invoke(
            TestUtils.getUnderlyingRuntime(), rayActor, functionDescriptor, args, returnType);
  }
}
