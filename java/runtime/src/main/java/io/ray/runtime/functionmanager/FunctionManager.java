package io.ray.runtime.functionmanager;

import com.google.common.base.Strings;
import io.ray.api.function.RayFunc;
import io.ray.api.id.JobId;
import io.ray.runtime.util.LambdaUtils;
import java.io.File;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Manages functions by job id.
 */
public class FunctionManager {

  private static final Logger LOGGER = LoggerFactory.getLogger(FunctionManager.class);

  static final String CONSTRUCTOR_NAME = "<init>";

  /**
   * Cache from a RayFunc object to its corresponding JavaFunctionDescriptor. Because
   * `LambdaUtils.getSerializedLambda` is expensive.
   */
  // If the cache is not thread local, we'll need a lock to protect it,
  // which means competition is highly possible.
  private static final ThreadLocal<WeakHashMap<Class<? extends RayFunc>, JavaFunctionDescriptor>>
      RAY_FUNC_CACHE = ThreadLocal.withInitial(WeakHashMap::new);

  /**
   * Mapping from the job id to the functions that belong to this job.
   */
  private ConcurrentMap<JobId, JobFunctionTable> jobFunctionTables = new ConcurrentHashMap<>();

  /**
   * The resource path which we can load the job's jar resources.
   */
  private final String jobResourcePath;

  /**
   * Construct a FunctionManager with the specified job resource path.
   *
   * @param jobResourcePath The specified job resource that can store the job's
   *                        resources.
   */
  public FunctionManager(String jobResourcePath) {
    this.jobResourcePath = jobResourcePath;
  }

  /**
   * Get the RayFunction from a RayFunc instance (a lambda).
   *
   * @param jobId current job id.
   * @param func  The lambda.
   * @return A RayFunction object.
   */
  public RayFunction getFunction(JobId jobId, RayFunc func) {
    JavaFunctionDescriptor functionDescriptor = RAY_FUNC_CACHE.get().get(func.getClass());
    if (functionDescriptor == null) {
      // It's OK to not lock here, because it's OK to have multiple JavaFunctionDescriptor instances
      // for the same RayFunc instance.
      SerializedLambda serializedLambda = LambdaUtils.getSerializedLambda(func);
      final String className = serializedLambda.getImplClass().replace('/', '.');
      final String methodName = serializedLambda.getImplMethodName();
      final String signature = serializedLambda.getImplMethodSignature();
      functionDescriptor = new JavaFunctionDescriptor(className, methodName, signature);
      RAY_FUNC_CACHE.get().put(func.getClass(), functionDescriptor);
    }
    return getFunction(jobId, functionDescriptor);
  }

  /**
   * Get the RayFunction from a function descriptor.
   *
   * @param jobId              Current job id.
   * @param functionDescriptor The function descriptor.
   * @return A RayFunction object.
   */
  public RayFunction getFunction(JobId jobId,
                                 JavaFunctionDescriptor functionDescriptor) {
    JobFunctionTable jobFunctionTable = jobFunctionTables.get(jobId);
    if (jobFunctionTable == null) {
      synchronized (this) {
        jobFunctionTable = jobFunctionTables.get(jobId);
        if (jobFunctionTable == null) {
          jobFunctionTable = createJobFunctionTable(jobId);
          jobFunctionTables.put(jobId, jobFunctionTable);
        }
      }
    }
    return jobFunctionTable.getFunction(functionDescriptor);
  }

  private JobFunctionTable createJobFunctionTable(JobId jobId) {
    ClassLoader classLoader;
    if (Strings.isNullOrEmpty(jobResourcePath)) {
      classLoader = getClass().getClassLoader();
    } else {
      File resourceDir = new File(jobResourcePath + "/" + jobId.toString() + "/");
      Collection<File> files = FileUtils.listFiles(resourceDir,
          new RegexFileFilter(".*\\.jar"), DirectoryFileFilter.DIRECTORY);
      files.add(resourceDir);
      final List<URL> urlList = files.stream().map(file -> {
        try {
          return file.toURI().toURL();
        } catch (MalformedURLException e) {
          throw new RuntimeException(e);
        }
      }).collect(Collectors.toList());
      classLoader = new URLClassLoader(urlList.toArray(new URL[urlList.size()]));
      LOGGER.debug("Resource loaded for job {} from path {}.", jobId,
          resourceDir.getAbsolutePath());
    }

    return new JobFunctionTable(classLoader);
  }

  /**
   * Manages all functions that belong to one job.
   */
  static class JobFunctionTable {

    /**
     * The job's corresponding class loader.
     */
    final ClassLoader classLoader;
    /**
     * Functions per class, per function name + type descriptor.
     */
    ConcurrentMap<String, Map<Pair<String, String>, RayFunction>> functions;

    JobFunctionTable(ClassLoader classLoader) {
      this.classLoader = classLoader;
      this.functions = new ConcurrentHashMap<>();
    }

    RayFunction getFunction(JavaFunctionDescriptor descriptor) {
      Map<Pair<String, String>, RayFunction> classFunctions = functions.get(descriptor.className);
      if (classFunctions == null) {
        synchronized (this) {
          classFunctions = functions.get(descriptor.className);
          if (classFunctions == null) {
            classFunctions = loadFunctionsForClass(descriptor.className);
            functions.put(descriptor.className, classFunctions);
          }
        }
      }
      final Pair<String, String> key = ImmutablePair.of(descriptor.name, descriptor.signature);
      RayFunction func = classFunctions.get(key);
      if (func == null) {
        if (classFunctions.containsKey(key)) {
          throw new RuntimeException(
                  String.format("RayFunction %s is overloaded, the signature can't be empty.",
                          descriptor.toString()));
        } else {
          throw new RuntimeException(
                  String.format("RayFunction %s not found", descriptor.toString()));
        }
      }
      return func;
    }

    /**
     * Load all functions from a class.
     */
    Map<Pair<String, String>, RayFunction> loadFunctionsForClass(String className) {
      // If RayFunction is null, the function is overloaded.
      Map<Pair<String, String>, RayFunction> map = new HashMap<>();
      try {
        Class clazz = Class.forName(className, true, classLoader);

        List<Executable> executables = new ArrayList<>();
        executables.addAll(Arrays.asList(clazz.getDeclaredMethods()));
        executables.addAll(Arrays.asList(clazz.getConstructors()));

        for (Executable e : executables) {
          e.setAccessible(true);
          final String methodName = e instanceof Method ? e.getName() : CONSTRUCTOR_NAME;
          final Type type =
              e instanceof Method ? Type.getType((Method) e) : Type.getType((Constructor) e);
          final String signature = type.getDescriptor();
          RayFunction rayFunction = new RayFunction(e, classLoader,
              new JavaFunctionDescriptor(className, methodName, signature));
          map.put(ImmutablePair.of(methodName, signature), rayFunction);
          // For cross language call java function without signature
          final Pair<String, String> emptyDescriptor = ImmutablePair.of(methodName, "");
          if (map.containsKey(emptyDescriptor)) {
            map.put(emptyDescriptor, null); // Mark this function as overloaded.
          } else {
            map.put(emptyDescriptor, rayFunction);
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to load functions from class " + className, e);
      }
      return map;
    }
  }
}
