package io.ray.runtime.functionmanager;

import com.google.common.collect.Lists;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.DirectoryFileFilter;
import org.apache.commons.io.filefilter.RegexFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.objectweb.asm.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Manages functions by job id. */
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

  /** Mapping from the job id to the functions that belong to this job. */
  private ConcurrentMap<JobId, JobFunctionTable> jobFunctionTables = new ConcurrentHashMap<>();

  /** The resource path which we can load the job's jar resources. */
  private final List<String> codeSearchPath;

  /**
   * Construct a FunctionManager with the specified code search path.
   *
   * @param codeSearchPath The specified job resource that can store the job's resources.
   */
  public FunctionManager(List<String> codeSearchPath) {
    this.codeSearchPath = codeSearchPath;
  }

  /**
   * Get the RayFunction from a RayFunc instance (a lambda).
   *
   * @param jobId current job id.
   * @param func The lambda.
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
   * @param jobId Current job id.
   * @param functionDescriptor The function descriptor.
   * @return A RayFunction object.
   */
  public RayFunction getFunction(JobId jobId, JavaFunctionDescriptor functionDescriptor) {
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
    if (codeSearchPath == null || codeSearchPath.isEmpty()) {
      classLoader = getClass().getClassLoader();
    } else {
      URL[] urls =
          codeSearchPath.stream()
              .filter(p -> StringUtils.isNotBlank(p) && Files.exists(Paths.get(p)))
              .flatMap(
                  p -> {
                    try {
                      if (!Files.isDirectory(Paths.get(p))) {
                        if (!p.endsWith(".jar")) {
                          return Stream.of(
                              Paths.get(p).getParent().toAbsolutePath().toUri().toURL());
                        } else {
                          return Stream.of(Paths.get(p).toAbsolutePath().toUri().toURL());
                        }
                      } else {
                        List<URL> subUrls = new ArrayList<>();
                        subUrls.add(Paths.get(p).toAbsolutePath().toUri().toURL());
                        Collection<File> jars =
                            FileUtils.listFiles(
                                new File(p),
                                new RegexFileFilter(".*\\.jar"),
                                DirectoryFileFilter.DIRECTORY);
                        for (File jar : jars) {
                          subUrls.add(jar.toPath().toUri().toURL());
                        }
                        return subUrls.stream();
                      }
                    } catch (MalformedURLException e) {
                      throw new RuntimeException(String.format("Illegal %s resource path", p));
                    }
                  })
              .toArray(URL[]::new);
      classLoader = new URLClassLoader(urls);
      LOGGER.debug("Resource loaded for job {} from path {}.", jobId, urls);
    }

    return new JobFunctionTable(classLoader);
  }

  /** Manages all functions that belong to one job. */
  static class JobFunctionTable {

    /** The job's corresponding class loader. */
    final ClassLoader classLoader;
    /** Functions per class, per function name + type descriptor. */
    ConcurrentMap<String, Map<Pair<String, String>, Pair<RayFunction, Boolean>>> functions;

    JobFunctionTable(ClassLoader classLoader) {
      this.classLoader = classLoader;
      this.functions = new ConcurrentHashMap<>();
    }

    RayFunction getFunction(JavaFunctionDescriptor descriptor) {
      Map<Pair<String, String>, Pair<RayFunction, Boolean>> classFunctions =
          functions.get(descriptor.className);
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
      RayFunction func = classFunctions.get(key).getLeft();
      if (func == null) {
        if (classFunctions.containsKey(key)) {
          throw new RuntimeException(
              String.format(
                  "RayFunction %s is overloaded, the signature can't be empty.",
                  descriptor.toString()));
        } else {
          throw new RuntimeException(
              String.format("RayFunction %s not found", descriptor.toString()));
        }
      }
      return func;
    }

    /** Load all functions from a class. */
    Map<Pair<String, String>, Pair<RayFunction, Boolean>> loadFunctionsForClass(String className) {
      // If RayFunction is null, the function is overloaded.
      // The value of this map is a pair of <rayFunction, isDefault>.
      // The `isDefault` is used to mark if the method is a marked as default keyword.
      Map<Pair<String, String>, Pair<RayFunction, Boolean>> map = new HashMap<>();
      try {
        Class clazz = Class.forName(className, true, classLoader);
        List<Executable> executables = new ArrayList<>();
        executables.addAll(Arrays.asList(clazz.getDeclaredMethods()));
        executables.addAll(Arrays.asList(clazz.getDeclaredConstructors()));

        Class clz = clazz;
        clz = clz.getSuperclass();
        while (clz != null && clz != Object.class) {
          executables.addAll(Arrays.asList(clz.getDeclaredMethods()));
          clz = clz.getSuperclass();
        }

        // Put interface methods ahead, so that in can be override by subclass methods in `map.put`
        for (Class baseInterface : clazz.getInterfaces()) {
          for (Method method : baseInterface.getDeclaredMethods()) {
            if (method.isDefault()) {
              executables.add(method);
            }
          }
        }

        // Use reverse order so that child class methods can override super class methods.
        for (Executable e : Lists.reverse(executables)) {
          e.setAccessible(true);
          final String methodName = e instanceof Method ? e.getName() : CONSTRUCTOR_NAME;
          final Type type =
              e instanceof Method ? Type.getType((Method) e) : Type.getType((Constructor) e);
          final String signature = type.getDescriptor();
          RayFunction rayFunction =
              new RayFunction(
                  e, classLoader, new JavaFunctionDescriptor(className, methodName, signature));
          final boolean isDefault = e instanceof Method && ((Method) e).isDefault();
          map.put(
              ImmutablePair.of(methodName, signature), ImmutablePair.of(rayFunction, isDefault));
          // For cross language call java function without signature
          final Pair<String, String> emptyDescriptor = ImmutablePair.of(methodName, "");
          /// default method is not overloaded, so we should filter it.
          if (map.containsKey(emptyDescriptor) && !map.get(emptyDescriptor).getRight()) {
            map.put(
                emptyDescriptor,
                ImmutablePair.of(null, false)); // Mark this function as overloaded.
          } else {
            map.put(emptyDescriptor, ImmutablePair.of(rayFunction, isDefault));
          }
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to load functions from class " + className, e);
      }
      return map;
    }
  }
}
