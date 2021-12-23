package io.ray.serialization.encoder;

import static io.ray.serialization.util.StringUtils.format;

import com.google.common.base.Preconditions;
import io.ray.serialization.codegen.CodeGenerator;
import io.ray.serialization.codegen.CodegenContext;
import io.ray.serialization.codegen.CompileUnit;
import io.ray.serialization.codegen.JaninoUtils;
import io.ray.serialization.util.Descriptor;
import io.ray.serialization.util.LoggerFactory;
import io.ray.serialization.util.ReflectionUtils;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import java.util.stream.Collectors;
import org.slf4j.Logger;

/**
 * Define accessor helper methods in beanClass's classloader and same package to avoid reflective
 * call overhead.
 */
public class AccessorHelper {
  private static final Logger LOG = LoggerFactory.getLogger(AccessorHelper.class);
  private static WeakHashMap<Class<?>, Boolean> defineAccessorStatus = new WeakHashMap<>();
  private static WeakHashMap<Class<?>, Object> defineAccessorLock = new WeakHashMap<>();
  private static final Object defineLock = new Object();

  private static final String SUFFIX = "FuryAccessor";
  private static final String OBJ_NAME = "obj";
  private static final String FIELD_VALUE = "fieldValue";

  public static String accessorClassName(Class<?> beanClass) {
    String name = ReflectionUtils.getClassNameWithoutPackage(beanClass) + SUFFIX;
    return name.replace("$", "_");
  }

  public static String qualifiedAccessorClassName(Class<?> beanClass) {
    return CodecUtils.getPackage(beanClass) + "." + accessorClassName(beanClass);
  }

  /** Don't gen code for super classes */
  public static String genCode(Class<?> beanClass) {
    CodegenContext ctx = new CodegenContext();
    ctx.setPackage(CodecUtils.getPackage(beanClass));
    String className = accessorClassName(beanClass);
    ctx.setClassName(className);
    ctx.addImport(beanClass);
    // filter out super classes
    List<Descriptor> descriptors =
        Descriptor.getDescriptors(beanClass).stream()
            .filter(descriptor -> descriptor.getField().getDeclaringClass() == beanClass)
            .collect(Collectors.toList());
    for (Descriptor descriptor : descriptors) {
      if (!Modifier.isPrivate(descriptor.getModifiers())) {
        {
          String methodName = descriptor.getName();
          String codeBody =
              format(
                  "return ${obj}.${fieldName};",
                  "obj",
                  OBJ_NAME,
                  "fieldName",
                  descriptor.getName());
          Class<?> returnType = descriptor.getTypeToken().getRawType();
          ctx.addStaticMethod(methodName, codeBody, returnType, beanClass, OBJ_NAME);
        }
        {
          String methodName = descriptor.getName();
          String codeBody =
              format(
                  "${obj}.${fieldName} = ${fieldValue};",
                  "obj",
                  OBJ_NAME,
                  "fieldName",
                  descriptor.getName(),
                  "fieldValue",
                  FIELD_VALUE);
          ctx.addStaticMethod(
              methodName,
              codeBody,
              void.class,
              beanClass,
              OBJ_NAME,
              descriptor.getTypeToken().getRawType(),
              FIELD_VALUE);
        }
      }
      // getter/setter may lose some inner state of an object, so we set them to null to avoid
      // creating getter/setter accessor.
    }

    return ctx.genCode();
  }

  /** Don't define accessor for super classes, because they maybe in different package. */
  public static boolean defineAccessorClass(Class<?> beanClass) {
    ClassLoader classLoader = beanClass.getClassLoader();
    if (classLoader == null) {
      // Maybe return null if this class was loaded by the bootstrap class loader.
      return false;
    }
    String qualifiedClassName = qualifiedAccessorClassName(beanClass);
    try {
      classLoader.loadClass(qualifiedClassName);
      return true;
    } catch (ClassNotFoundException ignored) {
      synchronized (defineLock) {
        if (defineAccessorStatus.containsKey(beanClass)) {
          return defineAccessorStatus.get(beanClass);
        }
      }
      synchronized (getDefineLock(beanClass)) {
        if (defineAccessorStatus.containsKey(beanClass)) {
          return defineAccessorStatus.get(beanClass);
        }
        long startTime = System.nanoTime();
        String code = genCode(beanClass);
        String pkg = CodecUtils.getPackage(beanClass);
        CompileUnit compileUnit = new CompileUnit(pkg, accessorClassName(beanClass), code);
        Map<String, byte[]> classByteCodes = JaninoUtils.toBytecode(classLoader, compileUnit);
        long durationMs = (System.nanoTime() - startTime) / 1000_000;
        LOG.info("Compile {} take {} ms", qualifiedClassName, durationMs);
        boolean succeed =
            CodeGenerator.tryDefineClassesInClassLoader(
                classLoader, qualifiedClassName, classByteCodes.values().iterator().next());
        defineAccessorStatus.put(beanClass, succeed);
        if (!succeed) {
          LOG.info("Define accessor {} in classloader {} failed.", qualifiedClassName, classLoader);
        }
        return succeed;
      }
    }
  }

  private static Object getDefineLock(Class<?> clazz) {
    synchronized (defineLock) {
      return defineAccessorLock.computeIfAbsent(clazz, k -> new Object());
    }
  }

  public static Class<?> getAccessorClass(Class<?> beanClass) {
    Preconditions.checkArgument(defineAccessorClass(beanClass));
    ClassLoader classLoader = beanClass.getClassLoader();
    String qualifiedClassName = qualifiedAccessorClassName(beanClass);
    try {
      return classLoader.loadClass(qualifiedClassName);
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("unreachable code", e);
    }
  }

  public static boolean defineAccessor(Field field) {
    Class<?> beanClass = field.getDeclaringClass();
    return defineAccessorClass(beanClass);
  }

  public static Class<?> getAccessorClass(Field field) {
    Class<?> beanClass = field.getDeclaringClass();
    return getAccessorClass(beanClass);
  }

  public static boolean defineAccessor(Method method) {
    Class<?> beanClass = method.getDeclaringClass();
    return defineAccessorClass(beanClass);
  }

  public static Class<?> getAccessorClass(Method method) {
    Class<?> beanClass = method.getDeclaringClass();
    return getAccessorClass(beanClass);
  }
}
