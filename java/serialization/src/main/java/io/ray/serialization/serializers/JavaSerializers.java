package io.ray.serialization.serializers;

import com.google.common.base.Preconditions;
import io.ray.serialization.Fury;
import io.ray.serialization.io.ClassLoaderObjectInputStream;
import io.ray.serialization.io.FuryObjectInput;
import io.ray.serialization.io.FuryObjectOutput;
import io.ray.serialization.util.LoggerFactory;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.Platform;
import io.ray.serialization.util.ReflectionUtils;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.util.Collection;
import java.util.Map;
import org.slf4j.Logger;

public class JavaSerializers {

  /**
   * Serializes objects using Java's built in serialization to be compatible with java
   * serialization. This is very inefficient and should be avoided if possible. User can call {@link
   * Fury#registerSerializer} to avoid this.
   *
   * <p>When a serializer not found and {@link JavaSerializer#requireJavaSerialization(Class)}
   * return true, this serializer will be used.
   */
  public static class JavaSerializer extends Serializer {
    private static final Logger LOG = LoggerFactory.getLogger(JavaSerializer.class);
    private final FuryObjectInput objectInput;
    private final FuryObjectOutput objectOutput;

    public JavaSerializer(Fury fury, Class<?> cls) {
      super(fury, cls);
      Preconditions.checkArgument(requireJavaSerialization(cls));
      LOG.warn(
          "{} use java built-in serialization, which is inefficient. "
              + "Please replace it with a {} or implements {}",
          cls,
          Serializer.class.getCanonicalName(),
          Externalizable.class.getCanonicalName());
      objectInput = new FuryObjectInput(fury, null);
      objectOutput = new FuryObjectOutput(fury, null);
    }

    /**
     * Return true if a class satisfy following requirements:
     * <li>implements {@link Serializable}
     * <li>is not an {@link Enum}
     * <li>is not an array
     * <li>has {@code readResolve}/{@code writePlace} method or class has {@code readObject}/{@code
     *     writeObject} method, but doesn't implements {@link Externalizable}
     * <li/>
     */
    public static boolean requireJavaSerialization(Class<?> clz) {
      if (clz.isEnum() || clz.isArray()) {
        return false;
      }
      if (isDynamicGeneratedCLass(clz)) {
        // use corresponding serializer.
        return false;
      }
      if (!ReflectionUtils.getAllInterfaces(clz).contains(Serializable.class)) {
        return false;
      }
      if (getWriteReplaceMethod(clz) != null) {
        return true;
      }
      if (getReadResolveMethod(clz) != null) {
        return true;
      }
      if (Externalizable.class.isAssignableFrom(clz)) {
        return false;
      } else {
        return getReadObjectMethod(clz) != null || getWriteObjectMethod(clz) != null;
      }
    }

    public static Class<? extends Serializer> getJavaSerializer(Class<?> clz) {
      if (Collection.class.isAssignableFrom(clz)) {
        return CollectionJavaSerializer.class;
      } else if (Map.class.isAssignableFrom(clz)) {
        return MapJavaSerializer.class;
      } else {
        return JavaSerializer.class;
      }
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Object value) {
      try {
        objectOutput.setBuffer(buffer);
        ObjectOutputStream objectOutputStream =
            (ObjectOutputStream) fury.getSerializationContext().get(objectOutput);
        if (objectOutputStream == null) {
          objectOutputStream = new ObjectOutputStream(objectOutput);
          fury.getSerializationContext().add(objectOutput, objectOutputStream);
        }
        objectOutputStream.writeObject(value);
        objectOutputStream.flush();
      } catch (IOException e) {
        Platform.throwException(e);
      }
    }

    @Override
    public Object read(Fury fury, MemoryBuffer buffer, Class type) {
      try {
        objectInput.setBuffer(buffer);
        ObjectInputStream objectInputStream =
            (ObjectInputStream) fury.getSerializationContext().get(objectInput);
        if (objectInputStream == null) {
          objectInputStream = new ClassLoaderObjectInputStream(fury.getClassLoader(), objectInput);
          fury.getSerializationContext().add(objectInput, objectInputStream);
        }
        return objectInputStream.readObject();
      } catch (IOException | ClassNotFoundException e) {
        Platform.throwException(e);
      }
      throw new IllegalStateException("unreachable code");
    }
  }

  public static class LambdaSerializer extends Serializer {
    private static final Class SERIALIZED_LAMBDA = java.lang.invoke.SerializedLambda.class;

    @SuppressWarnings("unchecked")
    public LambdaSerializer(Fury fury, Class cls) {
      super(fury, cls);
      if (cls != ReplaceStub.class) {
        if (!Serializable.class.isAssignableFrom(cls)) {
          String msg =
              String.format("Lambda needs to implement %s for serialization", Serializable.class);
          throw new UnsupportedOperationException(msg);
        }
      }
    }

    /** Returns true if the specified class is a lambda. */
    public static boolean isLambda(Class clz) {
      Preconditions.checkNotNull(clz);
      return clz.getName().indexOf('/') >= 0;
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Object value) {
      try {
        Method writeReplaceMethod = getWriteReplaceMethod(value.getClass());
        Preconditions.checkNotNull(writeReplaceMethod);
        writeReplaceMethod.setAccessible(true);
        Object replacement = writeReplaceMethod.invoke(value);
        Preconditions.checkArgument(SERIALIZED_LAMBDA.isInstance(replacement));
        fury.serializeReferencableToJava(buffer, replacement);
      } catch (Exception e) {
        throw new RuntimeException("Can't serialize lambda " + value, e);
      }
    }

    @Override
    public Object read(Fury fury, MemoryBuffer buffer, Class type) {
      Preconditions.checkArgument(type == ReplaceStub.class);
      try {
        return fury.deserializeReferencableFromJava(buffer);
      } catch (Exception e) {
        throw new RuntimeException("Can't deserialize lambda", e);
      }
    }

    /**
     * Class name of dynamic generated class is not fixed, so we use a stub class to mock dynamic
     * class.
     */
    public static class ReplaceStub {}
  }

  private static Method getWriteObjectMethod(Class<?> clz) {
    Method writeObject = getMethod(clz, "writeObject");
    if (writeObject != null) {
      if (writeObject.getParameterTypes().length == 1
          && writeObject.getParameterTypes()[0] == ObjectOutputStream.class
          && writeObject.getReturnType() == void.class
          && Modifier.isPrivate(writeObject.getModifiers())) {
        return writeObject;
      }
    }
    return null;
  }

  private static Method getReadObjectMethod(Class<?> clz) {
    Method readObject = getMethod(clz, "readObject");
    if (readObject != null) {
      if (readObject.getParameterTypes().length == 1
          && readObject.getParameterTypes()[0] == ObjectInputStream.class
          && readObject.getReturnType() == void.class
          && Modifier.isPrivate(readObject.getModifiers())) {
        return readObject;
      }
    }
    return null;
  }

  private static Method getReadResolveMethod(Class<?> clz) {
    Method readResolve = getMethod(clz, "readResolve");
    if (readResolve != null) {
      if (readResolve.getParameterTypes().length == 0
          && readResolve.getReturnType() == Object.class) {
        return readResolve;
      }
    }
    return null;
  }

  private static Method getWriteReplaceMethod(Class<?> clz) {
    Method writeReplace = getMethod(clz, "writeReplace");
    if (writeReplace != null) {
      if (writeReplace.getParameterTypes().length == 0
          && writeReplace.getReturnType() == Object.class) {
        return writeReplace;
      }
    }
    return null;
  }

  private static Method getMethod(Class<?> clz, String methodName) {
    Class<?> cls = clz;
    while (cls != null) {
      for (Method method : cls.getDeclaredMethods()) {
        if (method.getName().equals(methodName)) {
          return method;
        }
      }
      cls = cls.getSuperclass();
    }
    return null;
  }

  public static boolean isDynamicGeneratedCLass(Class<?> cls) {
    // TODO(mubai) add cglib check
    return LambdaSerializer.isLambda(cls) || JdkProxySerializer.isJdkProxy(cls);
  }

  public static class JdkProxySerializer extends Serializer {

    @SuppressWarnings("unchecked")
    public JdkProxySerializer(Fury fury, Class cls) {
      super(fury, cls);
      if (cls != ReplaceStub.class) {
        Preconditions.checkArgument(isJdkProxy(cls), "Require a jdk proxy class");
      }
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, Object value) {
      fury.serializeReferencableToJava(buffer, Proxy.getInvocationHandler(value));
      fury.serializeReferencableToJava(buffer, value.getClass().getInterfaces());
    }

    @Override
    public Object read(Fury fury, MemoryBuffer buffer, Class type) {
      Preconditions.checkArgument(type == ReplaceStub.class);
      InvocationHandler invocationHandler =
          (InvocationHandler) fury.deserializeReferencableFromJava(buffer);
      Preconditions.checkNotNull(invocationHandler);
      final Class<?>[] interfaces = (Class<?>[]) fury.deserializeReferencableFromJava(buffer);
      Preconditions.checkNotNull(interfaces);
      return Proxy.newProxyInstance(fury.getClassLoader(), interfaces, invocationHandler);
    }

    public static boolean isJdkProxy(Class<?> clz) {
      return Proxy.isProxyClass(clz);
    }

    public static class ReplaceStub {}
  }

  public static final class CollectionJavaSerializer<T extends Collection>
      extends CollectionSerializers.CollectionSerializer<T> {
    private final JavaSerializer javaSerializer;

    public CollectionJavaSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false);
      javaSerializer = new JavaSerializer(fury, cls);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
      return (T) javaSerializer.read(fury, buffer, type);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, T value) {
      javaSerializer.write(fury, buffer, value);
    }
  }

  public static class MapJavaSerializer<T extends Map> extends MapSerializers.MapSerializer<T> {
    private final JavaSerializer javaSerializer;

    public MapJavaSerializer(Fury fury, Class<T> cls) {
      super(fury, cls, false);
      javaSerializer = new JavaSerializer(fury, cls);
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
      return (T) javaSerializer.read(fury, buffer, type);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, T value) {
      javaSerializer.write(fury, buffer, value);
    }
  }
}
