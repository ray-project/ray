package io.ray.serialization.serializers;

import com.google.common.reflect.TypeToken;
import io.ray.serialization.Fury;
import io.ray.serialization.encoder.CodecUtils;
import io.ray.serialization.util.Descriptor;
import io.ray.serialization.util.LoggerFactory;
import io.ray.serialization.util.MemoryBuffer;
import java.lang.reflect.Modifier;
import org.slf4j.Logger;

@SuppressWarnings("UnstableApiUsage")
public final class CodegenSerializer {
  private static final Logger LOG = LoggerFactory.getLogger(CodegenSerializer.class);

  public static boolean support(Fury fury, Class<?> cls) {
    // Codegen doesn't support duplicate fields for now.
    if (Descriptor.hasDuplicateNameField(cls)) {
      return false;
    }
    return isJavaBean(TypeToken.of(cls));
  }

  @SuppressWarnings("unchecked")
  public static <T> Class<Serializer<T>> loadCodegenSerializer(Fury fury, Class<T> cls) {
    LOG.info("Create serializer for class {}", cls);
    try {
      return (Class<Serializer<T>>) CodecUtils.loadOrGenSeqCodecClass(cls, fury);
    } catch (Exception e) {
      String msg = String.format("Create sequential encoder failed, \nbeanClass: %s", cls);
      throw new RuntimeException(msg, e);
    }
  }

  private static boolean isJavaBean(TypeToken<?> type) {
    Class<?> rawType = type.getRawType();
    // since we need to access class in generated code in our package, the class must be public.
    if (Modifier.isPublic(rawType.getModifiers())) {
      // bean class can be static nested class, but can't be a non-static inner class
      return rawType.getEnclosingClass() == null || Modifier.isStatic(rawType.getModifiers());
    } else {
      return false;
    }
  }

  /**
   * A bean serializer which initializes lazily on first call read/write method.
   *
   * <p>This class is used by {@link io.ray.serialization.encoder.SeqCodecBuilder} to avoid
   * potential recursive bean serializer creation when there is a circular reference in class
   * children fields.
   */
  public static final class LazyInitBeanSerializer<T> extends Serializer<T> {
    private Serializer<T> serializer;

    public LazyInitBeanSerializer(Fury fury, Class<T> cls) {
      super(fury, cls);
    }

    @Override
    public void write(Fury fury, MemoryBuffer buffer, T value) {
      getOrCreateGeneratedSerializer().write(fury, buffer, value);
    }

    @Override
    public T read(Fury fury, MemoryBuffer buffer, Class<T> type) {
      return getOrCreateGeneratedSerializer().read(fury, buffer, type);
    }

    @SuppressWarnings("unchecked")
    private Serializer<T> getOrCreateGeneratedSerializer() {
      if (serializer == null) {
        serializer = (Serializer<T>) fury.getClassResolver().getSerializer(cls);
      }
      return serializer;
    }
  }

}
