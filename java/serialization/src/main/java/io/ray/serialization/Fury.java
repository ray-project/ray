package io.ray.serialization;

import com.google.common.base.Preconditions;
import io.ray.serialization.codegen.CodeGenerator;
import io.ray.serialization.resolver.ClassResolver;
import io.ray.serialization.resolver.MapReferenceResolver;
import io.ray.serialization.resolver.NoReferenceResolver;
import io.ray.serialization.resolver.ReferenceResolver;
import io.ray.serialization.resolver.SerializationContext;
import io.ray.serialization.serializers.BufferCallback;
import io.ray.serialization.serializers.SerializedObject;
import io.ray.serialization.serializers.Serializer;
import io.ray.serialization.serializers.SerializerFactory;
import io.ray.serialization.util.BitUtils;
import io.ray.serialization.util.MemoryBuffer;
import io.ray.serialization.util.MemoryUtils;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Iterator;

public final class Fury {
  public static final byte NULL = 0;
  // When reference tracking is enabled, this flag also indicates that the object is first read.
  public static final byte NOT_NULL = 1;
  // This flag also indicates that object is not null.
  // We don't use another byte to indicate REF, so that we can save one byte.
  public static final byte NOT_NULL_REF = 2;
  public static final byte NOT_SUPPORT_CROSS_LANGUAGE = 0;

  private final boolean isLittleEndian;
  private BufferCallback bufferCallback;
  private Iterator<ByteBuffer> outOfBandBuffers;
  private final boolean referenceTracking;
  private final ReferenceResolver referenceResolver;
  private final ClassResolver classResolver;
  private final SerializationContext serializationContext;
  private final ClassLoader classLoader;
  private final boolean codeGenEnabled;
  private final boolean checkJdkClassSerializable;
  private final CodeGenerator codeGenerator;
  private final boolean checkClassVersion;
  private final MemoryBuffer buffer;

  private Fury(FuryBuilder builder) {
    this.isLittleEndian = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN;
    this.referenceTracking = builder.referenceTracking;
    if (referenceTracking) {
      this.referenceResolver = new MapReferenceResolver(this);
    } else {
      this.referenceResolver = new NoReferenceResolver();
    }
    classResolver = new ClassResolver(this);
    classResolver.initialize();
    serializationContext = new SerializationContext();
    classLoader = builder.classLoader;
    codeGenEnabled = builder.codeGenEnabled;
    codeGenerator = CodeGenerator.getSharedCodeGenerator(classLoader);
    checkClassVersion = builder.checkClassVersion;
    checkJdkClassSerializable = builder.jdkClassSerializableCheck;
    buffer = MemoryUtils.buffer(32);
  }

  /** register class */
  public void register(Class<?> cls) {
    classResolver.register(cls);
  }

  /** register class with given id */
  public void register(Class<?> cls, Short id) {
    classResolver.register(cls, id);
  }

  public <T> void registerSerializer(
      Class<T> type, Class<? extends Serializer<T>> serializerClass) {
    classResolver.registerSerializer(type, serializerClass);
  }

  public void registerSerializer(Class<?> type, Serializer<?> serializer) {
    classResolver.registerSerializer(type, serializer);
  }

  public void setSerializerFactory(SerializerFactory serializerFactory) {
    classResolver.setSerializerFactory(serializerFactory);
  }

  public SerializerFactory getSerializerFactory() {
    return classResolver.getSerializerFactory();
  }

  /**
   * Serialize <code>obj</code> to a off-heap buffer specified by <code>address</code> and <code>
   * size</code>
   */
  public MemoryBuffer serialize(Object obj, long address, int size) {
    MemoryBuffer buffer = MemoryUtils.buffer(address, size);
    return serialize(buffer, obj);
  }

  /** Return serialized <code>obj</code> as a byte array */
  public byte[] serialize(Object obj) {
    return serialize(obj, null);
  }

  /** Return serialized <code>obj</code> as a byte array */
  public byte[] serialize(Object obj, BufferCallback callback) {
    buffer.writerIndex(0);
    serialize(buffer, obj, callback);
    return buffer.getBytes(0, buffer.writerIndex());
  }

  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj) {
    return serialize(buffer, obj, null);
  }

  /** Serialize <code>obj</code> to a <code>buffer</code> */
  public MemoryBuffer serialize(MemoryBuffer buffer, Object obj, BufferCallback callback) {
    this.bufferCallback = callback;
    int maskIndex = buffer.writerIndex();
    // 1byte used for bit mask
    buffer.ensure(maskIndex + 1);
    buffer.writerIndex(maskIndex + 1);
    if (obj == null) {
      BitUtils.unset(buffer, maskIndex, 0);
      return buffer;
    } else {
      BitUtils.set(buffer, maskIndex, 0);
    }
    // set endian.
    if (isLittleEndian) {
      BitUtils.unset(buffer, maskIndex, 1);
    } else {
      BitUtils.set(buffer, maskIndex, 1);
    }
    if (bufferCallback != null) {
      BitUtils.set(buffer, maskIndex, 3);
    } else {
      BitUtils.unset(buffer, maskIndex, 3);
    }
    serializeReferencableToJava(buffer, obj);
    resetWrite();
    return buffer;
  }

  /** Serialize a nullable referencable object to <code>buffer</code> */
  public void serializeReferencableToJava(MemoryBuffer buffer, Object obj) {
    if (!referenceResolver.writeReferenceOrNull(buffer, obj)) {
      serializeNonReferenceToJava(buffer, obj);
    }
  }

  /**
   * Serialize a not-null and non-reference object to <code>buffer</code>.
   *
   * <p>If reference is enabled, this method should be called only the object is first seen in the
   * object graph.
   */
  public void serializeNonReferenceToJava(MemoryBuffer buffer, Object obj) {
    Class<?> cls = obj.getClass();
    classResolver.writeClass(buffer, cls);
    writeData(buffer, cls, obj);
  }

  /** Write not null data to buffer */
  @SuppressWarnings("unchecked")
  private void writeData(MemoryBuffer buffer, Class<?> cls, Object obj) {
    // fast path for common type
    if (cls == Long.class) {
      buffer.writeLong((Long) obj);
    } else if (cls == Integer.class) {
      buffer.writeInt((Integer) obj);
    } else if (cls == Double.class) {
      buffer.writeDouble((Double) obj);
    } else {
      Serializer serializer = classResolver.getSerializer(cls);
      serializer.write(this, buffer, obj);
    }
  }

  public void writeSerializedObject(MemoryBuffer buffer, SerializedObject serializedObject) {
    if (bufferCallback == null || bufferCallback.apply(serializedObject)) {
      // writer length.
      buffer.writeInt(-1);
      int writerIndex = buffer.writerIndex();
      buffer.grow(serializedObject.totalBytes());
      serializedObject.writeTo(buffer);
      int size = buffer.writerIndex() - writerIndex;
      Preconditions.checkArgument(size == serializedObject.totalBytes());
      buffer.putInt(writerIndex - 4, size);
    }
  }

  public ByteBuffer readSerializedObject(MemoryBuffer buffer) {
    if (outOfBandBuffers != null) {
      Preconditions.checkArgument(outOfBandBuffers.hasNext());
      return outOfBandBuffers.next();
    } else {
      int size = buffer.readInt();
      ByteBuffer byteBuffer = buffer.sliceAsByteBuffer(buffer.readerIndex(), size);
      buffer.readerIndex(buffer.readerIndex() + size);
      return byteBuffer;
    }
  }

  /** Deserialize <code>obj</code> from a byte array. */
  public Object deserialize(byte[] bytes) {
    return deserialize(bytes, null);
  }

  public Object deserialize(byte[] bytes, Iterable<ByteBuffer> outOfBandBuffers) {
    MemoryBuffer buffer = MemoryUtils.wrap(bytes);
    return deserialize(buffer, outOfBandBuffers);
  }

  /**
   * Deserialize <code>obj</code> from a off-heap buffer specified by <code>address</code> and
   * <code>size</code>
   */
  public Object deserialize(long address, int size) {
    MemoryBuffer buffer = MemoryUtils.buffer(address, size);
    return deserialize(buffer);
  }

  /** Deserialize <code>obj</code> from a <code>buffer</code> */
  public Object deserialize(MemoryBuffer buffer) {
    return deserialize(buffer, null);
  }

  /**
   * Deserialize <code>obj</code> from a <code>buffer</code> and <code>outOfBandBuffers</code>
   *
   * @param buffer serialized data.
   * @param outOfBandBuffers If <code>buffers</code> is not None, it should be an iterable of
   *     buffer-enabled objects that is consumed each time the pickle stream references an
   *     out-of-band {@link SerializedObject}. Such buffers have been given in order to the
   *     `bufferCallback` of a Fury object. If <code>outOfBandBuffers</code> is null (the default),
   *     then the buffers are taken from the serialized stream, assuming they are serialized there.
   *     It is an error for <code>outOfBandBuffers</code> to be null if the serialized stream was
   *     produced with a non-null `bufferCallback`.
   */
  public Object deserialize(MemoryBuffer buffer, Iterable<ByteBuffer> outOfBandBuffers) {
    int maskIndex = buffer.readerIndex();
    buffer.readerIndex(maskIndex + 1);
    if (BitUtils.isNotSet(buffer, maskIndex, 0)) {
      return null;
    }
    boolean isLittleEndian = BitUtils.isNotSet(buffer, maskIndex, 1);
    Preconditions.checkArgument(this.isLittleEndian, isLittleEndian);

    boolean isOutOfBandSerializationEnabled = BitUtils.isSet(buffer, maskIndex, 3);
    if (isOutOfBandSerializationEnabled) {
      Preconditions.checkNotNull(
          outOfBandBuffers,
          "outOfBandBuffers shouldn't be null when the serialized stream is "
              + "produced with bufferCallback not null.");
      this.outOfBandBuffers = outOfBandBuffers.iterator();
      Preconditions.checkNotNull(bufferCallback);
    } else {
      Preconditions.checkArgument(
          outOfBandBuffers == null,
          "outOfBandBuffers should be null when the serialized stream is "
              + "produced with bufferCallback null.");
    }
    Object obj = deserializeReferencableFromJava(buffer);
    resetRead();
    return obj;
  }

  /** Deserialize nullable referencable object from <code>buffer</code> */
  public Object deserializeReferencableFromJava(MemoryBuffer buffer) {
    ReferenceResolver referenceResolver = this.referenceResolver;
    byte headFlag = referenceResolver.readReferenceOrNull(buffer);
    // indicates that the object is first read.
    if (headFlag == NOT_NULL) {
      int nextReadRefId = referenceResolver.preserveReferenceId();
      Object o = readData(buffer, classResolver.readClass(buffer));
      if (referenceTracking) {
        referenceResolver.setReadObject(nextReadRefId, o);
      }
      return o;
    } else {
      return referenceResolver.getReadObject();
    }
  }

  /** Deserialize not-null and non-reference object from <code>buffer</code> */
  public Object deserializeNonReferenceFromJava(MemoryBuffer buffer) {
    return readData(buffer, classResolver.readClass(buffer));
  }

  @SuppressWarnings("unchecked")
  private Object readData(MemoryBuffer buffer, Class<?> cls) {
    // fast path for common type
    if (cls == Long.class) {
      return buffer.readLong();
    } else if (cls == Integer.class) {
      return buffer.readInt();
    } else if (cls == Double.class) {
      return buffer.readDouble();
    } else {
      Serializer serializer = classResolver.getSerializer(cls);
      return serializer.read(this, buffer, cls);
    }
  }

  public void reset() {
    referenceResolver.reset();
    classResolver.reset();
    serializationContext.reset();
  }

  public void resetWrite() {
    referenceResolver.resetWrite();
    classResolver.resetWrite();
    serializationContext.reset();
  }

  public void resetRead() {
    referenceResolver.resetRead();
    classResolver.resetRead();
    serializationContext.reset();
  }

  public boolean isLittleEndian() {
    return isLittleEndian;
  }

  public boolean isReferenceTracking() {
    return referenceTracking;
  }

  public ReferenceResolver getReferenceResolver() {
    return referenceResolver;
  }

  public ClassResolver getClassResolver() {
    return classResolver;
  }

  public SerializationContext getSerializationContext() {
    return serializationContext;
  }

  public ClassLoader getClassLoader() {
    return classLoader;
  }

  public boolean isCodeGenEnabled() {
    return codeGenEnabled;
  }

  public CodeGenerator getCodeGenerator() {
    return codeGenerator;
  }

  public boolean checkClassVersion() {
    return checkClassVersion;
  }

  public boolean checkJdkClassSerializable() {
    return checkJdkClassSerializable;
  }

  public static FuryBuilder builder() {
    return new FuryBuilder();
  }

  public static final class FuryBuilder {
    private boolean checkClassVersion = true;
    private boolean referenceTracking = true;
    private ClassLoader classLoader;
    private boolean codeGenEnabled = true;
    private boolean jdkClassSerializableCheck = true;

    private FuryBuilder() {}

    public FuryBuilder withReferenceTracking(boolean referenceTracking) {
      this.referenceTracking = referenceTracking;
      return this;
    }

    public FuryBuilder withClassLoader(ClassLoader classLoader) {
      this.classLoader = classLoader;
      return this;
    }

    public FuryBuilder withCodegen(boolean codeGenEnabled) {
      this.codeGenEnabled = codeGenEnabled;
      return this;
    }

    public FuryBuilder withClassVersionCheck(boolean checkClassVersion) {
      this.checkClassVersion = checkClassVersion;
      return this;
    }

    public FuryBuilder withJdkClassSerializableCheck(boolean jdkClassSerializableCheck) {
      this.jdkClassSerializableCheck = jdkClassSerializableCheck;
      return this;
    }

    private void finish() {
      if (classLoader == null) {
        classLoader = Thread.currentThread().getContextClassLoader();
      }
    }

    public Fury build() {
      finish();
      return new Fury(this);
    }

    public ThreadSafeFury buildThreadSafeFury() {
      finish();
      return new ThreadSafeFury(() -> new Fury(this));
    }
  }
}
