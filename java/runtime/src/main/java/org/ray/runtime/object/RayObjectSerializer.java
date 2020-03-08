package org.ray.runtime.object;

import org.nustaq.serialization.FSTBasicObjectSerializer;
import org.nustaq.serialization.FSTClazzInfo;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;
import org.ray.api.id.ObjectId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RayObjectSerializer extends FSTBasicObjectSerializer {

  static ThreadLocal<Set<ObjectId>> innerIds = ThreadLocal.withInitial(HashSet::new);

  @Override
  public void writeObject(FSTObjectOutput out, Object toWrite, FSTClazzInfo clzInfo,
                          FSTClazzInfo.FSTFieldInfo referencedBy, int streamPosition) throws IOException {
    RayObjectImpl object = (RayObjectImpl) toWrite;
    out.writeObject(object.getId());
    innerIds.get().add(object.getId());
  }

  @Override
  public void readObject(FSTObjectInput in, Object toRead, FSTClazzInfo clzInfo,
                         FSTClazzInfo.FSTFieldInfo referencedBy) throws Exception {
    RayObjectImpl object = (RayObjectImpl) toRead;
    object.readObject(in);
  }

  public static List<ObjectId> getAndClearContainedObjectIds() {
    List<ObjectId> ids = new ArrayList<>();
    ids.addAll(innerIds.get());
    innerIds.get().clear();
    return ids;
  }
}
