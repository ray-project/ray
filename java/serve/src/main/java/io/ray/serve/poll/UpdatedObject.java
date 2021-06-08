package io.ray.serve.poll;

import java.io.Serializable;

/** The updated object that long poll client received. */
public class UpdatedObject implements Serializable {

  private static final long serialVersionUID = 6245682414826079438L;

  private Object objectSnapshot;

  /**
   * The identifier for the object's version. There is not sequential relation among different
   * object's snapshot_ids.
   */
  private int snapshotId;

  public Object getObjectSnapshot() {
    return objectSnapshot;
  }

  public void setObjectSnapshot(Object objectSnapshot) {
    this.objectSnapshot = objectSnapshot;
  }

  public int getSnapshotId() {
    return snapshotId;
  }

  public void setSnapshotId(int snapshotId) {
    this.snapshotId = snapshotId;
  }
}
