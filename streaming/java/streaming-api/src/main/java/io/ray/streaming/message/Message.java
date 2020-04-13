package io.ray.streaming.message;

import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;

public class Message implements Serializable {

  private int taskId;
  private long batchId;
  private String stream;
  private List<Record> recordList;

  public Message(int taskId, long batchId, String stream, List<Record> recordList) {
    this.taskId = taskId;
    this.batchId = batchId;
    this.stream = stream;
    this.recordList = recordList;
  }

  public Message(int taskId, long batchId, String stream, Record record) {
    this.taskId = taskId;
    this.batchId = batchId;
    this.stream = stream;
    this.recordList = Lists.newArrayList(record);
  }

  public int getTaskId() {
    return taskId;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public long getBatchId() {
    return batchId;
  }

  public void setBatchId(long batchId) {
    this.batchId = batchId;
  }

  public String getStream() {
    return stream;
  }

  public void setStream(String stream) {
    this.stream = stream;
  }

  public List<Record> getRecordList() {
    return recordList;
  }

  public void setRecordList(List<Record> recordList) {
    this.recordList = recordList;
  }

  public Record getRecord(int index) {
    return recordList.get(0);
  }

}
