package io.ray.streaming.runtime.message;

import com.google.common.base.MoreObjects;
import java.io.Serializable;

public class CallResult<T> implements Serializable {

  protected T resultObj;
  private boolean success;
  private int resultCode;
  private String resultMsg;

  public CallResult() {}

  public CallResult(boolean success, int resultCode, String resultMsg, T resultObj) {
    this.success = success;
    this.resultCode = resultCode;
    this.resultMsg = resultMsg;
    this.resultObj = resultObj;
  }

  public static <T> CallResult<T> success() {
    return new CallResult<>(true, CallResultEnum.SUCCESS.code, CallResultEnum.SUCCESS.msg, null);
  }

  public static <T> CallResult<T> success(T payload) {
    return new CallResult<>(true, CallResultEnum.SUCCESS.code, CallResultEnum.SUCCESS.msg, payload);
  }

  public static <T> CallResult<T> skipped(String msg) {
    return new CallResult<>(true, CallResultEnum.SKIPPED.code, msg, null);
  }

  public static <T> CallResult<T> fail() {
    return new CallResult<>(false, CallResultEnum.FAILED.code, CallResultEnum.FAILED.msg, null);
  }

  public static <T> CallResult<T> fail(T payload) {
    return new CallResult<>(false, CallResultEnum.FAILED.code, CallResultEnum.FAILED.msg, payload);
  }

  public static <T> CallResult<T> fail(String msg) {
    return new CallResult<>(false, CallResultEnum.FAILED.code, msg, null);
  }

  public static <T> CallResult<T> fail(CallResultEnum resultEnum, T payload) {
    return new CallResult<>(false, resultEnum.code, resultEnum.msg, payload);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("resultObj", resultObj)
        .add("success", success)
        .add("resultCode", resultCode)
        .add("resultMsg", resultMsg)
        .toString();
  }

  public boolean isSuccess() {
    return this.success;
  }

  public void setSuccess(boolean success) {
    this.success = success;
  }

  public int getResultCode() {
    return this.resultCode;
  }

  public void setResultCode(int resultCode) {
    this.resultCode = resultCode;
  }

  public CallResultEnum getResultEnum() {
    return CallResultEnum.getEnum(this.resultCode);
  }

  public String getResultMsg() {
    return this.resultMsg;
  }

  public void setResultMsg(String resultMsg) {
    this.resultMsg = resultMsg;
  }

  public T getResultObj() {
    return this.resultObj;
  }

  public void setResultObj(T resultObj) {
    this.resultObj = resultObj;
  }

  public enum CallResultEnum implements Serializable {
    /** call result enum */
    SUCCESS(0, "SUCCESS"),
    FAILED(1, "FAILED"),
    SKIPPED(2, "SKIPPED");

    public final int code;
    public final String msg;

    CallResultEnum(int code, String msg) {
      this.code = code;
      this.msg = msg;
    }

    public static CallResultEnum getEnum(int code) {
      for (CallResultEnum value : CallResultEnum.values()) {
        if (code == value.code) {
          return value;
        }
      }
      return FAILED;
    }
  }
}
