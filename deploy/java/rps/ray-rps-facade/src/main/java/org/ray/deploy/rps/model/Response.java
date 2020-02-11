package org.ray.deploy.rps.model;

public abstract class Response {

  public Integer errorCode;
  public String errorMsg;

  public Integer getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(Integer errorCode) {
    this.errorCode = errorCode;
  }

  public String getErrorMsg() {
    return errorMsg;
  }

  public void setErrorMsg(String errorMsg) {
    this.errorMsg = errorMsg;
  }

  @Override
  public String toString() {
    return "Response{" +
        "errorCode=" + errorCode +
        ", errorMsg='" + errorMsg + '\'' +
        '}';
  }
}
