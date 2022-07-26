package io.ray.serve;

import org.apache.hc.core5.http.Header;

public class HttpRequest {

  private Header[] headers;

  private byte[] content;

  private String path;

  public Header[] getHeaders() {
    return headers;
  }

  public void setHeaders(Header[] headers) {
    this.headers = headers;
  }

  public byte[] getContent() {
    return content;
  }

  public void setContent(byte[] content) {
    this.content = content;
  }

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }
}
