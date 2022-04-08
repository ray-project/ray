package io.ray.serve;

import org.apache.hc.core5.http.Header;

public class HttpResponse {

  private Header[] headers;

  private byte[] content;

  public Header[] getHeaders() {
    return headers;
  }

  public void setHeaders(Header[] headers) {
    this.headers = headers;
  }

  public byte[] getContent() {
    return content;
  }

  public HttpResponse setContent(byte[] content) {
    this.content = content;
    return this;
  }
}
