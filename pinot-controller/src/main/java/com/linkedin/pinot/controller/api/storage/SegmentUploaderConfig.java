package com.linkedin.pinot.controller.api.storage;

import javax.ws.rs.core.HttpHeaders;
import org.glassfish.grizzly.http.server.Request;


/**
 * Wrapper class that encapsulates all parameters sent by clients during segment upload
 */
public class SegmentUploaderConfig {
  private final HttpHeaders _headers;
  private final Request _request;


  private SegmentUploaderConfig(HttpHeaders headers, Request request) {
    _headers = headers;
    _request = request;
  }

  public HttpHeaders getHeaders() {
    return _headers;
  }

  public Request getRequest() {
    return _request;
  }

  public static class SegmentUploaderConfigBuilder {
    private HttpHeaders _headers;
    private Request _request;

    private SegmentUploaderConfigBuilder() {
    }

    public SegmentUploaderConfigBuilder setHeaders(HttpHeaders headers) {
      _headers = headers;
      return this;
    }

    public SegmentUploaderConfigBuilder setRequest(Request request) {
      _request = request;
      return this;
    }

    public SegmentUploaderConfig build() {
      return new SegmentUploaderConfig(_headers, _request);
    }
  }
}
