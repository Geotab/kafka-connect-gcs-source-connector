package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.exceptions;

public class GcpResourceException extends RuntimeException {

  public GcpResourceException(String message) {
    super(message);
  }

  public GcpResourceException(String message, Throwable cause) {
    super(message, cause);
  }
}
