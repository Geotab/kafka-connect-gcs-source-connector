package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.pubsub;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import lombok.Data;

@Data
@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationItem {
  private String kind;
  private String selfLink;
  private String id;
  private String topic;
  private String etag;
  @JsonProperty("payload_format")
  private String payloadFormat;
  @JsonProperty("event_types")
  private List<String> eventTypes;
}
