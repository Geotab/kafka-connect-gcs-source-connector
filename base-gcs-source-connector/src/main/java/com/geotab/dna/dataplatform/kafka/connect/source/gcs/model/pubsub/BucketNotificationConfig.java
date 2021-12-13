package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.pubsub;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Collections;
import java.util.List;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class BucketNotificationConfig {

  private String topic;

  @Builder.Default
  @JsonProperty("payload_format")
  private String payloadFormat = "JSON_API_V1";

  @Builder.Default
  @JsonProperty("event_types")
  private List<String> eventTypes = Collections.singletonList(NotificationType.OBJECT_FINALIZE.name());
}
