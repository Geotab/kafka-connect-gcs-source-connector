package com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.pubsub;

import java.util.List;
import lombok.Data;

@Data
public class BucketNotification {
  private String kind;
  private List<NotificationItem> items;

  public boolean hasThisTopic(String topicName) {
    if (null == items || items.isEmpty()) {
      return false;
    }

    for (NotificationItem item : items) {
      if (item.getTopic().contains(topicName)) {
        return true;
      }
    }

    return false;
  }
}
