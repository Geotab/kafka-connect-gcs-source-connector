package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;

/**
 * modify for timestamp logicalType support
 */
public class TimestampTypeParser implements TypeParser {

  public TimestampTypeParser() {
  }

  @Override
  public Object parseString(String s, Schema schema) {
    long timestamp = Long.parseLong(s);
    return new Date(timestamp);
  }

  @Override
  public Class<?> expectedClass() {
    return Date.class;
  }

  @Override
  public Object parseJsonNode(JsonNode input, Schema schema) {
    if (input.isNumber()) {
      return new Date(input.longValue());
    } else if (input.isTextual()) {
      return parseString(input.textValue(), schema);
    } else {
      throw new IllegalStateException(
          String.format(
              "NodeType:%s '%s' could not be converted to %s",
              input.getNodeType(),
              input.textValue(),
              expectedClass().getSimpleName()
          )
      );
    }
  }
}
