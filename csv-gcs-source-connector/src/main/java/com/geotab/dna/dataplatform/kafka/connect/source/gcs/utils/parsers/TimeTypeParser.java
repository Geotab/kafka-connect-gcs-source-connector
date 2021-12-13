package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;


import com.fasterxml.jackson.databind.JsonNode;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;

/**
 * modify for time logicalType support
 */
public class TimeTypeParser implements TypeParser {


  public TimeTypeParser() {
  }

  @Override
  public Object parseString(String s, Schema schema) {
    long time = Long.parseLong(s);
    return new Date(time);
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