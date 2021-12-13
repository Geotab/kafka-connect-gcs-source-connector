package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

public class BooleanParser implements TypeParser {
  public BooleanParser() {
  }

  public Object parseString(String s, Schema schema) {
    return Boolean.parseBoolean(s);
  }

  public Class<?> expectedClass() {
    return Boolean.class;
  }

  public Object parseJsonNode(JsonNode input, Schema schema) {
    Object result;
    if (input.isBoolean()) {
      result = input.booleanValue();
    } else {
      if (!input.isTextual()) {
        throw new UnsupportedOperationException(String.format("Could not parse '%s' to %s", input, this.expectedClass().getSimpleName()));
      }

      result = this.parseString(input.textValue(), schema);
    }

    return result;
  }
}
