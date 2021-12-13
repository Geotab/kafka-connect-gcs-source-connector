package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

public class Int32TypeParser implements TypeParser {
  public Int32TypeParser() {
  }

  public Object parseString(String s, Schema schema) {
    return Integer.parseInt(s);
  }

  public Class<?> expectedClass() {
    return Integer.class;
  }

  public Object parseJsonNode(JsonNode input, Schema schema) {
    Object result;
    if (input.isNumber()) {
      result = input.intValue();
    } else {
      if (!input.isTextual()) {
        throw new UnsupportedOperationException(String.format("Could not parse '%s' to %s", input, this.expectedClass().getSimpleName()));
      }

      result = this.parseString(input.textValue(), schema);
    }

    return result;
  }
}