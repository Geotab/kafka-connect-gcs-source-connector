package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

public class Int64TypeParser implements TypeParser {
  @Override
  public Object parseString(String s, final Schema schema) {
    return Long.parseLong(s);
  }

  @Override
  public Class<?> expectedClass() {
    return Long.class;
  }

  @Override
  public Object parseJsonNode(JsonNode input, Schema schema) {
    Object result;
    if (input.isNumber()) {
      result = input.longValue();
    } else if (input.isTextual()) {
      result = parseString(input.textValue(), schema);
    } else {
      throw new UnsupportedOperationException(
          String.format(
              "Could not parse '%s' to %s",
              input,
              this.expectedClass().getSimpleName()
          )
      );
    }
    return result;
  }
}
