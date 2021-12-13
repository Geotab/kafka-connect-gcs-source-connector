package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

public class Float64TypeParser implements TypeParser {
  @Override
  public Object parseString(String s, final Schema schema) {
    return Double.parseDouble(s);
  }

  @Override
  public Class<?> expectedClass() {
    return Double.class;
  }

  @Override
  public Object parseJsonNode(JsonNode input, Schema schema) {
    Object result;
    if (input.isNumber()) {
      result = input.doubleValue();
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
