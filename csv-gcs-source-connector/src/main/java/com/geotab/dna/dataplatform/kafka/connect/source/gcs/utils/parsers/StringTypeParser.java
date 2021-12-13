package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

public class StringTypeParser implements TypeParser {
  public StringTypeParser() {
  }

  public Object parseString(String s, Schema schema) {
    return s;
  }

  public Class<?> expectedClass() {
    return String.class;
  }

  public Object parseJsonNode(JsonNode input, Schema schema) {
    return input.textValue();
  }
}
