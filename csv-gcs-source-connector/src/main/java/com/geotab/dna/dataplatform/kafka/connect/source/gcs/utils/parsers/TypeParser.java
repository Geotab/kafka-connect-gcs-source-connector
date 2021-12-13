package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.connect.data.Schema;

public interface TypeParser {
  /**
   * Method is used to parse a String to an object representation of a Kafka Connect Type
   *
   * @param s      input string to parseString
   * @param schema Schema to parse the JsonNode for.
   * @return Object representation of the Kafka Connect Type
   */
  Object parseString(String s, Schema schema);

  /**
   * Method is used to return the expected class for the conversion. This is mainly used for
   * error messages when a type cannot be parsed.
   *
   * @return Class the parser will return.
   */
  Class<?> expectedClass();

  /**
   * Method is used to parse a JsonNode to an object representation of a Kafka Connect Type.
   *
   * @param input  JsonNode containing the value to be parsed.
   * @param schema Schema to parse the JsonNode for.
   * @return Object representation of the Kafka Connect Type
   */
  Object parseJsonNode(JsonNode input, Schema schema);
}
