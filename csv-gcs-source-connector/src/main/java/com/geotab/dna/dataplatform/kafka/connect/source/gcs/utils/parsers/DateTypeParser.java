package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;

/**
 * modify for date logicalType support
 */
public class DateTypeParser implements TypeParser {
  public DateTypeParser() {
  }


  @Override
  public Object parseString(String s, Schema schema) {
    int date = Integer.parseInt(s);
    long dateInMillions = TimeUnit.DAYS.toMillis(date);
    return new Date(dateInMillions);
  }

  @Override
  public Class<?> expectedClass() {
    return Date.class;
  }

  @Override
  public Object parseJsonNode(JsonNode input, Schema schema) {
    Date result;
    if (input.isNumber()) {
      int date = input.intValue();
      long dateInMillions = TimeUnit.DAYS.toMillis(date);
      result = new Date(dateInMillions);
    } else if (input.isTextual()) {
      result = (Date) parseString(input.textValue(), schema);
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

    return result;
  }
}
