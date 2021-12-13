package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.DataException;

public class DecimalTypeParser implements TypeParser {
  static final String NOT_FOUND_MESSAGE = String.format("Invalid Decimal schema: %s parameter not found.", "scale");
  static final String NOT_PARSABLE_MESSAGE = String.format("Invalid Decimal schema: %s parameter could not be converted to an integer.", "scale");
  final Cache<Schema, Integer> schemaCache;

  public DecimalTypeParser() {
    this.schemaCache = CacheBuilder.newBuilder().expireAfterWrite(60L, TimeUnit.SECONDS).build();
  }

  private static int scaleInternal(Schema schema) {
    if (null == schema.parameters()) {
      throw new DataException(NOT_FOUND_MESSAGE);
    } else {
      String scaleString = schema.parameters().get("scale");
      if (scaleString == null) {
        throw new DataException(NOT_FOUND_MESSAGE);
      } else {
        try {
          return Integer.parseInt(scaleString);
        } catch (NumberFormatException var3) {
          throw new DataException(NOT_PARSABLE_MESSAGE, var3);
        }
      }
    }
  }

  int scale(Schema schema) {
    try {
      int scale = this.schemaCache.get(schema, () -> {
        return scaleInternal(schema);
      });
      return scale;
    } catch (Exception var4) {
      throw new DataException(var4);
    }
  }

  public Object parseString(String s, Schema schema) {
    int scale = this.scale(schema);
    return (new BigDecimal(s)).setScale(scale);
  }

  public Class<?> expectedClass() {
    return BigDecimal.class;
  }

  public Object parseJsonNode(JsonNode input, Schema schema) {
    Object result;
    if (input.isNumber()) {
      int scale = this.scale(schema);
      result = input.decimalValue().setScale(scale);
    } else {
      if (!input.isTextual()) {
        throw new UnsupportedOperationException(String.format("Could not parse '%s' to %s", input, this.expectedClass().getSimpleName()));
      }

      result = this.parseString(input.textValue(), schema);
    }

    return result;
  }
}
