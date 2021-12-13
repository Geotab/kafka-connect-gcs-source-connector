package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.kafka.connect.data.Schema;

class ParserKey implements Comparable<ParserKey> {
  public final Schema.Type type;
  public final String logicalName;

  ParserKey(Schema schema) {
    this(schema.type(), schema.name());
  }

  ParserKey(Schema.Type type, String logicalName) {
    this.type = type;
    this.logicalName = logicalName == null ? "" : logicalName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.type, this.logicalName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("type", this.type)
        .add("logicalName", this.logicalName)
        .toString();
  }


  @Override
  public int compareTo(ParserKey that) {
    if (null == that) {
      return 1;
    }
    return ComparisonChain.start()
        .compare(this.type, that.type)
        .compare(this.logicalName, that.logicalName)
        .result();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof ParserKey) {
      return compareTo((ParserKey) obj) == 0;
    } else {
      return false;
    }
  }
}
