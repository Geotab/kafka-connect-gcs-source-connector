package com.geotab.dna.dataplatform.kafka.connect.source.gcs.config;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.ConfigKeyBuilder;
import com.opencsv.CSVParser;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;

public class CSVBlobReaderConfig extends BlobReaderConfig {
  public static final String CSV_SKIP_LINES_CONF = "csv.skip.lines";
  public static final String CSV_SEPARATOR_CHAR_CONF = "csv.separator.char";
  public static final String CSV_QUOTE_CHAR_CONF = "csv.quote.char";
  public static final String CSV_IGNORE_QUOTATIONS_CONF = "csv.ignore.quotations";
  public static final String CSV_BATCH_SIZE = "csv.batch.size";
  static final String CSV_SKIP_LINES_DOC = "Number of lines to skip in the beginning of the file.";
  static final String CSV_SEPARATOR_CHAR_DOC = "The character that separates each field in the form " +
      "of an integer. Typically in a CSV this is a ,(44) character. A TSV would use a tab(9) character.";
  static final String CSV_QUOTE_CHAR_DOC =
      "The character that is used to quote a field. This typically happens when the " + CSV_SEPARATOR_CHAR_CONF + " character is within the data.";

  static final String CSV_IGNORE_QUOTATIONS_DOC = "Sets the ignore quotations mode - if true, quotations are ignored.";
  static final String CSV_BATCH_SIZE_DOC = "Batch size of the record list processed each time";

  static final int CSV_SKIP_LINES_DEFAULT = 1;
  static final String CSV_SEPARATOR_CHAR_DEFAULT = String.valueOf(CSVParser.DEFAULT_SEPARATOR);
  static final String CSV_QUOTE_CHAR_DEFAULT = String.valueOf(CSVParser.DEFAULT_QUOTE_CHARACTER);
  static final int CSV_BATCH_SIZE_DEFAULT = 5;
  static final boolean CSV_IGNORE_QUOTATIONS_DEFAULT = CSVParser.DEFAULT_IGNORE_QUOTATIONS;


  static final String CSV_GROUP = "CSV Parsing";

  public CSVBlobReaderConfig(ConfigDef configDef, Map<String, String> originalProperties) {
    super(configDef, originalProperties);
  }

  public CSVBlobReaderConfig(Map<String, String> originalProperties) {
    this(addFileProcessConfig(), originalProperties);
  }

  static ConfigDef addFileProcessConfig() {
    ConfigDef configDef = new ConfigDef();
    configDef.define(
        ConfigKeyBuilder.of(CSV_SKIP_LINES_CONF, ConfigDef.Type.INT)
            .defaultValue(CSV_SKIP_LINES_DEFAULT)
            .importance(ConfigDef.Importance.HIGH)
            .documentation(CSV_SKIP_LINES_DOC)
            .group(CSV_GROUP)
            .width(ConfigDef.Width.LONG)
            .build()
    );
    configDef.define(ConfigKeyBuilder.of(CSV_SEPARATOR_CHAR_CONF, ConfigDef.Type.STRING)
        .defaultValue(CSV_SEPARATOR_CHAR_DEFAULT)
        .importance(ConfigDef.Importance.HIGH)
        .documentation(CSV_SEPARATOR_CHAR_DOC)
        .group(CSV_GROUP)
        .build());
    configDef.define(ConfigKeyBuilder.of(CSV_QUOTE_CHAR_CONF, ConfigDef.Type.STRING)
        .defaultValue(CSV_QUOTE_CHAR_DEFAULT)
        .importance(ConfigDef.Importance.HIGH)
        .documentation(CSV_QUOTE_CHAR_DOC)
        .group(CSV_GROUP)
        .build());
    configDef.define(ConfigKeyBuilder.of(CSV_IGNORE_QUOTATIONS_CONF, ConfigDef.Type.BOOLEAN)
        .defaultValue(CSV_IGNORE_QUOTATIONS_DEFAULT)
        .importance(ConfigDef.Importance.LOW)
        .documentation(CSV_IGNORE_QUOTATIONS_DOC)
        .group(CSV_GROUP)
        .build());
    configDef.define(ConfigKeyBuilder.of(CSV_BATCH_SIZE, ConfigDef.Type.INT)
        .defaultValue(CSV_BATCH_SIZE_DEFAULT)
        .importance(ConfigDef.Importance.HIGH)
        .documentation(CSV_BATCH_SIZE_DOC)
        .group(CSV_GROUP)
        .width(ConfigDef.Width.LONG)
        .build());
    return configDef;
  }

  //TODO: validator
  public static void validate(Map<String, String> props) {

  }
}
