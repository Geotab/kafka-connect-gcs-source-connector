package com.geotab.dna.dataplatform.kafka.connect.source.gcs.reader;

import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.BlobReaderConfig.SCHEMA_SUBJECT;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.BlobReaderConfig.SCHEMA_SUBJECT_VERSION;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.BlobReaderConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.CSVBlobReaderConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems.FileSystem;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.filesystems.FileSystemService;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileSystemType;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.schema.SchemaRetriever;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.parsers.Parser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.opencsv.ICSVParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;

@Slf4j
public class CSVRowBlobReader extends RowBlobReader {
  public char separatorChar;
  public char quoteChar;
  public boolean ignoreQuotations;
  public int batchSize;
  String[] fieldNames;
  private ICSVParser csvParser;
  private Parser parser;
  private CSVBlobReaderConfig config;
  private FileSystem fileSystem;
  private FileSystemService fileSystemService;
  private CredentialProvider credentialProvider;
  private SchemaRetriever schemaRegistrySchemaRetriever;
  private int timeoutSeconds;
  private int skipLines;
  private boolean isSkipRecords;
  private boolean isSkipped;

  @Override
  public void configure(AbstractConfig config) {
    this.config = (CSVBlobReaderConfig) config;
    fileSystemService = new FileSystemService(this.config);
    this.fileSystem = fileSystemService.getFileSystem(FileSystemType.GCS);
    this.timeoutSeconds = this.config.getInt(BlobReaderConfig.FILE_PROCESS_TIMEOUT);
    this.skipLines = this.config.getInt(CSVBlobReaderConfig.CSV_SKIP_LINES_CONF);
    this.separatorChar = this.config.getString(CSVBlobReaderConfig.CSV_SEPARATOR_CHAR_CONF).charAt(0);
    this.quoteChar = this.config.getString(CSVBlobReaderConfig.CSV_QUOTE_CHAR_CONF).charAt(0);
    this.ignoreQuotations = this.config.getBoolean(CSVBlobReaderConfig.CSV_IGNORE_QUOTATIONS_CONF);
    this.batchSize = this.config.getInt(CSVBlobReaderConfig.CSV_BATCH_SIZE);
    this.csvParser = this.createCSVParserBuilder();
    this.parser = new Parser();
    this.isSkipRecords = this.config.getBoolean(CSVBlobReaderConfig.SKIP_BAD_RECORDS);
    this.isSkipped = false;
  }

  @Override
  public void setSchemaRetriever(SchemaRetriever schemaRetriever) {
    this.schemaRegistrySchemaRetriever = schemaRetriever;
  }

  public ICSVParser createCSVParserBuilder() {
    return new CSVParserBuilder()
        .withIgnoreQuotations(this.ignoreQuotations)
        .withQuoteChar(this.quoteChar)
        .withSeparator(this.separatorChar)
        .build();
  }

  @Override
  public Boolean isFileSkipped() {
    return this.isSkipped;
  }

  @Override
  public List<SourceRecord> processFile(FileMetaData fileMetaData) {
    List<SourceRecord> records = new ArrayList<>(this.batchSize);
    Schema schema =
        schemaRegistrySchemaRetriever.retrieveSchema(this.config.getString(SCHEMA_SUBJECT), this.config.getString(SCHEMA_SUBJECT_VERSION));
    fieldNames = new String[schema.fields().size()];
    schema.fields().stream().map(Field::name).collect(Collectors.toList()).toArray(fieldNames);
    //log.info("field length {} fieldnames{}", fieldNames.length, Arrays.toString(fieldNames));

    CSVReader reader;
    try (BufferedReader br = fileSystem.getBufferedReader(fileMetaData)) {
      if (br == null) {
        this.isSkipped = true;
        return new ArrayList<>();
      }
      reader = new CSVReaderBuilder(br).withSkipLines(this.skipLines).withCSVParser(csvParser).build();

      for (String[] row : reader) {
        if (row == null) {
          continue;
        }

        // to confirm -> what is a bad record
        // id, name. country, age
        // 1,a,b,c -> OK
        // 2 ,  b   ,c,d -> bad
        // 3,a,b -> missing column -> bad
        // 4,a,b,, -> more column -> bad

        if (row.length != fieldNames.length) {
          throw new RuntimeException(
              String.format("[Record fields not matched with the schema]: Error while parsing data for %s. linenumber = %d", Arrays.toString(row),
                  reader.getLinesRead() - this.skipLines));
        }

        Struct valueStruct = this.parseRow(reader, row, schema);

        Map<String, Object> offset = new HashMap<>();
        offset.put(this.config.getTopicName(), reader.getLinesRead() - this.skipLines);
        Map<String, ?> partition = Collections.singletonMap("filename", fileMetaData.getFileID());
        SourceRecord sourceRecord = new SourceRecord(partition, offset, this.config.getTopicName(), schema, valueStruct);
        records.add(sourceRecord);
      }
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e.getCause());
    }

    return records;
  }

  private Struct parseRow(CSVReader reader, String[] row, Schema schema) {
    Struct valueStruct = new Struct(schema);

    for (int i = 0; i < this.fieldNames.length; i++) {
      String fieldName = this.fieldNames[i];
      String input = row[i];
      try {
        Field field = schema.field(fieldName);
        if (null == field) {
          throw new RuntimeException(String.format("process() - Field %s is not defined in the schema.", fieldName));
        }

        Object fieldValue = this.parser.parseString(field.schema(), input);
        valueStruct.put(field, fieldValue);

      } catch (Exception ex) {
        String message =
            String.format("Exception thrown while parsing data for '%s'. linenumber=%s", fieldName, reader.getLinesRead() - this.skipLines);
        throw new DataException(message, ex);

      }
    }

    return valueStruct;
  }


}
