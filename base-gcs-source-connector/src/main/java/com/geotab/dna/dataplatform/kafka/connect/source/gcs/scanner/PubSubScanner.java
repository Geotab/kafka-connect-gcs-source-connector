package com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Getter
@Slf4j
public abstract class PubSubScanner implements BaseScanner {
  private static final String UPDATED_COLUMN_DEFAULT = "updated";
  private static final String BUCKET_COLUMN_DEFAULT = "bucket";
  private static final String NAME_COLUMN_DEFAULT = "name";
  private static final String SIZE_COLUMN_DEFAULT = "size";
  private static String UPDATED_COLUMN;
  private static String BUCKET_COLUMN;
  private static String NAME_COLUMN;
  private static String SIZE_COLUMN;
  private final HashSet<String> metadataAvoidList = new HashSet<>();
  DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz");
  private ProjectSubscriptionName subscriptionName;
  private boolean deserilize = true;

  protected void configure(String projectId, String subscriptionId, String messageFormat) {
    subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
    switch (messageFormat) {
      case "GCSPUBSUB":
        UPDATED_COLUMN = UPDATED_COLUMN_DEFAULT;
        BUCKET_COLUMN = BUCKET_COLUMN_DEFAULT;
        NAME_COLUMN = NAME_COLUMN_DEFAULT;
        SIZE_COLUMN = SIZE_COLUMN_DEFAULT;
        break;
      case "PROTOBUF":
        deserilize = false;
        break;
      default:
        JsonObject jsonMessageFormat = JsonParser.parseString(messageFormat).getAsJsonObject();
        UPDATED_COLUMN = jsonMessageFormat.get(UPDATED_COLUMN_DEFAULT).getAsString();
        BUCKET_COLUMN = jsonMessageFormat.get(BUCKET_COLUMN_DEFAULT).getAsString();
        NAME_COLUMN = jsonMessageFormat.get(NAME_COLUMN_DEFAULT).getAsString();
        SIZE_COLUMN = jsonMessageFormat.get(SIZE_COLUMN_DEFAULT).getAsString();
        break;
    }
    metadataAvoidList.add(UPDATED_COLUMN);
    metadataAvoidList.add(BUCKET_COLUMN);
    metadataAvoidList.add(NAME_COLUMN);
    metadataAvoidList.add(SIZE_COLUMN);

  }

  protected FileMetaData parseMessage(PubsubMessage message) throws URISyntaxException, UnsupportedEncodingException {
//    io.confluent.kafka.serializers.subject.SubjectNameStrategy
    if (!deserilize) {
      return FileMetaData.builder().fileID(message.getMessageId()).data(message.getData().toByteArray()).build();
    }
    JsonObject messageData = JsonParser.parseString(message.getData().toStringUtf8()).getAsJsonObject();
    ZonedDateTime updatedTime = ZonedDateTime.parse(messageData.get(UPDATED_COLUMN).getAsString(), this.formatter);
    Map<String, Object> attributeList = message.getAttributesMap().entrySet()
        .stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    attributeList.putAll(messageData.entrySet().stream().filter(entry -> metadataAvoidList.contains(entry.getKey()))
        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    return FileMetaData.builder().fileID(message.getMessageId()).bucket(messageData.get(BUCKET_COLUMN).getAsString())
        .lastModified(updatedTime.toEpochSecond())
        .path(messageData.get(NAME_COLUMN).getAsString()).contentLength(messageData.get(SIZE_COLUMN).getAsLong()).userDefinedMetadata(attributeList)
        .pulledAtEpochTime(System.currentTimeMillis()).build();
  }

}
