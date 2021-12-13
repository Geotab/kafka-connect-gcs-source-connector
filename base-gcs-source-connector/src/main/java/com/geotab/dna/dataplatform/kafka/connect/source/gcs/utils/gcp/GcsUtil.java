package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.gcp;

import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.ARCHIVE_BUCKET_CREATE_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.BUCKET_CREATE_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.ERROR_BUCKET_CREATE_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.GCS_ARCHIVE_BUCKET_NAME;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.GCS_BUCKET_NAME;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.GCS_ERROR_BUCKET_NAME;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PROJECT_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PROJECT_LOCATION;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_TOPIC_NAME;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.exceptions.GcpResourceException;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.pubsub.BucketNotification;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.pubsub.BucketNotificationConfig;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import java.net.URI;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class GcsUtil {

  private final String gcpStorageApiHost = "storage.googleapis.com";
  private final String apiResource = "/storage/v1/b/{BUCKET_NAME}/notificationConfigs";
  private final String bucketNameReplacement = "{BUCKET_NAME}";

  private final String notificationTopic = "projects/{PROJECT_ID}/topics/{TOPIC_NAME}";
  private final String projectIdReplacement = "{PROJECT_ID}";
  private final String pubsubTopicNameReplacement = "{TOPIC_NAME}";

  private final CredentialProvider credentialProvider;
  private final AbstractConfig config;
  private final ObjectMapper objectMapper;
  private final GcpCommonUtil gcpCommonUtil;

  public GcsUtil(CredentialProvider credentialProvider, AbstractConfig config, ObjectMapper objectMapper, GcpCommonUtil gcpCommonUtil) {
    this.credentialProvider = credentialProvider;
    this.config = config;
    this.objectMapper = objectMapper;
    this.gcpCommonUtil = gcpCommonUtil;
  }

  private void createBucket(String bucketName) {
    String bucketLocation = this.config.getString(PROJECT_LOCATION);
    Storage storage = this.gcpCommonUtil.getGcsStorage();
    BucketInfo bucketInfo = BucketInfo.newBuilder(bucketName).setLocation(bucketLocation).build();
    Bucket createdBucket = storage.create(bucketInfo);
    if (!createdBucket.exists()) {
      throw new GcpResourceException(String.format("Bucket %s doesn't create successfully", bucketName));
    }
  }

  public void createIfNotExitsGcsBucket() {
    boolean autoCreateBucket = this.config.getBoolean(BUCKET_CREATE_CONFIG);
    if (!autoCreateBucket) {
      log.info("Auto bucket creation is false, please create the bucket manually");
      return;
    }
    String bucketName = this.config.getString(GCS_BUCKET_NAME);
    Storage storage = this.gcpCommonUtil.getGcsStorage();
    if (null != storage.get(bucketName)) {
      log.info("bucket {} is existed, doesn't need to create", bucketName);
      return;
    }

    log.info("Bucket {} not existed, will create one", bucketName);
    this.createBucket(bucketName);

    log.info("Bucket {} has been created", bucketName);
  }

  public void createIfNotExistsGcsErrorBucket() {
    boolean autoCreateErrorBucket = this.config.getBoolean(ERROR_BUCKET_CREATE_CONFIG);
    if (!autoCreateErrorBucket) {
      log.info("Auto error bucket creation is false, please create the error bucket manually");
      return;
    }

    String errorBucketName = this.config.getString(GCS_ERROR_BUCKET_NAME);
    Storage storage = this.gcpCommonUtil.getGcsStorage();
    if (null != storage.get(errorBucketName)) {
      log.info("error bucket {} is existed, doesn't need to create", errorBucketName);
      return;
    }

    log.info("Error bucket {} not existed, will create one", errorBucketName);

    this.createBucket(errorBucketName);
    log.info("Error bucket {} has been created", errorBucketName);

  }

  public void createIfNotExistsGcsArchiveBucket() {
    boolean autoCreateArchiveBucket = this.config.getBoolean(ARCHIVE_BUCKET_CREATE_CONFIG);
    if (!autoCreateArchiveBucket) {
      log.info("Auto archive bucket creation is false, please create the archive bucket manually");
      return;
    }

    String archiveBucketName = this.config.getString(GCS_ARCHIVE_BUCKET_NAME);
    Storage storage = this.gcpCommonUtil.getGcsStorage();
    if (null != storage.get(archiveBucketName)) {
      log.info("archive bucket {} is existed, doesn't need to create", archiveBucketName);
      return;
    }

    log.info("Archive bucket {} not existed, will create one", archiveBucketName);
    this.createBucket(archiveBucketName);
    log.info("Archive bucket {} has been created", archiveBucketName);
  }

  public void createPubSubNotification() {
    boolean autoCreateNotification = this.config.getBoolean(BUCKET_NOTIFICATION_TO_PUB_SUB_CREATE_CONFIG);
    if (!autoCreateNotification) {
      log.info("auto create bucket notification is false, please create it manually");
      return;
    }

    String bucketName = this.config.getString(GCS_BUCKET_NAME);
    String bucketResource = this.apiResource.replace(this.bucketNameReplacement, bucketName);
    if (hasBucketNotification(bucketResource)) {
      log.info("bucket {} has notification, doesn't need to create it", bucketName);
      return;
    }

    String pubsubTopicName = this.config.getString(PUB_SUB_TOPIC_NAME);
    String projectId = this.config.getString(PROJECT_CONFIG);
    BucketNotificationConfig bucketNotificationConfig = BucketNotificationConfig.builder().topic(this.notificationTopic
        .replace(this.pubsubTopicNameReplacement, pubsubTopicName)
        .replace(this.projectIdReplacement, projectId)).build();


    GoogleCredentials googleCredentials = this.credentialProvider.getCredentials();

    try (CloseableHttpClient client = HttpClients.createDefault()) {
      URI uri = new URIBuilder().setScheme("https").setHost(this.gcpStorageApiHost).setPath(bucketResource).build();
      HttpPost httpPost = new HttpPost(uri);

      googleCredentials.refresh();
      httpPost.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");
      httpPost.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + googleCredentials.getAccessToken().getTokenValue());

      String entityJson = this.objectMapper.writeValueAsString(bucketNotificationConfig);
      httpPost.setEntity(new StringEntity(entityJson));

      CloseableHttpResponse response = client.execute(httpPost);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        log.error("cannot create the bucket notification due to {}", response.getStatusLine().getReasonPhrase());
        throw new GcpResourceException(response.getStatusLine().getReasonPhrase());
      }

    } catch (Exception e) {
      throw new GcpResourceException(e.getMessage(), e.getCause());
    }

    log.info("created the bucket {} notification successfully", bucketName);
  }

  private boolean hasBucketNotification(String bucketResource) {
    GoogleCredentials googleCredentials = this.credentialProvider.getCredentials();
    try (CloseableHttpClient client = HttpClients.createDefault()) {
      URI uri = new URIBuilder().setScheme("https").setHost(this.gcpStorageApiHost).setPath(bucketResource).build();
      HttpGet httpGet = new HttpGet(uri);
      httpGet.setHeader(HttpHeaders.CONTENT_TYPE, "application/json");

      googleCredentials.refresh();
      httpGet.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + googleCredentials.getAccessToken().getTokenValue());

      CloseableHttpResponse response = client.execute(httpGet);
      if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK) {
        log.error("cannot get the bucket notification due to {}", response.getStatusLine().getReasonPhrase());
        throw new GcpResourceException(response.getStatusLine().getReasonPhrase());
      }

      BucketNotification bucketNotification = this.objectMapper.readValue(EntityUtils.toString(response.getEntity()), BucketNotification.class);
      if (null == bucketNotification.getItems() || bucketNotification.getItems().isEmpty()) {
        return false;
      }

      return bucketNotification.hasThisTopic(this.config.getString(PUB_SUB_TOPIC_NAME));

    } catch (Exception e) {
      throw new GcpResourceException(e.getMessage(), e.getCause());
    }

  }

  public boolean doesBucketExist(String bucketName) {
    Storage storage = this.gcpCommonUtil.getGcsStorage();
    Bucket res = storage.get(bucketName);
    if (res == null) {
      log.info("Bucket {} does not exist.", bucketName);
      return false;
    }
    return true;
  }

}
