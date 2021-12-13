package com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner;

import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubConfig.PUB_SUB_MESSAGE_MAX_NUMBER;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.AsyncPubSubConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.google.api.gax.batching.FlowControlSettings;
import com.google.api.gax.core.ExecutorProvider;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.InstantiatingExecutorProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.threeten.bp.Duration;

@Slf4j
public class AsyncPubSubScanner extends PubSubScanner {
  private AsyncPubSubConfig config;
  private String projectID;
  private String subscriptionName;
  private MessageReceiver receiver;
  private CredentialProvider credentialProvider;
  private Subscriber subscriber;
  private List<FileMetaData> scanned;
  private Map<String, AckReplyConsumer> messageAckMap;
  //TODO use locks or some different way
  private AtomicBoolean lockBoolean;

  public static void main(String[] args) {

    AsyncPubSubScanner pubSubDetection = new AsyncPubSubScanner();
    try {
      pubSubDetection.startPoll();
      pubSubDetection.subscriber.awaitTerminated(30, TimeUnit.SECONDS);
    } catch (TimeoutException timeoutException) {
      System.out.println("time out");
      pubSubDetection.stopPoll();
    }
  }

  @Override
  public void ackMessage(FileMetaData fileMetaData) {
    String messageID = fileMetaData.getFileID();
    AckReplyConsumer consumer = this.messageAckMap.get(messageID);
    consumer.ack();
    this.messageAckMap.remove(messageID);
  }

  @Override
  public void nackMessage(FileMetaData fileMetaData) {
    String messageID = fileMetaData.getFileID();
    AckReplyConsumer consumer = this.messageAckMap.get(messageID);
    consumer.nack();
    this.messageAckMap.remove(messageID);
  }

  @Override
  public void configure(Map<String, String> originalConfigs) {

    config = new AsyncPubSubConfig(originalConfigs);
    this.credentialProvider = CredentialProvider.getCredentialProvider(this.config);
    this.projectID = config.getString(PubSubGcsSourceConnectorConfig.PROJECT_CONFIG);
    this.subscriptionName = config.getString(PubSubGcsSourceConnectorConfig.PUB_SUB_TOPIC_SUBSCRIPTION_NAME);
    log.info("configuring with project id {} and subscription name {}", projectID, subscriptionName);
    super.configure(this.projectID, this.subscriptionName, config.getString(AsyncPubSubConfig.PUB_SUB_MESSAGE_FORMAT));
    FlowControlSettings flowControlSettings =
        FlowControlSettings.newBuilder().setMaxOutstandingElementCount(this.config.getLong(PUB_SUB_MESSAGE_MAX_NUMBER)).build();
    scanned = new CopyOnWriteArrayList<>();
    messageAckMap = new ConcurrentHashMap<>();
    lockBoolean = new AtomicBoolean();
    lockBoolean.set(false);
    this.receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
      // Handle incoming message, then ack the received message.

      try {
        FileMetaData e = parseMessage(message);
        lockBoolean.set(true);
        scanned.add(e);
        messageAckMap.put(message.getMessageId(), consumer);
        lockBoolean.set(false);
      } catch (URISyntaxException | UnsupportedEncodingException e) {
        log.error("exception when parse the message from pub-sub {} {}", e.getMessage(), e.getCause());
      }
    };
    ExecutorProvider executorProvider =
        InstantiatingExecutorProvider.newBuilder().setExecutorThreadCount(this.config.getInt(AsyncPubSubConfig.ASYNC_PULL_THREAD_POOL)).build();
    subscriber = Subscriber.newBuilder(super.getSubscriptionName(), this.receiver)
        .setCredentialsProvider(FixedCredentialsProvider.create(this.credentialProvider.getCredentials()))
        .setParallelPullCount(this.config.getInt(AsyncPubSubConfig.ASYNC_PULL_PARALLEL_PULL)).setExecutorProvider(executorProvider)
        .setFlowControlSettings(flowControlSettings).setMaxAckExtensionPeriod(Duration.ofSeconds(1200L)).build();
  }

  @Override
  public List<FileMetaData> poll() {
    /*
        TODO check thread safety of scanned set. if there are chances of scanned being modified when getFiles is running use synchronized or some
            atomic boolean based on test      */
    if (!lockBoolean.get() && this.scanned.size() > 0) {
      List<FileMetaData> ret = this.scanned;
      this.scanned = new CopyOnWriteArrayList<>();
      log.info("async scanner returning {} records", ret.size());
      return ret;
    }
    return null;
  }

  @Override
  public void startPoll() {
    this.subscriber.startAsync().awaitRunning();
  }

  @Override
  public void stopPoll() {
    this.subscriber.stopAsync();
  }
}
