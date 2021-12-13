package com.geotab.dna.dataplatform.kafka.connect.source.gcs.scanner;


import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubConfig.PROJECT_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubConfig.PUB_SUB_MESSAGE_FORMAT;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubConfig.PUB_SUB_TOPIC_SUBSCRIPTION_NAME;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.SyncPubSubConfig.PUB_SUB_MESSAGE_ACK_DEADLINE;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.SyncPubSubConfig.PUB_SUB_MESSAGE_MAX_NUMBER;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.SyncPubSubConfig.PUB_SUB_MESSAGE_MAX_SIZE;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.SyncPubSubConfig;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.FileMetaData;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.stub.GrpcSubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStub;
import com.google.cloud.pubsub.v1.stub.SubscriberStubSettings;
import com.google.pubsub.v1.AcknowledgeRequest;
import com.google.pubsub.v1.ModifyAckDeadlineRequest;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PullRequest;
import com.google.pubsub.v1.PullResponse;
import com.google.pubsub.v1.ReceivedMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.ConfigException;

@Slf4j
@Setter
public class SyncPubSubScanner extends PubSubScanner {

  private final int SUBSCRIBER_SHUTDOWN_WAITING_TIME = 30;
  protected String subscriptionName;
  private SyncPubSubConfig config;
  private CredentialProvider credentialProvider;
  private SubscriberStubSettings subscriberStubSettings;
  private SubscriberStub subscriber;
  private PullRequest pullRequest;
  private ConcurrentHashMap<String, String> messageAckMap;

  public void initializeSubscriptionName(Map<String, String> originalConfigs) {
    this.subscriptionName = this.config.getString(PUB_SUB_TOPIC_SUBSCRIPTION_NAME);
  }

  @Override
  public void configure(Map<String, String> originalConfigs) {
    log.info("configuring Sync PubSub Scanner with properties {}", originalConfigs.toString());
    messageAckMap = new ConcurrentHashMap<>();
    config = new SyncPubSubConfig(originalConfigs);
    this.initializeSubscriptionName(originalConfigs);
    this.credentialProvider = CredentialProvider.getCredentialProvider(this.config);
    GoogleCredentials googleCredentials = this.credentialProvider.getCredentials();
    String projectID = this.config.getString(PROJECT_CONFIG);
    log.info("configuring with project id {} and subscription name {}", projectID, subscriptionName);
    super.configure(projectID, this.subscriptionName, config.getString(PUB_SUB_MESSAGE_FORMAT));
    subscriptionName = ProjectSubscriptionName.format(projectID, this.subscriptionName);

    try {
      subscriberStubSettings = SubscriberStubSettings.newBuilder()
          .setTransportChannelProvider(
              SubscriberStubSettings.defaultGrpcTransportProviderBuilder()
                  .setMaxInboundMessageSize(this.config.getInt(PUB_SUB_MESSAGE_MAX_SIZE) * 1024 * 1024) // 20MB (maximum message size).
                  .build())
          .setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials))
          .build();
      this.subscriber = GrpcSubscriberStub.create(subscriberStubSettings);
      pullRequest =
          PullRequest.newBuilder().setMaxMessages(this.config.getLong(PUB_SUB_MESSAGE_MAX_NUMBER).intValue()).setSubscription(subscriptionName)
              .build();


    } catch (IOException e) {
      throw new ConfigException("could not construct pubsub sync scanner", e);
    }

  }

  public void increaseDeadLine(String messageAckId) {
    ModifyAckDeadlineRequest modifyAckDeadlineRequest =
        ModifyAckDeadlineRequest.newBuilder()
            .setSubscription(subscriptionName)
            .addAckIds(messageAckId)
            .setAckDeadlineSeconds(this.config.getInt(PUB_SUB_MESSAGE_ACK_DEADLINE))
            .build();
    this.subscriber.modifyAckDeadlineCallable().call(modifyAckDeadlineRequest);
  }

  @Override
  public List<FileMetaData> poll() {
    if (this.messageAckMap.size() > Math.ceil(this.config.getLong(PUB_SUB_MESSAGE_MAX_NUMBER).floatValue() / 2)) {
      return new ArrayList<>();
    }

    PullResponse pullResponse;
    try {
      pullResponse = subscriber.pullCallable().call(pullRequest);
    } catch (Exception e) {
      log.warn("cannot pull message from the pubsub due to {}, {}", e.getMessage(), e.getStackTrace());
      return new ArrayList<>();
    }

    log.debug(" the size of pull messages is {} and msgAckMap size is {}", pullResponse.getReceivedMessagesList().size(),
        this.messageAckMap.size());
    List<FileMetaData> fileMetaDataList = new LinkedList<>();
    for (ReceivedMessage message : pullResponse.getReceivedMessagesList()) {
      try {
        this.increaseDeadLine(message.getAckId());
        fileMetaDataList.add(this.parseMessage(message.getMessage()));
        this.messageAckMap.put(message.getMessage().getMessageId(), message.getAckId());
      } catch (Exception e) {
        log.error("cannot poll messages from pub sub with exception", e);
      }
    }
    return fileMetaDataList;
  }

  @Override
  public void startPoll() {
    log.info("we don't need to start for Sync Scanner");
  }

  @Override
  public void stopPoll() {
    this.subscriber.shutdownNow();
    try {
      this.subscriber.awaitTermination(SUBSCRIBER_SHUTDOWN_WAITING_TIME, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.warn("sync subscriber shutdown timeout after {}", this.SUBSCRIBER_SHUTDOWN_WAITING_TIME);
    }

    this.subscriber.close();
  }

  @Override
  public void ackMessage(FileMetaData fileMetaData) {
    // Acknowledge received messages.
    String messageID = fileMetaData.getFileID();
    String messageAckId = this.messageAckMap.get(messageID);
    AcknowledgeRequest acknowledgeRequest =
        AcknowledgeRequest.newBuilder()
            .setSubscription(subscriptionName)
            .addAckIds(messageAckId)
            .build();
    subscriber.acknowledgeCallable().call(acknowledgeRequest);
    this.messageAckMap.remove(messageID);
  }

  // note:modify the time ack deadline seconds to be 0 will stop pubsub subscriber from pulling msgs
  @Override
  public void nackMessage(FileMetaData fileMetaData) {
    String messageID = fileMetaData.getFileID();
    log.warn("nack message id is :{}, file name: {}, bucket: {}", messageID, fileMetaData.getPath(), fileMetaData.getBucket());
    String messageNackID = this.messageAckMap.get(messageID);
    ModifyAckDeadlineRequest modifyAckDeadlineRequest =
        ModifyAckDeadlineRequest.newBuilder()
            .setSubscription(subscriptionName)
            .addAckIds(messageNackID)
            .setAckDeadlineSeconds(30)
            .build();
    this.subscriber.modifyAckDeadlineCallable().call(modifyAckDeadlineRequest);
    this.messageAckMap.remove(messageNackID);
  }
}
