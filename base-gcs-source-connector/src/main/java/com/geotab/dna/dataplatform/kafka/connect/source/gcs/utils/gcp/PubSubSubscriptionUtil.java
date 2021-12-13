package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.gcp;

import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PROJECT_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_BACK_OFF_TIME;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_DEAD_LETTER_TOPIC_NAME;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_MAX_RETRY;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_MESSAGE_HOLDING_DEADLINE;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_TOPIC_NAME;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_TOPIC_SUBSCRIPTION_NAME;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.exceptions.GcpResourceException;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.rpc.NotFoundException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.storage.ServiceAccount;
import com.google.iam.v1.Binding;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.protobuf.Duration;
import com.google.pubsub.v1.DeadLetterPolicy;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PushConfig;
import com.google.pubsub.v1.RetryPolicy;
import com.google.pubsub.v1.Subscription;
import com.google.pubsub.v1.TopicName;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class PubSubSubscriptionUtil {

  private final AbstractConfig config;
  private final CredentialProvider credentialProvider;
  private final String projectId;
  private final boolean autoCreateSubscription;
  private final GcpCommonUtil gcpCommonUtil;
  private final String storageDomain = "gs-project-accounts";
  private final String pubSubDomain = "gcp-sa-pubsub";

  public PubSubSubscriptionUtil(AbstractConfig config, CredentialProvider credentialProvider, GcpCommonUtil gcpCommonUtil) {
    this.config = config;
    this.credentialProvider = credentialProvider;
    this.projectId = this.config.getString(PROJECT_CONFIG);
    this.autoCreateSubscription = this.config.getBoolean(PUB_SUB_TOPIC_SUBSCRIPTION_CREATE_CONFIG);
    this.gcpCommonUtil = gcpCommonUtil;
  }

  private SubscriptionAdminClient getSubscriptionClient() {
    GoogleCredentials googleCredentials = this.credentialProvider.getCredentials();

    try {
      SubscriptionAdminSettings settings = SubscriptionAdminSettings.newBuilder()
          .setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials)).build();
      return SubscriptionAdminClient.create(settings);
    } catch (IOException ioe) {
      throw new GcpResourceException(ioe.getMessage(), ioe.getCause());
    }

  }

  private boolean hasSubscription(ProjectSubscriptionName pubsubTopicSubscription) {
    try (SubscriptionAdminClient subscriptionAdminClient = this.getSubscriptionClient()) {
      Subscription existedPubSubSubs = subscriptionAdminClient.getSubscription(pubsubTopicSubscription);
      if (null != existedPubSubSubs) {
        log.info("subscription {} already exists, doesn't need to create it", pubsubTopicSubscription.getSubscription());
        return true;
      }
    } catch (NotFoundException nfe) {
      log.info("subscription {} is not existed, will create one", pubsubTopicSubscription.getSubscription());
    } catch (Exception e) {
      throw new GcpResourceException(e.getMessage(), e.getCause());
    }

    return false;

  }

  public void createIfNotExitsTopicSubscription() {
    if (!autoCreateSubscription) {
      log.info("auto create subscription is false, please create it manually");
      return;
    }

    String subscriptionName = this.config.getString(PUB_SUB_TOPIC_SUBSCRIPTION_NAME);
    ProjectSubscriptionName pubsubTopicSubscription = ProjectSubscriptionName.of(projectId, subscriptionName);

    if (this.hasSubscription(pubsubTopicSubscription)) {
      return;
    }
    SubscriptionAdminClient subscriptionAdminClient = this.getSubscriptionClient();
    // create a new subscription
    String pubsubTopicName = this.config.getString(PUB_SUB_TOPIC_NAME);
    TopicName pubsubTopic = TopicName.of(projectId, pubsubTopicName);

    String deadLetterTopicName = this.config.getString(PUB_SUB_DEAD_LETTER_TOPIC_NAME);
    TopicName pubsubTopicDeadletter = TopicName.of(projectId, deadLetterTopicName);
    int retryBackOffTime = this.config.getInt(PUB_SUB_BACK_OFF_TIME);
    int retryTimes = this.config.getInt(PUB_SUB_MAX_RETRY);
    int messageHoldDeadline = this.config.getInt(PUB_SUB_MESSAGE_HOLDING_DEADLINE);

    RetryPolicy retryPolicy = RetryPolicy.newBuilder()
        .setMaximumBackoff(Duration.newBuilder().setSeconds(retryBackOffTime).build())
        .setMinimumBackoff(Duration.newBuilder().setSeconds(retryBackOffTime).build())
        .build();

    DeadLetterPolicy deadLetterPolicy = DeadLetterPolicy.newBuilder().setMaxDeliveryAttempts(retryTimes)
        .setDeadLetterTopic(pubsubTopicDeadletter.toString()).build();

    Subscription newSubscription = Subscription.newBuilder().setPushConfig(PushConfig.getDefaultInstance())
        .setAckDeadlineSeconds(messageHoldDeadline)
        .setTopic(pubsubTopic.toString())
        .setName(pubsubTopicSubscription.toString())
        .setRetryPolicy(retryPolicy)
        .setDeadLetterPolicy(deadLetterPolicy)
        .build();

    subscriptionAdminClient.createSubscription(newSubscription);
    subscriptionAdminClient.close();
    log.info("Already created a pull subscription {}", subscriptionName);
    try (TopicAdminClient topicAdminClient = this.gcpCommonUtil.getTopicAdminClient()) {
      this.updateSubscriptionSubscriberRole(topicAdminClient, pubsubTopicSubscription);
    } catch (Exception e) {
      log.error("Cannot update the role of subscription: {}, {}", e.getMessage(), e.getCause());
    }

  }

  public void createIfNotExitsDeadLetterSubscription() {

    if (!autoCreateSubscription) {
      log.info("auto create subscription is false, please create it manually");
      return;
    }

    String deadLetterSubsName = this.config.getString(PUB_SUB_TOPIC_DEADLETTER_SUBSCRIPTION_NAME);
    ProjectSubscriptionName deadLetterSubsFull = ProjectSubscriptionName.of(projectId, deadLetterSubsName);

    if (hasSubscription(deadLetterSubsFull)) {
      return;
    }

    SubscriptionAdminClient subscriptionAdminClient = this.getSubscriptionClient();
    String deadLetterTopicName = this.config.getString(PUB_SUB_DEAD_LETTER_TOPIC_NAME);
    TopicName pubsubTopicDeadletter = TopicName.of(projectId, deadLetterTopicName);
    int messageHoldDeadline = this.config.getInt(PUB_SUB_MESSAGE_HOLDING_DEADLINE);

    Subscription deadLetterSubs = Subscription.newBuilder().setName(deadLetterSubsFull.toString())
        .setTopic(pubsubTopicDeadletter.toString())
        .setAckDeadlineSeconds(messageHoldDeadline)
        .build();

    subscriptionAdminClient.createSubscription(deadLetterSubs);
    log.info("Already created a pull subscription {}", deadLetterSubsName);
    subscriptionAdminClient.shutdownNow();
    subscriptionAdminClient.close();

  }

  private void updateSubscriptionSubscriberRole(TopicAdminClient topicAdminClient, ProjectSubscriptionName subscriptionName) {
    GetIamPolicyRequest getIamPolicyRequest = GetIamPolicyRequest.newBuilder().setResource(subscriptionName.toString()).build();
    Policy oldPolicy = topicAdminClient.getIamPolicy(getIamPolicyRequest);

    Policy updatedPolicy = Policy.newBuilder(oldPolicy)
        .addBindings(this.getSubscriptionSubscriberBinding()).build();

    SetIamPolicyRequest setIamPolicyRequest =
        SetIamPolicyRequest.newBuilder()
            .setResource(subscriptionName.toString())
            .setPolicy(updatedPolicy).build();

    topicAdminClient.setIamPolicy(setIamPolicyRequest);
  }

  private Binding getSubscriptionSubscriberBinding() {
    ServiceAccount serviceAccount = this.gcpCommonUtil.getServiceAccount();
    String email = serviceAccount.getEmail();
    String pubSubAgent = email.replace(storageDomain, pubSubDomain);
    String member = "serviceAccount:" + pubSubAgent;
    return Binding.newBuilder().setRole("roles/pubsub.subscriber").addMembers(member).build();
  }
}
