package com.geotab.dna.dataplatform.kafka.connect.source.gcs.utils.gcp;

import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PROJECT_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_DEAD_LETTER_TOPIC_NAME;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_TOPIC_CREATE_CONFIG;
import static com.geotab.dna.dataplatform.kafka.connect.source.gcs.config.PubSubGcsSourceConnectorConfig.PUB_SUB_TOPIC_NAME;

import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.CredentialProvider;
import com.geotab.dna.dataplatform.kafka.connect.source.gcs.model.exceptions.GcpResourceException;
import com.google.api.gax.rpc.NotFoundException;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.storage.ServiceAccount;
import com.google.iam.v1.Binding;
import com.google.iam.v1.GetIamPolicyRequest;
import com.google.iam.v1.Policy;
import com.google.iam.v1.SetIamPolicyRequest;
import com.google.pubsub.v1.Topic;
import com.google.pubsub.v1.TopicName;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.config.AbstractConfig;

@Slf4j
public class PubSubTopicUtil {

  private final CredentialProvider credentialProvider;
  private final AbstractConfig config;
  private final GcpCommonUtil gcpCommonUtil;
  private final String storageDomain = "gs-project-accounts";
  private final String pubSubDomain = "gcp-sa-pubsub";

  public PubSubTopicUtil(CredentialProvider credentialProvider, AbstractConfig config, GcpCommonUtil gcpCommonUtil) {
    this.credentialProvider = credentialProvider;
    this.config = config;
    this.gcpCommonUtil = gcpCommonUtil;
  }

  public boolean isTopicExisted(TopicName topicName) {
    try (TopicAdminClient topicAdminClient = this.gcpCommonUtil.getTopicAdminClient()) {
      Topic pubsubTopic = topicAdminClient.getTopic(topicName);
      if (null != pubsubTopic) {
        log.info("pubsub topic {} is existed, doesn't need to create", topicName.getTopic());
        return true;
      }
    } catch (NotFoundException nfe) {
      log.info("topic {} not exists, will create one", topicName);
      return false;
    } catch (Exception e) {
      throw new GcpResourceException(e.getMessage(), e.getCause());
    }
    return false;
  }

  public void createPubSubTopic(TopicName topicName) {
    try (TopicAdminClient topicAdminClient = this.gcpCommonUtil.getTopicAdminClient()) {
      topicAdminClient.createTopic(topicName);
      log.info("pubsub topic {} has been created successfully", topicName.getTopic());

      updateTopicPublisherRole(topicAdminClient, topicName);
      log.info("grant publisher role to topic {}", topicName.toString());

    } catch (Exception e) {
      throw new GcpResourceException(String.format("cannot create topic with %s", topicName.getTopic()), e.getCause());
    }
  }

  public void createIfNotExitsPubSubTopics() {
    boolean autoCreatePubsubTopic = this.config.getBoolean(PUB_SUB_TOPIC_CREATE_CONFIG);
    if (!autoCreatePubsubTopic) {
      log.info("Auto pubsub topic creation is false, please create the pubsub topic manually");
      return;
    }
    String projectId = this.config.getString(PROJECT_CONFIG);
    String pubsubTopicName = this.config.getString(PUB_SUB_TOPIC_NAME);
    TopicName topicName = TopicName.of(projectId, pubsubTopicName);
    if (isTopicExisted(topicName)) {
      return;
    }
    createPubSubTopic(topicName);

  }

  public void createIfNotExistsDeadLetterTopic() {
    boolean autoCreate = this.config.getBoolean(PUB_SUB_DEAD_LETTER_TOPIC_AUTO_CREATE);
    if (!autoCreate) {
      log.info("Auto creation for the deadletter topic is false, please create it manually");
      return;
    }

    String projectId = this.config.getString(PROJECT_CONFIG);
    String deadLetterTopic = this.config.getString(PUB_SUB_DEAD_LETTER_TOPIC_NAME);
    TopicName deadLetterTopicName = TopicName.of(projectId, deadLetterTopic);
    if (isTopicExisted(deadLetterTopicName)) {
      return;
    }
    createPubSubTopic(deadLetterTopicName);

  }

  private void updateTopicPublisherRole(TopicAdminClient topicAdminClient, TopicName topicName) {
    GetIamPolicyRequest getIamPolicyRequest = GetIamPolicyRequest.newBuilder().setResource(topicName.toString()).build();
    Policy oldPolicy = topicAdminClient.getIamPolicy(getIamPolicyRequest);

    Policy updatedPolicy = Policy.newBuilder(oldPolicy)
        .addBindings(this.getStoragePublisherBinding())
        .addBindings(this.getPubsubPublisherBinding()).build();

    SetIamPolicyRequest setIamPolicyRequest =
        SetIamPolicyRequest.newBuilder()
            .setResource(topicName.toString())
            .setPolicy(updatedPolicy)
            .build();
    topicAdminClient.setIamPolicy(setIamPolicyRequest);

  }


  private Binding getStoragePublisherBinding() {
    // bind google cloud storage agent email to the pubsub.publisher role
    ServiceAccount serviceAccount = this.gcpCommonUtil.getServiceAccount();
    String member = "serviceAccount:" + serviceAccount.getEmail();
    return Binding.newBuilder().setRole("roles/pubsub.publisher").addMembers(member).build();
  }

  private Binding getPubsubPublisherBinding() {
    // bind google cloud pubsub agent email to the pubsub.publisher role
    ServiceAccount serviceAccount = this.gcpCommonUtil.getServiceAccount();
    String email = serviceAccount.getEmail();
    String pubSubAgent = email.replace(storageDomain, pubSubDomain);
    String member = "serviceAccount:" + pubSubAgent;
    return Binding.newBuilder().setRole("roles/pubsub.publisher").addMembers(member).build();
  }

}
