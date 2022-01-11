/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.atlas.kafka;

import org.apache.atlas.model.notification.HookNotification;
import org.apache.atlas.notification.AbstractNotificationConsumer;
import org.apache.atlas.notification.AtlasNotificationMessageDeserializer;
import org.apache.atlas.notification.NotificationInterface;
import org.apache.atlas.v1.model.notification.HookNotificationV1;
import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

/**
 * Kafka specific notification consumer.
 *
 * @param <T> the notification type returned by this consumer
 */
public class AtlasKafkaConsumer<T> extends AbstractNotificationConsumer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(AtlasKafkaConsumer.class);

    private final KafkaConsumer kafkaConsumer;
    private final boolean       autoCommitEnabled;
    private       long          pollTimeoutMilliSeconds = 1000L;
    private boolean isExtraConsumer;

    public AtlasKafkaConsumer(NotificationInterface.NotificationType notificationType, KafkaConsumer kafkaConsumer, boolean autoCommitEnabled, long pollTimeoutMilliSeconds) {
        this(notificationType.getDeserializer(), kafkaConsumer, autoCommitEnabled, pollTimeoutMilliSeconds, false);
    }
    public AtlasKafkaConsumer(NotificationInterface.NotificationType notificationType, KafkaConsumer kafkaConsumer, boolean autoCommitEnabled, long pollTimeoutMilliSeconds, boolean isExtraConsumer) {
        this(notificationType.getDeserializer(), kafkaConsumer, autoCommitEnabled, pollTimeoutMilliSeconds, isExtraConsumer);
    }

    public AtlasKafkaConsumer(AtlasNotificationMessageDeserializer<T> deserializer, KafkaConsumer kafkaConsumer, boolean autoCommitEnabled, long pollTimeoutMilliSeconds, boolean isExtraConsumer) {
        super(deserializer);

        this.autoCommitEnabled       = autoCommitEnabled;
        this.kafkaConsumer           = kafkaConsumer;
        this.pollTimeoutMilliSeconds = pollTimeoutMilliSeconds;
        this.isExtraConsumer = isExtraConsumer;
    }

    public List<AtlasKafkaMessage<T>> receive() {
        return this.receive(this.pollTimeoutMilliSeconds);
    }

    @Override
    public List<AtlasKafkaMessage<T>> receive(long timeoutMilliSeconds) {
        return receive(this.pollTimeoutMilliSeconds, null);
    }

    @Override
    public List<AtlasKafkaMessage<T>> receiveWithCheckedCommit(Map<TopicPartition, Long> lastCommittedPartitionOffset) {
        return receive(this.pollTimeoutMilliSeconds, lastCommittedPartitionOffset);
    }

    @Override
    public boolean isExtraConsumer() {
        return this.isExtraConsumer;
    }


    @Override
    public void commit(TopicPartition partition, long offset) {
        if (!autoCommitEnabled) {
            if (LOG.isDebugEnabled()) {
                LOG.info(" commiting the offset ==>> " + offset);
            }
            kafkaConsumer.commitSync(Collections.singletonMap(partition, new OffsetAndMetadata(offset)));
        }
    }

    @Override
    public void close() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
        }
    }

    @Override
    public void wakeup() {
        if (kafkaConsumer != null) {
            kafkaConsumer.wakeup();
        }
    }

    private List<AtlasKafkaMessage<T>> receive(long timeoutMilliSeconds, Map<TopicPartition, Long> lastCommittedPartitionOffset) {
        List<AtlasKafkaMessage<T>> messages = new ArrayList();

        ConsumerRecords<?, ?> records = kafkaConsumer != null ? kafkaConsumer.poll(timeoutMilliSeconds) : null;

        if (records != null) {
            for (ConsumerRecord<?, ?> record : records) {
                if (LOG.isDebugEnabled()) {
                    LOG.debug("Received Message topic ={}, partition ={}, offset = {}, key = {}, value = {}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value());
                }

                TopicPartition topicPartition = new TopicPartition(record.topic(), record.partition());
                if (MapUtils.isNotEmpty(lastCommittedPartitionOffset)
                        && lastCommittedPartitionOffset.containsKey(topicPartition)
                        && record.offset() < lastCommittedPartitionOffset.get(topicPartition)) {

                    commit(topicPartition, record.offset());
                    LOG.info("Skipping already processed message: topic={}, partition={} offset={}. Last processed offset={}",
                                record.topic(), record.partition(), record.offset(), lastCommittedPartitionOffset.get(topicPartition));
                    continue;
                }

                T message = null;

                try {
                    message = deserializer.deserialize(record.value().toString());
                } catch (OutOfMemoryError excp) {
                    LOG.error("Ignoring message that failed to deserialize: topic={}, partition={}, offset={}, key={}, value={}",
                            record.topic(), record.partition(), record.offset(), record.key(), record.value(), excp);
                }

                if (message == null) {
                    continue;
                }
                String key = extractKey(message);

                messages.add(new AtlasKafkaMessage(message, record.offset(), record.topic(), record.partition(),
                                                            deserializer.getMsgCreated(), deserializer.getSpooled(), key, record.value().toString()));
//                key = ((HookNotification.EntityCreateRequestV2) message).getEntities().getEntities().get(0).getAttribute("qualifiedName").toString()
            }
        }

        return messages;

    }

    private String extractKey(T message) {
//        deserializer.deserialize(message)
       if(message instanceof HookNotification.EntityCreateRequestV2)
           return ((HookNotification.EntityCreateRequestV2) message).getEntities().getEntities().get(0).getAttribute("qualifiedName").toString();
       else if(message instanceof HookNotification.EntityDeleteRequestV2)
            return ((HookNotification.EntityDeleteRequestV2) message).getEntities().get(0).getUniqueAttributes().get("qualifiedName").toString();
        else if(message instanceof HookNotification.EntityUpdateRequestV2)
            return ((HookNotification.EntityUpdateRequestV2) message).getEntities().getEntities().get(0).getAttribute("qualifiedName").toString();
       else if(message instanceof HookNotification.EntityPartialUpdateRequestV2)
           return ((HookNotification.EntityPartialUpdateRequestV2) message).getEntity().getEntity().getAttribute("qualifiedName").toString();
        return null;
    }
}
