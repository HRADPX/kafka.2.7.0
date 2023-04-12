/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer.internals;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Abstract assignor implementation which does some common grunt work (in particular collecting
 * partition counts which are always needed in assignors).
 */
public abstract class AbstractPartitionAssignor implements ConsumerPartitionAssignor {
    private static final Logger log = LoggerFactory.getLogger(AbstractPartitionAssignor.class);

    /**
     * Perform the group assignment given the partition counts and member subscriptions
     * @param partitionsPerTopic The number of partitions for each subscribed topic. Topics not in metadata will be excluded
     *                           from this map.
     * @param subscriptions Map from the member id to their respective topic subscription
     * @return Map from each member to the list of partitions assigned to them.
     * 对给定的分区数和消费组进行分区分配。
     * {@param partitionsPerTopic} topic 和其分区数量的映射
     * {@param subscriptions} 消费者和其订阅的 topic 的映射
     * Kafka 默认的分区器是 {@link org.apache.kafka.clients.consumer.RangeAssignor}
     */
    public abstract Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                             Map<String, Subscription> subscriptions);

    /**
     * 执行消费组分区分配，返回一个 Map， key 为 memberId, value 是分配的分区列表
     * {@param metadata} topic/broker 的元数据
     * {@param groupSubscription} 所有消费者的订阅
     */
    @Override
    public GroupAssignment assign(Cluster metadata, GroupSubscription groupSubscription) {
        // key: memberId
        Map<String, Subscription> subscriptions = groupSubscription.groupSubscription();
        // 所有订阅的主题
        Set<String> allSubscribedTopics = new HashSet<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : subscriptions.entrySet())
            allSubscribedTopics.addAll(subscriptionEntry.getValue().topics());

        // key: topic, value: topic 的分区数
        Map<String, Integer> partitionsPerTopic = new HashMap<>();
        // 遍历所有订阅的主题，获取每个主题的分区数量
        for (String topic : allSubscribedTopics) {
            Integer numPartitions = metadata.partitionCountForTopic(topic);
            if (numPartitions != null && numPartitions > 0)
                partitionsPerTopic.put(topic, numPartitions);
            else
                log.debug("Skipping assignment for topic {} since no metadata is available", topic);
        }

        // 执行分配
        Map<String, List<TopicPartition>> rawAssignments = assign(partitionsPerTopic, subscriptions);

        // this class maintains no user data, so just wrap the results
        // 封装下结果
        Map<String, Assignment> assignments = new HashMap<>();
        for (Map.Entry<String, List<TopicPartition>> assignmentEntry : rawAssignments.entrySet())
            assignments.put(assignmentEntry.getKey(), new Assignment(assignmentEntry.getValue()));
        return new GroupAssignment(assignments);
    }

    protected static <K, V> void put(Map<K, List<V>> map, K key, V value) {
        List<V> list = map.computeIfAbsent(key, k -> new ArrayList<>());
        list.add(value);
    }

    protected static List<TopicPartition> partitions(String topic, int numPartitions) {
        List<TopicPartition> partitions = new ArrayList<>(numPartitions);
        for (int i = 0; i < numPartitions; i++)
            partitions.add(new TopicPartition(topic, i));
        return partitions;
    }

    /**
     * 排序规则，有两个字段进行排序，分别是 group.instance.id 和 member.id
     * 优先级： group.instance.id > member.id
     */
    public static class MemberInfo implements Comparable<MemberInfo> {
        public final String memberId;
        public final Optional<String> groupInstanceId;

        public MemberInfo(String memberId, Optional<String> groupInstanceId) {
            this.memberId = memberId;
            this.groupInstanceId = groupInstanceId;
        }

        @Override
        public int compareTo(MemberInfo otherMemberInfo) {
            if (this.groupInstanceId.isPresent() &&
                    otherMemberInfo.groupInstanceId.isPresent()) {
                return this.groupInstanceId.get()
                        .compareTo(otherMemberInfo.groupInstanceId.get());
            } else if (this.groupInstanceId.isPresent()) {
                return -1;
            } else if (otherMemberInfo.groupInstanceId.isPresent()) {
                return 1;
            } else {
                return this.memberId.compareTo(otherMemberInfo.memberId);
            }
        }

        @Override
        public boolean equals(Object o) {
            return o instanceof MemberInfo && this.memberId.equals(((MemberInfo) o).memberId);
        }

        /**
         * We could just use member.id to be the hashcode, since it's unique
         * across the group.
         */
        @Override
        public int hashCode() {
            return memberId.hashCode();
        }

        @Override
        public String toString() {
            return "MemberInfo [member.id: " + memberId
                    + ", group.instance.id: " + groupInstanceId.orElse("{}")
                    + "]";
        }
    }
}
