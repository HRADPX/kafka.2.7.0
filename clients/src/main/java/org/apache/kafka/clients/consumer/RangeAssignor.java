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
package org.apache.kafka.clients.consumer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.internals.AbstractPartitionAssignor;
import org.apache.kafka.common.TopicPartition;

/**
 * <p>The range assignor works on a per-topic basis. For each topic, we lay out the available partitions in numeric order
 * and the consumers in lexicographic order. We then divide the number of partitions by the total number of
 * consumers to determine the number of partitions to assign to each consumer. If it does not evenly
 * divide, then the first few consumers will have one extra partition.
 *
 * <p>For example, suppose there are two consumers <code>C0</code> and <code>C1</code>, two topics <code>t0</code> and
 * <code>t1</code>, and each topic has 3 partitions, resulting in partitions <code>t0p0</code>, <code>t0p1</code>,
 * <code>t0p2</code>, <code>t1p0</code>, <code>t1p1</code>, and <code>t1p2</code>.
 *
 * <p>The assignment will be:
 * <ul>
 * <li><code>C0: [t0p0, t0p1, t1p0, t1p1]</code></li>
 * <li><code>C1: [t0p2, t1p2]</code></li>
 * </ul>
 *
 * Since the introduction of static membership, we could leverage <code>group.instance.id</code> to make the assignment behavior more sticky.
 * For the above example, after one rolling bounce, group coordinator will attempt to assign new <code>member.id</code> towards consumers,
 * for example <code>C0</code> -&gt; <code>C3</code> <code>C1</code> -&gt; <code>C2</code>.
 *
 * <p>The assignment could be completely shuffled to:
 * <ul>
 * <li><code>C3 (was C0): [t0p2, t1p2] (before was [t0p0, t0p1, t1p0, t1p1])</code>
 * <li><code>C2 (was C1): [t0p0, t0p1, t1p0, t1p1] (before was [t0p2, t1p2])</code>
 * </ul>
 *
 * The assignment change was caused by the change of <code>member.id</code> relative order, and
 * can be avoided by setting the group.instance.id.
 * Consumers will have individual instance ids <code>I1</code>, <code>I2</code>. As long as
 * 1. Number of members remain the same across generation
 * 2. Static members' identities persist across generation
 * 3. Subscription pattern doesn't change for any member
 *
 * <p>The assignment will always be:
 * <ul>
 * <li><code>I0: [t0p0, t0p1, t1p0, t1p1]</code>
 * <li><code>I1: [t0p2, t1p2]</code>
 * </ul>
 *
 * 范围分区分配器
 *    分配规则：范围分区分配器是基于每个 topic。对每个 topic，按照可用的分区数排列分区，按照字典的的顺序排列消费者，然后用分区数除以消费者数来
 * 确定每个消费者分配的分区数量。如果不够平分，则前几个消费者会多分配一个分区。
 *    example：假设有两个消费者（C1，C2），两个 topic（t0， t1），每个 topic 都有 3 个分区，即 t0p0, t0p1, t0p2, t1p0, t1p1, t1p2。
 * 按照分配规则，首先对 t1 的 3 个分区进行分配，3/2=1，每个消费者分配一个分区，剩下一个分区分配给第一个消费者。t2 的 3 个分区分配规则同 t1，
 * 即最后分配的结果是：
 *    C0: [t0p0, t0p1, t1p0, t1p1]
 *    C1: [t0p2, t1p2]
 *
 *    这种分区是根据消费者 member.id 进行排序，所以其分配规则受到消费者 member.id 的影响（排在前面的消费者更可能获取更多的分区）。
 *  如果消费者的 member.id 发生变化，那么其分配的分区可能会发生很大的变化。如上面的例子，如果 C1 换成了 C3，那么分配的分区则完全相反。
 *    如果要避免消费者 member.id 的变化导致的分配分区的变化，可以通过设置 group.instance.id 来避免。只要消费者数量不变，订阅 topic 不变
 *  分配的分区数就不会发生变化。
 */
public class RangeAssignor extends AbstractPartitionAssignor {

    @Override
    public String name() {
        return "range";
    }

    private Map<String, List<MemberInfo>> consumersPerTopic(Map<String, Subscription> consumerMetadata) {
        Map<String, List<MemberInfo>> topicToConsumers = new HashMap<>();
        for (Map.Entry<String, Subscription> subscriptionEntry : consumerMetadata.entrySet()) {
            String consumerId = subscriptionEntry.getKey();
            MemberInfo memberInfo = new MemberInfo(consumerId, subscriptionEntry.getValue().groupInstanceId());
            for (String topic : subscriptionEntry.getValue().topics()) {
                put(topicToConsumers, topic, memberInfo);
            }
        }
        return topicToConsumers;
    }

    /**
     * @param partitionsPerTopic topic 和其分区数量的映射
     * @param subscriptions 每个消费者订阅的主题
     * @return key: member.id，value: 分配到的分区列表
     * 分配规则见上 doc comment
     */
    @Override
    public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
                                                    Map<String, Subscription> subscriptions) {

        // key: topic, value: consumers
        Map<String, List<MemberInfo>> consumersPerTopic = consumersPerTopic(subscriptions);

        Map<String, List<TopicPartition>> assignment = new HashMap<>();
        for (String memberId : subscriptions.keySet())
            assignment.put(memberId, new ArrayList<>());

        // 范围分配器是基于每个 topic 进行分配
        for (Map.Entry<String, List<MemberInfo>> topicEntry : consumersPerTopic.entrySet()) {
            // topic
            String topic = topicEntry.getKey();
            // 订阅该 topic 的消费者列表
            List<MemberInfo> consumersForTopic = topicEntry.getValue();

            Integer numPartitionsForTopic = partitionsPerTopic.get(topic);
            if (numPartitionsForTopic == null)
                continue;

            // 对消费者进行排序（group.instance.id > member.id）
            Collections.sort(consumersForTopic);

            // 每个消费者分配的分区数量
            int numPartitionsPerConsumer = numPartitionsForTopic / consumersForTopic.size();
            // 如果不够平分，剩下的分区会让排在前面的消费者分区数 + 1
            int consumersWithExtraPartition = numPartitionsForTopic % consumersForTopic.size();

            // 当前 topic 的所有分区，带序号
            List<TopicPartition> partitions = AbstractPartitionAssignor.partitions(topic, numPartitionsForTopic);
            for (int i = 0, n = consumersForTopic.size(); i < n; i++) {
                // 分配到的分区的开始位置
                int start = numPartitionsPerConsumer * i + Math.min(i, consumersWithExtraPartition);
                // 分配到的分区的个数
                int length = numPartitionsPerConsumer + (i + 1 > consumersWithExtraPartition ? 0 : 1);
                assignment.get(consumersForTopic.get(i).memberId).addAll(partitions.subList(start, start + length));
            }
        }
        return assignment;
    }
}
