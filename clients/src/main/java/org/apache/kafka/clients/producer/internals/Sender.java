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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.*;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.*;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.*;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.io.IOException;
import java.util.*;

import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * The background thread that handles the sending of produce requests to the Kafka cluster. This thread makes metadata
 * requests to renew its view of the cluster and then sends produce requests to the appropriate nodes.
 */
public class Sender implements Runnable {

    private final Logger log;

    /* the state of each nodes connection */
    /* NetworkClient */
    private final KafkaClient client;

    /* the record accumulator that batches records */
    private final RecordAccumulator accumulator;

    /* the metadata for the client */
    private final ProducerMetadata metadata;

    /* the flag indicating whether the producer should guarantee the message order on the broker or not. */
    private final boolean guaranteeMessageOrder;

    /* the maximum request size to attempt to send to the server */
    private final int maxRequestSize;

    /* the number of acknowledgements to request from the server */
    private final short acks;

    /* the number of times to retry a failed request before giving up */
    private final int retries;

    /* the clock instance used for getting the time */
    private final Time time;

    /* true while the sender thread is still running */
    private volatile boolean running;

    /* true when the caller wants to ignore all unsent/inflight messages and force close.  */
    private volatile boolean forceClose;

    /* metrics */
    private final SenderMetrics sensors;

    /* the max time to wait for the server to respond to the request*/
    private final int requestTimeoutMs;

    /* The max time to wait before retrying a request which has failed */
    private final long retryBackoffMs;

    /* current request API versions supported by the known brokers */
    private final ApiVersions apiVersions;

    /* all the state related to transactions, in particular the producer id, producer epoch, and sequence numbers */
    private final TransactionManager transactionManager;

    // A per-partition queue of batches ordered by creation time for tracking the in-flight batches
    private final Map<TopicPartition, List<ProducerBatch>> inFlightBatches;

    // 在初始化 KafkaProducer 时创建 Sender 对象
    public Sender(LogContext logContext,
                  KafkaClient client,
                  ProducerMetadata metadata,
                  RecordAccumulator accumulator,
                  boolean guaranteeMessageOrder,
                  int maxRequestSize,
                  short acks,
                  int retries,
                  SenderMetricsRegistry metricsRegistry,
                  Time time,
                  int requestTimeoutMs,
                  long retryBackoffMs,
                  TransactionManager transactionManager,
                  ApiVersions apiVersions) {
        this.log = logContext.logger(Sender.class);
        this.client = client;
        this.accumulator = accumulator;
        this.metadata = metadata;
        this.guaranteeMessageOrder = guaranteeMessageOrder;
        this.maxRequestSize = maxRequestSize;
        this.running = true;
        this.acks = acks;
        this.retries = retries;
        this.time = time;
        this.sensors = new SenderMetrics(metricsRegistry, metadata, client, time);
        this.requestTimeoutMs = requestTimeoutMs;
        this.retryBackoffMs = retryBackoffMs;
        this.apiVersions = apiVersions;
        this.transactionManager = transactionManager;
        this.inFlightBatches = new HashMap<>();
    }

    public List<ProducerBatch> inFlightBatches(TopicPartition tp) {
        return inFlightBatches.containsKey(tp) ? inFlightBatches.get(tp) : new ArrayList<>();
    }

    private void maybeRemoveFromInflightBatches(ProducerBatch batch) {
        List<ProducerBatch> batches = inFlightBatches.get(batch.topicPartition);
        if (batches != null) {
            batches.remove(batch);
            if (batches.isEmpty()) {
                inFlightBatches.remove(batch.topicPartition);
            }
        }
    }

    private void maybeRemoveAndDeallocateBatch(ProducerBatch batch) {
        maybeRemoveFromInflightBatches(batch);
        this.accumulator.deallocate(batch);
    }

    /**
     *  Get the in-flight batches that has reached delivery timeout.
     *  处理可以发送但是长时间没有发送的消息
     *  注意这里的 inFlightBatches 是 Sender 对象里的属性，里面存储的是批次。
     */
    private List<ProducerBatch> getExpiredInflightBatches(long now) {
        List<ProducerBatch> expiredBatches = new ArrayList<>();

        // inFlightBatches 存储了所有可以发送的批次
        for (Iterator<Map.Entry<TopicPartition, List<ProducerBatch>>> batchIt = inFlightBatches.entrySet().iterator(); batchIt.hasNext();) {
            Map.Entry<TopicPartition, List<ProducerBatch>> entry = batchIt.next();
            List<ProducerBatch> partitionInFlightBatches = entry.getValue();
            if (partitionInFlightBatches != null) {
                Iterator<ProducerBatch> iter = partitionInFlightBatches.iterator();
                while (iter.hasNext()) {
                    ProducerBatch batch = iter.next();
                    // 判断是否超时，当前时间-批次创建的时间 是否大于超时时间
                    if (batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now)) {
                        iter.remove();
                        // expireBatches is called in Sender.sendProducerData, before client.poll.
                        // The !batch.isDone() invariant should always hold. An IllegalStateException
                        // exception will be thrown if the invariant is violated.
                        if (!batch.isDone()) {
                            expiredBatches.add(batch);
                        } else {
                            throw new IllegalStateException(batch.topicPartition + " batch created at " +
                                batch.createdMs + " gets unexpected final state " + batch.finalState());
                        }
                    } else {
                        // 更新下次超时的时间
                        accumulator.maybeUpdateNextBatchExpiryTime(batch);
                        break;
                    }
                }
                if (partitionInFlightBatches.isEmpty()) {
                    batchIt.remove();
                }
            }
        }
        return expiredBatches;
    }

    private void addToInflightBatches(List<ProducerBatch> batches) {
        for (ProducerBatch batch : batches) {
            List<ProducerBatch> inflightBatchList = inFlightBatches.get(batch.topicPartition);
            if (inflightBatchList == null) {
                inflightBatchList = new ArrayList<>();
                inFlightBatches.put(batch.topicPartition, inflightBatchList);
            }
            inflightBatchList.add(batch);
        }
    }

    public void addToInflightBatches(Map<Integer, List<ProducerBatch>> batches) {
        for (List<ProducerBatch> batchList : batches.values()) {
            addToInflightBatches(batchList);
        }
    }

    private boolean hasPendingTransactionalRequests() {
        return transactionManager != null && transactionManager.hasPendingRequests() && transactionManager.hasOngoingTransaction();
    }

    /**
     * The main run loop for the sender thread
     */
    @Override
    public void run() {
        log.debug("Starting Kafka producer I/O thread.");

        // main loop, runs until close is called
        // runing 默认为 true， 发送线程一直会循环直到线程关闭
        while (running) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        log.debug("Beginning shutdown of Kafka producer I/O thread, sending remaining records.");

        // okay we stopped accepting requests but there may still be
        // requests in the transaction manager, accumulator or waiting for acknowledgment,
        // wait until these are completed.
        while (!forceClose && ((this.accumulator.hasUndrained() || this.client.inFlightRequestCount() > 0) || hasPendingTransactionalRequests())) {
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        // Abort the transaction if any commit or abort didn't go through the transaction manager's queue
        while (!forceClose && transactionManager != null && transactionManager.hasOngoingTransaction()) {
            if (!transactionManager.isCompleting()) {
                log.info("Aborting incomplete transaction due to shutdown");
                transactionManager.beginAbort();
            }
            try {
                runOnce();
            } catch (Exception e) {
                log.error("Uncaught error in kafka producer I/O thread: ", e);
            }
        }

        if (forceClose) {
            // We need to fail all the incomplete transactional requests and batches and wake up the threads waiting on
            // the futures.
            if (transactionManager != null) {
                log.debug("Aborting incomplete transactional requests due to forced shutdown");
                transactionManager.close();
            }
            log.debug("Aborting incomplete batches due to forced shutdown");
            this.accumulator.abortIncompleteBatches();
        }
        try {
            this.client.close();
        } catch (Exception e) {
            log.error("Failed to close network client", e);
        }

        log.debug("Shutdown of Kafka producer I/O thread has completed.");
    }

    /**
     * Run a single iteration of sending
     *
     */
    void runOnce() {
        // 默认是无事务
        if (transactionManager != null) {
            try {
                transactionManager.maybeResolveSequences();

                // do not continue sending if the transaction manager is in a failed state
                if (transactionManager.hasFatalError()) {
                    RuntimeException lastError = transactionManager.lastError();
                    if (lastError != null)
                        maybeAbortBatches(lastError);
                    client.poll(retryBackoffMs, time.milliseconds());
                    return;
                }

                // Check whether we need a new producerId. If so, we will enqueue an InitProducerId
                // request which will be sent below
                transactionManager.bumpIdempotentEpochAndResetIdIfNeeded();

                if (maybeSendAndPollTransactionalRequest()) {
                    return;
                }
            } catch (AuthenticationException e) {
                // This is already logged as error, but propagated here to perform any clean ups.
                log.trace("Authentication exception while processing transactional request", e);
                transactionManager.authenticationFailed(e);
            }
        }

        long currentTimeMs = time.milliseconds();
        // 一些必要的校验和准备动作（如建立网络连接，封装请求等）
        long pollTimeout = sendProducerData(currentTimeMs);
        // 真正执行网络操作，包括发送请求，接收响应
        client.poll(pollTimeout, currentTimeMs);
    }

    /**
     * 遍历消息收集器中的队列，找出所有要发送消息的主机，判断连接是否就绪（如果没有就绪则建立连接）
     */
    private long sendProducerData(long now) {
        // 1) 获取服务端集群，首次还没有获取
        Cluster cluster = metadata.fetch();
        // get the list of partitions with data ready to send
        // 2) 获取所有准备发送的分区，判断哪些 partition 有消息可以发送，获取 partition 的 leader partition 对应的 broker 主机
        RecordAccumulator.ReadyCheckResult result = this.accumulator.ready(cluster, now);

        // if there are any partitions whose leaders are not known yet, force metadata update
        // 3) 有分区没有找到对应的主副本节点，标记下，下次拉取元数据会拉取
        if (!result.unknownLeaderTopics.isEmpty()) {
            // The set of topics with unknown leader contains topics with leader election pending as well as
            // topics which may have expired. Add the topic again to metadata to ensure it is included
            // and request metadata update, since there are messages to send to the topic.
            for (String topic : result.unknownLeaderTopics)
                this.metadata.add(topic, now);

            log.debug("Requesting metadata update due to unknown leader topics from the batched records: {}",
                result.unknownLeaderTopics);
            // 标记需要全量更新元数据
            this.metadata.requestUpdate();
        }

        // remove any nodes we aren't ready to send to
        // 4) 移除没有准备发送数据的节点（是否已经建立连接等）
        // 如果是服务首次启动的话，这里所有的都是没有连接就绪的，会被移除，等到连接建立后，在下次完成真正的发送动作...
        Iterator<Node> iter = result.readyNodes.iterator();
        long notReadyTimeout = Long.MAX_VALUE;
        while (iter.hasNext()) {
            Node node = iter.next();
            // 判断是否可以发送消息
            if (!this.client.ready(node, now)) {
                iter.remove();
                notReadyTimeout = Math.min(notReadyTimeout, this.client.pollDelayMs(node, now));
            }
        }

        // create produce requests
        // 5) 获取记录收集器，返回每个主副本节点对应的批记录列表，每个批记录对应一个分区
        // key: nodeId, value: 批记录列表
        // 如果是首次，网络没有建立，这里的 batches 为空，后续的动作不会执行
        Map<Integer, List<ProducerBatch>> batches = this.accumulator.drain(cluster, result.readyNodes, this.maxRequestSize, now);
        // 将所有可以发送的批次保存到 inFlightBatches 这个 Map 中，用来超时判断
        addToInflightBatches(batches);
        if (guaranteeMessageOrder) {
            // Mute all the partitions drained
            for (List<ProducerBatch> batchList : batches.values()) {
                for (ProducerBatch batch : batchList)
                    this.accumulator.mutePartition(batch.topicPartition);
            }
        }

        // 处理超时的请求，在 poll() 方法执行之前
        accumulator.resetNextBatchExpiryTime();
        // 已经发送但是没有收到响应的批次
        List<ProducerBatch> expiredInflightBatches = getExpiredInflightBatches(now);
        // 获取消息收集器中过期的批次
        List<ProducerBatch> expiredBatches = this.accumulator.expiredBatches(now);
        expiredBatches.addAll(expiredInflightBatches);

        // todo huangran @TransactionState.resetIdempotentProducerId
        // Reset the producer id if an expired batch has previously been sent to the broker. Also update the metrics
        // for expired batches. see the documentation of @TransactionState.resetIdempotentProducerId to understand why
        // we need to reset the producer id here.
        if (!expiredBatches.isEmpty())
            log.trace("Expired {} batches in accumulator", expiredBatches.size());
        for (ProducerBatch expiredBatch : expiredBatches) {
            String errorMessage = "Expiring " + expiredBatch.recordCount + " record(s) for " + expiredBatch.topicPartition
                + ":" + (now - expiredBatch.createdMs) + " ms has passed since batch creation";
            // 处理超时的批次, 该批次的 finalState 会从 null 被设置为 FAILED，并从 inFlightBatches 中移除，释放批次资源
            failBatch(expiredBatch, -1, NO_TIMESTAMP, new TimeoutException(errorMessage), false);
            if (transactionManager != null && expiredBatch.inRetry()) {
                // This ensures that no new batches are drained until the current in flight batches are fully resolved.
                transactionManager.markSequenceUnresolved(expiredBatch);
            }
        }
        sensors.updateProduceRequestMetrics(batches);

        // If we have any nodes that are ready to send + have sendable data, poll with 0 timeout so this can immediately
        // loop and try sending more data. Otherwise, the timeout will be the smaller value between next batch expiry
        // time, and the delay time for checking data availability. Note that the nodes may have data that isn't yet
        // sendable due to lingering, backing off, etc. This specifically does not include nodes with sendable data
        // that aren't ready to send since they would cause busy looping.
        // 如果有数据需要发送，则设置 pollTimeout = 0 保证数据被发送
        long pollTimeout = Math.min(result.nextReadyCheckDelayMs, notReadyTimeout);
        pollTimeout = Math.min(pollTimeout, this.accumulator.nextExpiryTimeMs() - now);
        pollTimeout = Math.max(pollTimeout, 0);

        if (!result.readyNodes.isEmpty()) {
            log.trace("Nodes with data ready to send: {}", result.readyNodes);
            // if some partitions are already ready to be sent, the select time would be 0;
            // otherwise if some partition already has some data accumulated but not ready yet,
            // the select time will be the time difference between now and its linger expiry time;
            // otherwise the select time will be the time difference between now and the metadata expiry time;
            pollTimeout = 0;
        }
        // 6) batches 就是要发送的数据
        sendProduceRequests(batches, now);
        return pollTimeout;
    }

    /**
     * Returns true if a transactional request is sent or polled, or if a FindCoordinator request is enqueued
     */
    private boolean maybeSendAndPollTransactionalRequest() {
        if (transactionManager.hasInFlightRequest()) {
            // as long as there are outstanding transactional requests, we simply wait for them to return
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        }

        if (transactionManager.hasAbortableError() || transactionManager.isAborting()) {
            if (accumulator.hasIncomplete()) {
                // Attempt to get the last error that caused this abort.
                RuntimeException exception = transactionManager.lastError();
                // If there was no error, but we are still aborting,
                // then this is most likely a case where there was no fatal error.
                if (exception == null) {
                    exception = new TransactionAbortedException();
                }
                accumulator.abortUndrainedBatches(exception);
            }
        }

        if (transactionManager.isCompleting() && !accumulator.flushInProgress()) {
            // There may still be requests left which are being retried. Since we do not know whether they had
            // been successfully appended to the broker log, we must resend them until their final status is clear.
            // If they had been appended and we did not receive the error, then our sequence number would no longer
            // be correct which would lead to an OutOfSequenceException.
            accumulator.beginFlush();
        }

        TransactionManager.TxnRequestHandler nextRequestHandler = transactionManager.nextRequest(accumulator.hasIncomplete());
        if (nextRequestHandler == null)
            return false;

        AbstractRequest.Builder<?> requestBuilder = nextRequestHandler.requestBuilder();
        Node targetNode = null;
        try {
            FindCoordinatorRequest.CoordinatorType coordinatorType = nextRequestHandler.coordinatorType();
            targetNode = coordinatorType != null ?
                    transactionManager.coordinator(coordinatorType) :
                    client.leastLoadedNode(time.milliseconds());
            if (targetNode != null) {
                if (!awaitNodeReady(targetNode, coordinatorType)) {
                    log.trace("Target node {} not ready within request timeout, will retry when node is ready.", targetNode);
                    maybeFindCoordinatorAndRetry(nextRequestHandler);
                    return true;
                }
            } else if (coordinatorType != null) {
                log.trace("Coordinator not known for {}, will retry {} after finding coordinator.", coordinatorType, requestBuilder.apiKey());
                maybeFindCoordinatorAndRetry(nextRequestHandler);
                return true;
            } else {
                log.trace("No nodes available to send requests, will poll and retry when until a node is ready.");
                transactionManager.retry(nextRequestHandler);
                client.poll(retryBackoffMs, time.milliseconds());
                return true;
            }

            if (nextRequestHandler.isRetry())
                time.sleep(nextRequestHandler.retryBackoffMs());

            long currentTimeMs = time.milliseconds();
            ClientRequest clientRequest = client.newClientRequest(
                targetNode.idString(), requestBuilder, currentTimeMs, true, requestTimeoutMs, nextRequestHandler);
            log.debug("Sending transactional request {} to node {} with correlation ID {}", requestBuilder, targetNode, clientRequest.correlationId());
            client.send(clientRequest, currentTimeMs);
            transactionManager.setInFlightCorrelationId(clientRequest.correlationId());
            client.poll(retryBackoffMs, time.milliseconds());
            return true;
        } catch (IOException e) {
            log.debug("Disconnect from {} while trying to send request {}. Going " +
                    "to back off and retry.", targetNode, requestBuilder, e);
            // We break here so that we pick up the FindCoordinator request immediately.
            maybeFindCoordinatorAndRetry(nextRequestHandler);
            return true;
        }
    }

    private void maybeFindCoordinatorAndRetry(TransactionManager.TxnRequestHandler nextRequestHandler) {
        if (nextRequestHandler.needsCoordinator()) {
            transactionManager.lookupCoordinator(nextRequestHandler);
        } else {
            // For non-coordinator requests, sleep here to prevent a tight loop when no node is available
            time.sleep(retryBackoffMs);
            metadata.requestUpdate();
        }

        transactionManager.retry(nextRequestHandler);
    }

    private void maybeAbortBatches(RuntimeException exception) {
        if (accumulator.hasIncomplete()) {
            log.error("Aborting producer batches due to fatal error", exception);
            accumulator.abortBatches(exception);
        }
    }

    /**
     * Start closing the sender (won't actually complete until all data is sent out)
     */
    public void initiateClose() {
        // Ensure accumulator is closed first to guarantee that no more appends are accepted after
        // breaking from the sender loop. Otherwise, we may miss some callbacks when shutting down.
        this.accumulator.close();
        this.running = false;
        this.wakeup();
    }

    /**
     * Closes the sender without sending out any pending messages.
     */
    public void forceClose() {
        this.forceClose = true;
        initiateClose();
    }

    public boolean isRunning() {
        return running;
    }

    private boolean awaitNodeReady(Node node, FindCoordinatorRequest.CoordinatorType coordinatorType) throws IOException {
        if (NetworkClientUtils.awaitReady(client, node, time, requestTimeoutMs)) {
            if (coordinatorType == FindCoordinatorRequest.CoordinatorType.TRANSACTION) {
                // Indicate to the transaction manager that the coordinator is ready, allowing it to check ApiVersions
                // This allows us to bump transactional epochs even if the coordinator is temporarily unavailable at
                // the time when the abortable error is handled
                transactionManager.handleCoordinatorReady();
            }
            return true;
        }
        return false;
    }

    /**
     * Handle a produce response
     * @see NetworkClient#poll(long, long)
     * @see org.apache.kafka.clients.NetworkClient#completeResponses
     * 回调方法
     */
    private void handleProduceResponse(ClientResponse response, Map<TopicPartition, ProducerBatch> batches, long now) {
        RequestHeader requestHeader = response.requestHeader();
        int correlationId = requestHeader.correlationId();
        // 发送请求，但是发现 broker 失去连接了，如果请求在规定的时间内还没有返回，就会走到这里
        if (response.wasDisconnected()) {
            log.trace("Cancelled request with header {} due to node {} being disconnected",
                requestHeader, response.destination());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NETWORK_EXCEPTION), correlationId, now);
        // 不支持的版本
        } else if (response.versionMismatch() != null) {
            log.warn("Cancelled request {} due to a version mismatch with node {}",
                    response, response.destination(), response.versionMismatch());
            for (ProducerBatch batch : batches.values())
                completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.UNSUPPORTED_VERSION), correlationId, now);
        } else {
            log.trace("Received produce response from node {} with correlation id {}", response.destination(), correlationId);
            // if we have a response, parse it
            // 有响应体
            if (response.hasResponse()) {
                ProduceResponse produceResponse = (ProduceResponse) response.responseBody();
                for (Map.Entry<TopicPartition, ProduceResponse.PartitionResponse> entry : produceResponse.responses().entrySet()) {
                    TopicPartition tp = entry.getKey();
                    ProduceResponse.PartitionResponse partResp = entry.getValue();
                    ProducerBatch batch = batches.get(tp);
                    // 处理响应
                    completeBatch(batch, partResp, correlationId, now);
                }
                this.sensors.recordLatency(response.destination(), response.requestLatencyMs());
            } else {
                // this is the acks = 0 case, just complete all requests
                // acks = 0 表示客户端发送的请求不需要返回响应
                // acks = 1 表示客户端发送的请求写入 leader 节点中，返回响应
                // acks = -1 表示客户端发送的请求写入 leader 和 follower 节点后，返回响应
                // 无响应体
                for (ProducerBatch batch : batches.values()) {
                    completeBatch(batch, new ProduceResponse.PartitionResponse(Errors.NONE), correlationId, now);
                }
            }
        }
    }

    /**
     * Complete or retry the given batch of records.
     *
     * @param batch The record batch
     * @param response The produce response
     * @param correlationId The correlation id for the request
     * @param now The current POSIX timestamp in milliseconds
     */
    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response, long correlationId,
                               long now) {
        Errors error = response.error;

        // request 大小超过服务端能接收的最大的大小，将大请求拆分成多个小的请求再次发送，不减少重试次数
        if (error == Errors.MESSAGE_TOO_LARGE && batch.recordCount > 1 && !batch.isDone() &&
                (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || batch.isCompressed())) {
            // If the batch is too large, we split the batch and send the split batches again. We do not decrement
            // the retry attempts in this case.
            log.warn(
                "Got error produce response in correlation id {} on topic-partition {}, splitting and retrying ({} attempts left). Error: {}",
                correlationId,
                batch.topicPartition,
                this.retries - batch.attempts(),
                error);
            if (transactionManager != null)
                transactionManager.removeInFlightBatch(batch);
            // todo huangran 请求如何拆分...
            this.accumulator.splitAndReenqueue(batch);
            maybeRemoveAndDeallocateBatch(batch);
            this.sensors.recordBatchSplit();
        } else if (error != Errors.NONE) {
            // 有异常，可重试，需要重新入消息收集器中的队列
            if (canRetry(batch, response, now)) {
                log.warn(
                    "Got error produce response with correlation id {} on topic-partition {}, retrying ({} attempts left). Error: {}",
                    correlationId,
                    batch.topicPartition,
                    this.retries - batch.attempts() - 1,
                    error);
                // 批次重新入队
                reenqueueBatch(batch, now);
            } else if (error == Errors.DUPLICATE_SEQUENCE_NUMBER) {
                // If we have received a duplicate sequence error, it means that the sequence number has advanced beyond
                // the sequence of the current batch, and we haven't retained batch metadata on the broker to return
                // the correct offset and timestamp.
                //
                // The only thing we can do is to return success to the user and not return a valid offset and timestamp.
                completeBatch(batch, response);
            } else {
                final RuntimeException exception;
                if (error == Errors.TOPIC_AUTHORIZATION_FAILED)
                    exception = new TopicAuthorizationException(Collections.singleton(batch.topicPartition.topic()));
                else if (error == Errors.CLUSTER_AUTHORIZATION_FAILED)
                    exception = new ClusterAuthorizationException("The producer is not authorized to do idempotent sends");
                else
                    exception = error.exception(response.errorMessage);
                // tell the user the result of their request. We only adjust sequence numbers if the batch didn't exhaust
                // its retries -- if it did, we don't know whether the sequence number was accepted or not, and
                // thus it is not safe to reassign the sequence.
                failBatch(batch, response, exception, batch.attempts() < this.retries);
            }
            if (error.exception() instanceof InvalidMetadataException) {
                if (error.exception() instanceof UnknownTopicOrPartitionException) {
                    log.warn("Received unknown topic or partition error in produce request on partition {}. The " +
                            "topic-partition may not exist or the user may not have Describe access to it",
                        batch.topicPartition);
                } else {
                    log.warn("Received invalid metadata error in produce request on partition {} due to {}. Going " +
                            "to request metadata update now", batch.topicPartition, error.exception(response.errorMessage).toString());
                }
                metadata.requestUpdate();
            }
        } else {
            // 无 error，调用用户的回调函数，释放资源
            completeBatch(batch, response);
        }

        // Unmute the completed partition.
        if (guaranteeMessageOrder)
            this.accumulator.unmutePartition(batch.topicPartition);
    }

    private void reenqueueBatch(ProducerBatch batch, long currentTimeMs) {
        this.accumulator.reenqueue(batch, currentTimeMs);
        // 从 inFlightBatches 中移除
        maybeRemoveFromInflightBatches(batch);
        this.sensors.recordRetries(batch.topicPartition.topic(), batch.recordCount);
    }

    private void completeBatch(ProducerBatch batch, ProduceResponse.PartitionResponse response) {
        if (transactionManager != null) {
            transactionManager.handleCompletedBatch(batch, response);
        }

        // 执行回调函数
        if (batch.done(response.baseOffset, response.logAppendTime, null)) {
            // 释放资源
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    private void failBatch(ProducerBatch batch,
                           ProduceResponse.PartitionResponse response,
                           RuntimeException exception,
                           boolean adjustSequenceNumbers) {
        failBatch(batch, response.baseOffset, response.logAppendTime, exception, adjustSequenceNumbers);
    }

    /**
     * 失败调用
     */
    private void failBatch(ProducerBatch batch,
                           long baseOffset,
                           long logAppendTime,
                           RuntimeException exception,
                           boolean adjustSequenceNumbers) {
        if (transactionManager != null) {
            transactionManager.handleFailedBatch(batch, exception, adjustSequenceNumbers);
        }

        this.sensors.recordErrors(batch.topicPartition.topic(), batch.recordCount);

        if (batch.done(baseOffset, logAppendTime, exception)) {
            // 从 inFlightBatches 中移除批次并释放批次资源
            maybeRemoveAndDeallocateBatch(batch);
        }
    }

    /**
     * We can retry a send if the error is transient and the number of attempts taken is fewer than the maximum allowed.
     * We can also retry OutOfOrderSequence exceptions for future batches, since if the first batch has failed, the
     * future batches are certain to fail with an OutOfOrderSequence exception.
     *
     * 重试条件：
     * 1）请求批次没有超时
     * 2）没有达到最大重试次数，默认为 {@link Integer#MAX_VALUE}，该参数受批次超时时间影响，如果批次超时，即使没有达到重试上限，也是失败
     * 3）批次没有状态为未完成（如果 Producer 被强制关闭了，没有发送的批次会被强制废弃）
     * 4）响应的异常为可重试的异常，表示重试可能会成功（非事务），对于事务的消息，如果是乱序达到服务端，也需要重试
     */
    private boolean canRetry(ProducerBatch batch, ProduceResponse.PartitionResponse response, long now) {
        return !batch.hasReachedDeliveryTimeout(accumulator.getDeliveryTimeoutMs(), now) &&
            batch.attempts() < this.retries &&
            !batch.isDone() &&
            (transactionManager == null ?
                    response.error.exception() instanceof RetriableException :
                    transactionManager.canRetry(response, batch));
    }

    /**
     * Transfer the record batches into a list of produce requests on a per-node basis
     * 这里要发送的消息已经是按照节点分组了，下面会为每个节点创建一个客户端连接，用于发送消息。
     *
     * 在向 partition 上发送消息的时候，有一些 partition 在一个服务器上的，如果一个分区一个分区发送网络请求，会造成网络压力。所以
     * 它把发送同一个服务器上 partition 的数据，合并为一个请求，然后一次性发送出去，这样可以减少网络请求...
     */
    private void sendProduceRequests(Map<Integer, List<ProducerBatch>> collated, long now) {
        for (Map.Entry<Integer, List<ProducerBatch>> entry : collated.entrySet())
            sendProduceRequest(now, entry.getKey(), acks, requestTimeoutMs, entry.getValue());
    }

    /**
     * Create a produce request from the given record batches
     * @param  destination the nodeId
     * @param acks 服务端应答
     * @param timeout 客户端等待服务端响应最大超时时间
     *
     * 这里的 batches 里的分区都是发送到同一个服务器节点
     */
    private void sendProduceRequest(long now, int destination, short acks, int timeout, List<ProducerBatch> batches) {
        if (batches.isEmpty())
            return;

        Map<TopicPartition, MemoryRecords> produceRecordsByPartition = new HashMap<>(batches.size());
        final Map<TopicPartition, ProducerBatch> recordsByPartition = new HashMap<>(batches.size());

        // find the minimum magic version used when creating the record sets
        byte minUsedMagic = apiVersions.maxUsableProduceMagic();
        for (ProducerBatch batch : batches) {
            if (batch.magic() < minUsedMagic)
                minUsedMagic = batch.magic();
        }

        for (ProducerBatch batch : batches) {
            TopicPartition tp = batch.topicPartition;
            MemoryRecords records = batch.records();

            // down convert if necessary to the minimum magic used. In general, there can be a delay between the time
            // that the producer starts building the batch and the time that we send the request, and we may have
            // chosen the message format based on out-dated metadata. In the worst case, we optimistically chose to use
            // the new message format, but found that the broker didn't support it, so we need to down-convert on the
            // client before sending. This is intended to handle edge cases around cluster upgrades where brokers may
            // not all support the same message format version. For example, if a partition migrates from a broker
            // which is supporting the new magic version to one which doesn't, then we will need to convert.
            if (!records.hasMatchingMagic(minUsedMagic))
                records = batch.records().downConvert(minUsedMagic, 0, time).records();
            produceRecordsByPartition.put(tp, records);
            recordsByPartition.put(tp, batch);
        }

        String transactionalId = null;
        if (transactionManager != null && transactionManager.isTransactional()) {
            transactionalId = transactionManager.transactionalId();
        }
        // 构造生产者请求，最后封装为统一的客户端请求
        ProduceRequest.Builder requestBuilder = ProduceRequest.Builder.forMagic(minUsedMagic, acks, timeout,
                produceRecordsByPartition, transactionalId);
        // 回调函数，它会作为客户端请求的成员变量，当客户端请求完成时，会调用回调函数
        RequestCompletionHandler callback = response -> handleProduceResponse(response, recordsByPartition, time.milliseconds());

        String nodeId = Integer.toString(destination);
        // 客户端请求，它会将回调函数封装到请求中...
        ClientRequest clientRequest = client.newClientRequest(nodeId, requestBuilder, now, acks != 0,
                requestTimeoutMs, callback);
        // 最后由客户端网络对象（NetworkClient）去完成发送
        client.send(clientRequest, now);
        log.trace("Sent produce request to {}: {}", nodeId, requestBuilder);
    }

    /**
     * Wake up the selector associated with this send thread
     */
    public void wakeup() {
        this.client.wakeup();
    }

    public static Sensor throttleTimeSensor(SenderMetricsRegistry metrics) {
        Sensor produceThrottleTimeSensor = metrics.sensor("produce-throttle-time");
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeAvg, new Avg());
        produceThrottleTimeSensor.add(metrics.produceThrottleTimeMax, new Max());
        return produceThrottleTimeSensor;
    }

    /**
     * A collection of sensors for the sender
     */
    private static class SenderMetrics {
        public final Sensor retrySensor;
        public final Sensor errorSensor;
        public final Sensor queueTimeSensor;
        public final Sensor requestTimeSensor;
        public final Sensor recordsPerRequestSensor;
        public final Sensor batchSizeSensor;
        public final Sensor compressionRateSensor;
        public final Sensor maxRecordSizeSensor;
        public final Sensor batchSplitSensor;
        private final SenderMetricsRegistry metrics;
        private final Time time;

        public SenderMetrics(SenderMetricsRegistry metrics, Metadata metadata, KafkaClient client, Time time) {
            this.metrics = metrics;
            this.time = time;

            this.batchSizeSensor = metrics.sensor("batch-size");
            this.batchSizeSensor.add(metrics.batchSizeAvg, new Avg());
            this.batchSizeSensor.add(metrics.batchSizeMax, new Max());

            this.compressionRateSensor = metrics.sensor("compression-rate");
            this.compressionRateSensor.add(metrics.compressionRateAvg, new Avg());

            this.queueTimeSensor = metrics.sensor("queue-time");
            this.queueTimeSensor.add(metrics.recordQueueTimeAvg, new Avg());
            this.queueTimeSensor.add(metrics.recordQueueTimeMax, new Max());

            this.requestTimeSensor = metrics.sensor("request-time");
            this.requestTimeSensor.add(metrics.requestLatencyAvg, new Avg());
            this.requestTimeSensor.add(metrics.requestLatencyMax, new Max());

            this.recordsPerRequestSensor = metrics.sensor("records-per-request");
            this.recordsPerRequestSensor.add(new Meter(metrics.recordSendRate, metrics.recordSendTotal));
            this.recordsPerRequestSensor.add(metrics.recordsPerRequestAvg, new Avg());

            this.retrySensor = metrics.sensor("record-retries");
            this.retrySensor.add(new Meter(metrics.recordRetryRate, metrics.recordRetryTotal));

            this.errorSensor = metrics.sensor("errors");
            this.errorSensor.add(new Meter(metrics.recordErrorRate, metrics.recordErrorTotal));

            this.maxRecordSizeSensor = metrics.sensor("record-size");
            this.maxRecordSizeSensor.add(metrics.recordSizeMax, new Max());
            this.maxRecordSizeSensor.add(metrics.recordSizeAvg, new Avg());

            this.metrics.addMetric(metrics.requestsInFlight, (config, now) -> client.inFlightRequestCount());
            this.metrics.addMetric(metrics.metadataAge,
                (config, now) -> (now - metadata.lastSuccessfulUpdate()) / 1000.0);

            this.batchSplitSensor = metrics.sensor("batch-split-rate");
            this.batchSplitSensor.add(new Meter(metrics.batchSplitRate, metrics.batchSplitTotal));
        }

        private void maybeRegisterTopicMetrics(String topic) {
            // if one sensor of the metrics has been registered for the topic,
            // then all other sensors should have been registered; and vice versa
            String topicRecordsCountName = "topic." + topic + ".records-per-batch";
            Sensor topicRecordCount = this.metrics.getSensor(topicRecordsCountName);
            if (topicRecordCount == null) {
                Map<String, String> metricTags = Collections.singletonMap("topic", topic);

                topicRecordCount = this.metrics.sensor(topicRecordsCountName);
                MetricName rateMetricName = this.metrics.topicRecordSendRate(metricTags);
                MetricName totalMetricName = this.metrics.topicRecordSendTotal(metricTags);
                topicRecordCount.add(new Meter(rateMetricName, totalMetricName));

                String topicByteRateName = "topic." + topic + ".bytes";
                Sensor topicByteRate = this.metrics.sensor(topicByteRateName);
                rateMetricName = this.metrics.topicByteRate(metricTags);
                totalMetricName = this.metrics.topicByteTotal(metricTags);
                topicByteRate.add(new Meter(rateMetricName, totalMetricName));

                String topicCompressionRateName = "topic." + topic + ".compression-rate";
                Sensor topicCompressionRate = this.metrics.sensor(topicCompressionRateName);
                MetricName m = this.metrics.topicCompressionRate(metricTags);
                topicCompressionRate.add(m, new Avg());

                String topicRetryName = "topic." + topic + ".record-retries";
                Sensor topicRetrySensor = this.metrics.sensor(topicRetryName);
                rateMetricName = this.metrics.topicRecordRetryRate(metricTags);
                totalMetricName = this.metrics.topicRecordRetryTotal(metricTags);
                topicRetrySensor.add(new Meter(rateMetricName, totalMetricName));

                String topicErrorName = "topic." + topic + ".record-errors";
                Sensor topicErrorSensor = this.metrics.sensor(topicErrorName);
                rateMetricName = this.metrics.topicRecordErrorRate(metricTags);
                totalMetricName = this.metrics.topicRecordErrorTotal(metricTags);
                topicErrorSensor.add(new Meter(rateMetricName, totalMetricName));
            }
        }

        public void updateProduceRequestMetrics(Map<Integer, List<ProducerBatch>> batches) {
            long now = time.milliseconds();
            for (List<ProducerBatch> nodeBatch : batches.values()) {
                int records = 0;
                for (ProducerBatch batch : nodeBatch) {
                    // register all per-topic metrics at once
                    String topic = batch.topicPartition.topic();
                    maybeRegisterTopicMetrics(topic);

                    // per-topic record send rate
                    String topicRecordsCountName = "topic." + topic + ".records-per-batch";
                    Sensor topicRecordCount = Objects.requireNonNull(this.metrics.getSensor(topicRecordsCountName));
                    topicRecordCount.record(batch.recordCount);

                    // per-topic bytes send rate
                    String topicByteRateName = "topic." + topic + ".bytes";
                    Sensor topicByteRate = Objects.requireNonNull(this.metrics.getSensor(topicByteRateName));
                    topicByteRate.record(batch.estimatedSizeInBytes());

                    // per-topic compression rate
                    String topicCompressionRateName = "topic." + topic + ".compression-rate";
                    Sensor topicCompressionRate = Objects.requireNonNull(this.metrics.getSensor(topicCompressionRateName));
                    topicCompressionRate.record(batch.compressionRatio());

                    // global metrics
                    this.batchSizeSensor.record(batch.estimatedSizeInBytes(), now);
                    this.queueTimeSensor.record(batch.queueTimeMs(), now);
                    this.compressionRateSensor.record(batch.compressionRatio());
                    this.maxRecordSizeSensor.record(batch.maxRecordSize, now);
                    records += batch.recordCount;
                }
                this.recordsPerRequestSensor.record(records, now);
            }
        }

        public void recordRetries(String topic, int count) {
            long now = time.milliseconds();
            this.retrySensor.record(count, now);
            String topicRetryName = "topic." + topic + ".record-retries";
            Sensor topicRetrySensor = this.metrics.getSensor(topicRetryName);
            if (topicRetrySensor != null)
                topicRetrySensor.record(count, now);
        }

        public void recordErrors(String topic, int count) {
            long now = time.milliseconds();
            this.errorSensor.record(count, now);
            String topicErrorName = "topic." + topic + ".record-errors";
            Sensor topicErrorSensor = this.metrics.getSensor(topicErrorName);
            if (topicErrorSensor != null)
                topicErrorSensor.record(count, now);
        }

        public void recordLatency(String node, long latency) {
            long now = time.milliseconds();
            this.requestTimeSensor.record(latency, now);
            if (!node.isEmpty()) {
                String nodeTimeName = "node-" + node + ".latency";
                Sensor nodeRequestTime = this.metrics.getSensor(nodeTimeName);
                if (nodeRequestTime != null)
                    nodeRequestTime.record(latency, now);
            }
        }

        void recordBatchSplit() {
            this.batchSplitSensor.record();
        }
    }

}
