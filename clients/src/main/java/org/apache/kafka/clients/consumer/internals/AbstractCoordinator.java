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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.GroupMaxSizeReachedException;
import org.apache.kafka.common.errors.IllegalGenerationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.MemberIdRequiredException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.UnknownMemberIdException;
import org.apache.kafka.common.message.FindCoordinatorRequestData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.CumulativeCount;
import org.apache.kafka.common.metrics.stats.CumulativeSum;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.metrics.stats.WindowedCount;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.FindCoordinatorResponse;
import org.apache.kafka.common.requests.HeartbeatRequest;
import org.apache.kafka.common.requests.HeartbeatResponse;
import org.apache.kafka.common.requests.JoinGroupRequest;
import org.apache.kafka.common.requests.JoinGroupResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.SyncGroupRequest;
import org.apache.kafka.common.requests.SyncGroupResponse;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

/**
 * AbstractCoordinator implements group management for a single group member by interacting with
 * a designated Kafka broker (the coordinator). Group semantics are provided by extending this class.
 * See {@link ConsumerCoordinator} for example usage.
 *
 * From a high level, Kafka's group management protocol consists of the following sequence of actions:
 *
 * <ol>
 *     <li>Group Registration: Group members register with the coordinator providing their own metadata
 *         (such as the set of topics they are interested in).</li>
 *     <li>Group/Leader Selection: The coordinator select the members of the group and chooses one member
 *         as the leader.</li>
 *     <li>State Assignment: The leader collects the metadata from all the members of the group and
 *         assigns state.</li>
 *     <li>Group Stabilization: Each member receives the state assigned by the leader and begins
 *         processing.</li>
 * </ol>
 *
 * To leverage this protocol, an implementation must define the format of metadata provided by each
 * member for group registration in {@link #metadata()} and the format of the state assignment provided
 * by the leader in {@link #performAssignment(String, String, List)} and becomes available to members in
 * {@link #onJoinComplete(int, String, String, ByteBuffer)}.
 *
 * Note on locking: this class shares state between the caller and a background thread which is
 * used for sending heartbeats after the client has joined the group. All mutable state as well as
 * state transitions are protected with the class's monitor. Generally this means acquiring the lock
 * before reading or writing the state of the group (e.g. generation, memberId) and holding the lock
 * when sending a request that affects the state of the group (e.g. JoinGroup, LeaveGroup).
 *
 * AbstractCoordinator 通过与指定的 Kafka broker 进行交互，实现对单个组成员的管理。
 *
 * Kafka 组管理协议由以下操作组成：
 *  1）组注册：组成员向协调器注册并提供它们的元数据（如 topics 信息）。
 *  2）leader 选举：协议器会从组成员里选择一个成员作为 leader。
 *  3）状态管理：leader 会从其他成员收集元数据并分配状态。
 *  4）Group Stabilization：每个成员会收到分配的状态并开始处理
 *
 */
public abstract class AbstractCoordinator implements Closeable {
    public static final String HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread";
    public static final int JOIN_GROUP_TIMEOUT_LAPSE = 5000;

    protected enum MemberState {
        UNJOINED,             // the client is not part of a group
        PREPARING_REBALANCE,  // the client has sent the join group request, but have not received response
        COMPLETING_REBALANCE, // the client has received join group response, but have not received assignment
        STABLE;               // the client has joined and is sending heartbeats

        public boolean hasNotJoinedGroup() {
            return equals(UNJOINED) || equals(PREPARING_REBALANCE);
        }
    }

    private final Logger log;
    private final Heartbeat heartbeat;
    private final GroupCoordinatorMetrics sensors;
    // 本质上是 consumer config
    // new KafkaConsumer -> ConsumerCoordinator -> rebalanceConfig
    private final GroupRebalanceConfig rebalanceConfig;

    protected final Time time;
    protected final ConsumerNetworkClient client;

    private Node coordinator = null;
    private boolean rejoinNeeded = true;
    private boolean needsJoinPrepare = true;
    private HeartbeatThread heartbeatThread = null;
    private RequestFuture<ByteBuffer> joinFuture = null;
    private RequestFuture<Void> findCoordinatorFuture = null;
    volatile private RuntimeException findCoordinatorException = null;
    private Generation generation = Generation.NO_GENERATION;
    private long lastRebalanceStartMs = -1L;
    private long lastRebalanceEndMs = -1L;

    protected MemberState state = MemberState.UNJOINED;


    /**
     * Initialize the coordination manager.
     */
    public AbstractCoordinator(GroupRebalanceConfig rebalanceConfig,
                               LogContext logContext,
                               ConsumerNetworkClient client,
                               Metrics metrics,
                               String metricGrpPrefix,
                               Time time) {
        Objects.requireNonNull(rebalanceConfig.groupId,
                               "Expected a non-null group id for coordinator construction");
        this.rebalanceConfig = rebalanceConfig;
        this.log = logContext.logger(AbstractCoordinator.class);
        this.client = client;
        this.time = time;
        this.heartbeat = new Heartbeat(rebalanceConfig, time);
        this.sensors = new GroupCoordinatorMetrics(metrics, metricGrpPrefix);
    }

    /**
     * Unique identifier for the class of supported protocols (e.g. "consumer" or "connect").
     * @return Non-null protocol type name
     */
    protected abstract String protocolType();

    /**
     * Get the current list of protocols and their associated metadata supported
     * by the local member. The order of the protocols in the list indicates the preference
     * of the protocol (the first entry is the most preferred). The coordinator takes this
     * preference into account when selecting the generation protocol (generally more preferred
     * protocols will be selected as long as all members support them and there is no disagreement
     * on the preference).
     * @return Non-empty map of supported protocols and metadata
     */
    protected abstract JoinGroupRequestData.JoinGroupRequestProtocolCollection metadata();

    /**
     * Invoked prior to each group join or rejoin. This is typically used to perform any
     * cleanup from the previous generation (such as committing offsets for the consumer)
     * @param generation The previous generation or -1 if there was none
     * @param memberId The identifier of this member in the previous group or "" if there was none
     */
    protected abstract void onJoinPrepare(int generation, String memberId);

    /**
     * Perform assignment for the group. This is used by the leader to push state to all the members
     * of the group (e.g. to push partition assignments in the case of the new consumer)
     * @param leaderId The id of the leader (which is this member)
     * @param protocol The protocol selected by the coordinator
     * @param allMemberMetadata Metadata from all members of the group
     * @return A map from each member to their state assignment
     */
    protected abstract Map<String, ByteBuffer> performAssignment(String leaderId,
                                                                 String protocol,
                                                                 List<JoinGroupResponseData.JoinGroupResponseMember> allMemberMetadata);

    /**
     * Invoked when a group member has successfully joined a group. If this call fails with an exception,
     * then it will be retried using the same assignment state on the next call to {@link #ensureActiveGroup()}.
     *
     * @param generation The generation that was joined
     * @param memberId The identifier for the local member in the group
     * @param protocol The protocol selected by the coordinator
     * @param memberAssignment The assignment propagated from the group leader
     */
    protected abstract void onJoinComplete(int generation,
                                           String memberId,
                                           String protocol,
                                           ByteBuffer memberAssignment);

    /**
     * Invoked prior to each leave group event. This is typically used to cleanup assigned partitions;
     * note it is triggered by the consumer's API caller thread (i.e. background heartbeat thread would
     * not trigger it even if it tries to force leaving group upon heartbeat session expiration)
     */
    protected void onLeavePrepare() {}

    /**
     * Visible for testing.
     *
     * Ensure that the coordinator is ready to receive requests.
     *
     * @param timer Timer bounding how long this method can block
     * @return true If coordinator discovery and initial connection succeeded, false otherwise
     *
     * 确保客户端连接上协调者对象
     */
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        if (!coordinatorUnknown())
            return true;

        do {
            if (findCoordinatorException != null && !(findCoordinatorException instanceof RetriableException)) {
                final RuntimeException fatalException = findCoordinatorException;
                findCoordinatorException = null;
                throw fatalException;
            }
            // 寻找协调器对象
            final RequestFuture<Void> future = lookupCoordinator();
            // 真正是在这里发送的请求，该方法返回，协调器对象创建完毕
            client.poll(future, timer);

            if (!future.isDone()) {
                // ran out of time
                break;
            }

            // 执行失败
            if (future.failed()) {
                if (future.isRetriable()) {
                    log.debug("Coordinator discovery failed, refreshing metadata", future.exception());
                    client.awaitMetadataUpdate(timer);
                } else
                    throw future.exception();
            } else if (coordinator != null && client.isUnavailable(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                // 找到协调节点，但是连接失败，重置 coordinator = null 并开始重试
                markCoordinatorUnknown("coordinator unavailable");
                timer.sleep(rebalanceConfig.retryBackoffMs);
            }
        } while (coordinatorUnknown() && timer.notExpired());

        return !coordinatorUnknown();
    }

    protected synchronized RequestFuture<Void> lookupCoordinator() {
        if (findCoordinatorFuture == null) {
            // find a node to ask about the coordinator
            // 从 broker 集群中找一个最新的节点（完成请求最少），它偏向选择已经建立连接的节点，但是如果所有连接的节点都是 busy 状态，
            // 它也会选择一个没有完成建立连接的节点，找这个节点的目的是为了发送请求
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                log.debug("No broker available to send FindCoordinator request");
                return RequestFuture.noBrokersAvailable();
            } else {
                // 发送寻找协调器请求
                findCoordinatorFuture = sendFindCoordinatorRequest(node);
                // remember the exception even after the future is cleared so that
                // it can still be thrown by the ensureCoordinatorReady caller
                // 失败记录异常，这样后续在调用的 ensureCoordinatorReady 方法时可以抛出
                findCoordinatorFuture.addListener(new RequestFutureListener<Void>() {
                    @Override
                    public void onSuccess(Void value) {} // do nothing

                    @Override
                    public void onFailure(RuntimeException e) {
                        findCoordinatorException = e;
                    }
                });
            }
        }
        return findCoordinatorFuture;
    }

    private synchronized void clearFindCoordinatorFuture() {
        findCoordinatorFuture = null;
    }

    /**
     * Check whether the group should be rejoined (e.g. if metadata changes) or whether a
     * rejoin request is already in flight and needs to be completed.
     *
     * @return true if it should, false otherwise
     */
    protected synchronized boolean rejoinNeededOrPending() {
        // if there's a pending joinFuture, we should try to complete handling it.
        // joinFuture != null 表示之前有一个 join 任务没有完成，需要完成它
        // 在完成 join 任务后，会设置joinFuture = null
        return rejoinNeeded || joinFuture != null;
    }

    /**
     * Check the status of the heartbeat thread (if it is active) and indicate the liveness
     * of the client. This must be called periodically after joining with {@link #ensureActiveGroup()}
     * to ensure that the member stays in the group. If an interval of time longer than the
     * provided rebalance timeout expires without calling this method, then the client will proactively
     * leave the group.
     *
     * @param now current time in milliseconds
     * @throws RuntimeException for unexpected errors raised from the heartbeat thread
     */
    protected synchronized void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            if (heartbeatThread.hasFailed()) {
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }
            // Awake the heartbeat thread if needed
            if (heartbeat.shouldHeartbeat(now)) {
                notify();
            }
            heartbeat.poll(now);
        }
    }

    protected synchronized long timeToNextHeartbeat(long now) {
        // if we have not joined the group or we are preparing rebalance,
        // we don't need to send heartbeats
        if (state.hasNotJoinedGroup())
            return Long.MAX_VALUE;
        return heartbeat.timeToNextHeartbeat(now);
    }

    /**
     * Ensure that the group is active (i.e. joined and synced)
     */
    public void ensureActiveGroup() {
        while (!ensureActiveGroup(time.timer(Long.MAX_VALUE))) {
            log.warn("still waiting to ensure active group");
        }
    }

    /**
     * Ensure the group is active (i.e., joined and synced)
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the group is active
     */
    boolean ensureActiveGroup(final Timer timer) {
        // always ensure that the coordinator is ready because we may have been disconnected
        // when sending heartbeats and does not necessarily require us to rejoin the group.
        // 确保 coordinator 节点就绪，因为心跳机制是通过 coordinator 进行的
        if (!ensureCoordinatorReady(timer)) {
            return false;
        }

        // 启动心跳线程，但是没有开启发送心跳请求，需要等当前消费者加入消费组后调用 HeartbeatThread.enable 后才开始进行心跳检测
        startHeartbeatThreadIfNeeded();
        // 加入消费组（加入用户组，执行再平衡等操作）
        return joinGroupIfNeeded(timer);
    }

    private synchronized void startHeartbeatThreadIfNeeded() {
        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        }
    }

    private void closeHeartbeatThread() {
        HeartbeatThread thread;
        synchronized (this) {
            if (heartbeatThread == null)
                return;
            heartbeatThread.close();
            thread = heartbeatThread;
            heartbeatThread = null;
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for consumer heartbeat thread to close");
            throw new InterruptException(e);
        }
    }

    /**
     * Joins the group without starting the heartbeat thread.
     *
     * If this function returns true, the state must always be in STABLE and heartbeat enabled.
     * If this function returns false, the state can be in one of the following:
     *  * UNJOINED: got error response but times out before being able to re-join, heartbeat disabled
     *  * PREPARING_REBALANCE: not yet received join-group response before timeout, heartbeat disabled
     *  * COMPLETING_REBALANCE: not yet received sync-group response before timeout, hearbeat enabled
     *
     * Visible for testing.
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the operation succeeded
     *
     * 在没有开始心跳检测前加入用户组。
     * 如果这个方法返回 true，消费者状态为 {@link MemberState#STABLE}, 表示消费者加入消费组并开始发送心跳。
     * 如果这个方法返回 false，消费者状态可能为：
     *  1）UNJOINED: 没有成功加入，但是可以在 timeout 之前重新加入，不能开始心跳检测。
     *  2）PREPARING_REBALANCE: 在超时之前没有收到成功加入消费组的响应，不能开始心跳检测。
     *  3）COMPLETING_REBALANCE: 在超时之前收到响应但是分配分区，正在 re-balance，可以开始心跳检测。
     */
    boolean joinGroupIfNeeded(final Timer timer) {
        // 退出循环的条件 rejoinNeeded = false && joinFuture = null;
        while (rejoinNeededOrPending()) {
            // 确保协调器已经创建并且连接就绪，这个之前已经完成
            if (!ensureCoordinatorReady(timer)) {
                return false;
            }

            // call onJoinPrepare if needed. We set a flag to make sure that we do not call it a second
            // time if the client is woken up before a pending rebalance completes. This must be called
            // on each iteration of the loop because an event requiring a rebalance (such as a metadata
            // refresh which changes the matched subscription set) can occur while another rebalance is
            // still in progress.
            // 每次执行加入消费组或重新加入消费组都需要执行 onJoinPrepare 方法，通常是用来执行清理上次操作产生的数据，如 offset 等。
            if (needsJoinPrepare) {
                // need to set the flag before calling onJoinPrepare since the user callback may throw
                // exception, in which case upon retry we should not retry onJoinPrepare either.
                // 调用 onJoinPrepare 方法的标记，一轮再平衡只需调用一次，后续再平衡完成会重新将这个字段设置为 true
                needsJoinPrepare = false;
                onJoinPrepare(generation.generationId, generation.memberId);
            }

            // 封装请求，放到 unsent 队列中，这里的请求处理逻辑同 findCoordinate
            final RequestFuture<ByteBuffer> future = initiateJoinGroup();
            // 执行网络I/O
            client.poll(future, timer);
            if (!future.isDone()) {
                // we ran out of time
                // 1）超时
                return false;
            }

            if (future.succeeded()) {
                Generation generationSnapshot;
                MemberState stateSnapshot;

                // Generation data maybe concurrently cleared by Heartbeat thread.
                // Can't use synchronized for {@code onJoinComplete}, because it can be long enough
                // and shouldn't block heartbeat thread.
                // See {@link PlaintextConsumerTest#testMaxPollIntervalMsDelayInAssignment}
                synchronized (AbstractCoordinator.this) {
                    generationSnapshot = this.generation;
                    stateSnapshot = this.state;
                }

                // 加入消费组成功
                // SyncGroupResponseHandler#handle 同步消费组成功后回调方法对这两个属性就行修改
                if (!generationSnapshot.equals(Generation.NO_GENERATION) && stateSnapshot == MemberState.STABLE) {
                    // Duplicate the buffer in case `onJoinComplete` does not complete and needs to be retried.
                    // 这里 future.value 的值就是服务端返回的当前消费组分配的分区列表
                    ByteBuffer memberAssignment = future.value().duplicate();

                    // 加入消费组成功会将分配到的分区保存到协调者中
                    onJoinComplete(generationSnapshot.generationId, generationSnapshot.memberId, generationSnapshot.protocolName, memberAssignment);

                    // Generally speaking we should always resetJoinGroupFuture once the future is done, but here
                    // we can only reset the join group future after the completion callback returns. This ensures
                    // that if the callback is woken up, we will retry it on the next joinGroupIfNeeded.
                    // And because of that we should explicitly trigger resetJoinGroupFuture in other conditions below.
                    // joinFuture = null 退出循环的条件之一
                    // 另外的条件在同步成功后的 SyncGroupResponseHandler 中设置了 rejoinNeeded = false
                    resetJoinGroupFuture();
                    // 加入消费组成功，此次再平衡已经结束了，在一次再平衡过程中，加入消费组前的准备工作只会做一次（onJoinPrepare 方法只会
                    // 调用一次），这里将这个标记设置为 true，让下轮再平衡时再次调用。
                    needsJoinPrepare = true;
                } else {
                    log.info("Generation data was cleared by heartbeat thread to {} and state is now {} before " +
                         "the rebalance callback is triggered, marking this rebalance as failed and retry",
                         generationSnapshot, stateSnapshot);
                    // Generation 等信息在 re-balance 回调前被心跳线程给清理了，需要比较 re-balance 失败，并且重新加入消费组
                    // 这里需要清理下状态 joinFuture = null, state = MemberState.UNJOINED, generation = Generation.NO_GENERATION
                    resetStateAndRejoin();
                    resetJoinGroupFuture();
                }
            } else {
                final RuntimeException exception = future.exception();

                // we do not need to log error for memberId required,
                // since it is not really an error and is transient
                if (!(exception instanceof MemberIdRequiredException)) {
                    log.info("Rebalance failed.", exception);
                }

                // reset joinFuture = null
                resetJoinGroupFuture();
                // 如果是这些异常不需要重置 state
                if (exception instanceof UnknownMemberIdException ||
                    exception instanceof RebalanceInProgressException ||
                    exception instanceof IllegalGenerationException ||
                    exception instanceof MemberIdRequiredException)
                    continue;
                else if (!future.isRetriable())
                    throw exception;

                resetStateAndRejoin();
                timer.sleep(rebalanceConfig.retryBackoffMs);
            }
        }
        return true;
    }

    private synchronized void resetJoinGroupFuture() {
        this.joinFuture = null;
    }

    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        // we store the join future in case we are woken up by the user after beginning the
        // rebalance in the call to poll below. This ensures that we do not mistakenly attempt
        // to rejoin before the pending rebalance has completed.
        // 加入成功后，会重置这个变量值为null
        if (joinFuture == null) {
            // 设置状态
            state = MemberState.PREPARING_REBALANCE;
            // a rebalance can be triggered consecutively if the previous one failed,
            // in this case we would not update the start time.
            // 如果上一次再平衡失败，它可以会连续不断的触发，需要更新下开始时间
            // 记录 re-balance 开始时间
            if (lastRebalanceStartMs == -1L)
                lastRebalanceStartMs = time.milliseconds();
            // 发送加入消费组请求
            joinFuture = sendJoinGroupRequest();
            joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // do nothing since all the handler logic are in SyncGroupResponseHandler already
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin;
                    // this can be triggered when either join or sync request failed
                    synchronized (AbstractCoordinator.this) {
                        sensors.failedRebalanceSensor.record();
                    }
                }
            });
        }
        return joinFuture;
    }

    /**
     * Join the group and return the assignment for the next generation. This function handles both
     * JoinGroup and SyncGroup, delegating to {@link #performAssignment(String, String, List)} if
     * elected leader by the coordinator.
     *
     * NOTE: This is visible only for testing
     *
     * @return A request future which wraps the assignment returned from the group leader
     */
    RequestFuture<ByteBuffer> sendJoinGroupRequest() {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.info("(Re-)joining group");
        // 构建请求，ApiKeys = JOIN_GROUP
        // KafkaApis 处理逻辑，对于 leader 消费者是先到先得
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
                new JoinGroupRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setSessionTimeoutMs(this.rebalanceConfig.sessionTimeoutMs)
                        .setMemberId(this.generation.memberId)  // 首次加入时是消费者的编号是 UNKNOWN_MEMBER_ID
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setProtocolType(protocolType())
                        .setProtocols(metadata())
                        .setRebalanceTimeoutMs(this.rebalanceConfig.rebalanceTimeoutMs) // 300s
        );

        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);

        // Note that we override the request timeout using the rebalance timeout since that is the
        // maximum time that it may block on the coordinator. We add an extra 5 seconds for small delays.
        int joinGroupTimeoutMs = Math.max(client.defaultRequestTimeoutMs(),
            rebalanceConfig.rebalanceTimeoutMs + JOIN_GROUP_TIMEOUT_LAPSE);
        // 复用 findCoordinator 的逻辑
        // 发送的逻辑在 poll 方法里，请求成功的回调逻辑在 JoinGroupResponseHandler 里
        return client.send(coordinator, requestBuilder, joinGroupTimeoutMs)
                .compose(new JoinGroupResponseHandler(generation));
    }

    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
        private JoinGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            Errors error = joinResponse.error();
            if (error == Errors.NONE) {
                if (isProtocolTypeInconsistent(joinResponse.data().protocolType())) {
                    log.error("JoinGroup failed: Inconsistent Protocol Type, received {} but expected {}",
                        joinResponse.data().protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    // 请求成功
                    log.debug("Received successful JoinGroup response: {}", joinResponse);
                    sensors.joinSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        // 状态在响应返回之前变了，可能是已经离开消费组，这种场景就不希望同步了
                        if (state != MemberState.PREPARING_REBALANCE) {
                            // if the consumer was woken up before a rebalance completes, we may have already left
                            // the group. In this case, we do not want to continue with the sync group.
                            future.raise(new UnjoinedGroupException());
                        } else {
                            // 收到响应，但是没有完成分区分配
                            state = MemberState.COMPLETING_REBALANCE;

                            // we only need to enable heartbeat thread whenever we transit to
                            // COMPLETING_REBALANCE state since we always transit from this state to STABLE
                            // 收到响应，可以开启心跳检测了
                            // 这里执行 enable 方法会唤醒心跳线程向下执行..
                            if (heartbeatThread != null)
                                heartbeatThread.enable();

                            // 更新 generation，主要更新了 generationId 和 memberId（之前默认是 ""）
                            // 因为主消费者可能会发生变化，所以每个消费者中都会记录 generationId，表示执行了多少次分区分配
                            AbstractCoordinator.this.generation = new Generation(
                                joinResponse.data().generationId(),
                                joinResponse.data().memberId(), joinResponse.data().protocolName());

                            log.info("Successfully joined group with generation {}", AbstractCoordinator.this.generation);

                            // 消费组里的 leader 是先到先得，该消费组第一个注册的消费者就是 leader
                            if (joinResponse.isLeader()) {
                                // 执行分区分配，分配的结果同步给服务端
                                // 这里的 future 是加入消费组请求（JOIN_GROUP）的异步请求结果，即通过 chain 方法向 SYNC_GROUP
                                // 的异步请求结果（不是 client.send）添加了一个监听器。
                                // 注意，虽然执行到这里，但是前面说过，这里的 future 是 JOIN_GROUP 的组合模式后的异步请求结果，
                                // 不是 client.send() 的返回结果，并且在这里并没有执行 future.complete 方法，所以 JOIN_GROUP
                                // 异步请求是还没有完成的（future.result 仍无值）
                                // 这个 future 会在 SYNC_GROUP 的 listener 里调用 complete 方法完成，并将 SYNC_GROUP 请求的结果
                                // 保存到 JOIN_GROUP 的异步请求结果中，所以虽然 JOIN_GROUP 请求比 SYNC_GROUP 早，但是实际完成却是在
                                // SYNC_GROUP 之后，并且在 JOIN_GROUP 完成后，就可以直接在 future 里获取到结果。
                                onJoinLeader(joinResponse).chain(future);
                            } else {
                                onJoinFollower().chain(future);
                            }
                        }
                    }
                }
            } else if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS) {
                log.info("JoinGroup failed: Coordinator {} is loading the group.", coordinator());
                // backoff and retry
                future.raise(error);
            } else if (error == Errors.UNKNOWN_MEMBER_ID) {
                log.info("JoinGroup failed: {} Need to re-join the group. Sent generation was {}",
                         error.message(), sentGeneration);
                // only need to reset the member id if generation has not been changed,
                // then retry immediately
                if (generationUnchanged())
                    resetGenerationOnResponseError(ApiKeys.JOIN_GROUP, error);

                future.raise(error);
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                // re-discover the coordinator and retry with backoff
                markCoordinatorUnknown(error);
                log.info("JoinGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                          error.message(), sentGeneration);
                future.raise(error);
            } else if (error == Errors.FENCED_INSTANCE_ID) {
                // for join-group request, even if the generation has changed we would not expect the instance id
                // gets fenced, and hence we always treat this as a fatal error
                log.error("JoinGroup failed: The group instance id {} has been fenced by another instance. " +
                              "Sent generation was {}", rebalanceConfig.groupInstanceId, sentGeneration);
                future.raise(error);
            } else if (error == Errors.INCONSISTENT_GROUP_PROTOCOL
                    || error == Errors.INVALID_SESSION_TIMEOUT
                    || error == Errors.INVALID_GROUP_ID
                    || error == Errors.GROUP_AUTHORIZATION_FAILED
                    || error == Errors.GROUP_MAX_SIZE_REACHED) {
                // log the error and re-throw the exception
                log.error("JoinGroup failed due to fatal error: {}", error.message());
                if (error == Errors.GROUP_MAX_SIZE_REACHED) {
                    future.raise(new GroupMaxSizeReachedException("Consumer group " + rebalanceConfig.groupId +
                            " already has the configured maximum number of members."));
                } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else {
                    future.raise(error);
                }
            } else if (error == Errors.UNSUPPORTED_VERSION) {
                log.error("JoinGroup failed due to unsupported version error. Please unset field group.instance.id " +
                          "and retry to see if the problem resolves");
                future.raise(error);
            } else if (error == Errors.MEMBER_ID_REQUIRED) {
                // Broker requires a concrete member id to be allowed to join the group. Update member id
                // and send another join group request in next cycle.
                String memberId = joinResponse.data().memberId();
                log.debug("JoinGroup failed due to non-fatal error: {} Will set the member id as {} and then rejoin. " +
                              "Sent generation was  {}", error, memberId, sentGeneration);
                synchronized (AbstractCoordinator.this) {
                    AbstractCoordinator.this.generation = new Generation(OffsetCommitRequest.DEFAULT_GENERATION_ID, memberId, null);
                }
                future.raise(error);
            } else {
                // unexpected error, throw the exception
                log.error("JoinGroup failed due to unexpected error: {}", error.message());
                future.raise(new KafkaException("Unexpected error in join group response: " + error.message()));
            }
        }
    }

    private RequestFuture<ByteBuffer> onJoinFollower() {
        // send follower's sync group with an empty assignment
        SyncGroupRequest.Builder requestBuilder =
                new SyncGroupRequest.Builder(
                        new SyncGroupRequestData()
                                .setGroupId(rebalanceConfig.groupId)
                                .setMemberId(generation.memberId)
                                .setProtocolType(protocolType())
                                .setProtocolName(generation.protocolName)
                                .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                                .setGenerationId(generation.generationId)
                                .setAssignments(Collections.emptyList())
                );
        log.debug("Sending follower SyncGroup to coordinator {} at generation {}: {}", this.coordinator, this.generation, requestBuilder);
        return sendSyncGroupRequest(requestBuilder);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            // leader 执行分区同步
            // 这里服务端协调者将所有的消费组成员列表及其订阅的主题和分区返回给主消费者，这里是主消费者执行分区分配
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.data().leader(), joinResponse.data().protocolName(),
                    joinResponse.data().members());

            List<SyncGroupRequestData.SyncGroupRequestAssignment> groupAssignmentList = new ArrayList<>();
            for (Map.Entry<String, ByteBuffer> assignment : groupAssignment.entrySet()) {
                groupAssignmentList.add(new SyncGroupRequestData.SyncGroupRequestAssignment()
                        .setMemberId(assignment.getKey())
                        .setAssignment(Utils.toArray(assignment.getValue()))
                );
            }

            // 将分配的结果封装到同步请求中，发送给服务端，这样服务端就可以知道每个消费者分配到的所有分区
            // ApiKeys = SYNC_GROUP
            SyncGroupRequest.Builder requestBuilder =
                    new SyncGroupRequest.Builder(
                            new SyncGroupRequestData()
                                    .setGroupId(rebalanceConfig.groupId)
                                    .setMemberId(generation.memberId)
                                    .setProtocolType(protocolType())
                                    .setProtocolName(generation.protocolName)
                                    .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                                    .setGenerationId(generation.generationId)
                                    .setAssignments(groupAssignmentList)
                    );
            log.debug("Sending leader SyncGroup to coordinator {} at generation {}: {}", this.coordinator, this.generation, requestBuilder);
            return sendSyncGroupRequest(requestBuilder);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        return client.send(coordinator, requestBuilder)
                .compose(new SyncGroupResponseHandler(generation)); // 请求成功后的回调
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
        private SyncGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(SyncGroupResponse syncResponse, RequestFuture<ByteBuffer> future) {
            Errors error = syncResponse.error();
            if (error == Errors.NONE) {
                if (isProtocolTypeInconsistent(syncResponse.data.protocolType())) {
                    log.error("SyncGroup failed due to inconsistent Protocol Type, received {} but expected {}",
                        syncResponse.data.protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    // 成功
                    log.debug("Received successful SyncGroup response: {}", syncResponse);
                    sensors.syncSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        // 加入消费组成功后，generation 会被重新赋值， state = MemberState.COMPLETING_REBALANCE
                        if (!generation.equals(Generation.NO_GENERATION) && state == MemberState.COMPLETING_REBALANCE) {
                            // check protocol name only if the generation is not reset
                            final String protocolName = syncResponse.data.protocolName();
                            final boolean protocolNameInconsistent = protocolName != null &&
                                !protocolName.equals(generation.protocolName);

                            if (protocolNameInconsistent) {
                                log.error("SyncGroup failed due to inconsistent Protocol Name, received {} but expected {}",
                                    protocolName, generation.protocolName);

                                future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                            } else {
                                // 同步消费组成功，修改状态为 MemberState.STABLE
                                log.info("Successfully synced group in generation {}", generation);
                                // 这两个是前面方法退出循环的条件
                                state = MemberState.STABLE;
                                rejoinNeeded = false;
                                // record rebalance latency
                                // 记录下 re-balance 结束时间
                                lastRebalanceEndMs = time.milliseconds();
                                sensors.successfulRebalanceSensor.record(lastRebalanceEndMs - lastRebalanceStartMs);
                                lastRebalanceStartMs = -1L;

                                // 这里的 future 是 compose() 方法返回值 = onJoinLeader().chain(future)/onJoinFollower.chain(future)
                                // 是 SYNC_GROUP 的异步请求（不是 client.send）结果。
                                // 这里调用 future.complete() 是会将方法的请求结果保存到 SYNC_GROUP 请求的异步请求结果中
                                // 同时还会调用 future 里的 listeners，在 JOIN_GROUP 的回调中，将 JOIN_GROUP 的异步请求结果作为监听器
                                // 加入到了这个 future 中。
                                // chain(future) 方法里参数的 future = sendJoinGroupRequest 的返回值，即 joinFuture.
                                // 所以这个 complete 方法里调用的监听器实际上是 joinFuture.complete() 方法，即将 SYNC_GROUP 的结果
                                // 也保存到 JOIN_GROUP 的异步请求结果中，至此 JOIN_GROUP 里的 result 就有值，这个请求就完成了。
                                future.complete(ByteBuffer.wrap(syncResponse.data.assignment()));
                            }
                        } else {
                            log.info("Generation data was cleared by heartbeat thread to {} and state is now {} before " +
                                "receiving SyncGroup response, marking this rebalance as failed and retry",
                                generation, state);
                            // use ILLEGAL_GENERATION error code to let it retry immediately
                            future.raise(Errors.ILLEGAL_GENERATION);
                        }
                    }
                }
            } else {
                // 同步失败，重置 rejoin 标识，表示这个消费者需要重新加入消费组
                requestRejoin();

                if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                    future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
                } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                    log.info("SyncGroup failed: The group began another rebalance. Need to re-join the group. " +
                                 "Sent generation was {}", sentGeneration);
                    future.raise(error);
                } else if (error == Errors.FENCED_INSTANCE_ID) {
                    // for sync-group request, even if the generation has changed we would not expect the instance id
                    // gets fenced, and hence we always treat this as a fatal error
                    log.error("SyncGroup failed: The group instance id {} has been fenced by another instance. " +
                        "Sent generation was {}", rebalanceConfig.groupInstanceId, sentGeneration);
                    future.raise(error);
                } else if (error == Errors.UNKNOWN_MEMBER_ID
                        || error == Errors.ILLEGAL_GENERATION) {
                    log.info("SyncGroup failed: {} Need to re-join the group. Sent generation was {}",
                            error.message(), sentGeneration);
                    if (generationUnchanged())
                        resetGenerationOnResponseError(ApiKeys.SYNC_GROUP, error);

                    future.raise(error);
                } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                        || error == Errors.NOT_COORDINATOR) {
                    log.info("SyncGroup failed: {} Marking coordinator unknown. Sent generation was {}",
                             error.message(), sentGeneration);
                    markCoordinatorUnknown(error);
                    future.raise(error);
                } else {
                    future.raise(new KafkaException("Unexpected error from SyncGroup: " + error.message()));
                }
            }
        }
    }

    /**
     * Discover the current coordinator for the group. Sends a GroupMetadata request to
     * one of the brokers. The returned future should be polled to get the result of the request.
     * @return A request future which indicates the completion of the metadata request
     */
    private RequestFuture<Void> sendFindCoordinatorRequest(Node node) {
        // initiate the group metadata request
        log.debug("Sending FindCoordinator request to broker {}", node);
        /*
         * 封装请求，ApiKeys = FIND_COORDINATOR
         * 在 KafkaApis 中的处理逻辑是: 先对 groupId 进行 hashCode，并对该消费组订阅的 topic 的 partition 数量进行取模
         * (Utils.abs(groupId.hashCode) % groupMetadataTopicPartitionCount) 得到 partitionIndex，然后找到这个分区
         * 号的 leader 分区在哪台服务节点上，就用该节点作为协调节点
         */
        FindCoordinatorRequest.Builder requestBuilder =
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                            .setKeyType(CoordinatorType.GROUP.id())
                            .setKey(this.rebalanceConfig.groupId));
        // 发送请求，请求中添加了 RequestFutureCompletionHandler 回调，在服务端完成响应后调用该回调函数会将该回调添加到
        // pendingCompletion 这个队列中。
        // 给 Future 添加新的 Handler，在请求完成处理后，最终会执行 FindCoordinatorResponseHandler 的 onSuccess 方法，完成协调器的创建
        return client.send(node, requestBuilder)
                .compose(new FindCoordinatorResponseHandler()); // 适配器模式
    }

    private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {
        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received FindCoordinator response {}", resp);
            clearFindCoordinatorFuture();

            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) resp.responseBody();
            Errors error = findCoordinatorResponse.error();
            if (error == Errors.NONE) {
                synchronized (AbstractCoordinator.this) {
                    // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                    // for the coordinator in the underlying network client layer
                    // 使用 Integer.MAX_VALUE - 找到的节点的id 协调器的 id，这样可以允许协调器单独连接
                    int coordinatorConnectionId = Integer.MAX_VALUE - findCoordinatorResponse.data().nodeId();

                    // 执行路径
                    // ConsumerCoordinator.poll -> ensureCoordinatorReady
                    //  1）lookupCoordinator -> sendFindCoordinatorRequest-> client.send().compose()  添加监听器
                    //  2）ConsumerNetworkClient.poll  --> NetworkClient.poll (发送请求，接收响应，执行回调 RequestFutureCompletionHandler)
                    //  3）ConsumerNetworkClient.poll 循环执行，firePendingCompletedRequests -> completionHandler.fireCompletion()
                    // 创建协调器对象
                    AbstractCoordinator.this.coordinator = new Node(
                            coordinatorConnectionId,                    // 找到作为协调器的服务节点
                            findCoordinatorResponse.data().host(),      // host
                            findCoordinatorResponse.data().port());     // port
                    log.info("Discovered group coordinator {}", coordinator);
                    // 尝试为协调器创建网络连接
                    client.tryConnect(coordinator);
                    // 重置 session timeout
                    heartbeat.resetSessionTimeout();
                }
                // 这里的 future 对象不是发送请求返回的 Future，而是 client.send(node, requestBuilder).compose() 方法里创建的
                // 一个新的 future
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                log.debug("Group coordinator lookup failed: {}", findCoordinatorResponse.data().errorMessage());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            clearFindCoordinatorFuture();
            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        return checkAndGetCoordinator() == null;
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     *
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node checkAndGetCoordinator() {
        if (coordinator != null && client.isUnavailable(coordinator)) {
            markCoordinatorUnknown(true, "coordinator unavailable");
            return null;
        }
        return this.coordinator;
    }

    private synchronized Node coordinator() {
        return this.coordinator;
    }


    protected synchronized void markCoordinatorUnknown(Errors error) {
        markCoordinatorUnknown(false, "error response " + error.name());
    }

    protected synchronized void markCoordinatorUnknown(String cause) {
        markCoordinatorUnknown(false, cause);
    }

    protected synchronized void markCoordinatorUnknown(boolean isDisconnected, String cause) {
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}."
                    + "isDisconnected: {}. Rediscovery will be attempted.", this.coordinator,
                    cause, isDisconnected);
            Node oldCoordinator = this.coordinator;

            // Mark the coordinator dead before disconnecting requests since the callbacks for any pending
            // requests may attempt to do likewise. This also prevents new requests from being sent to the
            // coordinator while the disconnect is in progress.
            // coordinator = null，1）阻止发向这个节点请求 2）其他流程可以开始找新的 coordinator
            this.coordinator = null;

            // Disconnect from the coordinator to ensure that there are no in-flight requests remaining.
            // Pending callbacks will be invoked with a DisconnectException on the next call to poll.
            if (!isDisconnected)
                client.disconnectAsync(oldCoordinator);
        }
    }

    /**
     * Get the current generation state, regardless of whether it is currently stable.
     * Note that the generation information can be updated while we are still in the middle
     * of a rebalance, after the join-group response is received.
     *
     * @return the current generation
     */
    protected synchronized Generation generation() {
        return generation;
    }

    /**
     * Get the current generation state if the group is stable, otherwise return null
     *
     * @return the current generation or null
     */
    protected synchronized Generation generationIfStable() {
        if (this.state != MemberState.STABLE)
            return null;
        return generation;
    }

    protected synchronized boolean rebalanceInProgress() {
        return this.state == MemberState.PREPARING_REBALANCE || this.state == MemberState.COMPLETING_REBALANCE;
    }

    protected synchronized String memberId() {
        return generation.memberId;
    }

    private synchronized void resetState() {
        state = MemberState.UNJOINED;
        generation = Generation.NO_GENERATION;
    }

    private synchronized void resetStateAndRejoin() {
        resetState();
        rejoinNeeded = true;
    }

    synchronized void resetGenerationOnResponseError(ApiKeys api, Errors error) {
        log.debug("Resetting generation after encountering {} from {} response and requesting re-join", error, api);

        resetStateAndRejoin();
    }

    synchronized void resetGenerationOnLeaveGroup() {
        log.debug("Resetting generation due to consumer pro-actively leaving the group");

        resetStateAndRejoin();
    }

    public synchronized void requestRejoin() {
        this.rejoinNeeded = true;
    }

    private boolean isProtocolTypeInconsistent(String protocolType) {
        return protocolType != null && !protocolType.equals(protocolType());
    }

    /**
     * Close the coordinator, waiting if needed to send LeaveGroup.
     */
    @Override
    public final void close() {
        close(time.timer(0));
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    protected void close(Timer timer) {
        try {
            closeHeartbeatThread();
        } finally {
            // Synchronize after closing the heartbeat thread since heartbeat thread
            // needs this lock to complete and terminate after close flag is set.
            synchronized (this) {
                if (rebalanceConfig.leaveGroupOnClose) {
                    onLeavePrepare();
                    maybeLeaveGroup("the consumer is being closed");
                }

                // At this point, there may be pending commits (async commits or sync commits that were
                // interrupted using wakeup) and the leave group request which have been queued, but not
                // yet sent to the broker. Wait up to close timeout for these pending requests to be processed.
                // If coordinator is not known, requests are aborted.
                Node coordinator = checkAndGetCoordinator();
                if (coordinator != null && !client.awaitPendingRequests(coordinator, timer))
                    log.warn("Close timed out with {} pending requests to coordinator, terminating client connections",
                            client.pendingRequestCount(coordinator));
            }
        }
    }

    /**
     * @throws KafkaException if the rebalance callback throws exception
     */
    public synchronized RequestFuture<Void> maybeLeaveGroup(String leaveReason) {
        RequestFuture<Void> future = null;

        // Starting from 2.3, only dynamic members will send LeaveGroupRequest to the broker,
        // consumer with valid group.instance.id is viewed as static member that never sends LeaveGroup,
        // and the membership expiration is only controlled by session timeout.
        if (isDynamicMember() && !coordinatorUnknown() &&
            state != MemberState.UNJOINED && generation.hasMemberId()) {
            // this is a minimal effort attempt to leave the group. we do not
            // attempt any resending if the request fails or times out.
            log.info("Member {} sending LeaveGroup request to coordinator {} due to {}",
                generation.memberId, coordinator, leaveReason);
            LeaveGroupRequest.Builder request = new LeaveGroupRequest.Builder(
                rebalanceConfig.groupId,
                Collections.singletonList(new MemberIdentity().setMemberId(generation.memberId))
            );

            future = client.send(coordinator, request).compose(new LeaveGroupResponseHandler(generation));
            client.pollNoWakeup();
        }

        resetGenerationOnLeaveGroup();

        return future;
    }

    protected boolean isDynamicMember() {
        return !rebalanceConfig.groupInstanceId.isPresent();
    }

    private class LeaveGroupResponseHandler extends CoordinatorResponseHandler<LeaveGroupResponse, Void> {
        private LeaveGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(LeaveGroupResponse leaveResponse, RequestFuture<Void> future) {
            final List<MemberResponse> members = leaveResponse.memberResponses();
            if (members.size() > 1) {
                future.raise(new IllegalStateException("The expected leave group response " +
                                                           "should only contain no more than one member info, however get " + members));
            }

            final Errors error = leaveResponse.error();
            if (error == Errors.NONE) {
                log.debug("LeaveGroup response with {} returned successfully: {}", sentGeneration, response);
                future.complete(null);
            } else {
                log.error("LeaveGroup request with {} failed with error: {}", sentGeneration, error.message());
                future.raise(error);
            }
        }
    }

    // visible for testing
    synchronized RequestFuture<Void> sendHeartbeatRequest() {
        log.debug("Sending Heartbeat request with generation {} and member id {} to coordinator {}",
            generation.generationId, generation.memberId, coordinator);
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(new HeartbeatRequestData()
                        .setGroupId(rebalanceConfig.groupId)
                        .setMemberId(this.generation.memberId)
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setGenerationId(this.generation.generationId));
        // 发送心跳给协调者
        return client.send(coordinator, requestBuilder)
                .compose(new HeartbeatResponseHandler(generation)); // 回调
    }

    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        private HeartbeatResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            sensors.heartbeatSensor.record(response.requestLatencyMs());
            Errors error = heartbeatResponse.error();

            if (error == Errors.NONE) {
                log.debug("Received successful Heartbeat response");
                future.complete(null);
                // 发送后 coordinator 断连或已经改变
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                log.info("Attempt to heartbeat failed since coordinator {} is either not started or not valid",
                        coordinator());
                // 协调者对象发生变化，重置协调者对象为空
                markCoordinatorUnknown(error);
                future.raise(error);
                // 正在 re-balance
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                // since we may be sending the request during rebalance, we should check
                // this case and ignore the REBALANCE_IN_PROGRESS error
                if (state == MemberState.STABLE) {
                    log.info("Attempt to heartbeat failed since group is rebalancing");
                    // 这里为什么要 rejoin，因为 state = STABLE 或者 CompleteRebalance，如果有新的消费者加入消费组，消费组状态会被
                    // 重置为 PreparingRebalance，服务端正在再平衡，所以需要重新加入消费组
                    requestRejoin();
                    future.raise(error);
                } else {
                    log.debug("Ignoring heartbeat response with error {} during {} state", error, state);
                    future.complete(null);
                }
            } else if (error == Errors.ILLEGAL_GENERATION ||
                       error == Errors.UNKNOWN_MEMBER_ID ||
                       error == Errors.FENCED_INSTANCE_ID) {
                if (generationUnchanged()) {
                    log.info("Attempt to heartbeat with {} and group instance id {} failed due to {}, resetting generation",
                        sentGeneration, rebalanceConfig.groupInstanceId, error);
                    resetGenerationOnResponseError(ApiKeys.HEARTBEAT, error);
                    future.raise(error);
                } else {
                    // if the generation has changed, then ignore this error
                    log.info("Attempt to heartbeat with stale {} and group instance id {} failed due to {}, ignoring the error",
                        sentGeneration, rebalanceConfig.groupInstanceId, error);
                    future.complete(null);
                }
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                future.raise(new KafkaException("Unexpected error in heartbeat response: " + error.message()));
            }
        }
    }

    protected abstract class CoordinatorResponseHandler<R, T> extends RequestFutureAdapter<ClientResponse, T> {
        CoordinatorResponseHandler(final Generation generation) {
            this.sentGeneration = generation;
        }

        final Generation sentGeneration;
        ClientResponse response;

        public abstract void handle(R response, RequestFuture<T> future);

        @Override
        public void onFailure(RuntimeException e, RequestFuture<T> future) {
            // mark the coordinator as dead
            if (e instanceof DisconnectException) {
                markCoordinatorUnknown(true, e.getMessage());
            }
            future.raise(e);
        }

        @Override
        @SuppressWarnings("unchecked")
        public void onSuccess(ClientResponse clientResponse, RequestFuture<T> future) {
            try {
                this.response = clientResponse;
                R responseObj = (R) clientResponse.responseBody();
                handle(responseObj, future);
            } catch (RuntimeException e) {
                if (!future.isDone())
                    future.raise(e);
            }
        }

        boolean generationUnchanged() {
            synchronized (AbstractCoordinator.this) {
                return generation.equals(sentGeneration);
            }
        }
    }

    protected Meter createMeter(Metrics metrics, String groupName, String baseName, String descriptiveName) {
        return new Meter(new WindowedCount(),
                metrics.metricName(baseName + "-rate", groupName,
                        String.format("The number of %s per second", descriptiveName)),
                metrics.metricName(baseName + "-total", groupName,
                        String.format("The total number of %s", descriptiveName)));
    }

    private class GroupCoordinatorMetrics {
        public final String metricGrpName;

        public final Sensor heartbeatSensor;
        public final Sensor joinSensor;
        public final Sensor syncSensor;
        public final Sensor successfulRebalanceSensor;
        public final Sensor failedRebalanceSensor;

        public GroupCoordinatorMetrics(Metrics metrics, String metricGrpPrefix) {
            this.metricGrpName = metricGrpPrefix + "-coordinator-metrics";

            this.heartbeatSensor = metrics.sensor("heartbeat-latency");
            this.heartbeatSensor.add(metrics.metricName("heartbeat-response-time-max",
                this.metricGrpName,
                "The max time taken to receive a response to a heartbeat request"), new Max());
            this.heartbeatSensor.add(createMeter(metrics, metricGrpName, "heartbeat", "heartbeats"));

            this.joinSensor = metrics.sensor("join-latency");
            this.joinSensor.add(metrics.metricName("join-time-avg",
                this.metricGrpName,
                "The average time taken for a group rejoin"), new Avg());
            this.joinSensor.add(metrics.metricName("join-time-max",
                this.metricGrpName,
                "The max time taken for a group rejoin"), new Max());
            this.joinSensor.add(createMeter(metrics, metricGrpName, "join", "group joins"));

            this.syncSensor = metrics.sensor("sync-latency");
            this.syncSensor.add(metrics.metricName("sync-time-avg",
                this.metricGrpName,
                "The average time taken for a group sync"), new Avg());
            this.syncSensor.add(metrics.metricName("sync-time-max",
                this.metricGrpName,
                "The max time taken for a group sync"), new Max());
            this.syncSensor.add(createMeter(metrics, metricGrpName, "sync", "group syncs"));

            this.successfulRebalanceSensor = metrics.sensor("rebalance-latency");
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-avg",
                this.metricGrpName,
                "The average time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Avg());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-max",
                this.metricGrpName,
                "The max time taken for a group to complete a successful rebalance, which may be composed of " +
                    "several failed re-trials until it succeeded"), new Max());
            this.successfulRebalanceSensor.add(metrics.metricName("rebalance-latency-total",
                this.metricGrpName,
                "The total number of milliseconds this consumer has spent in successful rebalances since creation"),
                new CumulativeSum());
            this.successfulRebalanceSensor.add(
                metrics.metricName("rebalance-total",
                    this.metricGrpName,
                    "The total number of successful rebalance events, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new CumulativeCount()
            );
            this.successfulRebalanceSensor.add(
                metrics.metricName(
                    "rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of successful rebalance events per hour, each event is composed of " +
                        "several failed re-trials until it succeeded"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            this.failedRebalanceSensor = metrics.sensor("failed-rebalance");
            this.failedRebalanceSensor.add(
                metrics.metricName("failed-rebalance-total",
                    this.metricGrpName,
                    "The total number of failed rebalance events"),
                new CumulativeCount()
            );
            this.failedRebalanceSensor.add(
                metrics.metricName(
                    "failed-rebalance-rate-per-hour",
                    this.metricGrpName,
                    "The number of failed rebalance events per hour"),
                new Rate(TimeUnit.HOURS, new WindowedCount())
            );

            Measurable lastRebalance = (config, now) -> {
                if (lastRebalanceEndMs == -1L)
                    // if no rebalance is ever triggered, we just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - lastRebalanceEndMs, TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-rebalance-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last successful rebalance event"),
                lastRebalance);

            Measurable lastHeartbeat = (config, now) -> {
                if (heartbeat.lastHeartbeatSend() == 0L)
                    // if no heartbeat is ever triggered, just return -1.
                    return -1d;
                else
                    return TimeUnit.SECONDS.convert(now - heartbeat.lastHeartbeatSend(), TimeUnit.MILLISECONDS);
            };
            metrics.addMetric(metrics.metricName("last-heartbeat-seconds-ago",
                this.metricGrpName,
                "The number of seconds since the last coordinator heartbeat was sent"),
                lastHeartbeat);
        }
    }

    private class HeartbeatThread extends KafkaThread implements AutoCloseable {
        // 是否可以发送心跳
        private boolean enabled = false;
        private boolean closed = false;
        private final AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        private HeartbeatThread() {
            super(HEARTBEAT_THREAD_PREFIX + (rebalanceConfig.groupId.isEmpty() ? "" : " | " + rebalanceConfig.groupId), true);
        }

        /**
         * 这个方法是在当前的消费者成功加入到消费组后心跳检测才能开始
         */
        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Enabling heartbeat thread");
                this.enabled = true;
                heartbeat.resetTimeouts();
                AbstractCoordinator.this.notify();
            }
        }

        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Disabling heartbeat thread");
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                AbstractCoordinator.this.notify();
            }
        }

        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread started");
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        if (closed)
                            return;

                        // enable = false，心跳检测暂时不能开始，阻塞直到消费者加入消费组后调用 enable() 唤醒后才开始执行...
                        if (!enabled) {
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        // we do not need to heartbeat we are not part of a group yet;
                        // also if we already have fatal error, the client will be
                        // crashed soon, hence we do not need to continue heartbeating either
                        // 1. 首次启动当前消费者还没有加入到消费组，不需要发送心跳。
                        // 2. 如果消费组之前是平衡状态，但是发生了再平衡，如新的消费者加入，此时心跳请求会返回再平衡异常，在回调中
                        // 它会将 rejoinNeeded 变量设置为 true，这样消费者在下次轮询时就会重新加入消费组，此时，state 就不再是
                        // Stable 而是 PREPARING_REBALANCE，所以在再平衡阶段，消费者客户端是停止心跳的。
                        // 3. 这里有个问题，再平衡阶段，消费者停止上报心跳信息，协调者是如何确认消费者还是存活的呢？在再平衡阶段，服务端也会维护
                        // 一个延迟的心跳任务，当消费者与协调者有任务交互时（如发送同步请求，返回响应等）时，就会执行一次心跳任务，
                        // 来保证消费者和协调者的连接状态。如果服务端心跳任务超时，协调者才会认为消费者掉线（超时不一定是掉线的充分条件），
                        // 将它从消费组中移除。这也是为什么服务端也需要心跳的原因。
                        if (state.hasNotJoinedGroup() || hasFailed()) {
                            // 再平衡时被 disable，如果消费者加入消费组成功，会重新调用 enable 方法
                            disable();
                            continue;
                        }

                        // poll, 执行网络 I/O，实际发送心跳请求
                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        if (coordinatorUnknown()) {
                            if (findCoordinatorFuture != null || lookupCoordinator().failed())
                                // the immediate future check ensures that we backoff properly in the case that no
                                // brokers are available to connect to.
                                AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            // 长时间没有收到心跳，需要重新找新的 coordinator 并且将之前的连接断开....
                            markCoordinatorUnknown("session timed out without receiving a "
                                    + "heartbeat response");
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll().
                            String leaveReason = "consumer poll timeout has expired. This means the time between subsequent calls to poll() " +
                                                    "was longer than the configured max.poll.interval.ms, which typically implies that " +
                                                    "the poll loop is spending too much time processing messages. " +
                                                    "You can address this either by increasing max.poll.interval.ms or by reducing " +
                                                    "the maximum size of batches returned in poll() with max.poll.records.";
                            // 这个操作可能会重置 state 和 generation 信息
                            maybeLeaveGroup(leaveReason);
                            // 是否到发送心跳检测的时间，等待到发送时间
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else {
                            heartbeat.sentHeartbeat(now);
                            // 发送心跳
                            final RequestFuture<Void> heartbeatFuture = sendHeartbeatRequest();
                            // 添加回调
                            heartbeatFuture.addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (AbstractCoordinator.this) {
                                        // 成功，更新下这次心跳的时间和成功标记
                                        heartbeat.receiveHeartbeat();
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
                                        // 如果正在 re-balance 暂时不能认为是失败的，如果后续 re-balance 成功后，会继续发送心跳检测
                                        // 防止 session timeout
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            heartbeat.receiveHeartbeat();
                                        } else if (e instanceof FencedInstanceIdException) {
                                            log.error("Caught fenced group.instance.id {} error in heartbeat thread", rebalanceConfig.groupInstanceId);
                                            heartbeatThread.failed.set(e);
                                        } else {
                                            heartbeat.failHeartbeat();
                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            AbstractCoordinator.this.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (AuthenticationException e) {
                log.error("An authentication error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (GroupAuthorizationException e) {
                log.error("A group authorization error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread", e);
                this.failed.set(new RuntimeException(e));
            } catch (Throwable e) {
                log.error("Heartbeat thread failed due to unexpected error", e);
                if (e instanceof RuntimeException)
                    this.failed.set((RuntimeException) e);
                else
                    this.failed.set(new RuntimeException(e));
            } finally {
                log.debug("Heartbeat thread has closed");
            }
        }

    }

    protected static class Generation {
        public static final Generation NO_GENERATION = new Generation(
                OffsetCommitRequest.DEFAULT_GENERATION_ID,
                JoinGroupRequest.UNKNOWN_MEMBER_ID,
                null);

        public final int generationId;
        public final String memberId;
        public final String protocolName;

        public Generation(int generationId, String memberId, String protocolName) {
            this.generationId = generationId;
            this.memberId = memberId;
            this.protocolName = protocolName;
        }

        /**
         * @return true if this generation has a valid member id, false otherwise. A member might have an id before
         * it becomes part of a group generation.
         */
        public boolean hasMemberId() {
            return !memberId.isEmpty();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Generation that = (Generation) o;
            return generationId == that.generationId &&
                    Objects.equals(memberId, that.memberId) &&
                    Objects.equals(protocolName, that.protocolName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(generationId, memberId, protocolName);
        }

        @Override
        public String toString() {
            return "Generation{" +
                    "generationId=" + generationId +
                    ", memberId='" + memberId + '\'' +
                    ", protocol='" + protocolName + '\'' +
                    '}';
        }
    }

    @SuppressWarnings("serial")
    private static class UnjoinedGroupException extends RetriableException {

    }

    // For testing only below
    final Heartbeat heartbeat() {
        return heartbeat;
    }

    final synchronized void setLastRebalanceTime(final long timestamp) {
        lastRebalanceEndMs = timestamp;
    }

    /**
     * Check whether given generation id is matching the record within current generation.
     *
     * @param generationId generation id
     * @return true if the two ids are matching.
     */
    final boolean hasMatchingGenerationId(int generationId) {
        return !generation.equals(Generation.NO_GENERATION) && generation.generationId == generationId;
    }

    final boolean hasUnknownGeneration() {
        return generation.equals(Generation.NO_GENERATION);
    }

    /**
     * @return true if the current generation's member ID is valid, false otherwise
     */
    final boolean hasValidMemberId() {
        return !hasUnknownGeneration() && generation.hasMemberId();
    }

    final synchronized void setNewGeneration(final Generation generation) {
        this.generation = generation;
    }

    final synchronized void setNewState(final MemberState state) {
        this.state = state;
    }
}
