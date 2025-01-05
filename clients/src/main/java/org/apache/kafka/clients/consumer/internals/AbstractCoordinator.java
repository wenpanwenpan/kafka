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

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

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
 */
public abstract class AbstractCoordinator implements Closeable {
    public static final String HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread";
    public static final int JOIN_GROUP_TIMEOUT_LAPSE = 5000;

    // 消费者协调器状态
    protected enum MemberState {
        // 未入组
        UNJOINED,             // the client is not part of a group
        // 准备入组中（此时已经发起了JOIN_GROUP请求，但还未收到响应）
        PREPARING_REBALANCE,  // the client has sent the join group request, but have not received response
        // 组重平衡中(此时已经收到了JOIN_GROUP请求响应，并且发起了SYNC_GROUP请求，正在等待组协调器发放分区分配方案)
        COMPLETING_REBALANCE, // the client has received join group response, but have not received assignment
        // 已经完成了入组，已经获取到了分区分配方案并且开始了心跳
        STABLE;               // the client has joined and is sending heartbeats

        public boolean hasNotJoinedGroup() {
            return equals(UNJOINED) || equals(PREPARING_REBALANCE);
        }
    }

    private final Logger log;
    // 心跳对象
    private final Heartbeat heartbeat;
    // 组协调器监控指标
    private final GroupCoordinatorMetrics sensors;
    // 消费者组重分配配置
    private final GroupRebalanceConfig rebalanceConfig;

    protected final Time time;
    // 负责网络通信
    protected final ConsumerNetworkClient client;

    // 组协调器所在的节点信息（该属性是在收到了FindCoordinator请求响应后通过响应信息构建出来的）
    private Node coordinator = null;
    // 是否需要重新加入组
    private boolean rejoinNeeded = true;
    // 是否需要重新准备加入组
    private boolean needsJoinPrepare = true;
    // 心跳线程
    private HeartbeatThread heartbeatThread = null;
    // 发送完加入组请求后返回的future对象
    private RequestFuture<ByteBuffer> joinFuture = null;
    // 发送完查找组协调器请求后返回的future对象
    private RequestFuture<Void> findCoordinatorFuture = null;
    // 查找组协调器时出现的异常
    private volatile RuntimeException fatalFindCoordinatorException = null;
    // 在接收到组协调器的JOIN_GROUP响应后赋值
    private Generation generation = Generation.NO_GENERATION;
    // 最近一次重平衡开始时间
    private long lastRebalanceStartMs = -1L;
    // 最近一次重平衡结束时间
    private long lastRebalanceEndMs = -1L;
    // 最近一次连接组协调器时间
    private long lastTimeOfConnectionMs = -1L; // starting logging a warning only after unable to connect for a while

    // 消费者协调器的状态
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
     */
    protected synchronized boolean ensureCoordinatorReady(final Timer timer) {
        // 如果组协调器已知，则直接返回true
        if (!coordinatorUnknown())
            return true;

        do {
            if (fatalFindCoordinatorException != null) {
                final RuntimeException fatalException = fatalFindCoordinatorException;
                fatalFindCoordinatorException = null;
                throw fatalException;
            }
            // 【重要】发起查找该消费者的组协调器请求（发送到unsent缓冲里并注册回调）
            final RequestFuture<Void> future = lookupCoordinator();
            // 将unsent里的所有请求队列里的请求发送出去（这里会循环的在future上等待发送的lookupCoordinator请求响应或超时）
            client.poll(future, timer);

            // 如果还没有收到响应(说明请求超时)，则跳出循环
            if (!future.isDone()) {
                // ran out of time
                break;
            }

            RuntimeException fatalException = null;

            // 如果查找组协调器的请求有响应了，且响应是失败
            if (future.failed()) {
                if (future.isRetriable()) {
                    log.debug("Coordinator discovery failed, refreshing metadata", future.exception());
                    client.awaitMetadataUpdate(timer);
                } else {
                    fatalException = future.exception();
                    log.info("FindCoordinator request hit fatal exception", fatalException);
                }
                // 如果组协调器未知，则休眠一会儿
            } else if (coordinator != null && client.isUnavailable(coordinator)) {
                // we found the coordinator, but the connection has failed, so mark
                // it dead and backoff before retrying discovery
                markCoordinatorUnknown("coordinator unavailable");
                // 休眠一会儿
                timer.sleep(rebalanceConfig.retryBackoffMs);
            }

            // 将发起查找组协调器请求时返回的future清空
            clearFindCoordinatorFuture();
            if (fatalException != null)
                throw fatalException;
            // 如果没有找到组协调器 并且 未超时，则一直循环
        } while (coordinatorUnknown() && timer.notExpired());

        // 返回是否有组协调器
        return !coordinatorUnknown();
    }

    protected synchronized RequestFuture<Void> lookupCoordinator() {
        // findCoordinatorFuture == null 说明该消费者没有正在进行中的查找组协调器请求
        if (findCoordinatorFuture == null) {
            // find a node to ask about the coordinator
            // 选择一个负载最小的broker
            Node node = this.client.leastLoadedNode();
            if (node == null) {
                log.debug("No broker available to send FindCoordinator request");
                return RequestFuture.noBrokersAvailable();
            } else {
                // 给该node发送一个查找组协调器的请求 find_coordinator
                findCoordinatorFuture = sendFindCoordinatorRequest(node);
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
        // rejoinNeeded = true说明需要重新加入组，joinFuture != null说明正在发送JOIN_GROUP请求
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
                // 如果心跳线程发生故障，则将heartbeatThread置为空并抛出异常。如果用户捕获了异常，则下次调用ensureActiveGroup()会重新创建一个心跳线程
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }
            // Awake the heartbeat thread if needed
            // 如果应该发起心跳了，则唤醒心跳线程
            if (heartbeat.shouldHeartbeat(now)) {
                notify();
            }
            // 执行心跳（这里只是更改了几个时间，以便于下次发起心跳请求）
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
     * 可以看到在该方法里 FindCoordinator请求和Heartbeat请求以及Join_Group请求和Sync_Group请求都是在这里发起的
     *
     * @param timer Timer bounding how long this method can block
     * @throws KafkaException if the callback throws exception
     * @return true iff the group is active
     */
    boolean ensureActiveGroup(final Timer timer) {
        // always ensure that the coordinator is ready because we may have been disconnected
        // when sending heartbeats and does not necessarily require us to rejoin the group.
        // 1、再次检查下是否有找到该消费者组对应的组协调器，如果没有，则继续发出查找组协调器请求，如果还是找不到，则返回false
        // 这里会发送 find_coordinator请求，并阻塞等待请求响应
        if (!ensureCoordinatorReady(timer)) {
            return false;
        }

        // 2、如果找到了组协调器，则开启心跳线程（这里还不会立即发起心跳，只是启动了线程，具体开启的时机是join_group请求响应后），
        // 每隔3s发送心跳请求给组协调器所在的broker
        startHeartbeatThreadIfNeeded();
        // 3、进入消费者加入消费者组的流程（即发送JOIN_GROUP、SYNC_GROUP请求）
        return joinGroupIfNeeded(timer);
    }

    // 开启心跳线程，注意这里只是启动了线程，并不一定会启动心跳动作，需要线程里的enable=true才会开始心跳
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
     */
    boolean joinGroupIfNeeded(final Timer timer) {
        // 如果需要重新加入或正在发送入组请求，则一直循环
        while (rejoinNeededOrPending()) {
            // 再次确认是否有找到该消费者组对应的组协调器（如果没有找到则会尝试发起查找组协调器的请求）
            if (!ensureCoordinatorReady(timer)) {
                return false;
            }

            // call onJoinPrepare if needed. We set a flag to make sure that we do not call it a second
            // time if the client is woken up before a pending rebalance completes. This must be called
            // on each iteration of the loop because an event requiring a rebalance (such as a metadata
            // refresh which changes the matched subscription set) can occur while another rebalance is
            // still in progress.
            // 如果需要进入准备入组阶段，则调用onJoinPrepare方法来准备加入消费者组
            if (needsJoinPrepare) {
                // need to set the flag before calling onJoinPrepare since the user callback may throw
                // exception, in which case upon retry we should not retry onJoinPrepare either.
                needsJoinPrepare = false;
                onJoinPrepare(generation.generationId, generation.memberId);
            }

            // 1、准备JOIN_GROUP请求并加入到unsent缓存队列中（JOIN_GROUP、SYNC_GROUP都是在这里发起的）
            final RequestFuture<ByteBuffer> future = initiateJoinGroup();
            // 2、调用poll方法轮询处理unsent队列中的待发送的请求，将他们最终发送到socket channel里
            // 这里会阻塞等待发送结果直到超时
            client.poll(future, timer);
            // 如果已经到超时时间了还没有等到响应结果，则返回false
            if (!future.isDone()) {
                // we ran out of time
                return false;
            }

            // JOIN_GROUP 请求响应成功
            if (future.succeeded()) {
                Generation generationSnapshot;
                MemberState stateSnapshot;

                // Generation data maybe concurrently cleared by Heartbeat thread.
                // Can't use synchronized for {@code onJoinComplete}, because it can be long enough
                // and shouldn't block heartbeat thread.
                // See {@link PlaintextConsumerTest#testMaxPollIntervalMsDelayInAssignment}
                synchronized (AbstractCoordinator.this) {
                    // 复制当前的generation和state数据，防止回调期间被更改
                    generationSnapshot = this.generation;
                    stateSnapshot = this.state;
                }

                // 如果当前的generation和state有效（当加入消费者组成功后state会变为STABLE）
                if (!generationSnapshot.equals(Generation.NO_GENERATION) && stateSnapshot == MemberState.STABLE) {
                    // Duplicate the buffer in case `onJoinComplete` does not complete and needs to be retried.
                    ByteBuffer memberAssignment = future.value().duplicate();

                    // 3、【重要】如果加入消费者组的异步请求成功完成（sync_group），则协调者会将当前消费者负责的分区发送过来，
                    // 消费者调用onJoinComplete方法来处理加入消费者组后的逻辑
                    onJoinComplete(generationSnapshot.generationId, generationSnapshot.memberId, generationSnapshot.protocolName, memberAssignment);

                    // Generally speaking we should always resetJoinGroupFuture once the future is done, but here
                    // we can only reset the join group future after the completion callback returns. This ensures
                    // that if the callback is woken up, we will retry it on the next joinGroupIfNeeded.
                    // And because of that we should explicitly trigger resetJoinGroupFuture in other conditions below.
                    // join_group请求已经处理完了，当然要把future置为空了
                    resetJoinGroupFuture();
                    needsJoinPrepare = true;
                } else {
                    log.info("Generation data was cleared by heartbeat thread to {} and state is now {} before " +
                         "the rebalance callback is triggered, marking this rebalance as failed and retry",
                         generationSnapshot, stateSnapshot);
                    // 4、如果当前的generation和state无效，则重置相关属性，下次循环时重新加入消费者组
                    resetStateAndRejoin();
                    // 将发起入组请求时返回的future置为空，下次循环时使用
                    resetJoinGroupFuture();
                }
            } else {
                // 入组失败，获取异常信息
                final RuntimeException exception = future.exception();

                // we do not need to log error for memberId required,
                // since it is not really an error and is transient
                if (!(exception instanceof MemberIdRequiredException)) {
                    log.info("Rebalance failed.", exception);
                }

                // 重置入组相关属性，以便下次循环时能重新入组
                resetJoinGroupFuture();
                rejoinNeeded = true;

                resetJoinGroupFuture();
                // 如果出现如下几种异常，则重试一下
                if (exception instanceof UnknownMemberIdException ||
                    exception instanceof RebalanceInProgressException ||
                    exception instanceof IllegalGenerationException ||
                    exception instanceof MemberIdRequiredException)
                    continue;
                else if (!future.isRetriable())
                    // 如果出现不可重试异常，则跑出去
                    throw exception;

                timer.sleep(rebalanceConfig.retryBackoffMs);
            }
        }
        return true;
    }

    private synchronized void resetJoinGroupFuture() {
        this.joinFuture = null;
    }

    // 初始化入组请求（发送JOIN_GROUP请求到unsent缓冲队列中）
    private synchronized RequestFuture<ByteBuffer> initiateJoinGroup() {
        // we store the join future in case we are woken up by the user after beginning the
        // rebalance in the call to poll below. This ensures that we do not mistakenly attempt
        // to rejoin before the pending rebalance has completed.
        // 1、joinFuture == null 说明没有正在进行中的JOIN_GROUP请求
        if (joinFuture == null) {
            // 将消费者协调器状态流转为PREPARING_REBALANCE（准备重平衡）
            state = MemberState.PREPARING_REBALANCE;
            // a rebalance can be triggered consecutively if the previous one failed,
            // in this case we would not update the start time.
            // 2、如果之前重平衡失败，则可能会连续发起重平衡，这种情况下不更新时间
            if (lastRebalanceStartMs == -1L)
                lastRebalanceStartMs = time.milliseconds();
            // 创建JOIN_GROUP请求并发送到unsent发送缓冲队列中并注册回调
            joinFuture = sendJoinGroupRequest();
            // 再次添加回调（注意：处理JOIN_GROUP请求响应的回调在JoinGroupResponseHandler里进行的）
            joinFuture.addListener(new RequestFutureListener<ByteBuffer>() {
                @Override
                public void onSuccess(ByteBuffer value) {
                    // do nothing since all the handler logic are in SyncGroupResponseHandler already
                    // JOIN_GROUP请求响应成功后不做任何操作，因为响应处理逻辑已经全部写在SyncGroupResponseHandler中完成了
                }

                @Override
                public void onFailure(RuntimeException e) {
                    // we handle failures below after the request finishes. if the join completes
                    // after having been woken up, the exception is ignored and we will rejoin;
                    // this can be triggered when either join or sync request failed
                    synchronized (AbstractCoordinator.this) {
                        // 记录监控指标
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
        // 检查组协调器是否可用，如果不可用则直接返回
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();

        // send a join group request to the coordinator
        log.info("(Re-)joining group");
        // 构建入组请求JOIN_GROUP
        JoinGroupRequest.Builder requestBuilder = new JoinGroupRequest.Builder(
                new JoinGroupRequestData()
                        // 消费者组ID
                        .setGroupId(rebalanceConfig.groupId)
                        // 客户端与broker的最大会话有效期，如果超过这个时间没有任何心跳，则有可能会发起Rebalance，属性：session.timeout.ms，默认10s
                        .setSessionTimeoutMs(this.rebalanceConfig.sessionTimeoutMs)
                        // 消费者成员ID，默认就是空字符串
                        .setMemberId(this.generation.memberId)
                        // 2.3版本引入，由用户指定的消费者实例唯一标识符，如果设置了，则消费者被视为静态成员，静态成员会分配较大的session超时时间
                        // 避免因成员临时不可用而导致Rebalance，如果不设置，则消费者被认为是动态成员。配置参数：group.instance.id
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        // 协议类型，消费者的协议类型都是 consumer，另一个可选项是 connect
                        .setProtocolType(protocolType())
                        // 配置分区分配策略和对应的订阅关系信息
                        .setProtocols(metadata())
                        // 重平衡超时时间
                        .setRebalanceTimeoutMs(this.rebalanceConfig.rebalanceTimeoutMs)
        );

        log.debug("Sending JoinGroup ({}) to coordinator {}", requestBuilder, this.coordinator);

        // Note that we override the request timeout using the rebalance timeout since that is the
        // maximum time that it may block on the coordinator. We add an extra 5 seconds for small delays.
        // 由于重平衡（JOIN_GROUP）超时时间是协调器可能阻塞的最长时间，所以我们使用重平衡超时时间来覆盖请求超时时间，
        // 并且添加了5秒的额外时间用于解决可能出现的小延时
        // 为什么呢？因为JOIN_GROUP请求响应时间比较长（broker端的组协调器可能会等1分钟，等待所有的消费者发起入组请求，1分钟后才会统一响应给各个发起请求的消费者）
        int joinGroupTimeoutMs = Math.max(client.defaultRequestTimeoutMs(),
            rebalanceConfig.rebalanceTimeoutMs + JOIN_GROUP_TIMEOUT_LAPSE);
        // 发送JoinGroup请求（添加到unsent队列中）
        return client.send(coordinator, requestBuilder, joinGroupTimeoutMs)
                // 【重要】注册请求回调（会在该回调中发起SYNC_GROUP请求）
                .compose(new JoinGroupResponseHandler(generation));
    }

    // 消费者入组请求回调方法
    private class JoinGroupResponseHandler extends CoordinatorResponseHandler<JoinGroupResponse, ByteBuffer> {
        private JoinGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(JoinGroupResponse joinResponse, RequestFuture<ByteBuffer> future) {
            Errors error = joinResponse.error();
            // JOIN_GROUP请求正常响应
            if (error == Errors.NONE) {
                if (isProtocolTypeInconsistent(joinResponse.data().protocolType())) {
                    log.error("JoinGroup failed: Inconsistent Protocol Type, received {} but expected {}",
                        joinResponse.data().protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    log.debug("Received successful JoinGroup response: {}", joinResponse);
                    sensors.joinSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        // JOIN_GROUP请求响应时要求消费者协调器的状态必须是PREPARING_REBALANCE，如果状态已经被流转则抛出异常
                        if (state != MemberState.PREPARING_REBALANCE) {
                            // if the consumer was woken up before a rebalance completes, we may have already left
                            // the group. In this case, we do not want to continue with the sync group.
                            future.raise(new UnjoinedGroupException());
                        } else {
                            // 将消费者协调器的状态流转为 COMPLETING_REBALANCE（等待分配负责的分区）
                            state = MemberState.COMPLETING_REBALANCE;

                            // we only need to enable heartbeat thread whenever we transit to
                            // COMPLETING_REBALANCE state since we always transit from this state to STABLE
                            // 【注意】这时才真正的启动了与组协调器通信的心跳线程
                            if (heartbeatThread != null)
                                heartbeatThread.enable();

                            AbstractCoordinator.this.generation = new Generation(
                                joinResponse.data().generationId(),
                                joinResponse.data().memberId(), joinResponse.data().protocolName());

                            log.info("Successfully joined group with generation {}", AbstractCoordinator.this.generation);

                            // 可以看到不论该consumer是否被选为了leader，都会向组协调器发起SYNC_GROUP请求，请求获取自己所负责的分区信息
                            // 如果该消费者协调器被选为了消费者组的leader，那么该consumer还会负责分区分配操作
                            if (joinResponse.isLeader()) {
                                // 【重要】消费者组的leader会负责分区分配操作
                                onJoinLeader(joinResponse).chain(future);
                            } else {
                                // 【重要】如果当前consumer没有被选为消费者组的leader，则直接向组协调器发起SYNC_GROUP请求，请求获取自己所负责的分区信息
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
        // 向组协调器发送SYNC_GROUP请求，请求获取当前消费者负责的分区信息
        return sendSyncGroupRequest(requestBuilder);
    }

    private RequestFuture<ByteBuffer> onJoinLeader(JoinGroupResponse joinResponse) {
        try {
            // perform the leader synchronization and send back the assignment for the group
            // 1、【重要】调用performAssignment方法进行分区分配
            Map<String, ByteBuffer> groupAssignment = performAssignment(joinResponse.data().leader(), joinResponse.data().protocolName(),
                    joinResponse.data().members());

            List<SyncGroupRequestData.SyncGroupRequestAssignment> groupAssignmentList = new ArrayList<>();
            for (Map.Entry<String, ByteBuffer> assignment : groupAssignment.entrySet()) {
                // 2、构建SYNC_GROUP请求数据
                groupAssignmentList.add(new SyncGroupRequestData.SyncGroupRequestAssignment()
                        // 哪个消费者成员
                        .setMemberId(assignment.getKey())
                        // 分配的分区是哪些
                        .setAssignment(Utils.toArray(assignment.getValue()))
                );
            }

            // 3、构建请求对象，请求的类型是 SYNC_GROUP
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
            // 4、发起SYNC_GROUP请求并注册回调
            return sendSyncGroupRequest(requestBuilder);
        } catch (RuntimeException e) {
            return RequestFuture.failure(e);
        }
    }

    private RequestFuture<ByteBuffer> sendSyncGroupRequest(SyncGroupRequest.Builder requestBuilder) {
        // 检查组协调器是否可用
        if (coordinatorUnknown())
            return RequestFuture.coordinatorNotAvailable();
        // 发送SYNC_GROUP请求到unsent缓冲队列
        return client.send(coordinator, requestBuilder)
                // 注册回调
                .compose(new SyncGroupResponseHandler(generation));
    }

    private class SyncGroupResponseHandler extends CoordinatorResponseHandler<SyncGroupResponse, ByteBuffer> {
        private SyncGroupResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(SyncGroupResponse syncResponse,
                           RequestFuture<ByteBuffer> future) {
            Errors error = syncResponse.error();
            // SYNC_GROUP请求正常响应
            if (error == Errors.NONE) {
                // 检查协议类型是否一致
                if (isProtocolTypeInconsistent(syncResponse.data.protocolType())) {
                    log.error("SyncGroup failed due to inconsistent Protocol Type, received {} but expected {}",
                        syncResponse.data.protocolType(), protocolType());
                    future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                } else {
                    log.debug("Received successful SyncGroup response: {}", syncResponse);
                    sensors.syncSensor.record(response.requestLatencyMs());

                    synchronized (AbstractCoordinator.this) {
                        // state == MemberState.COMPLETING_REBALANCE 说明当前消费者正在等待分配分区（发起SYNC_GROUP请求后状态就会流转为COMPLETING_REBALANCE）
                        if (!generation.equals(Generation.NO_GENERATION) && state == MemberState.COMPLETING_REBALANCE) {
                            // check protocol name only if the generation is not reset
                            final String protocolName = syncResponse.data.protocolName();
                            final boolean protocolNameInconsistent = protocolName != null &&
                                !protocolName.equals(generation.protocolName);

                            // 协议名称不一致，则抛出异常
                            if (protocolNameInconsistent) {
                                log.error("SyncGroup failed due to inconsistent Protocol Name, received {} but expected {}",
                                    protocolName, generation.protocolName);

                                future.raise(Errors.INCONSISTENT_GROUP_PROTOCOL);
                            } else {
                                log.info("Successfully synced group in generation {}", generation);
                                // 状态流转为STABLE（表示此时该消费者可以正常消费分区了）
                                state = MemberState.STABLE;
                                // 不需要重新入组了
                                rejoinNeeded = false;
                                // record rebalance latency
                                // 记录最近一次重平衡的时间戳
                                lastRebalanceEndMs = time.milliseconds();
                                // 记录指标
                                sensors.successfulRebalanceSensor.record(lastRebalanceEndMs - lastRebalanceStartMs);
                                lastRebalanceStartMs = -1L;

                                // 回调注册在该future上的等待分区分配的回调方法
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
                // 需要重新入组
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
        // 构建请求信息（里面设置的请求类型是FIND_COORDINATOR）
        FindCoordinatorRequest.Builder requestBuilder =
                new FindCoordinatorRequest.Builder(
                        new FindCoordinatorRequestData()
                            .setKeyType(CoordinatorType.GROUP.id())
                            .setKey(this.rebalanceConfig.groupId));
        // 发送请求（其实是将请求放入client里的unsent发送队列里）
        return client.send(node, requestBuilder)
                // 注册请求回调
                .compose(new FindCoordinatorResponseHandler());
    }

    // 查找组协调器请求响应处理handler
    private class FindCoordinatorResponseHandler extends RequestFutureAdapter<ClientResponse, Void> {

        @Override
        public void onSuccess(ClientResponse resp, RequestFuture<Void> future) {
            log.debug("Received FindCoordinator response {}", resp);

            // 解析FindCoordinator请求的响应数据
            FindCoordinatorResponse findCoordinatorResponse = (FindCoordinatorResponse) resp.responseBody();
            Errors error = findCoordinatorResponse.error();
            if (error == Errors.NONE) {
                synchronized (AbstractCoordinator.this) {
                    // use MAX_VALUE - node.id as the coordinator id to allow separate connections
                    // for the coordinator in the underlying network client layer
                    int coordinatorConnectionId = Integer.MAX_VALUE - findCoordinatorResponse.data().nodeId();

                    // 根据响应信息构建coordinator协调器节点信息
                    AbstractCoordinator.this.coordinator = new Node(
                            coordinatorConnectionId,
                            findCoordinatorResponse.data().host(),
                            findCoordinatorResponse.data().port());
                    log.info("Discovered group coordinator {}", coordinator);
                    // 尝试和组协调器连接
                    client.tryConnect(coordinator);
                    // 重置心跳的会话超时时间
                    heartbeat.resetSessionTimeout();
                }
                // 回调future的complete方法
                future.complete(null);
            } else if (error == Errors.GROUP_AUTHORIZATION_FAILED) {
                // 请求认证失败回调
                future.raise(GroupAuthorizationException.forGroupId(rebalanceConfig.groupId));
            } else {
                log.debug("Group coordinator lookup failed: {}", findCoordinatorResponse.data().errorMessage());
                future.raise(error);
            }
        }

        @Override
        public void onFailure(RuntimeException e, RequestFuture<Void> future) {
            log.debug("FindCoordinator request failed due to {}", e);

            if (!(e instanceof RetriableException)) {
                // Remember the exception if fatal so we can ensure it gets thrown by the main thread
                // 如果移出不可重试，则记住该异常，确保他能在主线程中被感知到
                fatalFindCoordinatorException = e;
            }

            super.onFailure(e, future);
        }
    }

    /**
     * Check if we know who the coordinator is and we have an active connection
     * @return true if the coordinator is unknown
     */
    public boolean coordinatorUnknown() {
        // 如果组协调器为空，则说明没有组协调器
        return checkAndGetCoordinator() == null;
    }

    /**
     * Get the coordinator if its connection is still active. Otherwise mark it unknown and
     * return null.
     *
     * @return the current coordinator or null if it is unknown
     */
    protected synchronized Node checkAndGetCoordinator() {
        // 如果没有可用的组协调器，则返回空
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

    // 将组协调器标记为未知
    protected synchronized void markCoordinatorUnknown(boolean isDisconnected, String cause) {
        if (this.coordinator != null) {
            log.info("Group coordinator {} is unavailable or invalid due to cause: {}."
                    + "isDisconnected: {}. Rediscovery will be attempted.", this.coordinator,
                    cause, isDisconnected);
            Node oldCoordinator = this.coordinator;

            // Mark the coordinator dead before disconnecting requests since the callbacks for any pending
            // requests may attempt to do likewise. This also prevents new requests from being sent to the
            // coordinator while the disconnect is in progress.
            this.coordinator = null;

            // Disconnect from the coordinator to ensure that there are no in-flight requests remaining.
            // Pending callbacks will be invoked with a DisconnectException on the next call to poll.
            if (!isDisconnected)
                client.disconnectAsync(oldCoordinator);

            lastTimeOfConnectionMs = time.milliseconds();
        } else {
            long durationOfOngoingDisconnect = time.milliseconds() - lastTimeOfConnectionMs;
            if (durationOfOngoingDisconnect > rebalanceConfig.rebalanceTimeoutMs)
                log.warn("Consumer has been disconnected from the group coordinator for {}ms", durationOfOngoingDisconnect);
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
        // 重置消费者协调器的状态
        resetState();
        // 需要重新入组
        rejoinNeeded = true;
        // 需要重新准备入组
        needsJoinPrepare = true;
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
        // 构建心跳请求，请求类型为 HEARTBEAT
        HeartbeatRequest.Builder requestBuilder =
                new HeartbeatRequest.Builder(new HeartbeatRequestData()
                        // 消费者组ID
                        .setGroupId(rebalanceConfig.groupId)
                        // 消费者ID
                        .setMemberId(this.generation.memberId)
                        // 消费者组实例ID
                        .setGroupInstanceId(this.rebalanceConfig.groupInstanceId.orElse(null))
                        .setGenerationId(this.generation.generationId));
        // 发送心跳请求
        return client.send(coordinator, requestBuilder)
                // 心跳请求处理回调
                .compose(new HeartbeatResponseHandler(generation));
    }

    // 心跳请求响应处理器
    private class HeartbeatResponseHandler extends CoordinatorResponseHandler<HeartbeatResponse, Void> {
        private HeartbeatResponseHandler(final Generation generation) {
            super(generation);
        }

        @Override
        public void handle(HeartbeatResponse heartbeatResponse, RequestFuture<Void> future) {
            // 记录心跳请求的延迟时间
            sensors.heartbeatSensor.record(response.requestLatencyMs());
            // 获取心跳请求响应的错误类型
            Errors error = heartbeatResponse.error();

            // 心跳成功
            if (error == Errors.NONE) {
                log.debug("Received successful Heartbeat response");
                // 回调注册在该future上的listeners
                future.complete(null);
                // 组协调器不可用或者不是组协调器
            } else if (error == Errors.COORDINATOR_NOT_AVAILABLE
                    || error == Errors.NOT_COORDINATOR) {
                log.info("Attempt to heartbeat failed since coordinator {} is either not started or not valid",
                        coordinator());
                // 标记组协调器未知
                markCoordinatorUnknown(error);
                // 抛出异常
                future.raise(error);
                // 正在重平衡中
            } else if (error == Errors.REBALANCE_IN_PROGRESS) {
                // since we may be sending the request during rebalance, we should check
                // this case and ignore the REBALANCE_IN_PROGRESS error
                if (state == MemberState.STABLE) {
                    log.info("Attempt to heartbeat failed since group is rebalancing");
                    // 如果此时消费者协调器已经是stable状态，则需要重新入组
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

    // 心跳线程
    private class HeartbeatThread extends KafkaThread implements AutoCloseable {
        // 心跳线程是否启动
        private boolean enabled = false;
        // 心跳线程是否关闭
        private boolean closed = false;
        // 心跳失败时的异常信息
        private final AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        private HeartbeatThread() {
            super(HEARTBEAT_THREAD_PREFIX + (rebalanceConfig.groupId.isEmpty() ? "" : " | " + rebalanceConfig.groupId), true);
        }

        // 开启心跳
        public void enable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Enabling heartbeat thread");
                this.enabled = true;
                heartbeat.resetTimeouts();
                // 唤醒心跳线程，继续进行心跳
                AbstractCoordinator.this.notify();
            }
        }

        // 关闭信息条
        public void disable() {
            synchronized (AbstractCoordinator.this) {
                log.debug("Disabling heartbeat thread");
                this.enabled = false;
            }
        }

        // 关闭心跳线程
        public void close() {
            synchronized (AbstractCoordinator.this) {
                this.closed = true;
                // 唤醒心跳线程，让线程进行退出
                AbstractCoordinator.this.notify();
            }
        }

        // 心跳线程是否有异常
        private boolean hasFailed() {
            return failed.get() != null;
        }

        private RuntimeException failureCause() {
            return failed.get();
        }

        // 心跳线程不仅仅只干了心跳的活，他还做了发送缓冲队列里的请求、重新入组等操作
        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread started");
                while (true) {
                    synchronized (AbstractCoordinator.this) {
                        if (closed)
                            return;

                        if (!enabled) {
                            // 如果没有开启心跳，则在这里等待，等待notify方法来唤醒
                            AbstractCoordinator.this.wait();
                            continue;
                        }

                        // we do not need to heartbeat we are not part of a group yet;
                        // also if we already have fatal error, the client will be
                        // crashed soon, hence we do not need to continue heartbeating either
                        // 如果该消费者协调者没有加入消费者组或者心跳有异常，则关闭心跳并进行下一次循环
                        if (state.hasNotJoinedGroup() || hasFailed()) {
                            disable();
                            continue;
                        }

                        // 调用ConsumerNetworkClient的poll方法，用于发送unsent请求缓冲队列里的请求
                        client.pollNoWakeup();
                        long now = time.milliseconds();

                        // 如果组协调器未知
                        if (coordinatorUnknown()) {
                            // 组协调器未知，但是future不为空，这种情况下需要清空future并等待一下
                            if (findCoordinatorFuture != null) {
                                // clear the future so that after the backoff, if the hb still sees coordinator unknown in
                                // the next iteration it will try to re-discover the coordinator in case the main thread cannot
                                clearFindCoordinatorFuture();

                                // backoff properly
                                AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                            } else {
                                // 查找组协调器
                                lookupCoordinator();
                            }
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            // 和组协调器心跳超时，则设置组协调器为未知
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
                            // 如果心跳轮询超时，则调用maybeLeaveGroup方法，退出消费者组
                            maybeLeaveGroup(leaveReason);
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            // 如果不需要进行心跳，则心跳线程阻塞
                            AbstractCoordinator.this.wait(rebalanceConfig.retryBackoffMs);
                        } else {
                            // 发送请求前的准备，更新时间，心跳间隔等。正常的心跳请求通过 sendHeartbeatRequest 发出，并在回调方法里处理响应
                            heartbeat.sentHeartbeat(now);
                            // 【重要】发送心跳请求，心跳请求不只是心跳，他还会探测组协调器的状态以及重新加入消费者组
                            final RequestFuture<Void> heartbeatFuture = sendHeartbeatRequest();
                            // 心跳请求的回调方法（这里只做了简单处理，具体的处理逻辑在 HeartbeatResponseHandler 里）
                            heartbeatFuture.addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (AbstractCoordinator.this) {
                                        heartbeat.receiveHeartbeat();
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (AbstractCoordinator.this) {
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

        // 每次Rebalance加一
        public final int generationId;
        // 在消费者组中对应的成员ID，由组协调器自动生成，客户端不可设置
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
