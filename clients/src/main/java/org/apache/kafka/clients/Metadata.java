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
package org.apache.kafka.clients;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InvalidMetadataException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.MetadataRequest;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;

/**
 * A class encapsulating some of the logic around metadata.
 * <p>
 * This class is shared by the client thread (for partitioning) and the background sender thread.
 *
 * Metadata is maintained for only a subset of topics, which can be added to over time. When we request metadata for a
 * topic we don't have any metadata for it will trigger a metadata update.
 * <p>
 * If topic expiry is enabled for the metadata, any topic that has not been used within the expiry interval
 * is removed from the metadata refresh set after an update. Consumers disable topic expiry since they explicitly
 * manage topics while producers rely on topic expiry to limit the refresh set.
 * 该类是元数据基类，封装了元数据的具体信息、版本控制、更新标识、响应解析等。该类是一个线程安全的类，所有方法都加了锁
 */
public class Metadata implements Closeable {
    private final Logger log;
    // 请求元数据失败后重试时间间隔，默认值：100ms
    private final long refreshBackoffMs;
    // 元数据过期时间，即多久动更新一次元数据，默认值是5分钟更新一次
    private final long metadataExpireMs;
    // 元数据版本号，每次请求服务端+1，保存在本地内存中
    private int updateVersion;  // bumped on every metadata response
    // 元数据加入到新topic集合的版本号
    private int requestVersion; // bumped on every new topic addition
    // 最后一次更新元数据的时间
    private long lastRefreshMs;
    // 最后一次成功更新全部topic元数据的时间
    private long lastSuccessfulRefreshMs;
    // 失败异常
    private KafkaException fatalException;
    // 无效的topic集合
    private Set<String> invalidTopics;
    // 无权限的topic集合
    private Set<String> unauthorizedTopics;
    // 元数据缓存，元数据信息就保存在这里
    private MetadataCache cache = MetadataCache.empty();
    // 是否全量更新topic，对生产者来说这里的全量指的是最近发送的topic集合
    private boolean needFullUpdate;
    // 是否部分更新topic，对生产者来说，这里的部分指的是新发送的topic集合
    private boolean needPartialUpdate;
    private final ClusterResourceListeners clusterResourceListeners;
    private boolean isClosed;
    private final Map<TopicPartition, Integer> lastSeenLeaderEpochs;

    /**
     * Create a new Metadata instance
     *
     * @param refreshBackoffMs         The minimum amount of time that must expire between metadata refreshes to avoid busy
     *                                 polling
     * @param metadataExpireMs         The maximum amount of time that metadata can be retained without refresh
     * @param logContext               Log context corresponding to the containing client
     * @param clusterResourceListeners List of ClusterResourceListeners which will receive metadata updates.
     */
    public Metadata(long refreshBackoffMs,
                    long metadataExpireMs,
                    LogContext logContext,
                    ClusterResourceListeners clusterResourceListeners) {
        this.log = logContext.logger(Metadata.class);
        this.refreshBackoffMs = refreshBackoffMs;
        this.metadataExpireMs = metadataExpireMs;
        this.lastRefreshMs = 0L;
        this.lastSuccessfulRefreshMs = 0L;
        this.requestVersion = 0;
        this.updateVersion = 0;
        this.needFullUpdate = false;
        this.needPartialUpdate = false;
        this.clusterResourceListeners = clusterResourceListeners;
        this.isClosed = false;
        this.lastSeenLeaderEpochs = new HashMap<>();
        this.invalidTopics = Collections.emptySet();
        this.unauthorizedTopics = Collections.emptySet();
    }

    /**
     * Get the current cluster info without blocking
     */
    public synchronized Cluster fetch() {
        // 返回元数据缓存基础信息
        return cache.cluster();
    }

    /**
     * Return the next time when the current cluster info can be updated (i.e., backoff time has elapsed).
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till the cluster info can be updated again
     */
    public synchronized long timeToAllowUpdate(long nowMs) {
        return Math.max(this.lastRefreshMs + this.refreshBackoffMs - nowMs, 0);
    }

    /**
     * The next time to update the cluster info is the maximum of the time the current info will expire and the time the
     * current info can be updated (i.e. backoff time has elapsed); If an update has been request then the expiry time
     * is now
     *
     * @param nowMs current time in ms
     * @return remaining time in ms till updating the cluster info
     */
    public synchronized long timeToNextUpdate(long nowMs) {
        long timeToExpire = updateRequested() ? 0 : Math.max(this.lastSuccessfulRefreshMs + this.metadataExpireMs - nowMs, 0);
        return Math.max(timeToExpire, timeToAllowUpdate(nowMs));
    }

    public long metadataExpireMs() {
        return this.metadataExpireMs;
    }

    /**
     * Request an update of the current cluster metadata info, return the current updateVersion before the update
     */
    public synchronized int requestUpdate() {
        this.needFullUpdate = true; // 全量更新
        return this.updateVersion;
    }

    public synchronized int requestUpdateForNewTopics() {
        // Override the timestamp of last refresh to let immediate update.
        // 重写上次刷新时间戳以允许立即更新
        this.lastRefreshMs = 0;
        this.needPartialUpdate = true; // 部分更新
        this.requestVersion++;
        return this.updateVersion;
    }

    /**
     * Request an update for the partition metadata iff we have seen a newer leader epoch. This is called by the client
     * any time it handles a response from the broker that includes leader epoch, except for UpdateMetadata which
     * follows a different code path ({@link #update}).
     *
     * @param topicPartition
     * @param leaderEpoch
     * @return true if we updated the last seen epoch, false otherwise
     */
    public synchronized boolean updateLastSeenEpochIfNewer(TopicPartition topicPartition, int leaderEpoch) {
        Objects.requireNonNull(topicPartition, "TopicPartition cannot be null");
        if (leaderEpoch < 0)
            throw new IllegalArgumentException("Invalid leader epoch " + leaderEpoch + " (must be non-negative)");

        Integer oldEpoch = lastSeenLeaderEpochs.get(topicPartition);
        log.trace("Determining if we should replace existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);

        final boolean updated;
        if (oldEpoch == null) {
            log.debug("Not replacing null epoch with new epoch {} for partition {}", leaderEpoch, topicPartition);
            updated = false;
        } else if (leaderEpoch > oldEpoch) {
            log.debug("Updating last seen epoch from {} to {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            lastSeenLeaderEpochs.put(topicPartition, leaderEpoch);
            updated = true;
        } else {
            log.debug("Not replacing existing epoch {} with new epoch {} for partition {}", oldEpoch, leaderEpoch, topicPartition);
            updated = false;
        }

        this.needFullUpdate = this.needFullUpdate || updated;
        return updated;
    }

    public Optional<Integer> lastSeenLeaderEpoch(TopicPartition topicPartition) {
        return Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition));
    }

    /**
     * Check whether an update has been explicitly requested.
     *
     * @return true if an update was requested, false otherwise
     */
    public synchronized boolean updateRequested() {
        return this.needFullUpdate || this.needPartialUpdate;
    }

    /**
     * Return the cached partition info if it exists and a newer leader epoch isn't known about.
     */
    synchronized Optional<MetadataResponse.PartitionMetadata> partitionMetadataIfCurrent(TopicPartition topicPartition) {
        Integer epoch = lastSeenLeaderEpochs.get(topicPartition);
        Optional<MetadataResponse.PartitionMetadata> partitionMetadata = cache.partitionMetadata(topicPartition);
        if (epoch == null) {
            // old cluster format (no epochs)
            return partitionMetadata;
        } else {
            return partitionMetadata.filter(metadata ->
                    metadata.leaderEpoch.orElse(NO_PARTITION_LEADER_EPOCH).equals(epoch));
        }
    }

    public synchronized LeaderAndEpoch currentLeader(TopicPartition topicPartition) {
        Optional<MetadataResponse.PartitionMetadata> maybeMetadata = partitionMetadataIfCurrent(topicPartition);
        if (!maybeMetadata.isPresent())
            return new LeaderAndEpoch(Optional.empty(), Optional.ofNullable(lastSeenLeaderEpochs.get(topicPartition)));

        MetadataResponse.PartitionMetadata partitionMetadata = maybeMetadata.get();
        Optional<Integer> leaderEpochOpt = partitionMetadata.leaderEpoch;
        Optional<Node> leaderNodeOpt = partitionMetadata.leaderId.flatMap(cache::nodeById);
        return new LeaderAndEpoch(leaderNodeOpt, leaderEpochOpt);
    }

    // 在producer启动时，构造函数中调用该方法
    public synchronized void bootstrap(List<InetSocketAddress> addresses) {
        // 第一次初始化时由于本地缓存是空的，所以需要全量更新
        this.needFullUpdate = true;
        this.updateVersion += 1; // 更新版本+1
        this.cache = MetadataCache.bootstrap(addresses); // 初始化元数据缓存
    }

    /**
     * Update metadata assuming the current request version.
     *
     * For testing only.
     */
    public synchronized void updateWithCurrentRequestVersion(MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        this.update(this.requestVersion, response, isPartialUpdate, nowMs);
    }

    /**
     * Updates the cluster metadata. If topic expiry is enabled, expiry time
     * is set for topics if required and expired topics are removed from the metadata.
     *
     * @param requestVersion The request version corresponding to the update response, as provided by
     *     {@link #newMetadataRequestAndVersion(long)}.
     * @param response metadata response received from the broker
     * @param isPartialUpdate whether the metadata request was for a subset of the active topics
     * @param nowMs current time in milliseconds
     */
    public synchronized void update(int requestVersion, MetadataResponse response, boolean isPartialUpdate, long nowMs) {
        Objects.requireNonNull(response, "Metadata response cannot be null");
        if (isClosed())
            throw new IllegalStateException("Update requested after metadata close");
        // 是否部分topic更新标记
        this.needPartialUpdate = requestVersion < this.requestVersion;
        // 将最后一次更新元数据的时间设置为当前时间
        this.lastRefreshMs = nowMs;
        // 元数据版本号 + 1
        this.updateVersion += 1;
        // 如果不是部分更新，则认为是全部更新
        if (!isPartialUpdate) {
            this.needFullUpdate = false;
            // 最后一次成功更新全部topic的元数据事件更新为当前时间
            this.lastSuccessfulRefreshMs = nowMs;
        }

        String previousClusterId = cache.clusterResource().clusterId();
        // 解析元数据响应结果并重新赋值到cache属性上（可以看到这里的本地缓存信息被重新赋值了）
        this.cache = handleMetadataResponse(response, isPartialUpdate, nowMs);

        Cluster cluster = cache.cluster();
        maybeSetMetadataError(cluster);

        this.lastSeenLeaderEpochs.keySet().removeIf(tp -> !retainTopic(tp.topic(), false, nowMs));

        String newClusterId = cache.clusterResource().clusterId();
        if (!Objects.equals(previousClusterId, newClusterId)) {
            log.info("Cluster ID: {}", newClusterId);
        }
        clusterResourceListeners.onUpdate(cache.clusterResource());

        log.debug("Updated cluster metadata updateVersion {} to {}", this.updateVersion, this.cache);
    }

    private void maybeSetMetadataError(Cluster cluster) {
        clearRecoverableErrors(); // 将无效的topic集合和无权限的topic集合赋值为空
        checkInvalidTopics(cluster);// 检查无效的topic
        checkUnauthorizedTopics(cluster); // 检查无权限的topic
    }

    private void checkInvalidTopics(Cluster cluster) {
        if (!cluster.invalidTopics().isEmpty()) {
            log.error("Metadata response reported invalid topics {}", cluster.invalidTopics());
            invalidTopics = new HashSet<>(cluster.invalidTopics());
        }
    }

    private void checkUnauthorizedTopics(Cluster cluster) {
        if (!cluster.unauthorizedTopics().isEmpty()) {
            log.error("Topic authorization failed for topics {}", cluster.unauthorizedTopics());
            unauthorizedTopics = new HashSet<>(cluster.unauthorizedTopics());
        }
    }

    /**
     * Transform a MetadataResponse into a new MetadataCache instance.
     */
    private MetadataCache handleMetadataResponse(MetadataResponse metadataResponse, boolean isPartialUpdate, long nowMs) {
        // All encountered topics.
        Set<String> topics = new HashSet<>();

        // Retained topics to be passed to the metadata cache.
        Set<String> internalTopics = new HashSet<>();
        Set<String> unauthorizedTopics = new HashSet<>();
        Set<String> invalidTopics = new HashSet<>();

        List<MetadataResponse.PartitionMetadata> partitions = new ArrayList<>();
        // 遍历响应的topic元数据
        for (MetadataResponse.TopicMetadata metadata : metadataResponse.topicMetadata()) {
            topics.add(metadata.topic());
            // 判断是否要保留该topic，如果返回为false，则说明本地缓存中不需要保留该topic的元数据的了（比如：某个topic的元数据在本地缓存中过期了）
            // 如果返回为true，则说明该topic的元数据会继续保留在本地缓存中
            if (!retainTopic(metadata.topic(), metadata.isInternal(), nowMs))
                continue;

            // 是否是内部topic
            if (metadata.isInternal())
                internalTopics.add(metadata.topic());

            // 元数据无问题，则更新该topic的分区信息
            if (metadata.error() == Errors.NONE) {
                for (MetadataResponse.PartitionMetadata partitionMetadata : metadata.partitionMetadata()) {
                    // Even if the partition's metadata includes an error, we need to handle
                    // the update to catch new epochs
                    updateLatestMetadata(partitionMetadata, metadataResponse.hasReliableLeaderEpochs())
                        .ifPresent(partitions::add);

                    // 分区元数据有无效异常，则需要再次重新更新
                    if (partitionMetadata.error.exception() instanceof InvalidMetadataException) {
                        log.debug("Requesting metadata update for partition {} due to error {}",
                                partitionMetadata.topicPartition, partitionMetadata.error);
                        requestUpdate();
                    }
                }
            } else {
                if (metadata.error().exception() instanceof InvalidMetadataException) {
                    log.debug("Requesting metadata update for topic {} due to error {}", metadata.topic(), metadata.error());
                    requestUpdate();
                }
                // 如果该topic是无效的，则直接加入到无效topic集合中
                if (metadata.error() == Errors.INVALID_TOPIC_EXCEPTION)
                    invalidTopics.add(metadata.topic());
                else if (metadata.error() == Errors.TOPIC_AUTHORIZATION_FAILED)
                    // 如果该topic是未授权的，则直接添加到集合
                    unauthorizedTopics.add(metadata.topic());
            }
        }

        Map<Integer, Node> nodes = metadataResponse.brokersById();
        // 如果是部分更新topic，则直接将解析的结果合并到原来的集合中即可
        if (isPartialUpdate)
            return this.cache.mergeWith(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller(),
                // 是否应该保留topic，判定条件：topic存在于刚拉下来的topic元数据集合里，并且本地topic元数据缓存未过期
                (topic, isInternal) -> !topics.contains(topic) && retainTopic(topic, isInternal, nowMs));
        else
            // 如果是全部更新topic的话，就重新初始化元数据缓存
            return new MetadataCache(metadataResponse.clusterId(), nodes, partitions,
                unauthorizedTopics, invalidTopics, internalTopics, metadataResponse.controller());
    }

    /**
     * Compute the latest partition metadata to cache given ordering by leader epochs (if both
     * available and reliable).
     */
    private Optional<MetadataResponse.PartitionMetadata> updateLatestMetadata(
            MetadataResponse.PartitionMetadata partitionMetadata,
            boolean hasReliableLeaderEpoch) {
        TopicPartition tp = partitionMetadata.topicPartition;
        if (hasReliableLeaderEpoch && partitionMetadata.leaderEpoch.isPresent()) {
            int newEpoch = partitionMetadata.leaderEpoch.get();
            // If the received leader epoch is at least the same as the previous one, update the metadata
            Integer currentEpoch = lastSeenLeaderEpochs.get(tp);
            if (currentEpoch == null || newEpoch >= currentEpoch) {
                log.debug("Updating last seen epoch for partition {} from {} to epoch {} from new metadata", tp, currentEpoch, newEpoch);
                lastSeenLeaderEpochs.put(tp, newEpoch);
                return Optional.of(partitionMetadata);
            } else {
                // Otherwise ignore the new metadata and use the previously cached info
                log.debug("Got metadata for an older epoch {} (current is {}) for partition {}, not updating", newEpoch, currentEpoch, tp);
                return cache.partitionMetadata(tp);
            }
        } else {
            // Handle old cluster formats as well as error responses where leader and epoch are missing
            lastSeenLeaderEpochs.remove(tp);
            return Optional.of(partitionMetadata.withoutLeaderEpoch());
        }
    }

    /**
     * If any non-retriable exceptions were encountered during metadata update, clear and throw the exception.
     * This is used by the consumer to propagate any fatal exceptions or topic exceptions for any of the topics
     * in the consumer's Metadata.
     */
    public synchronized void maybeThrowAnyException() {
        clearErrorsAndMaybeThrowException(this::recoverableException);
    }

    /**
     * If any fatal exceptions were encountered during metadata update, throw the exception. This is used by
     * the producer to abort waiting for metadata if there were fatal exceptions (e.g. authentication failures)
     * in the last metadata update.
     */
    protected synchronized void maybeThrowFatalException() {
        KafkaException metadataException = this.fatalException;
        if (metadataException != null) {
            fatalException = null;
            throw metadataException;
        }
    }

    /**
     * If any non-retriable exceptions were encountered during metadata update, throw exception if the exception
     * is fatal or related to the specified topic. All exceptions from the last metadata update are cleared.
     * This is used by the producer to propagate topic metadata errors for send requests.
     */
    public synchronized void maybeThrowExceptionForTopic(String topic) {
        clearErrorsAndMaybeThrowException(() -> recoverableExceptionForTopic(topic));
    }

    private void clearErrorsAndMaybeThrowException(Supplier<KafkaException> recoverableExceptionSupplier) {
        KafkaException metadataException = Optional.ofNullable(fatalException).orElseGet(recoverableExceptionSupplier);
        fatalException = null;
        clearRecoverableErrors();
        if (metadataException != null)
            throw metadataException;
    }

    // We may be able to recover from this exception if metadata for this topic is no longer needed
    private KafkaException recoverableException() {
        if (!unauthorizedTopics.isEmpty())
            return new TopicAuthorizationException(unauthorizedTopics);
        else if (!invalidTopics.isEmpty())
            return new InvalidTopicException(invalidTopics);
        else
            return null;
    }

    private KafkaException recoverableExceptionForTopic(String topic) {
        if (unauthorizedTopics.contains(topic))
            return new TopicAuthorizationException(Collections.singleton(topic));
        else if (invalidTopics.contains(topic))
            return new InvalidTopicException(Collections.singleton(topic));
        else
            return null;
    }

    private void clearRecoverableErrors() {
        invalidTopics = Collections.emptySet();
        unauthorizedTopics = Collections.emptySet();
    }

    /**
     * Record an attempt to update the metadata that failed. We need to keep track of this
     * to avoid retrying immediately.
     */
    public synchronized void failedUpdate(long now) {
        // 元数据更新失败，则最后一次更新元数据的时间设置为当前时间
        this.lastRefreshMs = now;
    }

    /**
     * Propagate a fatal error which affects the ability to fetch metadata for the cluster.
     * Two examples are authentication and unsupported version exceptions.
     *
     * @param exception The fatal exception
     */
    public synchronized void fatalError(KafkaException exception) {
        this.fatalException = exception;
    }

    /**
     * @return The current metadata updateVersion
     */
    public synchronized int updateVersion() {
        return this.updateVersion;
    }

    /**
     * The last time metadata was successfully updated.
     */
    public synchronized long lastSuccessfulUpdate() {
        return this.lastSuccessfulRefreshMs;
    }

    /**
     * Close this metadata instance to indicate that metadata updates are no longer possible.
     */
    @Override
    public synchronized void close() {
        this.isClosed = true;
    }

    /**
     * Check if this metadata instance has been closed. See {@link #close()} for more information.
     *
     * @return True if this instance has been closed; false otherwise
     */
    public synchronized boolean isClosed() {
        return this.isClosed;
    }

    public synchronized MetadataRequestAndVersion newMetadataRequestAndVersion(long nowMs) {
        MetadataRequest.Builder request = null;
        boolean isPartialUpdate = false;

        // Perform a partial update only if a full update hasn't been requested, and the last successful
        // hasn't exceeded the metadata refresh time.
        if (!this.needFullUpdate && this.lastSuccessfulRefreshMs + this.metadataExpireMs > nowMs) {
            request = newMetadataRequestBuilderForNewTopics();
            isPartialUpdate = true;
        }
        if (request == null) {
            request = newMetadataRequestBuilder();
            isPartialUpdate = false;
        }
        return new MetadataRequestAndVersion(request, requestVersion, isPartialUpdate);
    }

    /**
     * Constructs and returns a metadata request builder for fetching cluster data and all active topics.
     *
     * @return the constructed non-null metadata builder
     */
    protected MetadataRequest.Builder newMetadataRequestBuilder() {
        return MetadataRequest.Builder.allTopics();
    }

    /**
     * Constructs and returns a metadata request builder for fetching cluster data and any uncached topics,
     * otherwise null if the functionality is not supported.
     *
     * @return the constructed metadata builder, or null if not supported
     */
    protected MetadataRequest.Builder newMetadataRequestBuilderForNewTopics() {
        return null;
    }

    protected boolean retainTopic(String topic, boolean isInternal, long nowMs) {
        return true;
    }

    public static class MetadataRequestAndVersion {
        public final MetadataRequest.Builder requestBuilder;
        public final int requestVersion;
        public final boolean isPartialUpdate;

        private MetadataRequestAndVersion(MetadataRequest.Builder requestBuilder,
                                          int requestVersion,
                                          boolean isPartialUpdate) {
            this.requestBuilder = requestBuilder;
            this.requestVersion = requestVersion;
            this.isPartialUpdate = isPartialUpdate;
        }
    }

    /**
     * Represents current leader state known in metadata. It is possible that we know the leader, but not the
     * epoch if the metadata is received from a broker which does not support a sufficient Metadata API version.
     * It is also possible that we know of the leader epoch, but not the leader when it is derived
     * from an external source (e.g. a committed offset).
     */
    public static class LeaderAndEpoch {
        private static final LeaderAndEpoch NO_LEADER_OR_EPOCH = new LeaderAndEpoch(Optional.empty(), Optional.empty());

        public final Optional<Node> leader;
        public final Optional<Integer> epoch;

        public LeaderAndEpoch(Optional<Node> leader, Optional<Integer> epoch) {
            this.leader = Objects.requireNonNull(leader);
            this.epoch = Objects.requireNonNull(epoch);
        }

        public static LeaderAndEpoch noLeaderOrEpoch() {
            return NO_LEADER_OR_EPOCH;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            LeaderAndEpoch that = (LeaderAndEpoch) o;

            if (!leader.equals(that.leader)) return false;
            return epoch.equals(that.epoch);
        }

        @Override
        public int hashCode() {
            int result = leader.hashCode();
            result = 31 * result + epoch.hashCode();
            return result;
        }

        @Override
        public String toString() {
            return "LeaderAndEpoch{" +
                    "leader=" + leader +
                    ", epoch=" + epoch.map(Number::toString).orElse("absent") +
                    '}';
        }
    }
}
