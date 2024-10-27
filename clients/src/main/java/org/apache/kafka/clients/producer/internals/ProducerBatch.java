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

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.common.utils.ProducerIdAndEpoch;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.AbstractRecords;
import org.apache.kafka.common.record.CompressionRatioEstimator;
import org.apache.kafka.common.record.CompressionType;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MemoryRecordsBuilder;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.record.Record;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.requests.ProduceResponse;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.common.record.RecordBatch.MAGIC_VALUE_V2;
import static org.apache.kafka.common.record.RecordBatch.NO_TIMESTAMP;

/**
 * A batch of records that is or will be sent.
 *
 * This class is not thread safe and external synchronization must be used when modifying it
 * 一个 ProducerBatch 里会存放多条用户线程发送的消息
 */
public final class ProducerBatch {

    private static final Logger log = LoggerFactory.getLogger(ProducerBatch.class);

    // 批次的最终状态
    private enum FinalState { ABORTED, FAILED, SUCCEEDED }

    // 该batch的创建时间
    final long createdMs;
    // 该批次的消息要发送到哪个topic的哪个分区
    final TopicPartition topicPartition;
    // 请求结果里的future
    final ProduceRequestResult produceFuture;
    // 用来存储消息的callback和响应数据
    private final List<Thunk> thunks = new ArrayList<>();
    // 【重要】批次里的消息就是存放在这的，MemoryRecordsBuilder 用于管理批次消息的写入（比如：该批次还剩多少空间可写）
    // recordsBuilder 里有一个bufferStream属性，bufferStream里包含了一个byteBuffer，用于真实存放写入批次的数据
    private final MemoryRecordsBuilder recordsBuilder;
    // 重试次数
    private final AtomicInteger attempts = new AtomicInteger(0);
    // 是否是被分裂的批次，比如：一个批次限制最大容纳16kb，假如用户发送了一个30kb的消息，那么就需要两个batch来存放
    private final boolean isSplitBatch;
    // producerBatch的最终状态
    private final AtomicReference<FinalState> finalState = new AtomicReference<>(null);

    // 写入该批次的消息数量
    int recordCount;
    // 该批次里最大的消息大小
    int maxRecordSize;
    // 最近一次重试发送该批次的时间戳
    private long lastAttemptMs;
    // 最近一次向该批次添加消息的时间戳
    private long lastAppendTime;
    // sender子线程最近一次从该批次里拉取消息的时间
    private long drainedMs;
    // 是否正在重试，producerBatch里的数据发送失败了会自动重试进行发送
    private boolean retry;
    private boolean reopened;

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs) {
        this(tp, recordsBuilder, createdMs, false);
    }

    public ProducerBatch(TopicPartition tp, MemoryRecordsBuilder recordsBuilder, long createdMs, boolean isSplitBatch) {
        this.createdMs = createdMs;
        this.lastAttemptMs = createdMs;
        this.recordsBuilder = recordsBuilder;
        this.topicPartition = tp;
        this.lastAppendTime = createdMs;
        this.produceFuture = new ProduceRequestResult(topicPartition);
        this.retry = false;
        this.isSplitBatch = isSplitBatch;
        float compressionRatioEstimation = CompressionRatioEstimator.estimation(topicPartition.topic(),
                                                                                recordsBuilder.compressionType());
        recordsBuilder.setEstimatedCompressionRatio(compressionRatioEstimation);
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     *
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Header[] headers, Callback callback, long now) {
        // 1、如果该批次已经没有足够的空间来容纳即将要写入的数据了，则返回null
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return null;
        } else {
            // 2、通过 recordsBuilder 往当前的producerBatch里继续添加消息
            Long checksum = this.recordsBuilder.append(timestamp, key, value, headers);
            // 3、更新该批次最大消息的大小
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            this.lastAppendTime = now;
            // 4、构建 FutureRecordMetadata 对象
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length,
                                                                   Time.SYSTEM);
            // we have to keep every future returned to the users in case the batch needs to be
            // split to several new batches and resent.
            // 5、将 FutureRecordMetadata 和callback封装到 thunks 集合中
            thunks.add(new Thunk(callback, future));
            this.recordCount++;
            // 6、返回 FutureRecordMetadata
            return future;
        }
    }

    /**
     * This method is only used by {@link #split(int)} when splitting a large batch to smaller ones.
     * @return true if the record has been successfully appended, false otherwise.
     * 添加分裂消息到批次里
     */
    private boolean tryAppendForSplit(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers, Thunk thunk) {
        if (!recordsBuilder.hasRoomFor(timestamp, key, value, headers)) {
            return false;
        } else {
            // No need to get the CRC.
            this.recordsBuilder.append(timestamp, key, value, headers);
            this.maxRecordSize = Math.max(this.maxRecordSize, AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                    recordsBuilder.compressionType(), key, value, headers));
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, thunk.future.checksumOrNull(),
                                                                   key == null ? -1 : key.remaining(),
                                                                   value == null ? -1 : value.remaining(),
                                                                   Time.SYSTEM);
            // Chain the future to the original thunk.
            thunk.future.chain(future);
            this.thunks.add(thunk);
            this.recordCount++;
            return true;
        }
    }

    /**
     * Abort the batch and complete the future and callbacks.
     *
     * @param exception The exception to use to complete the future and awaiting callbacks.
     */
    public void abort(RuntimeException exception) {
        if (!finalState.compareAndSet(null, FinalState.ABORTED))
            throw new IllegalStateException("Batch has already been completed in final state " + finalState.get());

        log.trace("Aborting batch for partition {}", topicPartition, exception);
        completeFutureAndFireCallbacks(ProduceResponse.INVALID_OFFSET, RecordBatch.NO_TIMESTAMP, exception);
    }

    /**
     * Return `true` if {@link #done(long, long, RuntimeException)} has been invoked at least once, `false` otherwise.
     */
    public boolean isDone() {
        return finalState() != null;
    }

    /**
     * Finalize the state of a batch. Final state, once set, is immutable. This function may be called
     * once or twice on a batch. It may be called twice if
     * 1. An inflight batch expires before a response from the broker is received. The batch's final
     * state is set to FAILED. But it could succeed on the broker and second time around batch.done() may
     * try to set SUCCEEDED final state.
     * 2. If a transaction abortion happens or if the producer is closed forcefully, the final state is
     * ABORTED but again it could succeed if broker responds with a success.
     *
     * Attempted transitions from [FAILED | ABORTED] --> SUCCEEDED are logged.
     * Attempted transitions from one failure state to the same or a different failed state are ignored.
     * Attempted transitions from SUCCEEDED to the same or a failed state throw an exception.
     *
     * @param baseOffset The base offset of the messages assigned by the server
     * @param logAppendTime The log append time or -1 if CreateTime is being used
     * @param exception The exception that occurred (or null if the request was successful)
     * @return true if the batch was completed successfully and false if the batch was previously aborted
     * 当 批次消息发送给broker是正常的还是失败的还是发送超时或关闭生产者，都会回调这个方法
     */
    public boolean done(long baseOffset, long logAppendTime, RuntimeException exception) {
        // 1、根据 exception 来决定本次batch发送的最终状态
        final FinalState tryFinalState = (exception == null) ? FinalState.SUCCEEDED : FinalState.FAILED;

        if (tryFinalState == FinalState.SUCCEEDED) {
            log.trace("Successfully produced messages to {} with base offset {}.", topicPartition, baseOffset);
        } else {
            log.trace("Failed to produce messages to {} with base offset {}.", topicPartition, baseOffset, exception);
        }

        // 2、通过CAS来更新该批次的最终状态，只有一次能更新成功，所以回调函数也只会被执行一次
        if (this.finalState.compareAndSet(null, tryFinalState)) {
            // 【重要】3、执行回调函数（比如：用户线程调用producer进行异步发送消息，然后注册了一个回调函数，那么这个回调函数就是在这里被回调的）
            completeFutureAndFireCallbacks(baseOffset, logAppendTime, exception);
            return true;
        }

        if (this.finalState.get() != FinalState.SUCCEEDED) {
            if (tryFinalState == FinalState.SUCCEEDED) {
                // Log if a previously unsuccessful batch succeeded later on.
                log.debug("ProduceResponse returned {} for {} after batch with base offset {} had already been {}.",
                    tryFinalState, topicPartition, baseOffset, this.finalState.get());
            } else {
                // FAILED --> FAILED and ABORTED --> FAILED transitions are ignored.
                log.debug("Ignored state transition {} -> {} for {} batch with base offset {}",
                    this.finalState.get(), tryFinalState, topicPartition, baseOffset);
            }
        } else {
            // A SUCCESSFUL batch must not attempt another state change.
            throw new IllegalStateException("A " + this.finalState.get() + " batch must not attempt another state change to " + tryFinalState);
        }
        return false;
    }

    private void completeFutureAndFireCallbacks(long baseOffset, long logAppendTime, RuntimeException exception) {
        // Set the future before invoking the callbacks as we rely on its state for the `onCompletion` call
        // 1、更新 ProduceRequestResult 里相关字段
        produceFuture.set(baseOffset, logAppendTime, exception);

        // execute callbacks
        // 2、遍历 thunks集合，调用每个record的回调函数，这里生产者发送的每条消息发送结果就会回调给发送方
        // @see org.apache.kafka.clients.producer.KafkaProducer#doSend
        for (Thunk thunk : thunks) {
            try {
                if (exception == null) {
                    // 3、获取消息元数据
                    RecordMetadata metadata = thunk.future.value();
                    if (thunk.callback != null)
                        // 4、发送成功回调
                        thunk.callback.onCompletion(metadata, null);
                } else {
                    if (thunk.callback != null)
                        // 5、发送失败回调
                        thunk.callback.onCompletion(null, exception);
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition '{}'", topicPartition, e);
            }
        }

        // 唤醒阻塞在 ProducerBatch上的主线程
        produceFuture.done();
    }

    public Deque<ProducerBatch> split(int splitBatchSize) {
        Deque<ProducerBatch> batches = new ArrayDeque<>();
        MemoryRecords memoryRecords = recordsBuilder.build();

        Iterator<MutableRecordBatch> recordBatchIter = memoryRecords.batches().iterator();
        if (!recordBatchIter.hasNext())
            throw new IllegalStateException("Cannot split an empty producer batch.");

        RecordBatch recordBatch = recordBatchIter.next();
        if (recordBatch.magic() < MAGIC_VALUE_V2 && !recordBatch.isCompressed())
            throw new IllegalArgumentException("Batch splitting cannot be used with non-compressed messages " +
                    "with version v0 and v1");

        if (recordBatchIter.hasNext())
            throw new IllegalArgumentException("A producer batch should only have one record batch.");

        Iterator<Thunk> thunkIter = thunks.iterator();
        // We always allocate batch size because we are already splitting a big batch.
        // And we also Retain the create time of the original batch.
        ProducerBatch batch = null;

        for (Record record : recordBatch) {
            assert thunkIter.hasNext();
            Thunk thunk = thunkIter.next();
            if (batch == null)
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);

            // A newly created batch can always host the first message.
            if (!batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk)) {
                batches.add(batch);
                batch.closeForRecordAppends();
                batch = createBatchOffAccumulatorForRecord(record, splitBatchSize);
                batch.tryAppendForSplit(record.timestamp(), record.key(), record.value(), record.headers(), thunk);
            }
        }

        // Close the last batch and add it to the batch list after split.
        if (batch != null) {
            batches.add(batch);
            batch.closeForRecordAppends();
        }

        produceFuture.set(ProduceResponse.INVALID_OFFSET, NO_TIMESTAMP, new RecordBatchTooLargeException());
        produceFuture.done();

        if (hasSequence()) {
            int sequence = baseSequence();
            ProducerIdAndEpoch producerIdAndEpoch = new ProducerIdAndEpoch(producerId(), producerEpoch());
            for (ProducerBatch newBatch : batches) {
                newBatch.setProducerState(producerIdAndEpoch, sequence, isTransactional());
                sequence += newBatch.recordCount;
            }
        }
        return batches;
    }

    private ProducerBatch createBatchOffAccumulatorForRecord(Record record, int batchSize) {
        int initialSize = Math.max(AbstractRecords.estimateSizeInBytesUpperBound(magic(),
                recordsBuilder.compressionType(), record.key(), record.value(), record.headers()), batchSize);
        ByteBuffer buffer = ByteBuffer.allocate(initialSize);

        // Note that we intentionally do not set producer state (producerId, epoch, sequence, and isTransactional)
        // for the newly created batch. This will be set when the batch is dequeued for sending (which is consistent
        // with how normal batches are handled).
        MemoryRecordsBuilder builder = MemoryRecords.builder(buffer, magic(), recordsBuilder.compressionType(),
                TimestampType.CREATE_TIME, 0L);
        return new ProducerBatch(topicPartition, builder, this.createdMs, true);
    }

    public boolean isCompressed() {
        return recordsBuilder.compressionType() != CompressionType.NONE;
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
        // 回调函数
        final Callback callback;
        // 消息的元数据
        final FutureRecordMetadata future;

        Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "ProducerBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    // ProducerBatch是否投递超时判定
    boolean hasReachedDeliveryTimeout(long deliveryTimeoutMs, long now) {
        // 当前时间 - 批次创建时间 >= 投递过期时间（默认120s）
        return deliveryTimeoutMs <= now - this.createdMs;
    }

    public FinalState finalState() {
        return this.finalState.get();
    }

    int attempts() {
        return attempts.get();
    }

    void reenqueued(long now) {
        // 重试次数+1
        attempts.getAndIncrement();
        // 更新最近一次重试的时间戳
        lastAttemptMs = Math.max(lastAppendTime, now);
        lastAppendTime = Math.max(lastAppendTime, now);
        retry = true;
    }

    long queueTimeMs() {
        return drainedMs - createdMs;
    }

    long waitedTimeMs(long nowMs) {
        return Math.max(0, nowMs - lastAttemptMs);
    }

    void drained(long nowMs) {
        this.drainedMs = Math.max(drainedMs, nowMs);
    }

    boolean isSplitBatch() {
        return isSplitBatch;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    public MemoryRecords records() {
        return recordsBuilder.build();
    }

    public int estimatedSizeInBytes() {
        return recordsBuilder.estimatedSizeInBytes();
    }

    public double compressionRatio() {
        return recordsBuilder.compressionRatio();
    }

    public boolean isFull() {
        return recordsBuilder.isFull();
    }

    public void setProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        recordsBuilder.setProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    public void resetProducerState(ProducerIdAndEpoch producerIdAndEpoch, int baseSequence, boolean isTransactional) {
        log.info("Resetting sequence number of batch with current sequence {} for partition {} to {}",
                this.baseSequence(), this.topicPartition, baseSequence);
        reopened = true;
        recordsBuilder.reopenAndRewriteProducerState(producerIdAndEpoch.producerId, producerIdAndEpoch.epoch, baseSequence, isTransactional);
    }

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     */
    public void closeForRecordAppends() {
        recordsBuilder.closeForRecordAppends();
    }

    public void close() {
        recordsBuilder.close();
        if (!recordsBuilder.isControlBatch()) {
            CompressionRatioEstimator.updateEstimation(topicPartition.topic(),
                                                       recordsBuilder.compressionType(),
                                                       (float) recordsBuilder.compressionRatio());
        }
        reopened = false;
    }

    /**
     * Abort the record builder and reset the state of the underlying buffer. This is used prior to aborting
     * the batch with {@link #abort(RuntimeException)} and ensures that no record previously appended can be
     * read. This is used in scenarios where we want to ensure a batch ultimately gets aborted, but in which
     * it is not safe to invoke the completion callbacks (e.g. because we are holding a lock,
     * {@link RecordAccumulator#abortBatches()}).
     */
    public void abortRecordAppends() {
        recordsBuilder.abort();
    }

    public boolean isClosed() {
        return recordsBuilder.isClosed();
    }

    public ByteBuffer buffer() {
        return recordsBuilder.buffer();
    }

    public int initialCapacity() {
        return recordsBuilder.initialCapacity();
    }

    public boolean isWritable() {
        return !recordsBuilder.isClosed();
    }

    public byte magic() {
        return recordsBuilder.magic();
    }

    public long producerId() {
        return recordsBuilder.producerId();
    }

    public short producerEpoch() {
        return recordsBuilder.producerEpoch();
    }

    public int baseSequence() {
        return recordsBuilder.baseSequence();
    }

    public int lastSequence() {
        return recordsBuilder.baseSequence() + recordsBuilder.numRecords() - 1;
    }

    public boolean hasSequence() {
        return baseSequence() != RecordBatch.NO_SEQUENCE;
    }

    public boolean isTransactional() {
        return recordsBuilder.isTransactional();
    }

    public boolean sequenceHasBeenReset() {
        return reopened;
    }
}
