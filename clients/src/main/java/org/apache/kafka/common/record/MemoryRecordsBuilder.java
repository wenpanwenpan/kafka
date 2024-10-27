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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class is used to write new log data in memory, i.e. this is the write path for {@link MemoryRecords}.
 * It transparently handles compression and exposes methods for appending new records, possibly with message
 * format conversion.
 *
 * In cases where keeping memory retention low is important and there's a gap between the time that record appends stop
 * and the builder is closed (e.g. the Producer), it's important to call `closeForRecordAppends` when the former happens.
 * This will release resources like compression buffers that can be relatively large (64 KB for LZ4).
 * MemoryRecordsBuilder 用于管理一个批次里的消息，比如该批次是否还能写入，写了多少消息以及继续往该批次里追加消息
 */
public class MemoryRecordsBuilder implements AutoCloseable {
    private static final float COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    // 写操作关闭的输出流，当一个batch写满后就会将 appendStream 赋值为 CLOSED_STREAM，此时如果还有用户线程想往该批次里追加消息就会报错
    private static final DataOutputStream CLOSED_STREAM = new DataOutputStream(new OutputStream() {
        @Override
        public void write(int b) {
            throw new IllegalStateException("MemoryRecordsBuilder is closed for record appends");
        }
    });

    // 日志时间
    private final TimestampType timestampType;
    // 消息压缩类型
    private final CompressionType compressionType;
    // Used to hold a reference to the underlying ByteBuffer so that we can write the record batch header and access
    // the written bytes. ByteBufferOutputStream allocates a new ByteBuffer if the existing one is not large enough,
    // so it's not safe to hold a direct reference to the underlying ByteBuffer.
    // 【重要】kafka对outputStream的实现类，对byteBuffer实现了自动扩容的能力（待发送的消息就存储在里面的 buffer 属性里，见下面的构造函数）
    private final ByteBufferOutputStream bufferStream;
    // 消息的版本
    private final byte magic;
    // byteBuffer的最初始位置
    private final int initialPosition;
    // 基本位移
    private final long baseOffset;
    // 消息的追加时间
    private final long logAppendTime;
    // 是否控制类的批次
    private final boolean isControlBatch;
    // 分区leader的版本
    private final int partitionLeaderEpoch;
    // 写入上限（也就是指定了一个producerBatch最多能写多少消息，如果写满了就需要将该producerBatch关闭并发送，不允许在写入了）
    private final int writeLimit;
    // 批次的头大小字节数
    private final int batchHeaderSizeInBytes;

    // Use a conservative estimate of the compression ratio. The producer overrides this using statistics
    // from previous batches before appending any records. 预估压缩比
    private float estimatedCompressionRatio = 1.0F;

    // Used to append records, may compress data on the fly
    // 对bufferStream添加压缩功能的输出流
    private DataOutputStream appendStream;
    // 是否是事务批次
    private boolean isTransactional;
    // 生产者ID
    private long producerId;
    // 生产者版本
    private short producerEpoch;
    // 批次序列号
    private int baseSequence;
    // 压缩前要写入的消息体大小（字节）
    private int uncompressedRecordsSizeInBytes = 0; // Number of bytes (excluding the header) written before compression
    // 压缩前写入的记录数（不包括头）
    private int numRecords = 0;
    // 实际压缩率
    private float actualCompressionRatio = 1;
    // 最大时间戳
    private long maxTimestamp = RecordBatch.NO_TIMESTAMP;
    // 最大时间戳偏移量
    private long offsetOfMaxTimestamp = -1;
    // 最后的偏移量（也就是batch中下一个能继续写入消息的位置）
    private Long lastOffset = null;
    // 第一次追加消息的时间戳
    private Long firstTimestamp = null;

    // 用于真正保存用户发送的消息的地方
    private MemoryRecords builtRecords;
    private boolean aborted = false;

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream,// 消息输出流，待发送的消息就是写入这个流里面的
                                byte magic,
                                CompressionType compressionType,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long producerId,
                                short producerEpoch,
                                int baseSequence,
                                boolean isTransactional,
                                boolean isControlBatch,
                                int partitionLeaderEpoch,
                                int writeLimit) {
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            if (isTransactional)
                throw new IllegalArgumentException("Transactional records are not supported for magic " + magic);
            if (isControlBatch)
                throw new IllegalArgumentException("Control records are not supported for magic " + magic);
            if (compressionType == CompressionType.ZSTD)
                throw new IllegalArgumentException("ZStandard compression is not supported for magic " + magic);
        }

        // 消息版本号 v0、v1、v2
        this.magic = magic;
        this.timestampType = timestampType;
        this.compressionType = compressionType;
        this.baseOffset = baseOffset;
        this.logAppendTime = logAppendTime;
        this.numRecords = 0;
        this.uncompressedRecordsSizeInBytes = 0;
        this.actualCompressionRatio = 1;
        this.maxTimestamp = RecordBatch.NO_TIMESTAMP;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
        this.isControlBatch = isControlBatch;
        this.partitionLeaderEpoch = partitionLeaderEpoch;
        this.writeLimit = writeLimit;
        // 初始位置
        this.initialPosition = bufferStream.position();
        // 根据不同版本的消息计算该批次消息头的大小
        this.batchHeaderSizeInBytes = AbstractRecords.recordBatchHeaderSizeInBytes(magic, compressionType);

        // 将bufferStream的position调整为 initialPosition + 批次头大小，用户发送的消息就可以直接写入 bufferStream 里了
        bufferStream.position(initialPosition + batchHeaderSizeInBytes);
        this.bufferStream = bufferStream;
        // bufferStream 是对outputStream的实现，增加了自动扩容byteBuffer的能力，
        // 然后又用compressionType.wrapForOutput 包装了 bufferStream，使得该输出流具有对写出数据的压缩能力
        // 最后又用 DataOutputStream 装饰了包装后的输出流（装饰者模式）
        this.appendStream = new DataOutputStream(compressionType.wrapForOutput(this.bufferStream, magic));
    }

    /**
     * Construct a new builder.
     *
     * @param buffer The underlying buffer to use (note that this class will allocate a new buffer if necessary
     *               to fit the records appended)
     * @param magic The magic value to use
     * @param compressionType The compression codec to use
     * @param timestampType The desired timestamp type. For magic > 0, this cannot be {@link TimestampType#NO_TIMESTAMP_TYPE}.
     * @param baseOffset The initial offset to use for
     * @param logAppendTime The log append time of this record set. Can be set to NO_TIMESTAMP if CREATE_TIME is used.
     * @param producerId The producer ID associated with the producer writing this record set
     * @param producerEpoch The epoch of the producer
     * @param baseSequence The sequence number of the first record in this set
     * @param isTransactional Whether or not the records are part of a transaction
     * @param isControlBatch Whether or not this is a control batch (e.g. for transaction markers)
     * @param partitionLeaderEpoch The epoch of the partition leader appending the record set to the log
     * @param writeLimit The desired limit on the total bytes for this record set (note that this can be exceeded
     *                   when compression is used since size estimates are rough, and in the case that the first
     *                   record added exceeds the size).
     */
    public MemoryRecordsBuilder(ByteBuffer buffer,
                                byte magic,
                                CompressionType compressionType,
                                TimestampType timestampType,
                                long baseOffset,
                                long logAppendTime,
                                long producerId,
                                short producerEpoch,
                                int baseSequence,
                                boolean isTransactional,
                                boolean isControlBatch,
                                int partitionLeaderEpoch,
                                int writeLimit) {
        // 这里可以看到用输出流包装了数据buffer
        this(new ByteBufferOutputStream(buffer), magic, compressionType, timestampType, baseOffset, logAppendTime,
                producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch,
                writeLimit);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public int initialCapacity() {
        return bufferStream.initialCapacity();
    }

    public double compressionRatio() {
        return actualCompressionRatio;
    }

    public CompressionType compressionType() {
        return compressionType;
    }

    public boolean isControlBatch() {
        return isControlBatch;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    /**
     * Close this builder and return the resulting buffer.
     * @return The built log buffer
     */
    public MemoryRecords build() {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        close();
        return builtRecords;
    }

    /**
     * Get the max timestamp and its offset. The details of the offset returned are a bit subtle.
     *
     * If the log append time is used, the offset will be the last offset unless no compression is used and
     * the message format version is 0 or 1, in which case, it will be the first offset.
     *
     * If create time is used, the offset will be the last offset unless no compression is used and the message
     * format version is 0 or 1, in which case, it will be the offset of the record with the max timestamp.
     *
     * @return The max timestamp and its offset
     */
    public RecordsInfo info() {
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            long shallowOffsetOfMaxTimestamp;
            // Use the last offset when dealing with record batches
            if (compressionType != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                shallowOffsetOfMaxTimestamp = lastOffset;
            else
                shallowOffsetOfMaxTimestamp = baseOffset;
            return new RecordsInfo(logAppendTime, shallowOffsetOfMaxTimestamp);
        } else if (maxTimestamp == RecordBatch.NO_TIMESTAMP) {
            return new RecordsInfo(RecordBatch.NO_TIMESTAMP, lastOffset);
        } else {
            long shallowOffsetOfMaxTimestamp;
            // Use the last offset when dealing with record batches
            if (compressionType != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                shallowOffsetOfMaxTimestamp = lastOffset;
            else
                shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp;
            return new RecordsInfo(maxTimestamp, shallowOffsetOfMaxTimestamp);
        }
    }

    public int numRecords() {
        return numRecords;
    }

    /**
     * Return the sum of the size of the batch header (always uncompressed) and the records (before compression).
     */
    public int uncompressedBytesWritten() {
        return uncompressedRecordsSizeInBytes + batchHeaderSizeInBytes;
    }

    public void setProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (isClosed()) {
            // Sequence numbers are assigned when the batch is closed while the accumulator is being drained.
            // If the resulting ProduceRequest to the partition leader failed for a retriable error, the batch will
            // be re queued. In this case, we should not attempt to set the state again, since changing the producerId and sequence
            // once a batch has been sent to the broker risks introducing duplicates.
            throw new IllegalStateException("Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }

    public void overrideLastOffset(long lastOffset) {
        if (builtRecords != null)
            throw new IllegalStateException("Cannot override the last offset after the records have been built");
        this.lastOffset = lastOffset;
    }

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     */
    public void closeForRecordAppends() {
        if (appendStream != CLOSED_STREAM) {
            try {
                appendStream.close();
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                // 将 appendStream 指向CLOSED_STREAM，表示该ProducerBatch不允许再写入了
                appendStream = CLOSED_STREAM;
            }
        }
    }

    public void abort() {
        closeForRecordAppends();
        buffer().position(initialPosition);
        aborted = true;
    }

    public void reopenAndRewriteProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (aborted)
            throw new IllegalStateException("Should not reopen a batch which is already aborted.");
        builtRecords = null;
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
        this.baseSequence = baseSequence;
        this.isTransactional = isTransactional;
    }


    public void close() {
        if (aborted)
            throw new IllegalStateException("Cannot close MemoryRecordsBuilder as it has already been aborted");

        if (builtRecords != null)
            return;

        validateProducerState();

        closeForRecordAppends();

        if (numRecords == 0L) {
            buffer().position(initialPosition);
            builtRecords = MemoryRecords.EMPTY;
        } else {
            if (magic > RecordBatch.MAGIC_VALUE_V1)
                this.actualCompressionRatio = (float) writeDefaultBatchHeader() / this.uncompressedRecordsSizeInBytes;
            else if (compressionType != CompressionType.NONE)
                this.actualCompressionRatio = (float) writeLegacyCompressedWrapperHeader() / this.uncompressedRecordsSizeInBytes;

            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initialPosition);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private void validateProducerState() {
        if (isTransactional && producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("Cannot write transactional messages without a valid producer ID");

        if (producerId != RecordBatch.NO_PRODUCER_ID) {
            if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH)
                throw new IllegalArgumentException("Invalid negative producer epoch");

            if (baseSequence < 0 && !isControlBatch)
                throw new IllegalArgumentException("Invalid negative sequence number used");

            if (magic < RecordBatch.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Idempotent messages are not supported for magic " + magic);
        }
    }

    /**
     * Write the header to the default batch.
     * @return the written compressed bytes.
     */
    private int writeDefaultBatchHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);
        int size = pos - initialPosition;
        int writtenCompressed = size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        int offsetDelta = (int) (lastOffset - baseOffset);

        final long maxTimestamp;
        if (timestampType == TimestampType.LOG_APPEND_TIME)
            maxTimestamp = logAppendTime;
        else
            maxTimestamp = this.maxTimestamp;

        DefaultRecordBatch.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compressionType, timestampType,
                firstTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch,
                partitionLeaderEpoch, numRecords);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * Write the header to the legacy batch.
     * @return the written compressed bytes.
     */
    private int writeLegacyCompressedWrapperHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int pos = buffer.position();
        buffer.position(initialPosition);

        int wrapperSize = pos - initialPosition - Records.LOG_OVERHEAD;
        int writtenCompressed = wrapperSize - LegacyRecord.recordOverhead(magic);
        AbstractLegacyRecordBatch.writeHeader(buffer, lastOffset, wrapperSize);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compressionType, timestampType);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * Append a record and return its checksum for message format v0 and v1, or null for v2 and above.
     */
    private Long appendWithOffset(long offset, boolean isControlRecord, long timestamp, ByteBuffer key,
                                  ByteBuffer value, Header[] headers) {
        try {
            // 检查 isControlRecord 标志是否一致
            if (isControlRecord != isControlBatch)
                throw new IllegalArgumentException("Control records can only be appended to control batches");

            // 严格限制消息必须从上次写入的位置开始往后写
            if (lastOffset != null && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %s following previous offset %s " +
                        "(Offsets must increase monotonically).", offset, lastOffset));
            // 检查时间戳
            if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);

            // 只有v2版本的才能有header
            if (magic < RecordBatch.MAGIC_VALUE_V2 && headers != null && headers.length > 0)
                throw new IllegalArgumentException("Magic v" + magic + " does not support record headers");

            // 如果该批次还没有写入过消息，那么就更新下该批次第一次写入消息的时间
            if (firstTimestamp == null)
                firstTimestamp = timestamp;

            // V2版本的批次消息写入
            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                appendDefaultRecord(offset, timestamp, key, value, headers);
                return null;
            } else {
                // V0、V1版本的批次消息写入
                return appendLegacyRecord(offset, timestamp, key, value, magic);
            }
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(offset, false, timestamp, key, value, headers);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        return appendWithOffset(offset, timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        return appendWithOffset(offset, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendWithOffset(long offset, SimpleRecord record) {
        return appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a control record at the given offset. The control record type must be known or
     * this method will raise an error.
     *
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendControlRecordWithOffset(long offset, SimpleRecord record) {
        short typeId = ControlRecordType.parseTypeId(record.key());
        ControlRecordType type = ControlRecordType.fromTypeId(typeId);
        if (type == ControlRecordType.UNKNOWN)
            throw new IllegalArgumentException("Cannot append record with unknown control record type " + typeId);

        return appendWithOffset(offset, true, record.timestamp(),
            record.key(), record.value(), record.headers());
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, ByteBuffer key, ByteBuffer value) {
        return append(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     * 追加新纪录到 batch 中
     */
    public Long append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        return appendWithOffset(nextSequentialOffset(), timestamp, key, value, headers);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, byte[] key, byte[] value) {
        return append(timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param key The record key
     * @param value The record value
     * @param headers The record headers if there are any
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        // 追加消息到批次中
        return append(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the next sequential offset.
     * @param record The record to append
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long append(SimpleRecord record) {
        return appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * Append a control record at the next sequential offset.
     * @param timestamp The record timestamp
     * @param type The control record type (cannot be UNKNOWN)
     * @param value The control record value
     * @return CRC of the record or null if record-level CRC is not supported for the message format
     */
    private Long appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        Struct keyStruct = type.recordKey();
        ByteBuffer key = ByteBuffer.allocate(keyStruct.sizeOf());
        keyStruct.writeTo(key);
        key.flip();
        return appendWithOffset(nextSequentialOffset(), true, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendEndTxnMarker(long timestamp, EndTransactionMarker marker) {
        if (producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("End transaction marker requires a valid producerId");
        if (!isTransactional)
            throw new IllegalArgumentException("End transaction marker depends on batch transactional flag being enabled");
        ByteBuffer value = marker.serializeValue();
        return appendControlRecord(timestamp, marker.controlType(), value);
    }

    /**
     * Return CRC of the record or null if record-level CRC is not supported for the message format
     */
    public Long appendLeaderChangeMessage(long timestamp, LeaderChangeMessage leaderChangeMessage) {
        if (partitionLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) {
            throw new IllegalArgumentException("Partition leader epoch must be valid, but get " + partitionLeaderEpoch);
        }

        ObjectSerializationCache cache = new ObjectSerializationCache();
        int size = leaderChangeMessage.size(cache, ControlRecordUtils.LEADER_CHANGE_SCHEMA_VERSION);
        ByteBuffer buffer = ByteBuffer.allocate(size);
        ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
        leaderChangeMessage.write(accessor, cache, ControlRecordUtils.LEADER_CHANGE_SCHEMA_VERSION);
        buffer.flip();
        return appendControlRecord(timestamp, ControlRecordType.LEADER_CHANGE, buffer);
    }

    /**
     * Add a legacy record without doing offset/magic validation (this should only be used in testing).
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, LegacyRecord record) {
        ensureOpenForRecordAppend();
        try {
            int size = record.sizeInBytes();
            AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

            ByteBuffer buffer = record.buffer().duplicate();
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            recordWritten(offset, record.timestamp(), size + Records.LOG_OVERHEAD);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a record without doing offset/magic validation (this should only be used in testing).
     *
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, SimpleRecord record) throws IOException {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            int offsetDelta = (int) (offset - baseOffset);
            long timestamp = record.timestamp();
            if (firstTimestamp == null)
                firstTimestamp = timestamp;

            int sizeInBytes = DefaultRecord.writeTo(appendStream,
                offsetDelta,
                timestamp - firstTimestamp,
                record.key(),
                record.value(),
                record.headers());
            recordWritten(offset, timestamp, sizeInBytes);
        } else {
            LegacyRecord legacyRecord = LegacyRecord.create(magic,
                record.timestamp(),
                Utils.toNullableArray(record.key()),
                Utils.toNullableArray(record.value()));
            appendUncheckedWithOffset(offset, legacyRecord);
        }
    }

    /**
     * Append a record at the next sequential offset.
     * @param record the record to add
     */
    public void append(Record record) {
        appendWithOffset(record.offset(), isControlBatch, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a log record using a different offset
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, Record record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Add a record with a given offset. The record must have a magic which matches the magic use to
     * construct this builder and the offset must be greater than the last appended record.
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, LegacyRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value());
    }

    /**
     * Append the record at the next consecutive offset. If no records have been appended yet, use the base
     * offset of this builder.
     * @param record The record to add
     */
    public void append(LegacyRecord record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value,
                                     Header[] headers) throws IOException {
        // 确认该批次是开启的（比如：已写满就会关闭批次）
        ensureOpenForRecordAppend();
        // 计算要写入的偏移量（也就是写入的消息报文有多大，要写入多少字节到appendStream）
        int offsetDelta = (int) (offset - baseOffset);
        // 计算本次写入与首次写入的时间差
        long timestampDelta = timestamp - firstTimestamp;
        // 将消息写入到 appendStream 并返回该批次中压缩前的消息体大小
        int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
        // 消息写入成功后更新该batch的元信息
        recordWritten(offset, timestamp, sizeInBytes);
    }

    private long appendLegacyRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value, byte magic) throws IOException {
        ensureOpenForRecordAppend();
        if (compressionType == CompressionType.NONE && timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;

        int size = LegacyRecord.recordSize(magic, key, value);
        AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

        if (timestampType == TimestampType.LOG_APPEND_TIME)
            timestamp = logAppendTime;
        long crc = LegacyRecord.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
        recordWritten(offset, timestamp, size + Records.LOG_OVERHEAD);
        return crc;
    }

    private long toInnerOffset(long offset) {
        // use relative offsets for compressed messages with magic v1
        if (magic > 0 && compressionType != CompressionType.NONE)
            return offset - baseOffset;
        return offset;
    }

    private void recordWritten(long offset, long timestamp, int size) {
        if (numRecords == Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum number of records per batch exceeded, max records: " + Integer.MAX_VALUE);
        if (offset - baseOffset > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum offset delta exceeded, base offset: " + baseOffset +
                    ", last offset: " + offset);

        // 写入该批次里的消息数量+1
        numRecords += 1;
        // 压缩前的消息体大小+size
        uncompressedRecordsSizeInBytes += size;
        // 最后一次写入的偏移量
        lastOffset = offset;

        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestamp > maxTimestamp) {
            maxTimestamp = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    private void ensureOpenForRecordAppend() {
        if (appendStream == CLOSED_STREAM)
            throw new IllegalStateException("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
    }

    private void ensureOpenForRecordBatchWrite() {
        if (isClosed())
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        if (aborted)
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
    }

    /**
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}.
     * @return The estimated number of bytes written
     * 计算预计写入的字节数大小
     */
    private int estimatedBytesWritten() {
        // 如果没有开启消息压缩，则写入的大小 = 消息头 + 消息体。如果开启了压缩消息体，则大小 = 消息头 + 未压缩的消息大小 * 压缩比例
        if (compressionType == CompressionType.NONE) {
            return batchHeaderSizeInBytes + uncompressedRecordsSizeInBytes;
        } else {
            // estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
            return batchHeaderSizeInBytes + (int) (uncompressedRecordsSizeInBytes * estimatedCompressionRatio * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    /**
     * Set the estimated compression ratio for the memory records builder.
     */
    public void setEstimatedCompressionRatio(float estimatedCompressionRatio) {
        this.estimatedCompressionRatio = estimatedCompressionRatio;
    }

    /**
     * Check if we have room for a new record containing the given key/value pair. If no records have been
     * appended, then this returns true.
     */
    public boolean hasRoomFor(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return hasRoomFor(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Check if we have room for a new record containing the given key/value pair. If no records have been
     * appended, then this returns true.
     *
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     */
    public boolean hasRoomFor(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        // 已经写满了，检查两个条件：1、appendStream流状态为未关闭，2、当前以写入的预估字节数是否已经超过了该batch的容量上线
        if (isFull())
            return false;

        // We always allow at least one record to be appended (the ByteBufferOutputStream will grow as needed)
        // 当前批次里写入的消息记录数量为0，说明没有写入过消息，每个批次至少可以写一条消息
        if (numRecords == 0)
            return true;

        final int recordSize;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            // 预估 v0、v1版本的record大小
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
        } else {
            int nextOffsetDelta = lastOffset == null ? 0 : (int) (lastOffset - baseOffset + 1);
            long timestampDelta = firstTimestamp == null ? 0 : timestamp - firstTimestamp;
            // 预估v2版本的record大小
            recordSize = DefaultRecord.sizeInBytes(nextOffsetDelta, timestampDelta, key, value, headers);
        }

        // Be conservative and not take compression of the new record into consideration.
        // 已经写入的消息大小 + 待写入的消息大小仍然小于该批次能写入的最大大小则该批次能继续写入消息
        return this.writeLimit >= estimatedBytesWritten() + recordSize;
    }

    public boolean isClosed() {
        return builtRecords != null;
    }

    public boolean isFull() {
        // note that the write limit is respected only after the first record is added which ensures we can always
        // create non-empty batches (this is used to disable batching when the producer's batch size is set to 0).
        // appendStream 不是 CLOSED_STREAM 或 （该批次里没有写入过消息 并且预估的写入量要小于该批次允许的最大写入限制，比如一个批次最大限制写入16kb的消息）
        return appendStream == CLOSED_STREAM || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
    }

    /**
     * Get an estimate of the number of bytes written to the underlying buffer. The returned value
     * is exactly correct if the record set is not compressed or if the builder has been closed.
     */
    public int estimatedSizeInBytes() {
        return builtRecords != null ? builtRecords.sizeInBytes() : estimatedBytesWritten();
    }

    public byte magic() {
        return magic;
    }

    // 计算下一个连续的偏移量
    private long nextSequentialOffset() {
        // 如果最近一次写入的位置为空，则从baseOffset开始写，否则从最近一次写入的位置后接着写
        return lastOffset == null ? baseOffset : lastOffset + 1;
    }

    public static class RecordsInfo {
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public RecordsInfo(long maxTimestamp,
                           long shallowOffsetOfMaxTimestamp) {
            this.maxTimestamp = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }

    /**
     * Return the producer id of the RecordBatches created by this builder.
     */
    public long producerId() {
        return this.producerId;
    }

    public short producerEpoch() {
        return this.producerEpoch;
    }

    public int baseSequence() {
        return this.baseSequence;
    }
}
