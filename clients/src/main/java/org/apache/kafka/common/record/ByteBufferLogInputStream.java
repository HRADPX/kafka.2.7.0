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

import static org.apache.kafka.common.record.Records.HEADER_SIZE_UP_TO_MAGIC;
import static org.apache.kafka.common.record.Records.LOG_OVERHEAD;
import static org.apache.kafka.common.record.Records.MAGIC_OFFSET;
import static org.apache.kafka.common.record.Records.SIZE_OFFSET;

import java.nio.ByteBuffer;

import org.apache.kafka.common.errors.CorruptRecordException;

/**
 * A byte buffer backed log input stream. This class avoids the need to copy records by returning
 * slices from the underlying byte buffer.
 */
class ByteBufferLogInputStream implements LogInputStream<MutableRecordBatch> {
    private final ByteBuffer buffer;
    private final int maxMessageSize;

    ByteBufferLogInputStream(ByteBuffer buffer, int maxMessageSize) {
        this.buffer = buffer;
        this.maxMessageSize = maxMessageSize;
    }

    public MutableRecordBatch nextBatch() {
        // 字节数据有多少字节
        int remaining = buffer.remaining();

        // 下一批占用的字节（包含 baseOffset 和 length 占用的字节数量）
        Integer batchSize = nextBatchSize();
        if (batchSize == null || remaining < batchSize)
            return null;

        // magic
        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);

        ByteBuffer batchSlice = buffer.slice();
        batchSlice.limit(batchSize);
        // 移动 position
        buffer.position(buffer.position() + batchSize);

        if (magic > RecordBatch.MAGIC_VALUE_V1)
            return new DefaultRecordBatch(batchSlice);
        else
            return new AbstractLegacyRecordBatch.ByteBufferLegacyRecordBatch(batchSlice);
    }

    /**
     * Validates the header of the next batch and returns batch size.
     * @return next batch size including LOG_OVERHEAD if buffer contains header up to
     *         magic byte, null otherwise
     * @throws CorruptRecordException if record size or magic is invalid
     */
    Integer nextBatchSize() throws CorruptRecordException {
        int remaining = buffer.remaining();
        // offset + batch size
        if (remaining < LOG_OVERHEAD)
            return null;
        // 下一批次的消息的占用的字节大小
        int recordSize = buffer.getInt(buffer.position() + SIZE_OFFSET);
        // V0 has the smallest overhead, stricter checking is done later
        if (recordSize < LegacyRecord.RECORD_OVERHEAD_V0)
            throw new CorruptRecordException(String.format("Record size %d is less than the minimum record overhead (%d)",
                    recordSize, LegacyRecord.RECORD_OVERHEAD_V0));
        if (recordSize > maxMessageSize)
            throw new CorruptRecordException(String.format("Record size %d exceeds the largest allowable message size (%d).",
                    recordSize, maxMessageSize));

        // 小于 17 （baseOffset + length + epoch + magic = 8 + 4 + 4 + 1）没有消息了
        if (remaining < HEADER_SIZE_UP_TO_MAGIC)
            return null;

        byte magic = buffer.get(buffer.position() + MAGIC_OFFSET);
        if (magic < 0 || magic > RecordBatch.CURRENT_MAGIC_VALUE)
            throw new CorruptRecordException("Invalid magic found in record: " + magic);

        // 因为在计算消息批次大小的时候去掉了 baseOffset + length 占用的 12 个字节，这里加上
        return recordSize + LOG_OVERHEAD;
    }
}
