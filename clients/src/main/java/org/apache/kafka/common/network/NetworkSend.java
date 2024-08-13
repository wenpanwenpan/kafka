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
package org.apache.kafka.common.network;

import java.nio.ByteBuffer;

/**
 * A size delimited Send that consists of a 4 byte network-ordered size N followed by N bytes of content
 * kafka对写buffer的封装
 */
public class NetworkSend extends ByteBufferSend {

    public NetworkSend(String destination, ByteBuffer buffer) {
        // buffer.remaining() 表示该buffer还有多少字节未写（不是指该buffer还有多少可写空间，这是写buffer不是读buffer）
        super(destination, sizeBuffer(buffer.remaining()), buffer);
    }

    // 用来构造四个字节的size buffer
    private static ByteBuffer sizeBuffer(int size) {
        // 先分配4个字节的buffer空间
        ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
        // 写入size长度
        sizeBuffer.putInt(size);
        // 重置position为 0
        sizeBuffer.rewind();
        return sizeBuffer;
    }

}
