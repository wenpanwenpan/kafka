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

/*
 * Transport layer for PLAINTEXT communication
 */

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.security.auth.KafkaPrincipal;

public class PlaintextTransportLayer implements TransportLayer {
    // Java NIO中的selectionKey事件
    private final SelectionKey key;
    // Java NIO中的socketChannel
    private final SocketChannel socketChannel;
    // 安全相关
    private final Principal principal = KafkaPrincipal.ANONYMOUS;

    public PlaintextTransportLayer(SelectionKey key) throws IOException {
        this.key = key;
        // 从selectionKey中获取对应的channel并进行保存到socketChannel属性
        this.socketChannel = (SocketChannel) key.channel();
    }

    @Override
    public boolean ready() {
        return true;
    }

    @Override
    public boolean finishConnect() throws IOException {
        // 调用socketChannel的finishConnect方法，返回该连接是否已经连接完成
        boolean connected = socketChannel.finishConnect();
        // 如果网络连接完成，则删除对 OP_CONNECT事件的监听，同时添加对 OP_READ 事件的监听，因为连接完成后就要开始收发数据了
        if (connected)
            // 这里通过位运算来表达增加或移除事件监听
            key.interestOps(key.interestOps() & ~SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        // 返回是否完成连接
        return connected;
    }

    @Override
    public void disconnect() {
        key.cancel();
    }

    @Override
    public SocketChannel socketChannel() {
        return socketChannel;
    }

    @Override
    public SelectionKey selectionKey() {
        return key;
    }

    @Override
    public boolean isOpen() {
        return socketChannel.isOpen();
    }

    @Override
    public boolean isConnected() {
        return socketChannel.isConnected();
    }

    @Override
    public void close() throws IOException {
        socketChannel.socket().close();
        socketChannel.close();
    }

    /**
     * Performs SSL handshake hence is a no-op for the non-secure
     * implementation
     */
    @Override
    public void handshake() {}

    /**
    * Reads a sequence of bytes from this channel into the given buffer.
    *
    * @param dst The buffer into which bytes are to be transferred
    * @return The number of bytes read, possible zero or -1 if the channel has reached end-of-stream
    * @throws IOException if some other I/O error occurs
    */
    @Override
    public int read(ByteBuffer dst) throws IOException {
        // 将NIO的channel里的数据读取到 byteBuffer中
        return socketChannel.read(dst);
    }

    /**
     * Reads a sequence of bytes from this channel into the given buffers.
     *
     * @param dsts - The buffers into which bytes are to be transferred.
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return socketChannel.read(dsts);
    }

    /**
     * Reads a sequence of bytes from this channel into a subsequence of the given buffers.
     * @param dsts - The buffers into which bytes are to be transferred
     * @param offset - The offset within the buffer array of the first buffer into which bytes are to be transferred; must be non-negative and no larger than dsts.length.
     * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than dsts.length - offset
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream.
     * @throws IOException if some other I/O error occurs
     */
    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return socketChannel.read(dsts, offset, length);
    }

    /**
    * Writes a sequence of bytes to this channel from the given buffer.
    *
    * @param src The buffer from which bytes are to be retrieved
    * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public int write(ByteBuffer src) throws IOException {
        // 将byteBuffer里的数据写入到nio的channel里
        return socketChannel.write(src);
    }

    /**
    * Writes a sequence of bytes to this channel from the given buffer.
    *
    * @param srcs The buffer from which bytes are to be retrieved
    * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return socketChannel.write(srcs);
    }

    /**
    * Writes a sequence of bytes to this channel from the subsequence of the given buffers.
    *
    * @param srcs The buffers from which bytes are to be retrieved
    * @param offset The offset within the buffer array of the first buffer from which bytes are to be retrieved; must be non-negative and no larger than srcs.length.
    * @param length - The maximum number of buffers to be accessed; must be non-negative and no larger than srcs.length - offset.
    * @return returns no.of bytes written , possibly zero.
    * @throws IOException If some other I/O error occurs
    */
    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return socketChannel.write(srcs, offset, length);
    }

    /**
     * always returns false as there will be not be any
     * pending writes since we directly write to socketChannel.
     */
    @Override
    public boolean hasPendingWrites() {
        return false;
    }

    /**
     * Returns ANONYMOUS as Principal.
     */
    @Override
    public Principal peerPrincipal() {
        return principal;
    }

    /**
     * Adds the interestOps to selectionKey.
     */
    @Override
    public void addInterestOps(int ops) {
        // 添加感兴趣的事件到selector上
        key.interestOps(key.interestOps() | ops);

    }

    /**
     * Removes the interestOps from selectionKey.
     */
    @Override
    public void removeInterestOps(int ops) {
        key.interestOps(key.interestOps() & ~ops);
    }

    @Override
    public boolean isMute() {
        return key.isValid() && (key.interestOps() & SelectionKey.OP_READ) == 0;
    }

    @Override
    public boolean hasBytesBuffered() {
        return false;
    }

    @Override
    public long transferFrom(FileChannel fileChannel, long position, long count) throws IOException {
        return fileChannel.transferTo(position, count, socketChannel);
    }
}
