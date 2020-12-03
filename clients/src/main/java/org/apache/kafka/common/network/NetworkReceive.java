/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.common.network;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.ScatteringByteChannel;

/**
 * A size delimited Receive that consists of a 4 byte network-ordered size N followed by N bytes of content
 */
public class NetworkReceive implements Receive {

    public final static String UNKNOWN_SOURCE = "";
    public final static int UNLIMITED = -1;

    private final String source;

    // 4个字节的 ByteBuffer  为什么叫size，因为这4个字节代表数据的大小
    /**
     *  每个响应中间必须插入一个特殊的几个字节的分隔符，
     *  一般来说用作分隔符很经典的就是在响应前先插入4个字节（integer）代表响应消息自己本身数据大小
     */
    private final ByteBuffer size;

    private final int maxSize;
    private ByteBuffer buffer;


    public NetworkReceive(String source, ByteBuffer buffer) {
        this.source = source;
        this.buffer = buffer;
        this.size = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = UNLIMITED;
    }

    public NetworkReceive(int maxSize, String source) {
        this.source = source;
        this.size = ByteBuffer.allocate(4);
        this.buffer = null;
        this.maxSize = maxSize;
    }

    public NetworkReceive() {
        this(UNKNOWN_SOURCE);
    }

    @Override
    public String source() {
        return source;
    }

    @Override
    public boolean complete() {
        // 解决拆包问题2，
        return !size.hasRemaining() && !buffer.hasRemaining();
    }

    public long readFrom(ScatteringByteChannel channel) throws IOException {
        return readFromReadableChannel(channel);
    }

    // Need a method to read from ReadableByteChannel because BlockingChannel requires read with timeout
    // See: http://stackoverflow.com/questions/2866557/timeout-for-socketchannel-doesnt-work
    // This can go away after we get rid of BlockingChannel
    @Deprecated
    public long readFromReadableChannel(ReadableByteChannel channel) throws IOException {

        /**
         * 粘包问题：
         * 读取数据的时候，粘包的情况怎么处理 ： 很多响应粘在一块，如何区分？
         *
         * 拆包问题：
         * 对方一条数据是分开发送的?
         *
         */

        int read = 0;
        if (size.hasRemaining()) {

            // 读取4个字节的数字，代表后面真实数据的长度
            int bytesRead = channel.read(size);

            if (bytesRead < 0)
                throw new EOFException();

            read += bytesRead;


            // 拆包问题解决1：如果size没有读满，就不会进入if里面，buffer也就是空，也就不会读数据，直到size读完整
            if (!size.hasRemaining()) {
                // 读满了就 rewind ，就是position设置为0，可读
                size.rewind();

                // 这个 receiveSize 代表响应消息的大小
                int receiveSize = size.getInt();

                if (receiveSize < 0)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + ")");

                if (maxSize != UNLIMITED && receiveSize > maxSize)
                    throw new InvalidReceiveException("Invalid receive (size = " + receiveSize + " larger than " + maxSize + ")");

                /**
                 * 分配实际大小的ByteBuffer
                 * 用来接收数据
                 */
                this.buffer = ByteBuffer.allocate(receiveSize);
            }
        }

        if (buffer != null) {
            int bytesRead = channel.read(buffer);
            if (bytesRead < 0)
                throw new EOFException();
            read += bytesRead;
        }

        return read;
    }

    public ByteBuffer payload() {
        return this.buffer;
    }

}
