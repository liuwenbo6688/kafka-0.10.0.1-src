/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import java.io.IOException;

import java.net.InetAddress;
import java.net.Socket;
import java.nio.channels.SelectionKey;

import java.security.Principal;

import org.apache.kafka.common.utils.Utils;

public class KafkaChannel {

    /**
     * broker.id
     */
    private final String id;

    /**
     *  TransportLayer封装底层的SocketChannel
     */
    private final TransportLayer transportLayer;

    private final Authenticator authenticator;
    private final int maxReceiveSize;

    /**
     * 最近一次读出来的响应
     * 也是不断变换的
     */
    private NetworkReceive receive;

    /**
     *  交给底层channel发送出去的请求
     *  是不断变换的
     */
    private Send send;


    public KafkaChannel(String id, TransportLayer transportLayer, Authenticator authenticator, int maxReceiveSize) throws IOException {
        this.id = id;
        this.transportLayer = transportLayer;
        this.authenticator = authenticator;
        this.maxReceiveSize = maxReceiveSize;
    }

    public void close() throws IOException {
        Utils.closeAll(transportLayer, authenticator);
    }

    /**
     * Returns the principal returned by `authenticator.principal()`.
     */
    public Principal principal() throws IOException {
        return authenticator.principal();
    }

    /**
     * Does handshake of transportLayer and authentication using configured authenticator
     */
    public void prepare() throws IOException {
        if (!transportLayer.ready())
            transportLayer.handshake();
        if (transportLayer.ready() && !authenticator.complete())
            authenticator.authenticate();
    }

    public void disconnect() {
        transportLayer.disconnect();
    }


    public boolean finishConnect() throws IOException {
        return transportLayer.finishConnect();
    }

    public boolean isConnected() {
        return transportLayer.isConnected();
    }

    public String id() {
        return id;
    }

    public void mute() {
        transportLayer.removeInterestOps(SelectionKey.OP_READ);
    }

    public void unmute() {
        transportLayer.addInterestOps(SelectionKey.OP_READ);
    }

    public boolean isMute() {
        return transportLayer.isMute();
    }

    public boolean ready() {
        return transportLayer.ready() && authenticator.complete();
    }

    public boolean hasSend() {
        return send != null;
    }

    /**
     * Returns the address to which this channel's socket is connected or `null` if the socket has never been connected.
     *
     * If the socket was connected prior to being closed, then this method will continue to return the
     * connected address after the socket is closed.
     */
    public InetAddress socketAddress() {
        return transportLayer.socketChannel().socket().getInetAddress();
    }

    public String socketDescription() {
        Socket socket = transportLayer.socketChannel().socket();
        if (socket.getInetAddress() == null)
            return socket.getLocalAddress().toString();
        return socket.getInetAddress().toString();
    }

    /**
     * 暂存发送的请求
     * 然后关注OP_WRITE事件
     */
    public void setSend(Send send) {
        if (this.send != null)
            throw new IllegalStateException("Attempt to begin a send operation with prior send operation still in progress.");
        /**
         * 缓存到 KafkaChannel 的 send 中
         */
        this.send = send;
        // 监听 OP_WRITE 事件
        this.transportLayer.addInterestOps(SelectionKey.OP_WRITE);
    }

    public NetworkReceive read() throws IOException {
        NetworkReceive result = null;

        if (receive == null) {
            receive = new NetworkReceive(maxReceiveSize, id);
        }

        // 读取消息
        receive(receive);


        // 拆包解决问题2：数据没有读完，就不会走这边的if代码段，下次还是可以继续往buffer里读数据的
        if (receive.complete()) {
            receive.payload().rewind();
            result = receive;

            // receive置为空了，可以继续读取数据了
            receive = null;
        }

        return result;
    }

    public Send write() throws IOException {
        Send result = null;
        if (send != null && send(send)) {
            /**
             * 只有请求发送完成，send才会置为nul
             */
            result = send;
            send = null;
        }
        return result;
    }

    private long receive(NetworkReceive receive) throws IOException {
        return receive.readFrom(transportLayer);
    }

    private boolean send(Send send) throws IOException {
        // ？？ 如果一次没有发送完，是怎么处理的？？？？

        send.writeTo(transportLayer);

        // 全部发送完成，拆包问题，如果没有发送完成，就不会去取消关注OP_WRITE， 下一个循环会继续发送数据
        if (send.completed())
            // 发送完之后取消对 OP_WRITE 事件的监听
            transportLayer.removeInterestOps(SelectionKey.OP_WRITE);

        // 判断这个请求发送是否完毕，有可能只发送了一部分，没有全部发送完毕
        return send.completed();
    }

}
