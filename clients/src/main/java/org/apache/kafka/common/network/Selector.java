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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.metrics.stats.Max;
import org.apache.kafka.common.metrics.stats.Rate;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A nioSelector interface for doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit size-delimited network requests and
 * responses.
 * <p>
 * A connection can be added to the nioSelector associated with an integer id by doing
 *
 * <pre>
 * nioSelector.connect(&quot;42&quot;, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established.
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 *
 * <pre>
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * </pre>
 *
 * The nioSelector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 *
 * This class is not thread safe!
 * 针对多个broker的网络连接，执行非阻塞的IO操作
 * 这个class是 非线程安全 的！！！！！！
 *
 * 通过 poll()方法来进行实际的 发送请求、接收响应、处理建立连接和断开连接
 */
public class Selector implements Selectable {

    private static final Logger log = LoggerFactory.getLogger(Selector.class);

    // 最核心的东西就是封装了java nio 的Selector
    private final java.nio.channels.Selector nioSelector;


    /**
     * 保存了每个 broker id 到 Channel的映射关系
     *  KafkaChannel 封装了 SocketChannel
     */
    private final Map<String, KafkaChannel> channels;

    // 已经发送出去的请求
    private final List<Send> completedSends;

    // 已经接收回来的响应
    private final List<NetworkReceive> completedReceives;

    // 每个broker接收到的，但是还没有被处理的响应
    // staged : 暂存的意思
    private final Map<KafkaChannel, Deque<NetworkReceive>> stagedReceives;

    // 立即就连接上的SelectionKey，缓存到immediatelyConnectedKeys中
    private final Set<SelectionKey> immediatelyConnectedKeys;

    private final List<String> disconnected;
    /**
     * 缓存已经建立好连接的节点
     */
    private final List<String> connected;
    private final List<String> failedSends;

    private final Time time;
    private final SelectorMetrics sensors;
    private final String metricGrpPrefix;
    private final Map<String, String> metricTags;

    /**
     *  构造KafkaChannel的构造器类
     */
    private final ChannelBuilder channelBuilder;


    private final Map<String, Long> lruConnections;
    private final long connectionsMaxIdleNanos;
    private final int maxReceiveSize;
    private final boolean metricsPerConnection;
    private long currentTimeNanos;
    private long nextIdleCloseCheckTime;


    /**
     * Create a new nioSelector
     */
    public Selector(int maxReceiveSize, long connectionMaxIdleMs, Metrics metrics, Time time, String metricGrpPrefix, Map<String, String> metricTags, boolean metricsPerConnection, ChannelBuilder channelBuilder) {

        try {
            // 最核心的一点，kafka selector的底层就是封装了java NIO 的 selector
            // 多路复用
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }

        this.maxReceiveSize = maxReceiveSize;
        // 一个网络连接最长空闲时间
        this.connectionsMaxIdleNanos = connectionMaxIdleMs * 1000 * 1000;
        this.time = time;
        this.metricGrpPrefix = metricGrpPrefix;
        this.metricTags = metricTags;

        /**
         *  保存了每个 broker id 到 Channel的映射关系
         */
        this.channels = new HashMap<>();

        // 已经发送出去的请求
        this.completedSends = new ArrayList<>();

        // 已经接收回来的响应
        this.completedReceives = new ArrayList<>();

        // 每个broker接收到的，但是还没有被处理的响应
        this.stagedReceives = new HashMap<>();


        this.immediatelyConnectedKeys = new HashSet<>();
        this.connected = new ArrayList<>();
        this.disconnected = new ArrayList<>();
        this.failedSends = new ArrayList<>();
        this.sensors = new SelectorMetrics(metrics);
        this.channelBuilder = channelBuilder;
        // initial capacity and load factor are default, we set them explicitly because we want to set accessOrder = true
        this.lruConnections = new LinkedHashMap<>(16, .75F, true);
        currentTimeNanos = time.nanoseconds();
        nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
        this.metricsPerConnection = metricsPerConnection;
    }

    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, new HashMap<String, String>(), true, channelBuilder);
    }

    /**
     * Begin connecting to the given address and add the connection to this nioSelector associated with the given id
     * number.
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
     *  这个方法只是初始化，会在 poll 方法中完成连接
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     *
     * 这个方法只是初始化连接， 最终会在 poll(long) 方法中完成
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        if (this.channels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);

        /**
         * 下面一段代码，就是标准的NIO写法，非常非常经典的
         * 工业级的NIO  KeepAlive  SocketBuffer  TcpNoDelay 都是如何设置的
         */
        SocketChannel socketChannel = SocketChannel.open();
        socketChannel.configureBlocking(false); // 非阻塞

        // 设置socket的参数，工业级配置
        Socket socket = socketChannel.socket();
        /**
         *
         */
        socket.setKeepAlive(true);
        /**
         * 设置发送、接收缓冲区
         */
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setSendBufferSize(sendBufferSize);
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setReceiveBufferSize(receiveBufferSize);

        /**
         * 不要延迟发送，关闭nagle算法
         */
        socket.setTcpNoDelay(true);

        boolean connected;
        try {
            /**
             * 如果设置为非阻塞的 configureBlocking(false)
             * 那么connect方法的调用，会初始化一个非阻塞的请求连接
             * connect 的语义
             */
            connected = socketChannel.connect(address);
        } catch (UnresolvedAddressException e) {
            socketChannel.close();
            throw new IOException("Can't resolve address: " + address, e);
        } catch (IOException e) {
            socketChannel.close();
            throw e;
        }

        // 把socket channel注册到nioSelector上，让他监视 OP_CONNECT 事件
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_CONNECT);


        /**
         * 设计模式- 构造者模式
         * 初始化一个 KafkaChannel
         */
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);

        /**
         * 又把KafkaChannel这东西 attach到SelectionKey上
         * 后面可以用SelectionKey直接拿到 KafkaChannel
         */
        key.attach(channel);

        /**
         *  把 KafkaChannel 缓存起来
         */
        this.channels.put(id, channel);


        if (connected) {
            // 如果立即就连接上了

            // OP_CONNECT won't trigger for immediately connected channels
            log.debug("Immediately connected to node {}", channel.id());
            // 立即就连接上的SelectionKey，缓存到immediatelyConnectedKeys中
            immediatelyConnectedKeys.add(key);
            key.interestOps(0); // 0是什么意思？
        }

    }

    /**
     * Register the nioSelector with an existing channel
     * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
     * Note that we are not checking if the connection id is valid - since the connection already exists
     */
    public void register(String id, SocketChannel socketChannel) throws ClosedChannelException {

        /**
         * socketChannel注册到Selector上，关注OP_REA事件
         */
        SelectionKey key = socketChannel.register(nioSelector, SelectionKey.OP_READ);

        /**
         * 创建 KafkaChannel，加入到缓存map中
         */
        KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize);
        key.attach(channel);// 反向把KafkaChannel 绑定到 SelectionKey 上
        this.channels.put(id, channel);
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     */
    @Override
    public void wakeup() {
        // 唤醒 nioSelector.select(), 立即返回
        this.nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        for (String id : connections)
            close(id);
        try {
            this.nioSelector.close();
        } catch (IOException | SecurityException e) {
            log.error("Exception closing nioSelector:", e);
        }
        sensors.close();
        channelBuilder.close();
    }

    /**
     * Queue the given request for sending in the subsequent {@link #poll(long)} calls
     * @param send The request to send
     */
    public void send(Send send) {
        KafkaChannel channel = channelOrFail(send.destination());
        try {
            // 把请求暂存到KafkaChannel里面
            channel.setSend(send);
        } catch (CancelledKeyException e) {
            this.failedSends.add(send.destination());
            close(channel);
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
     * disconnections, initiating new sends, or making progress on in-progress sends or receives.
     *
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each `poll` call and repopulated by the call if there is
     * any completed I/O.
     *
     * In the "Plaintext" setting, we are using socketChannel to read & write to the network. But for the "SSL" setting,
     * we encrypt the data before we use socketChannel to write data to the network, and decrypt before we return the responses.
     * This requires additional buffers to be maintained as we are reading from network, since the data on the wire is encrypted
     * we won't be able to read exact no.of bytes as kafka protocol requires. We read as many bytes as we can, up to SSLEngine's
     * application buffer size. This means we might be reading additional bytes than the requested size.
     * If there is no further data to read from socketChannel selector won't invoke that channel and we've have additional bytes
     * in the buffer. To overcome this issue we added "stagedReceives" map which contains per-channel deque. When we are
     * reading a channel we read as many responses as we can and store them into "stagedReceives" and pop one response during
     * the poll to add the completedReceives. If there are any active channels in the "stagedReceives" we set "timeout" to 0
     * and pop response and add to the completedReceives.
     *
     * @param timeout The amount of time to wait, in milliseconds, which must be non-negative
     * @throws IllegalArgumentException If `timeout` is negative
     * @throws IllegalStateException If a send is given for which we have no existing connection or for which there is
     *         already an in-progress send
     */
    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout should be >= 0");

        clear();

        if (hasStagedReceives() || !immediatelyConnectedKeys.isEmpty())
            timeout = 0;

        /* check ready keys */
        long startSelect = time.nanoseconds();

        /**
         * 调用 nioSelector的select方法
         * 是可以被wakeup唤醒的
         * 返回准备就绪的SelectionKey的数量
         */
        int readyKeys = select(timeout);

        long endSelect = time.nanoseconds();
        currentTimeNanos = endSelect;
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

        if (readyKeys > 0 || !immediatelyConnectedKeys.isEmpty()) {
            /**
             * 获取到一堆的 SelectionKey 进行处理
             */
            pollSelectionKeys(this.nioSelector.selectedKeys(), false);

            /**
             * 立即就连接上的SelectionKey,也可以进行处理了
             */
            pollSelectionKeys(immediatelyConnectedKeys, true);
        }

        // 暂存的staged 转移到 completedReceives 列表中
        addToCompletedReceives();

        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());


        /**
         * 根据lru的思想，把最近没有使用的一些连接关掉
         */
        maybeCloseOldestConnection();
    }

    private void pollSelectionKeys(Iterable<SelectionKey> selectionKeys, boolean isImmediatelyConnected) {

        Iterator<SelectionKey> iterator = selectionKeys.iterator();
        while (iterator.hasNext()) {
            SelectionKey key = iterator.next();
            iterator.remove();

            // 这里就从SelectionKey拿到之前 atach 的 KafkaChannel
            // SelectionKey 和 KafkaChannel 是一一对应的
            KafkaChannel channel = channel(key);

            // register all per-connection metrics at once
            sensors.maybeRegisterConnectionMetrics(channel.id());
            lruConnections.put(channel.id(), currentTimeNanos);// 采用lru的方式，保留最近使用的连接

            try {

                /* complete any connections that have finished their handshake (either normally or immediately) */
                if (isImmediatelyConnected || key.isConnectable()) {
                    // 处理连接的事件
                    if (channel.finishConnect()) {
                        // 调用KafkaChannel的finishConnect() 方法，建立连接成功, 并且关注 OP_READ 事件
                        this.connected.add(channel.id());
                        this.sensors.connectionCreated.record();
                    } else
                        // 连接还没有建立，继续循环处理下一个 SelectionKey
                        continue;
                }


                // 这一块可以忽略
                /* if channel is not ready finish prepare */
                if (channel.isConnected() && !channel.ready())
                    channel.prepare();

                /**
                 * 接收数据的过程
                 */
                /* if channel is ready read from any connections that have readable data */
                if (channel.ready() && key.isReadable()
                        && !hasStagedReceive(channel) /*没有积压的Staged消息*/ ) {
                    NetworkReceive networkReceive;

                    /**
                     * 在这个环节是可能读取多条请求数据的
                     * while循环读取的，直到读不到数据
                     */
                    // 针对一个broker的连接反复的读，粘包和拆包问题的解决方式是大亮点
                    while ((networkReceive = channel.read()) != null)
                        // 读完一个完整的Receive就暂存起来
                        addToStagedReceives(channel, networkReceive);
                }

                /**
                 *  发送请求的过程
                 */
                /* if channel is ready write to any sockets that have space in their buffer and for which we have data */
                if (channel.ready() && key.isWritable()) {
                    Send send = channel.write(); // 调用KafkaChannel的send方法，把请求发送出去

                    if (send != null) {
                        // 请求发送完成后，暂时缓存在completedSends里
                        this.completedSends.add(send);
                        this.sensors.recordBytesSent(channel.id(), send.size());
                    }
                }

                /* cancel any defunct sockets */
                if (!key.isValid()) {
                    close(channel);
                    this.disconnected.add(channel.id());
                }

            } catch (Exception e) {
                String desc = channel.socketDescription();
                if (e instanceof IOException)
                    log.debug("Connection with {} disconnected", desc, e);
                else
                    log.warn("Unexpected error from {}; closing connection", desc, e);
                close(channel);
                /**
                 * 异常处理，放入断开的连接列表中
                 */
                this.disconnected.add(channel.id());
            }
        }
    }

    @Override
    public List<Send> completedSends() {
        return this.completedSends;
    }

    @Override
    public List<NetworkReceive> completedReceives() {
        return this.completedReceives;
    }

    @Override
    public List<String> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = channelOrFail(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = channelOrFail(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        channel.unmute();
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values())
            mute(channel);
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values())
            unmute(channel);
    }

    private void maybeCloseOldestConnection() {
        if (currentTimeNanos > nextIdleCloseCheckTime) {
            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
            } else {
                Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();
                Long connectionLastActiveTime = oldestConnectionEntry.getValue();
                nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;
                if (currentTimeNanos > nextIdleCloseCheckTime) {
                    String connectionId = oldestConnectionEntry.getKey();
                    if (log.isTraceEnabled())
                        log.trace("About to close the idle connection from " + connectionId
                                + " due to being idle for " + (currentTimeNanos - connectionLastActiveTime) / 1000 / 1000 + " millis");

                    disconnected.add(connectionId);
                    close(connectionId);
                }
            }
        }
    }

    /**
     * Clear the results from the prior poll
     */
    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();
        this.disconnected.addAll(this.failedSends);
        this.failedSends.clear();
    }

    /**
     * Check for data, waiting up to the given timeout.
     *
     * @param ms Length of time to wait, in milliseconds, which must be non-negative
     * @return The number of keys ready
     * @throws IllegalArgumentException
     * @throws IOException
     */
    private int select(long ms) throws IOException {
        if (ms < 0L)
            throw new IllegalArgumentException("timeout should be >= 0");

        if (ms == 0L)
            return this.nioSelector.selectNow();
        else
            return this.nioSelector.select(ms);
    }

    /**
     * Close the connection identified by the given id
     */
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null)
            close(channel);
    }

    /**
     * Begin closing this connection
     */
    private void close(KafkaChannel channel) {
        try {
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", channel.id(), e);
        }
        this.stagedReceives.remove(channel);
        this.channels.remove(channel.id());
        this.lruConnections.remove(channel.id());
        this.sensors.connectionClosed.record();
    }


    /**
     * check if channel is ready
     */
    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }

    private KafkaChannel channelOrFail(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel == null)
            throw new IllegalStateException("Attempt to retrieve channel for which there is no open connection. Connection id " + id + " existing connections " + channels.keySet());
        return channel;
    }

    /**
     * Return the selector channels.
     */
    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }

    /**
     * Return the channel associated with this connection or `null` if there is no channel associated with the
     * connection.
     */
    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    /**
     * Get the channel associated with selectionKey
     */
    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    /**
     * Check if given channel has a staged receive
     */
    private boolean hasStagedReceive(KafkaChannel channel) {
        return stagedReceives.containsKey(channel);
    }

    /**
     * check if stagedReceives have unmuted channel
     */
    private boolean hasStagedReceives() {
        for (KafkaChannel channel : this.stagedReceives.keySet()) {
            if (!channel.isMute())
                return true;
        }
        return false;
    }


    /**
     * adds a receive to staged receives
     */
    private void addToStagedReceives(KafkaChannel channel, NetworkReceive receive) {
        if (!stagedReceives.containsKey(channel))
            stagedReceives.put(channel, new ArrayDeque<NetworkReceive>());

        Deque<NetworkReceive> deque = stagedReceives.get(channel);
        deque.add(receive);
    }

    /**
     * checks if there are any staged receives and adds to completedReceives
     */
    private void addToCompletedReceives() {
        if (!this.stagedReceives.isEmpty()) {
            Iterator<Map.Entry<KafkaChannel, Deque<NetworkReceive>>> iter = this.stagedReceives.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<KafkaChannel, Deque<NetworkReceive>> entry = iter.next();
                KafkaChannel channel = entry.getKey();

                if (!channel.isMute()) {//  isMute的判断？
                    Deque<NetworkReceive> deque = entry.getValue();

                    /**
                     * 只从暂存队列中取出(poll)一个响应，然后放到completedReceives队列中
                     * 如果一个连接一次 OP_READ 读取出来多个响应消息的话，这里仅仅会把每个连接对应的第一个响应消息放到 completedReceives 里面去
                     */
                    NetworkReceive networkReceive = deque.poll();
                    this.completedReceives.add(networkReceive);

                    this.sensors.recordBytesReceived(channel.id(), networkReceive.payload().limit());
                    if (deque.isEmpty())
                        iter.remove();
                }

            }
        }
    }


    private class SelectorMetrics {
        private final Metrics metrics;
        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor bytesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;

        /* Names of metrics that are not registered through sensors */
        private final List<MetricName> topLevelMetricNames = new ArrayList<>();
        private final List<Sensor> sensors = new ArrayList<>();

        public SelectorMetrics(Metrics metrics) {
            this.metrics = metrics;
            String metricGrpName = metricGrpPrefix + "-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag: metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = sensor("connections-closed:" + tagsSuffix.toString());
            MetricName metricName = metrics.metricName("connection-close-rate", metricGrpName, "Connections closed per second in the window.", metricTags);
            this.connectionClosed.add(metricName, new Rate());

            this.connectionCreated = sensor("connections-created:" + tagsSuffix.toString());
            metricName = metrics.metricName("connection-creation-rate", metricGrpName, "New connections established per second in the window.", metricTags);
            this.connectionCreated.add(metricName, new Rate());

            this.bytesTransferred = sensor("bytes-sent-received:" + tagsSuffix.toString());
            metricName = metrics.metricName("network-io-rate", metricGrpName, "The average number of network operations (reads or writes) on all connections per second.", metricTags);
            bytesTransferred.add(metricName, new Rate(new Count()));

            this.bytesSent = sensor("bytes-sent:" + tagsSuffix.toString(), bytesTransferred);
            metricName = metrics.metricName("outgoing-byte-rate", metricGrpName, "The average number of outgoing bytes sent per second to all servers.", metricTags);
            this.bytesSent.add(metricName, new Rate());
            metricName = metrics.metricName("request-rate", metricGrpName, "The average number of requests sent per second.", metricTags);
            this.bytesSent.add(metricName, new Rate(new Count()));
            metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", metricTags);
            this.bytesSent.add(metricName, new Avg());
            metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", metricTags);
            this.bytesSent.add(metricName, new Max());

            this.bytesReceived = sensor("bytes-received:" + tagsSuffix.toString(), bytesTransferred);
            metricName = metrics.metricName("incoming-byte-rate", metricGrpName, "Bytes/second read off all sockets", metricTags);
            this.bytesReceived.add(metricName, new Rate());
            metricName = metrics.metricName("response-rate", metricGrpName, "Responses received sent per second.", metricTags);
            this.bytesReceived.add(metricName, new Rate(new Count()));

            this.selectTime = sensor("select-time:" + tagsSuffix.toString());
            metricName = metrics.metricName("select-rate", metricGrpName, "Number of times the I/O layer checked for new I/O to perform per second", metricTags);
            this.selectTime.add(metricName, new Rate(new Count()));
            metricName = metrics.metricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            metricName = metrics.metricName("io-wait-ratio", metricGrpName, "The fraction of time the I/O thread spent waiting.", metricTags);
            this.selectTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            this.ioTime = sensor("io-time:" + tagsSuffix.toString());
            metricName = metrics.metricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            metricName = metrics.metricName("io-ratio", metricGrpName, "The fraction of time the I/O thread spent doing I/O", metricTags);
            this.ioTime.add(metricName, new Rate(TimeUnit.NANOSECONDS));

            metricName = metrics.metricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            topLevelMetricNames.add(metricName);
            this.metrics.addMetric(metricName, new Measurable() {
                public double measure(MetricConfig config, long now) {
                    return channels.size();
                }
            });
        }

        private Sensor sensor(String name, Sensor... parents) {
            Sensor sensor = metrics.sensor(name, parents);
            sensors.add(sensor);
            return sensor;
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    String metricGrpName = metricGrpPrefix + "-node-metrics";

                    Map<String, String> tags = new LinkedHashMap<>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = sensor(nodeRequestName);
                    MetricName metricName = metrics.metricName("outgoing-byte-rate", metricGrpName, tags);
                    nodeRequest.add(metricName, new Rate());
                    metricName = metrics.metricName("request-rate", metricGrpName, "The average number of requests sent per second.", tags);
                    nodeRequest.add(metricName, new Rate(new Count()));
                    metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of all requests in the window..", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent in the window.", tags);
                    nodeRequest.add(metricName, new Max());

                    String nodeResponseName = "node-" + connectionId + ".bytes-received";
                    Sensor nodeResponse = sensor(nodeResponseName);
                    metricName = metrics.metricName("incoming-byte-rate", metricGrpName, tags);
                    nodeResponse.add(metricName, new Rate());
                    metricName = metrics.metricName("response-rate", metricGrpName, "The average number of responses received per second.", tags);
                    nodeResponse.add(metricName, new Rate(new Count()));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = sensor(nodeTimeName);
                    metricName = metrics.metricName("request-latency-avg", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = metrics.metricName("request-latency-max", metricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes) {
            long now = time.milliseconds();
            this.bytesSent.record(bytes, now);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".bytes-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void recordBytesReceived(String connection, int bytes) {
            long now = time.milliseconds();
            this.bytesReceived.record(bytes, now);
            if (!connection.isEmpty()) {
                String nodeRequestName = "node-" + connection + ".bytes-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(bytes, now);
            }
        }

        public void close() {
            for (MetricName metricName : topLevelMetricNames)
                metrics.removeMetric(metricName);
            for (Sensor sensor : sensors)
                metrics.removeSensor(sensor.name());
        }
    }

}
