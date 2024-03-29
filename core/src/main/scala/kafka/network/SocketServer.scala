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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, LoginType, Mode, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}




//===============================================SocketServer start==================================================================================
/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time) extends Logging with KafkaMetricsGroup {

  /**
   * broker是可以监听多个端口号的，一般我们就设置一个9092
   */
  private val endpoints = config.listeners

  // num.network.threads
  private val numProcessorThreads = config.numNetworkThreads

  // queued.max.requests
  private val maxQueuedRequests = config.queuedMaxRequests
  private val totalProcessorThreads = numProcessorThreads * endpoints.size

  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "

  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)
  private val processors = new Array[Processor](totalProcessorThreads)

  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
  private var connectionQuotas: ConnectionQuotas = _

  private val allMetricNames = (0 until totalProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /**
   * Start the socket server
   * 启动网络socket服务
   */
  def startup() {
    this.synchronized {

      // 每个ip可以和broker建立的最大连接数
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)

      // socket.send.buffer.bytes      发送缓冲区
      val sendBufferSize = config.socketSendBufferBytes
      // socket.receive.buffer.bytes   接收缓冲区
      val recvBufferSize = config.socketReceiveBufferBytes
      // broker.id
      val brokerId = config.brokerId

      var processorBeginIndex = 0

      endpoints.values.foreach { endpoint =>
        val protocol = endpoint.protocolType
        val processorEndIndex = processorBeginIndex + numProcessorThreads

        /**
          * 初始化 processors
          */
        for (i <- processorBeginIndex until processorEndIndex)
                processors(i) = newProcessor(i, connectionQuotas, protocol)


        /**
         * 初始化 Acceptor线程，其实每一个端口号对应一个 Acceptor
         * 并且传入分配的processors
         */
        val acceptor = new Acceptor(endpoint,
          sendBufferSize,
          recvBufferSize,
          brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), // 把processors传进去
          connectionQuotas)

        acceptors.put(endpoint, acceptor)

        //启动 Acceptor 线程
        Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString, endpoint.port),
          acceptor,
          false).start()

        acceptor.awaitStartup()

        processorBeginIndex = processorEndIndex
      }

    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map( metricName =>
          metrics.metrics().get(metricName).value()).sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {
      acceptors.values.foreach(_.shutdown)
      processors.foreach(_.shutdown)
    }
    info("Shutdown completed")
  }

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
    try {
      acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, protocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      protocol,
      config.values,
      metrics
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

//===========================SocketServer end======================================================================================================



//===========================AbstractServerThread start======================================================================================================
/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(true)

  def wakeup()

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)
    wakeup()
    shutdownLatch.await()
  }

  /**
   * Wait for the thread to completely start up
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
   */
  protected def shutdownComplete() = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get

  /**
   * Close the connection identified by `connectionId` and decrement the connection count.
   */
  def close(selector: KSelector, connectionId: String) {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)
      selector.close(connectionId)
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel) {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}
//===========================AbstractServerThread end======================================================================================================

//===============================Acceptor start==================================================================================================

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  //java nio里牛逼哄哄的Selector
  private val nioSelector = NSelector.open()

  // java nio ServerSocketChannel
  // 打开 ServerSocketChannel，监听9092端口
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  /**
    * 启动 Acceptor 里的所有Processor线程
    */
  this.synchronized {
    processors.foreach { processor =>
      Utils.newThread(
        "kafka-network-thread-%d-%s-%d".format(brokerId, endPoint.protocolType.toString, processor.id),
        processor,
        false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
    *  循环接收新的连接请求
   */
  def run() {
    /**
      *把ServerSocketChannel 注册到Selector上，监听ACCEPT事件
      */
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()

    try {
      var currentProcessor = 0 // 轮询 Processor 的索引号

      while (isRunning) {
        try {
          // 500ms ，看有没有新的连接请求进来 OP_ACCEPT
          val ready = nioSelector.select(500)
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()

                if (key.isAcceptable)
                  /**
                    * 只能处理accept事件(连接请求)
                    * 把SocketChannel交给processor处理
                    */
                  accept(key, processors(currentProcessor))
                else
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                // round 轮询
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want the
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }


    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()
    }
  }

  /*
   * Create a server socket to listen for connections on.
   * 创建 ServerSocketChannel 的标准代码
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)

    /**
      * 创建 ServerSocketChannel
      */
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)

    serverChannel.socket().setReceiveBufferSize(recvBufferSize)
    try {
      // 绑定到 9092 端口
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   * 接收一个新的连接，转交给processor去处理
   * 这也是一段socket编程经典的代码
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]

    /**
      * 接收一个请求，返回和新连接的 SocketChannel
      */
    val socketChannel = serverSocketChannel.accept()

    try {
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))


      /**
        * ****************************
       * socketChannel 交给 processor
        * ****************************
       */
      processor.accept(socketChannel)

    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

//============================================Acceptor end=====================================================================================


//==========================================Processor start=======================================================================================
/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               protocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    /**
      * 连接两端的ip+port进行拼接，作为唯一ID
      * @return
      */
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }

  /**
    *  Processor线程管理新的连接请求的队列
   *  有新的请求进来，Connection会被Acceptor放到这个队列中
    */
  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()

  /**
    * 发送中的响应
    */
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()


  private val metricTags = Map("networkProcessor" -> id.toString).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)).value()
      }
    },
    metricTags.asScala
  )

  /**
    * kafka selector，用了scala里的语法糖，重命名为KSelector
    */
  private val selector = new KSelector(
        maxRequestSize,
        connectionsMaxIdleMs,
        metrics,
        time,
        "socket-server",
        metricTags,
        false,
        ChannelBuilders.create(protocol, Mode.SERVER,
                                LoginType.SERVER,
                                channelConfigs,
                                null,
                                true)
  )

  override def run() {
    startupComplete()


    /**
      * 核心逻辑：
      * 不断的处理新链接、读写请求等
      */
    while (isRunning) {
      try {
        /**
          * setup any new connections that have been queued up
          * 1. 只要这个while循环到这里,就一定会把队列里等待的连接(newConnections) 进行处理
          */
        configureNewConnections()
        /**
          * register any new responses for writing
          * 4-1. 处理新的响应结果,暂存到kafka channel，关注op_write事件
          */
        processNewResponses()
        /**
          * 2. 和生产端的poll是一样的，Kakfa Selector
          * 把请求放到 kafka selector的CompletedReceives
         *  4-2. 将响应发送出去
         *  6-1. 处理过程发生异常
          */
        poll()
        /**
          * 3. 对已经接收完毕的请求进行处理
          */
        processCompletedReceives()
        /**
          * 5. 对已经发送完毕的响应进行处理
          */
        processCompletedSends()
        /**
          * 6-2. 处理断开的连接
          */
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()
  }

  private def processNewResponses() {

    /**
      * 从属于该processor的响应队列中，取出一个response响应
      */
    var curr = requestChannel.receiveResponse(id)

    /**
      * 循环的发送响应
      */
    while (curr != null) {
      try {
        curr.responseAction match {

          /**
            * 1.不需要返回给客户端
            */
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            selector.unmute(curr.request.connectionId)

          /**
            * 2.发送响应消息
            */
          case RequestChannel.SendAction =>
            sendResponse(curr)

          /**
            * 3.关闭连接
            */
          case RequestChannel.CloseConnectionAction =>
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)
        }
      } finally {
        /**
          *   不停地去获取响应，然后发送出去，知道没有响应需要发送
          */
        curr = requestChannel.receiveResponse(id)
      }
    }


  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    val channel = selector.channel(response.responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
      response.request.updateRequestMetrics()
    }
    else {
      /**
       * 发送响应结果 response
       * 然后把发送的response暂存到 inflightResponses 中
       */
      selector.send(response.responseSend)
      inflightResponses += (response.request.connectionId -> response)
    }
  }

  private def poll() {
    try
      /**
        * kafka Selector，300ms超时
        */
      selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())
        shutdownComplete()
        throw e
    }
  }

  private def processCompletedReceives() {
    selector.completedReceives.asScala.foreach { receive =>
      // receive就是接收到的一个请求
      try {
        // 拿到对应客户端的KafkaChannel
        val channel = selector.channel(receive.source)
        val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName),
          channel.socketAddress)

        /**
          * 封装成一个 Request
          */
        val req = RequestChannel.Request(processor = id,
                                          connectionId = receive.source,
                                          session = session,
                                          buffer = receive.payload,  // 请求的 ByteBuffer，就是二进制的请求
                                          startTimeMs = time.milliseconds,
                                          securityProtocol = protocol)

        /**
          * 放到requestChannel的 请求队列 中去
          * RequestChannel是很重要的一个组件
          */
        requestChannel.sendRequest(req)

        // 取消对 OP_READ 的监听
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }

  /**
    * 对发送完毕的响应，进行善后处理
    * 其实就是从inflightResponses中移除，然后继续关注OP_READ事件
    */
  private def processCompletedSends() {

    selector.completedSends.asScala.foreach { send =>

      val resp = inflightResponses.remove(send.destination).getOrElse {
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      resp.request.updateRequestMetrics()

      // 继续监听 OP_READ 事件
      selector.unmute(send.destination)
    }
  }

  private def processDisconnected() {
    selector.disconnected.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
      // the channel has been closed by the selector but the quotas still need to be updated
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    /**
      * 加入新客户端连接的队列中
      */
    newConnections.add(socketChannel)
    /**
      * 唤醒selector的select()方法，来处理新链接
      */
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {
      // 从队列里把新的连接poll出来，poll会从队列移除
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")

        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        /**
          * 初始化connectionId，作为后面KafkaChannel的id
          */
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString

        /**
          * 注册到selector上
          * 并且监听OP_READ事件
          */
        selector.register(connectionId, channel)

      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from ${channel.getRemoteAddress}", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup() //kafka Selector

}
//===========================================================Processor  end======================================================================






class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
