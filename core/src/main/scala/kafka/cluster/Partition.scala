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
package kafka.cluster

import kafka.common._
import kafka.utils._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.log.LogConfig
import kafka.server._
import kafka.metrics.KafkaMetricsGroup
import kafka.controller.KafkaController
import kafka.message.ByteBufferMessageSet
import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException}
import org.apache.kafka.common.protocol.Errors

import scala.collection.JavaConverters._
import com.yammer.metrics.core.Gauge
import org.apache.kafka.common.requests.PartitionState

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
  *
  * 代表了一个topic的分区
  * 维护了很多变量
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {

  // 当前节点的brokerId
  private val localBrokerId = replicaManager.config.brokerId

  private val logManager = replicaManager.logManager
  private val zkUtils = replicaManager.zkUtils

  /**
    * 维护了所有副本对应的 Replica
    * brokerId -> Replica
    */
  private val assignedReplicaMap = new Pool[Int, Replica]

  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock()
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1

  // partition的leader brokerId
  @volatile var leaderReplicaIdOpt: Option[Int] = None

  /**
   * 当前分区的isr列表
   * 每个Partition分区都有自己的ISR列表
   */
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  private def isReplicaLocal(replicaId: Int) : Boolean = (replicaId == localBrokerId)

  val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  def isUnderReplicated(): Boolean = {
    leaderReplicaIfLocal() match {
      case Some(_) =>
        inSyncReplicas.size < assignedReplicas.size
      case None =>
        false
    }
  }

  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    val replicaOpt = getReplica(replicaId)
    replicaOpt match {
      case Some(replica) => replica

      case None =>
        if (isReplicaLocal(replicaId)) {// 本地
          val config = LogConfig.fromProps(logManager.defaultConfig.originals,
                                           AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
          val log = logManager.createLog(TopicAndPartition(topic, partitionId), config)
          val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)
          val offsetMap = checkpoint.read
          if (!offsetMap.contains(TopicAndPartition(topic, partitionId)))
            info("No checkpointed highwatermark is found for partition [%s,%d]".format(topic, partitionId))
          val offset = offsetMap.getOrElse(TopicAndPartition(topic, partitionId), 0L).min(log.logEndOffset)
          val localReplica = new Replica(replicaId, this, time, offset, Some(log))
          addReplicaIfNotExists(localReplica)
        } else {
          val remoteReplica = new Replica(replicaId, this, time)
          addReplicaIfNotExists(remoteReplica)
        }
        getReplica(replicaId).get
    }
  }

  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = {
    val replica = assignedReplicaMap.get(replicaId)
    if (replica == null)
      None
    else
      Some(replica)
  }


  /**
    * 这个方法是什么意思？？
    * @return
    */
  def leaderReplicaIfLocal(): Option[Replica] = {
    leaderReplicaIdOpt match {
      case Some(leaderReplicaId) =>
        if (leaderReplicaId == localBrokerId)// leader的brokerId 和 当前节点的brokerId相同，
          getReplica(localBrokerId)
        else
          None
      case None => None
    }
  }

  def addReplicaIfNotExists(replica: Replica) = {
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)
  }

  def assignedReplicas(): Set[Replica] = {
    assignedReplicaMap.values.toSet
  }

  def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.deleteLog(TopicAndPartition(topic, partitionId))
        removePartitionMetrics()
      } catch {
        case e: IOException =>
          fatal("Error deleting the log for partition [%s,%d]".format(topic, partitionId), e)
          Runtime.getRuntime().halt(1)
      }
    }
  }

  def getLeaderEpoch(): Int = {
    return this.leaderEpoch
  }

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  def makeLeader(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      allReplicas.foreach(replica => getOrCreateReplica(replica))
      val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      inSyncReplicas = newInSyncReplicas
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion
      val isNewLeader =
        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {
          false
        } else {
          leaderReplicaIdOpt = Some(localBrokerId)
          true
        }
      val leaderReplica = getReplica().get
      // we may need to increment high watermark since ISR could be down to 1
      if (isNewLeader) {
        // construct the high watermark metadata for the new leader replica
        leaderReplica.convertHWToLocalOffsetMetadata()
        // reset log end offset for remote replicas
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   */
  def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      allReplicas.foreach(r => getOrCreateReplica(r))
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas().map(_.brokerId) -- allReplicas).foreach(removeReplica(_))
      inSyncReplicas = Set.empty[Replica]
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion

      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      }
      else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the log end offset of a certain replica of this partition
   */
  def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {
    getReplica(replicaId) match {
      case Some(replica) =>

        // 1、更新follower replica 存储的 LEO
        replica.updateLogReadResult(logReadResult)

        // check if we need to expand ISR to include this replica
        // if it is not in the ISR yet
        /**
          * 2、每次更新了replica的LEO之后，都要判断是否要扩张 isr 列表
          * 把不在isr列表中的副本分区，重新加回到isr列表
          * 在这个方法里可能更新leader 的 HW
          */
        maybeExpandIsr(replicaId)

        debug("Recorded replica %d log end offset (LEO) position %d for partition %s."
          .format(replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  TopicAndPartition(topic, partitionId)))
      case None =>
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
          " is not recognized to be one of the assigned replicas %s for partition %s.")
          .format(localBrokerId,
                  replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  assignedReplicas().map(_.brokerId).mkString(","),
                  TopicAndPartition(topic, partitionId)))
    }
  }

  /**
   * Check and maybe expand the ISR of the partition.
   *
   * This function can be triggered when a replica's LEO has incremented
   */
  def maybeExpandIsr(replicaId: Int) {


    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          val leaderHW = leaderReplica.highWatermark

          /**
            * 判断是否重新加入isr的条件
            * 1、 首先follower partition不在 ISR 列表中
            * 2、 follower brokerid 在 已分配的列表中
            * 3、 （关键条件）follower的LEO >= leader 的 HW
            */
          if(!inSyncReplicas.contains(replica) &&
             assignedReplicas.map(_.brokerId).contains(replicaId) &&
                  replica.logEndOffset.offsetDiff(leaderHW) >= 0) {

            // 当前副本加入ISR 列表中
            val newInSyncReplicas = inSyncReplicas + replica
            info("Expanding ISR for partition [%s,%d] from %s to %s"
                         .format(topic, partitionId, inSyncReplicas.map(_.brokerId).mkString(","),
                                 newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in ZK and cache
            updateIsr(newInSyncReplicas)
            replicaManager.isrExpandRate.mark()
          }

          /**
            * 判断是否可以更新 HW，可以更新就
            */
          // check if the HW of the partition can now be incremented
          // since the replica maybe now be in the ISR and its LEO has just incremented
          maybeIncrementLeaderHW(leaderReplica)

        case None => false // nothing to do if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  /*
   * Note that this method will only be called if requiredAcks = -1
   * and we are waiting for all replicas in ISR to be fully caught up to
   * the (local) leader's offset corresponding to this produce request
   * before we acknowledge the produce request.
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Short) = {
    leaderReplicaIfLocal() match {
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        val curInSyncReplicas = inSyncReplicas
        val numAcks = curInSyncReplicas.count(r => {
          if (!r.isLocal)
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace("Replica %d of %s-%d received offset %d".format(r.brokerId, topic, partitionId, requiredOffset))
              true
            }
            else
              false
          else
            true /* also count the local (leader) replica */
        })

        trace("%d acks satisfied for %s-%d with acks = -1".format(numAcks, topic, partitionId))

        val minIsr = leaderReplica.log.get.config.minInSyncReplicas

        if (leaderReplica.highWatermark.messageOffset >= requiredOffset ) {
          /*
          * The topic may be configured not to accept messages if there are not enough replicas in ISR
          * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
          */
          if (minIsr <= curInSyncReplicas.size) {
            (true, Errors.NONE.code)
          } else {
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND.code)
          }
        } else
          (false, Errors.NONE.code)
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION.code)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
    *
    * 有两种情况可以触发这个方法
    * 1、 ISR 的变更
    * 2、 replica 的 LEO 发生了变化
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica): Boolean = {

    // 拿到所有isr中副本的LEO ，找到最小的赋值给 newHighWatermark
    val allLogEndOffsets = inSyncReplicas.map(_.logEndOffset)
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)

    // 老的HW
    val oldHighWatermark = leaderReplica.highWatermark


    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset
          || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      /**
        *  如果当前isr中最小的LEO大于旧的的HW 或者 在老的segment里面，就可以更新hw了
        */
      leaderReplica.highWatermark = newHighWatermark
      debug("High watermark for partition [%s,%d] updated to %s".format(topic, partitionId, newHighWatermark))
      true
    } else {
      debug("Skipping update high watermark since Old hw %s is larger than new hw %s for partition [%s,%d]. All leo's are %s"
        .format(oldHighWatermark, newHighWatermark, topic, partitionId, allLogEndOffsets.mkString(",")))
      false
    }
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(this.topic, this.partitionId)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
  }

  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {

    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal() match {
        case Some(leaderReplica) =>
          // 1、找出落后的 replica
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)

          if(outOfSyncReplicas.size > 0) {
            // 2、从isr列表中移除落后的副本
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas

            assert(newInSyncReplicas.size > 0)
            info("Shrinking ISR for partition [%s,%d] from %s to %s".format(topic, partitionId,
              inSyncReplicas.map(_.brokerId).mkString(","), newInSyncReplicas.map(_.brokerId).mkString(",")))

            // update ISR in zk and in cache
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1

            replicaManager.isrShrinkRate.mark()

            //  3、可能会更新 leader 的 HW
            // 有可能因为这些落后太多的副本，导致HW一直没办法更新，现在移除了落后的副本，有可能是会更新HW的
            maybeIncrementLeaderHW(leaderReplica)

          } else {
            false
          }

        case None => false
        // do nothing if no longer leader 不是leader，任何事情都不做
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
      *
      * 两种情况
      * 1 可能挂了或者fullGC
      * 2 就是慢，网络、读写性能不佳
     *
     **/

    // 从源码里看，只做了一种判断，follower超过10s没有来拉取数据的情况
    val leaderLogEndOffset = leaderReplica.logEndOffset
    val candidateReplicas = inSyncReplicas - leaderReplica // 扣减掉leader，就是所有的follower

    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if(laggingReplicas.size > 0)
      debug("Lagging replicas for partition %s are %s".format(TopicAndPartition(topic, partitionId), laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  def appendMessagesToLeader(messages: ByteBufferMessageSet, requiredAcks: Int = 0) = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      val leaderReplicaOpt = leaderReplicaIfLocal()

      leaderReplicaOpt match {
        case Some(leaderReplica) =>
          /**
            * 分区的leader就是当前节点
            */
          val log = leaderReplica.log.get
          val minIsr = log.config.minInSyncReplicas // min.insync.replicas
          val inSyncSize = inSyncReplicas.size      // 当前分区ISR包含的副本数量

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          if (inSyncSize < minIsr && requiredAcks == -1) {
            /**
             * 很重要的一个判断逻辑
             * 当前ISR列表中的数量 < 设置的“min.insync.replicas”
             * 并且 ack 设置的是-1
             */
            throw new NotEnoughReplicasException("Number of insync replicas for partition [%s,%d] is [%d], below required minimum [%d]"
              .format(topic, partitionId, inSyncSize, minIsr))
          }

          /**
           * *************************************************************
            * 获取这个Partition对应的Log, 基于这个Log对象把数据写入进去
           * *************************************************************
            */
          val info = log.append(messages, assignOffsets = true)

          /**
           * probably unblock some follower fetch requests since log end offset has been updated
           * 可能有follower在fetch数据的时候，没有拉取到数据，此时延迟调度，等待一段时间
           * 此时就能唤醒在时间轮等待的DelayedFetch任务
           */
          replicaManager.tryCompleteDelayedFetch(new TopicPartitionOperationKey(this.topic, this.partitionId))


          // we may need to increment high watermark since ISR could be down to 1
          (info, maybeIncrementLeaderHW(leaderReplica))

        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition [%s,%d] on broker %d"
            .format(topic, partitionId, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(r => r.brokerId).toList, zkVersion)
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion)

    if(updateSucceeded) {
      replicaManager.recordIsrChange(new TopicAndPartition(topic, partitionId))
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  private def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Partition]))
      return false
    val other = that.asInstanceOf[Partition]
    if(topic.equals(other.topic) && partitionId == other.partitionId)
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*partitionId
  }

  override def toString(): String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString()
  }
}
