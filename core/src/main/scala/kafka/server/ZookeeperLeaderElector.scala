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
package kafka.server

import kafka.utils.ZkUtils._
import kafka.utils.CoreUtils._
import kafka.utils.{Json, SystemTime, Logging, ZKCheckedEphemeral}
import org.I0Itec.zkclient.exception.ZkNodeExistsException
import org.I0Itec.zkclient.IZkDataListener
import kafka.controller.ControllerContext
import kafka.controller.KafkaController
import org.apache.kafka.common.security.JaasUtils

/**
 * This class handles zookeeper based leader election based on an ephemeral path. The election module does not handle
 * session expiration, instead it assumes the caller will handle it by probably try to re-elect again. If the existing
 * leader is dead, this class will handle automatic re-election and if it succeeds, it invokes the leader state change
 * callback
 */
class ZookeeperLeaderElector(controllerContext: ControllerContext,
                             electionPath: String, // "/controller"
                             onBecomingLeader: () => Unit,
                             onResigningAsLeader: () => Unit, // Resigning 辞职，辞去
                             brokerId: Int)
  extends LeaderElector with Logging {
  var leaderId = -1
  // create the election path in ZK, if one does not exist
  val index = electionPath.lastIndexOf("/")
  if (index > 0)
    controllerContext.zkUtils.makeSurePersistentPathExists(electionPath.substring(0, index))

  // leader controller 状态变化的监听器
  // 当"/controller"节点删除， LeaderChangeListener的handleDataDeleted() 方法进行重新选举
  val leaderChangeListener = new LeaderChangeListener

  def startup {
    inLock(controllerContext.controllerLock) {
      // 在 "/controller" 这个znode上，注册一个监听器
      // 如果有人竞争成为 controller，那么他会感知到
      // 如果 有人已经成为controller或者不再是controller ，也会感知到
      controllerContext.zkUtils.zkClient.subscribeDataChanges(electionPath, leaderChangeListener)

      // 发起选举
      elect
    }
  }

  private def getControllerID(): Int = {
    controllerContext.zkUtils.readDataMaybeNull(electionPath)._1 match {
       case Some(controller) => KafkaController.parseControllerId(controller)
       case None => -1
    }
  }

  def elect: Boolean = {
    val timestamp = SystemTime.milliseconds.toString

    /**
      * {"version" : 1,
      * "brokerid" : xxx,
      * "timestamp" : xxxx}
      */
    val electString = Json.encode(Map("version" -> 1, "brokerid" -> brokerId, "timestamp" -> timestamp))

    // 解析 /controller 上的信息，拿到leader brokerid
   leaderId = getControllerID 
    /* 
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition, 
     * it's possible that the controller has already been elected when we get here. This check will prevent the following 
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    if(leaderId != -1) {
       debug("Broker %d has been elected as leader, so stopping the election process.".format(leaderId))
       // 如果 leader brokerid 不等于-1，说明已经有人选举为leader controller了
       return amILeader
    }

    // 尝试创建 "/controller" zonde

    try {
      // 尝试去创建一个 /controller的 ephemeral znode
      val zkCheckedEphemeral = new ZKCheckedEphemeral(electionPath,
                                                      electString,
                                                      controllerContext.zkUtils.zkConnection.getZookeeper,
                                                      JaasUtils.isZkSecurityEnabled())
      // zk会保证，同一时间只能有一个节点，成功创建 "/controller" 节点
      // "/controller" 节点内容包含的就是brokerId
      zkCheckedEphemeral.create()
      info(brokerId + " successfully elected as leader")
      leaderId = brokerId

      /**
        * 非常重要的方法，调用onBecomingLeader方法，做一些自己成为active controller该做的事情
        */
      onBecomingLeader()

    } catch {
      case e: ZkNodeExistsException =>
        // 说明被别人创建成功了"/Controller" 节点，就去解析znode的内容，拿到leader的brokerId
        // If someone else has written the path, then
        leaderId = getControllerID 

        if (leaderId != -1)
          debug("Broker %d was elected as leader instead of broker %d".format(leaderId, brokerId))
        else
          warn("A leader has been elected but just resigned, this will result in another round of election")

      case e2: Throwable =>
        error("Error while electing or becoming leader on broker %d".format(brokerId), e2)
        resign()
    }
    amILeader
  }

  def close = {
    leaderId = -1
  }

  def amILeader : Boolean = leaderId == brokerId

  def resign() = {
    leaderId = -1
    controllerContext.zkUtils.deletePath(electionPath)
  }

  /**
   * We do not have session expiration listen in the ZkElection, but assuming the caller who uses this module will
   * have its own session expiration listener and handler
   */
  class LeaderChangeListener extends IZkDataListener with Logging {
    /**
     * Called when the leader information stored in zookeeper has changed. Record the new leader in memory
     * @throws Exception On any error.
     */
    @throws(classOf[Exception])
    def handleDataChange(dataPath: String, data: Object) {
      inLock(controllerContext.controllerLock) {
        val amILeaderBeforeDataChange = amILeader
        leaderId = KafkaController.parseControllerId(data.toString)
        info("New leader is %d".format(leaderId))
        // The old leader needs to resign leadership if it is no longer the leader
        if (amILeaderBeforeDataChange && !amILeader)
          onResigningAsLeader()
      }
    }

    /**
     * Called when the leader information stored in zookeeper has been delete. Try to elect as the leader
      * 当leader节点删除之后，通知所有节点重新选举leader
     * @throws Exception
     *             On any error.
     */
    @throws(classOf[Exception])
    def handleDataDeleted(dataPath: String) {
      inLock(controllerContext.controllerLock) {
        debug("%s leader change listener fired for path %s to handle data deleted: trying to elect as a leader"
          .format(brokerId, dataPath))

        if(amILeader)
          onResigningAsLeader()

        // 重新选举的方法
        elect
      }
    }
  }
}
