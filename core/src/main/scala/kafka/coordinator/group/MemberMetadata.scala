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

package kafka.coordinator.group

import java.util

import kafka.utils.nonthreadsafe

// 消费者组成员元数据概要，内部提取了最核心的元数据信息
case class MemberSummary(memberId: String,// 消费者组成员ID，由kafka自动生成，目前是硬编码，无法自己指定
                         groupInstanceId: Option[String], // 用来表示消费者组静态成员ID，【静态成员】机制的引入能够规避不必要的消费者组Rebalance操作
                         clientId: String,// 用来标识消费者组成员配置的client.id参数，由于memberId是硬编码的无法被设置，所以你可以用这个字段来区分消费者组下不同成员
                         clientHost: String,// 用来标识运行消费者足迹名，他记录了该客户端是从哪台机器发出的消费请求
                         // 用来表示消费者组成员分区分配策略的字节数组，它是由消费者端参数partition.assignment.strategy值来设定的
                        // 默认分区分配策略是：RangeAssignor
                         metadata: Array[Byte],
                        // 用来保存分配给该成员的订阅分区
                         assignment: Array[Byte])

// 伴生类
private object MemberMetadata {
  // 提取分区策略集合
  def plainProtocolSet(supportedProtocols: List[(String, Array[Byte])]) = supportedProtocols.map(_._1).toSet
}

/**
 * Member metadata contains the following metadata:
 *
 * Heartbeat metadata:
 * 1. negotiated heartbeat session timeout
 * 2. timestamp of the latest heartbeat
 *
 * Protocol metadata:
 * 1. the list of supported protocols (ordered by preference)
 * 2. the metadata associated with each protocol
 *
 * In addition, it also contains the following state information:
 *
 * 1. Awaiting rebalance callback: when the group is in the prepare-rebalance state,
 *                                 its rebalance callback will be kept in the metadata if the
 *                                 member has sent the join group request
 * 2. Awaiting sync callback: when the group is in the awaiting-sync state, its sync callback
 *                            is kept in metadata until the leader provides the group assignment
 *                            and the group transitions to stable
 * 消费者成员元数据
 */
// 消费者组成员元数据
@nonthreadsafe
private[group] class MemberMetadata(var memberId: String,// 消费者组成员的ID
                                    val groupId: String,// 消费者组ID
                                    val groupInstanceId: Option[String],//用来标识消费者组静态成员的ID
                                    val clientId: String,// 用来标识消费者组成员配置的client.id参数
                                    val clientHost: String,// 消费者组成员所在的主机名称
                                    val rebalanceTimeoutMs: Int, // Rebalance操作超时时间
                                    val sessionTimeoutMs: Int, // 会话超时时间
                                    val protocolType: String, // 协议类型，对消费者来说就是consumer
                                   // 该成员支持的分区分配方案
                                    var supportedProtocols: List[(String, Array[Byte])]) {

  // 分区分配方案集合
  var assignment: Array[Byte] = Array.empty[Byte]
  // 该成员是否在等待加入消费者组
  var awaitingJoinCallback: JoinGroupResult => Unit = null
  // 该成员是否在等待组协调器发送分配方案
  var awaitingSyncCallback: SyncGroupResult => Unit = null
  // 该成员是否正在发起离组操作
  var isLeaving: Boolean = false
  // 该成员是否是消费者组下的新成员
  var isNew: Boolean = false
  // 该成员是否是静态成员
  val isStaticMember: Boolean = groupInstanceId.isDefined

  // This variable is used to track heartbeat completion through the delayed
  // heartbeat purgatory. When scheduling a new heartbeat expiration, we set
  // this value to `false`. Upon receiving the heartbeat (or any other event
  // indicating the liveness of the client), we set it to `true` so that the
  // delayed heartbeat can be completed.
  // 当心跳过期时设置为false，接收到心跳时设置为true
  var heartbeatSatisfied: Boolean = false

  // 该成员是否在等待加入消费者组
  def isAwaitingJoin = awaitingJoinCallback != null
  // 该成员是否在等待组协调器发送分配方案
  def isAwaitingSync = awaitingSyncCallback != null

  /**
   * Get metadata corresponding to the provided protocol.
   * 根据提供的协议来获取相应的元数据
   */
  def metadata(protocol: String): Array[Byte] = {
    // 从该成员配置的分区分配方案列表中寻找给定的策略详情
    supportedProtocols.find(_._1 == protocol) match {
      case Some((_, metadata)) => metadata
      case None =>
        throw new IllegalArgumentException("Member does not support protocol")
    }
  }

  // 用来检查成员是否满足预期的心跳
  def hasSatisfiedHeartbeat: Boolean = {
    // 如果成员状态是new，则首先检查 heartbeatSatisfied 是否为true
    if (isNew) {
      // New members can be expired while awaiting join, so we have to check this first
      heartbeatSatisfied
      // 如果成员正在等待入组或等待分配分区方案，则返回true
    } else if (isAwaitingJoin || isAwaitingSync) {
      // Members that are awaiting a rebalance automatically satisfy expected heartbeats
      true
    } else {
      // Otherwise we require the next heartbeat
      // 否则需要下一个心跳才能满足预期
      heartbeatSatisfied
    }
  }

  /**
   * Check if the provided protocol metadata matches the currently stored metadata.
   * 用来检查提供的协议元数据是否与当前存储的元数据匹配
   */
  def matches(protocols: List[(String, Array[Byte])]): Boolean = {
    // 首先检查协议数量是否相等，不等，则直接返回false
    if (protocols.size != this.supportedProtocols.size)
      return false

    // 逐个比较
    for (i <- protocols.indices) {
      val p1 = protocols(i)
      val p2 = supportedProtocols(i)
      if (p1._1 != p2._1 || !util.Arrays.equals(p1._2, p2._2))
        return false
    }
    true
  }

  // 根据提供的协议，创建成员的摘要信息并返回。摘要信息包括成员ID、组实例ID、客户端ID、客户端主机、协议对应的元数据和分配
  def summary(protocol: String): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, metadata(protocol), assignment)
  }

  // 创建没有元数据的成员摘要信息
  def summaryNoMetadata(): MemberSummary = {
    MemberSummary(memberId, groupInstanceId, clientId, clientHost, Array.empty[Byte], Array.empty[Byte])
  }

  /**
   * Vote for one of the potential group protocols. This takes into account the protocol preference as
   * indicated by the order of supported protocols and returns the first one also contained in the set
   * 用来为潜在的组协议进行投票。根据支持的协议顺序和候选协议集合，确定协议首选项，并返回第一个同时在集合中的协议
   */
  def vote(candidates: Set[String]): String = {
    // 在支持的协议中查找第一个同时出现在候选协议集合中的协议
    supportedProtocols.find({ case (protocol, _) => candidates.contains(protocol)}) match {
      // 找到了就返回
      case Some((protocol, _)) => protocol
      case None =>
        throw new IllegalArgumentException("Member does not support any of the candidate protocols")
    }
  }

  override def toString: String = {
    "MemberMetadata(" +
      s"memberId=$memberId, " +
      s"groupInstanceId=$groupInstanceId, " +
      s"clientId=$clientId, " +
      s"clientHost=$clientHost, " +
      s"sessionTimeoutMs=$sessionTimeoutMs, " +
      s"rebalanceTimeoutMs=$rebalanceTimeoutMs, " +
      s"supportedProtocols=${supportedProtocols.map(_._1)}, " +
      ")"
  }
}
