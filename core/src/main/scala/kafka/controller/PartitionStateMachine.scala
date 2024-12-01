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
package kafka.controller

import kafka.api.LeaderAndIsr
import kafka.common.StateChangeFailedException
import kafka.controller.Election._
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.TopicPartitionStateZNode
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code
import scala.collection.{Map, Seq, mutable}

// 分区状态机，每个broker启动的时候都会创建分区状态机，但是只有controller所在的broker才会启动他
abstract class PartitionStateMachine(controllerContext: ControllerContext) extends Logging {
  /**
   * Invoked on successful controller election.
   * controller选举成功后执行
   */
  def startup(): Unit = {
    info("Initializing partition state")
    // 初始化所有分区的状态
    initializePartitionState()
    info("Triggering online partition state changes")
    // 触发所有分区的状态流转（这里会选取分区leader）
    triggerOnlinePartitionStateChange()
    debug(s"Started partition state machine with initial state -> ${controllerContext.partitionStates}")
  }

  /**
   * Invoked on controller shutdown.
   * controller关闭时执行
   */
  def shutdown(): Unit = {
    info("Stopped partition state machine")
  }

  /**
   * This API invokes the OnlinePartition state change on all partitions in either the NewPartition or OfflinePartition
   * state. This is called on a successful controller election and on broker changes
   * 该方法会被几种情况调用：
   * 1、controller选举成功时，会调用 kafka.controller.PartitionStateMachine#startup() 方法，startUp方法里会调用该方法
   * 2、broker启动时
   * 3、
   */
  def triggerOnlinePartitionStateChange(): Unit = {
    // 从controller的元数据缓存中过滤出所有处于NewPartition或者OfflinePartition状态的分区
    val partitions = controllerContext.partitionsInStates(Set(OfflinePartition, NewPartition))
    // 状态流转
    triggerOnlineStateChangeForPartitions(partitions)
  }

  def triggerOnlinePartitionStateChange(topic: String): Unit = {
    // 从controller的元数据缓存中过滤出topic下所有处于NewPartition或者OfflinePartition状态的分区
    val partitions = controllerContext.partitionsInStates(topic, Set(OfflinePartition, NewPartition))
    // 状态流转
    triggerOnlineStateChangeForPartitions(partitions)
  }

  private def triggerOnlineStateChangeForPartitions(partitions: collection.Set[TopicPartition]): Unit = {
    // try to move all partitions in NewPartition or OfflinePartition state to OnlinePartition state except partitions
    // that belong to topics to be deleted
    // 过滤出不属于正在删除的topic的分区
    val partitionsToTrigger = partitions.filter { partition =>
      !controllerContext.isTopicQueuedUpForDeletion(partition.topic)
    }.toSeq

    // 【重要】状态流转
    handleStateChanges(partitionsToTrigger, OnlinePartition, Some(OfflinePartitionLeaderElectionStrategy(false)))
    // TODO: If handleStateChanges catches an exception, it is not enough to bail out and log an error.
    // It is important to trigger leader election for those partitions.
  }

  /**
   * Invoked on startup of the partition's state machine to set the initial state for all existing partitions in
   * zookeeper
   * 在分区状态机启动的时候初始化所有分区的状态（存在于zk上的所有分区）
   */
  private def initializePartitionState(): Unit = {
    // 遍历元数据里保存的所有分区
    for (topicPartition <- controllerContext.allPartitions) {
      // check if leader and isr path exists for partition. If not, then it is in NEW state
      controllerContext.partitionLeadershipInfo(topicPartition) match {
        case Some(currentLeaderIsrAndEpoch) =>
          // else, check if the leader for partition is alive. If yes, it is in Online state, else it is in Offline state
          if (controllerContext.isReplicaOnline(currentLeaderIsrAndEpoch.leaderAndIsr.leader, topicPartition))
          // leader is alive
            controllerContext.putPartitionState(topicPartition, OnlinePartition)
          else
            controllerContext.putPartitionState(topicPartition, OfflinePartition)
        case None =>
          controllerContext.putPartitionState(topicPartition, NewPartition)
      }
    }
  }

  // 处理状态流转
  def handleStateChanges(
    partitions: Seq[TopicPartition],
    targetState: PartitionState
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    handleStateChanges(partitions, targetState, None)
  }

  // 处理状态流转的抽象方法，留给子类实现
  def handleStateChanges(
    partitions: Seq[TopicPartition], // 要流转状态的分区集合
    targetState: PartitionState, // 流转到目标状态
    leaderElectionStrategy: Option[PartitionLeaderElectionStrategy] // 分区选举策略
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]]

}

/**
 * This class represents the state machine for partitions. It defines the states that a partition can be in, and
 * transitions to move the partition to another legal state. The different states that a partition can be in are -
 * 1. NonExistentPartition: This state indicates that the partition was either never created or was created and then
 *                          deleted. Valid previous state, if one exists, is OfflinePartition
 * 2. NewPartition        : After creation, the partition is in the NewPartition state. In this state, the partition should have
 *                          replicas assigned to it, but no leader/isr yet. Valid previous states are NonExistentPartition
 * 3. OnlinePartition     : Once a leader is elected for a partition, it is in the OnlinePartition state.
 *                          Valid previous states are NewPartition/OfflinePartition
 * 4. OfflinePartition    : If, after successful leader election, the leader for partition dies, then the partition
 *                          moves to the OfflinePartition state. Valid previous states are NewPartition/OnlinePartition
 * 分区状态机唯一实现类
 */
class ZkPartitionStateMachine(config: KafkaConfig, // 相关配置信息
                              stateChangeLogger: StateChangeLogger,
                              controllerContext: ControllerContext, // 保存了该controller下的所有元数据
                              zkClient: KafkaZkClient, // zk客户端
                              controllerBrokerRequestBatch: ControllerBrokerRequestBatch)
  extends PartitionStateMachine(controllerContext) {

  private val controllerId = config.brokerId
  this.logIdent = s"[PartitionStateMachine controllerId=$controllerId] "

  /**
   * Try to change the state of the given partitions to the given targetState, using the given
   * partitionLeaderElectionStrategyOpt if a leader election is required.
   * @param partitions The partitions
   * @param targetState The state
   * @param partitionLeaderElectionStrategyOpt The leader election strategy if a leader election is required.
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   */
  override def handleStateChanges(
    partitions: Seq[TopicPartition], // 待流转状态的分区集合
    targetState: PartitionState, // 流转到目标状态
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy] // 分区选举策略（可选）
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = { // 返回分区以及分区流转结果
    if (partitions.nonEmpty) {
      try {
        // 创建一个请求批次
        controllerBrokerRequestBatch.newBatch()
        // 【重要】实际执行状态流转的地方（这里做了很多事情，包括选取分区leader，请求分区相关的broker更新leader和isr信息等）
        val result = doHandleStateChanges(
          partitions,
          targetState,
          partitionLeaderElectionStrategyOpt
        )
        // 利用上面创建的请求对象，向broker发送请求，告知brokers状态流转了，broker收到请求后会更新leader和isr信息
        controllerBrokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        // 返回状态流转的结果
        result
      } catch {
        case e: ControllerMovedException =>
          error(s"Controller moved to another broker when moving some partitions to $targetState state", e)
          throw e
        case e: Throwable =>
          error(s"Error while moving some partitions to $targetState state", e)
          partitions.iterator.map(_ -> Left(e)).toMap
      }
    } else {
      Map.empty
    }
  }

  // 获取分区的状态
  private def partitionState(partition: TopicPartition): PartitionState = {
    controllerContext.partitionState(partition)
  }

  /**
   * This API exercises the partition's state machine. It ensures that every state transition happens from a legal
   * previous state to the target state. Valid state transitions are:
   * NonExistentPartition -> NewPartition:
   * --load assigned replicas from ZK to controller cache
   *
   * NewPartition -> OnlinePartition
   * --assign first live replica as the leader and all live replicas as the isr; write leader and isr to ZK for this partition
   * --send LeaderAndIsr request to every live replica and UpdateMetadata request to every live broker
   *
   * OnlinePartition,OfflinePartition -> OnlinePartition
   * --select new leader and isr for this partition and a set of replicas to receive the LeaderAndIsr request, and write leader and isr to ZK
   * --for this partition, send LeaderAndIsr request to every receiving replica and UpdateMetadata request to every live broker
   *
   * NewPartition,OnlinePartition,OfflinePartition -> OfflinePartition
   * --nothing other than marking partition state as Offline
   *
   * OfflinePartition -> NonExistentPartition
   * --nothing other than marking the partition state as NonExistentPartition
   * @param partitions  The partitions for which the state transition is invoked
   * @param targetState The end state that the partition should be moved to
   * @return A map of failed and successful elections when targetState is OnlinePartitions. The keys are the
   *         topic partitions and the corresponding values are either the exception that was thrown or new
   *         leader & ISR.
   */
  private def doHandleStateChanges(
    partitions: Seq[TopicPartition], // 需要流转状态的分区集合
    targetState: PartitionState, // 流转的目标状态
    partitionLeaderElectionStrategyOpt: Option[PartitionLeaderElectionStrategy] // 分区leader选举策略（可选）
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    val traceEnabled = stateChangeLog.isTraceEnabled
    // 将分区状态添加到controller的元数据缓存中
    partitions.foreach(partition => controllerContext.putPartitionStateIfNotExists(partition, NonExistentPartition))
    // 检查状态流转是否合法，返回合法的分区集合和不合法的分区集合
    val (validPartitions, invalidPartitions) = controllerContext.checkValidPartitionStateChange(partitions, targetState)
    // 非法状态流转的分区集合处理（打印日志）
    invalidPartitions.foreach(partition => logInvalidTransition(partition, targetState))

    // 通过要流转的目标状态匹配对应的方法来执行
    targetState match {
      case NewPartition =>
        validPartitions.foreach { partition =>
          stateChangeLog.info(s"Changed partition $partition state from ${partitionState(partition)} to $targetState with " +
            s"assigned replicas ${controllerContext.partitionReplicaAssignment(partition).mkString(",")}")
          // 将分区的状态流转为目标状态
          controllerContext.putPartitionState(partition, NewPartition)
        }
        Map.empty
      case OnlinePartition =>
        // 过滤出状态等于 NewPartition 的分区（分区刚创建）
        val uninitializedPartitions = validPartitions.filter(partition => partitionState(partition) == NewPartition)
        // 过滤出状态等于 OfflinePartition 或 OnlinePartition 的分区
        val partitionsToElectLeader = validPartitions.filter(partition => partitionState(partition) == OfflinePartition || partitionState(partition) == OnlinePartition)
        // 状态等于 NewPartition 的分区，说明该分区刚创建，需要去初始化该分区的leader和isr集合
        if (uninitializedPartitions.nonEmpty) {
          // 初始化该分区的leader和isr集合(这里会写入zk中对应的分区目录下)
          val successfulInitializations = initializeLeaderAndIsrForPartitions(uninitializedPartitions)
          successfulInitializations.foreach { partition =>
            stateChangeLog.info(s"Changed partition $partition from ${partitionState(partition)} to $targetState with state " +
              s"${controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr}")
            // 将该分区状态保存到controller元数据里
            controllerContext.putPartitionState(partition, OnlinePartition)
          }
        }
        // 处理需要选举leader的分区集合
        if (partitionsToElectLeader.nonEmpty) {
          // 【重要】选举分区leader并返回选取结果
          val electionResults = electLeaderForPartitions(
            partitionsToElectLeader,
            partitionLeaderElectionStrategyOpt.getOrElse(
              throw new IllegalArgumentException("Election strategy is a required field when the target state is OnlinePartition")
            )
          )

          electionResults.foreach {
            case (partition, Right(leaderAndIsr)) =>
              stateChangeLog.info(
                s"Changed partition $partition from ${partitionState(partition)} to $targetState with state $leaderAndIsr"
              )
              // 更新controller元数据中分区的状态
              controllerContext.putPartitionState(partition, OnlinePartition)
            case (_, Left(_)) => // Ignore; no need to update partition state on election error
          }

          // 返回选举结果
          electionResults
        } else {
          Map.empty
        }
      case OfflinePartition =>
        // 遍历所有能够执行转换的分区对象
        validPartitions.foreach { partition =>
          if (traceEnabled)
            stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          // 将分区的状态流转为目标状态
          controllerContext.putPartitionState(partition, OfflinePartition)
        }
        Map.empty
      case NonExistentPartition =>
        // 遍历所有能够执行转换的分区对象
        validPartitions.foreach { partition =>
          if (traceEnabled) {
            stateChangeLog.trace(s"Changed partition $partition state from ${partitionState(partition)} to $targetState")
          }
          // 将分区的状态流转为目标状态
          controllerContext.putPartitionState(partition, NonExistentPartition)
        }
        Map.empty
    }
  }

  /**
   * Initialize leader and isr partition state in zookeeper.
   * @param partitions The partitions  that we're trying to initialize.
   * @return The partitions that have been successfully initialized.
   */
  private def initializeLeaderAndIsrForPartitions(partitions: Seq[TopicPartition]): Seq[TopicPartition] = {
    // 创建一个可变集合，用于保存成功初始化的分区
    val successfulInitializations = mutable.Buffer.empty[TopicPartition]
    // 每个分区对应的副本集合
    val replicasPerPartition = partitions.map(partition => partition -> controllerContext.partitionReplicaAssignment(partition))
    // 每个分区对应的存活的副本集合
    val liveReplicasPerPartition = replicasPerPartition.map { case (partition, replicas) =>
        val liveReplicasForPartition = replicas.filter(replica => controllerContext.isReplicaOnline(replica, partition))
        partition -> liveReplicasForPartition
    }
    val (partitionsWithoutLiveReplicas, partitionsWithLiveReplicas) = liveReplicasPerPartition.partition { case (_, liveReplicas) => liveReplicas.isEmpty }

    partitionsWithoutLiveReplicas.foreach { case (partition, replicas) =>
      val failMsg = s"Controller $controllerId epoch ${controllerContext.epoch} encountered error during state change of " +
        s"partition $partition from New to Online, assigned replicas are " +
        s"[${replicas.mkString(",")}], live brokers are [${controllerContext.liveBrokerIds}]. No assigned " +
        "replica is alive."
      logFailedStateChange(partition, NewPartition, OnlinePartition, new StateChangeFailedException(failMsg))
    }
    // 创建每个分区对应的leader和isr集合
    val leaderIsrAndControllerEpochs = partitionsWithLiveReplicas.map { case (partition, liveReplicas) =>
      val leaderAndIsr = LeaderAndIsr(liveReplicas.head, liveReplicas.toList)
      val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
      partition -> leaderIsrAndControllerEpoch
    }.toMap
    val createResponses = try {
      zkClient.createTopicPartitionStatesRaw(leaderIsrAndControllerEpochs, controllerContext.epochZkVersion)
    } catch {
      case e: ControllerMovedException =>
        error("Controller moved to another broker when trying to create the topic partition state znode", e)
        throw e
      case e: Exception =>
        partitionsWithLiveReplicas.foreach { case (partition, _) => logFailedStateChange(partition, partitionState(partition), NewPartition, e) }
        Seq.empty
    }
    createResponses.foreach { createResponse =>
      val code = createResponse.resultCode
      val partition = createResponse.ctx.get.asInstanceOf[TopicPartition]
      val leaderIsrAndControllerEpoch = leaderIsrAndControllerEpochs(partition)
      if (code == Code.OK) {
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(leaderIsrAndControllerEpoch.leaderAndIsr.isr,
          partition, leaderIsrAndControllerEpoch, controllerContext.partitionFullReplicaAssignment(partition), isNew = true)
        successfulInitializations += partition
      } else {
        logFailedStateChange(partition, NewPartition, OnlinePartition, code)
      }
    }
    successfulInitializations
  }

  /**
   * Repeatedly attempt to elect leaders for multiple partitions until there are no more remaining partitions to retry.
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
   *         either the exception that was thrown or new leader & ISR.
   */
  private def electLeaderForPartitions(
    partitions: Seq[TopicPartition], // 待选举leader的分区
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy // 分区leader选举策略
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = { // 选举结果
    var remaining = partitions
    // 创建一个可变集合，用于保存成功选举的分区
    val finishedElections = mutable.Map.empty[TopicPartition, Either[Throwable, LeaderAndIsr]]

    // 如果有未完成分区leader选举的分区，则一直重试执行
    while (remaining.nonEmpty) {
      // 选举分区leader，finished表示完成leader选举的分区集合，updatesToRetry表示未完成leader选举的分区集合
      // 【重要】这里选举完分区的leader后会更新到controller的元数据，并且更新到zk中，并且会向这些分区相关的broker发送request，broker来同步更新分区数据
      val (finished, updatesToRetry) = doElectLeaderForPartitions(remaining, partitionLeaderElectionStrategy)
      remaining = updatesToRetry

      finished.foreach {
        case (partition, Left(e)) =>
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, e)
        case (_, Right(_)) => // Ignore; success so no need to log failed state change
      }

      finishedElections ++= finished

      if (remaining.nonEmpty)
        logger.info(s"Retrying leader election with strategy $partitionLeaderElectionStrategy for partitions $remaining")
    }

    // 返回选举结果
    finishedElections.toMap
  }

  /**
   * Try to elect leaders for multiple partitions.
   * Electing a leader for a partition updates partition state in zookeeper.
   *
   * @param partitions The partitions that we're trying to elect leaders for.
   * @param partitionLeaderElectionStrategy The election strategy to use.
   * @return A tuple of two values:
   *         1. The partitions and the expected leader and isr that successfully had a leader elected. And exceptions
   *         corresponding to failed elections that should not be retried.
   *         2. The partitions that we should retry due to a zookeeper BADVERSION conflict. Version conflicts can occur if
   *         the partition leader updated partition state while the controller attempted to update partition state.
   *         真实为分区选取leader的实现，会被循环的调用
   */
  private def doElectLeaderForPartitions(
    partitions: Seq[TopicPartition], // 待选举leader的分区集合
    partitionLeaderElectionStrategy: PartitionLeaderElectionStrategy // 分区leader选举策略
  ): (Map[TopicPartition, Either[Exception, LeaderAndIsr]], Seq[TopicPartition]) = {
    // 从zk上去获取这些分区的信息
    val getDataResponses = try {
      zkClient.getTopicPartitionStatesRaw(partitions)
    } catch {
      case e: Exception =>
        return (partitions.iterator.map(_ -> Left(e)).toMap, Seq.empty)
    }
    // 创建一个可变集合，用于保存失败的选举
    val failedElections = mutable.Map.empty[TopicPartition, Either[Exception, LeaderAndIsr]]
    // 创建一个可变集合，用于保存成功的选举
    val validLeaderAndIsrs = mutable.Buffer.empty[(TopicPartition, LeaderAndIsr)]

    // 遍历从zk上获取的每个分区数据
    getDataResponses.foreach { getDataResponse =>
      // 获取分区
      val partition = getDataResponse.ctx.get.asInstanceOf[TopicPartition]
      // 获取分区当前的状态
      val currState = partitionState(partition)
      if (getDataResponse.resultCode == Code.OK) {
        // 解码从zk里获取到的分区数据
        TopicPartitionStateZNode.decode(getDataResponse.data, getDataResponse.stat) match {
          case Some(leaderIsrAndControllerEpoch) =>
            // 如果当前的controller epoch比zk上的controller epoch小，则说明当前controller epoch已经过期，则放弃此次选举
            // 也就是说当前broker已经不是集群的controller了
            if (leaderIsrAndControllerEpoch.controllerEpoch > controllerContext.epoch) {
              val failMsg = s"Aborted leader election for partition $partition since the LeaderAndIsr path was " +
                s"already written by another controller. This probably means that the current controller $controllerId went through " +
                s"a soft failure and another controller was elected with epoch ${leaderIsrAndControllerEpoch.controllerEpoch}."
              failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
            } else {
              // 将从zk上获取到的该分区的leader和isr集合保存到 validLeaderAndIsrs 集合中
              validLeaderAndIsrs += partition -> leaderIsrAndControllerEpoch.leaderAndIsr
            }

          case None =>
            val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
            failedElections.put(partition, Left(exception))
        }

      } else if (getDataResponse.resultCode == Code.NONODE) {
        // zk中没有该分区信息（说明该分区可能是刚创建，只是在controller的缓存中存在，还没有写入到zk）
        val exception = new StateChangeFailedException(s"LeaderAndIsr information doesn't exist for partition $partition in $currState state")
        failedElections.put(partition, Left(exception))
      } else {
        failedElections.put(partition, Left(getDataResponse.resultException.get))
      }
    }

    // 如果没有成功选举的分区，则返回失败的选举，成功选举的分区返回空
    if (validLeaderAndIsrs.isEmpty) {
      return (failedElections.toMap, Seq.empty)
    }

    // 根据不同的分区leader选举策略，执行不同的方法进行分区leader选举
    val (partitionsWithoutLeaders, partitionsWithLeaders) = partitionLeaderElectionStrategy match {
      // leader副本下线而引起的分区leader选举策略
      case OfflinePartitionLeaderElectionStrategy(allowUnclean) =>
        val partitionsWithUncleanLeaderElectionState = collectUncleanLeaderElectionState(
          validLeaderAndIsrs,
          allowUnclean
        )
        leaderForOffline(controllerContext, partitionsWithUncleanLeaderElectionState).partition(_.leaderAndIsr.isEmpty)
      // 执行分区副本重分配操作而引发的分区leader选举策略
      case ReassignPartitionLeaderElectionStrategy =>
        // 重新选举leader
        leaderForReassign(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      // 执行Perferred副本leader选举而引发的分区leader选举策略
      case PreferredReplicaPartitionLeaderElectionStrategy =>
        leaderForPreferredReplica(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
      case ControlledShutdownPartitionLeaderElectionStrategy =>
        // 正常关闭broker而引起的分区leader选举策略
        leaderForControlledShutdown(controllerContext, validLeaderAndIsrs).partition(_.leaderAndIsr.isEmpty)
    }

    // 处理没有分区leader的分区
    partitionsWithoutLeaders.foreach { electionResult =>
      val partition = electionResult.topicPartition
      val failMsg = s"Failed to elect leader for partition $partition under strategy $partitionLeaderElectionStrategy"
      failedElections.put(partition, Left(new StateChangeFailedException(failMsg)))
    }
    // 处理有分区leader的分区，获取这些分区的所有存活的副本
    val recipientsPerPartition = partitionsWithLeaders.map(result => result.topicPartition -> result.liveReplicas).toMap
    // 获取这些分区的leaderAndIsr
    val adjustedLeaderAndIsrs = partitionsWithLeaders.map(result => result.topicPartition -> result.leaderAndIsr.get).toMap
    // 将分区的leaderAndIsr更新到zk，返回完成更新和更新失败的集合
    val UpdateLeaderAndIsrResult(finishedUpdates, updatesToRetry) = zkClient.updateLeaderAndIsr(
      adjustedLeaderAndIsrs, controllerContext.epoch, controllerContext.epochZkVersion)
    // 【重要】处理更新成功的分区
    finishedUpdates.forKeyValue { (partition, result) =>
      result.foreach { leaderAndIsr =>
        val replicaAssignment = controllerContext.partitionFullReplicaAssignment(partition)
        // 创建 LeaderAndIsr 请求，并将这些请求加入到controller待发送的请求集合里，controller会向相关的broker发送这些请求
        val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, controllerContext.epoch)
        controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
        // 给这些分区所在的broker发送leaderAndIsr请求，让这些brokers同步分区数据
        controllerBrokerRequestBatch.addLeaderAndIsrRequestForBrokers(recipientsPerPartition(partition), partition,
          leaderIsrAndControllerEpoch, replicaAssignment, isNew = false)
      }
    }

    if (isDebugEnabled) {
      updatesToRetry.foreach { partition =>
        debug(s"Controller failed to elect leader for partition $partition. " +
          s"Attempted to write state ${adjustedLeaderAndIsrs(partition)}, but failed with bad ZK version. This will be retried.")
      }
    }

    (finishedUpdates ++ failedElections, updatesToRetry)
  }

  /* For the provided set of topic partition and partition sync state it attempts to determine if unclean
   * leader election should be performed. Unclean election should be performed if there are no live
   * replica which are in sync and unclean leader election is allowed (allowUnclean parameter is true or
   * the topic has been configured to allow unclean election).
   *
   * @param leaderIsrAndControllerEpochs set of partition to determine if unclean leader election should be
   *                                     allowed
   * @param allowUnclean whether to allow unclean election without having to read the topic configuration
   * @return a sequence of three element tuple:
   *         1. topic partition
   *         2. leader, isr and controller epoc. Some means election should be performed
   *         3. allow unclean
   */
  private def collectUncleanLeaderElectionState(
    leaderAndIsrs: Seq[(TopicPartition, LeaderAndIsr)],
    allowUnclean: Boolean
  ): Seq[(TopicPartition, Option[LeaderAndIsr], Boolean)] = {
    val (partitionsWithNoLiveInSyncReplicas, partitionsWithLiveInSyncReplicas) = leaderAndIsrs.partition {
      case (partition, leaderAndIsr) =>
        val liveInSyncReplicas = leaderAndIsr.isr.filter(controllerContext.isReplicaOnline(_, partition))
        liveInSyncReplicas.isEmpty
    }

    val electionForPartitionWithoutLiveReplicas = if (allowUnclean) {
      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        (partition, Option(leaderAndIsr), true)
      }
    } else {
      val (logConfigs, failed) = zkClient.getLogConfigs(
        partitionsWithNoLiveInSyncReplicas.iterator.map { case (partition, _) => partition.topic }.toSet,
        config.originals()
      )

      partitionsWithNoLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
        if (failed.contains(partition.topic)) {
          logFailedStateChange(partition, partitionState(partition), OnlinePartition, failed(partition.topic))
          (partition, None, false)
        } else {
          (
            partition,
            Option(leaderAndIsr),
            logConfigs(partition.topic).uncleanLeaderElectionEnable.booleanValue()
          )
        }
      }
    }

    electionForPartitionWithoutLiveReplicas ++
    partitionsWithLiveInSyncReplicas.map { case (partition, leaderAndIsr) =>
      (partition, Option(leaderAndIsr), false)
    }
  }

  // 打印一下非法流转分区状态的日志
  private def logInvalidTransition(partition: TopicPartition, targetState: PartitionState): Unit = {
    val currState = partitionState(partition)
    val e = new IllegalStateException(s"Partition $partition should be in one of " +
      s"${targetState.validPreviousStates.mkString(",")} states before moving to $targetState state. Instead it is in " +
      s"$currState state")
    logFailedStateChange(partition, currState, targetState, e)
  }

  // 打印一下非法流转分区状态的日志
  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, code: Code): Unit = {
    logFailedStateChange(partition, currState, targetState, KeeperException.create(code))
  }

  // 打印一下非法流转分区状态的日志
  private def logFailedStateChange(partition: TopicPartition, currState: PartitionState, targetState: PartitionState, t: Throwable): Unit = {
    stateChangeLogger.withControllerEpoch(controllerContext.epoch)
      .error(s"Controller $controllerId epoch ${controllerContext.epoch} failed to change state for partition $partition " +
        s"from $currState to $targetState", t)
  }
}

// 分区leader选举算法，不同的策略对应这里不同的方法
object PartitionLeaderElectionAlgorithms {

  // 分区leader选举
  def offlinePartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], uncleanLeaderElectionEnabled: Boolean, controllerContext: ControllerContext): Option[Int] = {
    // 【重要】这里就是leader的选举逻辑了，从当前分区的所有存活列表中查找第一个存活且存在于isr集合的分区副本，该副本即作为leader
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id)).orElse {
      // 如果不存在存活且存在于isr的分区副本，则根据是否允许unclean leader election来决定返回结果
      if (uncleanLeaderElectionEnabled) {
        // 如果开启了允许脏选（broker端参数：unclean.leader.election.enable），则返回从该分区的所有存活列表中查找第一个存活的分区副本，该副本即作为leader
        val leaderOpt = assignment.find(liveReplicas.contains)
        if (leaderOpt.isDefined)
          controllerContext.stats.uncleanLeaderElectionRate.mark()
        leaderOpt
      } else {
        // 如果不允许脏选，则返回null，表示无法选取leader
        None
      }
    }
  }

  def reassignPartitionLeaderElection(reassignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    // 从当前分区的所有存活列表中查找第一个存活且存在于isr集合的分区副本，该副本即作为leader
    reassignment.find(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def preferredReplicaPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int]): Option[Int] = {
    // 从当前分区的所有存活列表中查找第一个存活且存在于isr集合的分区副本，该副本即作为leader
    assignment.headOption.filter(id => liveReplicas.contains(id) && isr.contains(id))
  }

  def controlledShutdownPartitionLeaderElection(assignment: Seq[Int], isr: Seq[Int], liveReplicas: Set[Int], shuttingDownBrokers: Set[Int]): Option[Int] = {
    // 从当前分区的所有存活列表中查找第一个存活且存在于isr集合的分区副本且该副本所在的broker没有关闭，该副本即作为leader
    assignment.find(id => liveReplicas.contains(id) && isr.contains(id) && !shuttingDownBrokers.contains(id))
  }
}

// 分区leader选举策略接口
sealed trait PartitionLeaderElectionStrategy
// leader副本下线而引起的分区leader选举策略
final case class OfflinePartitionLeaderElectionStrategy(allowUnclean: Boolean) extends PartitionLeaderElectionStrategy
// 执行分区副本重分配操作而引发的分区leader选举策略
final case object ReassignPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
// 执行Perferred副本leader选举而引发的分区leader选举策略
final case object PreferredReplicaPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy
// 正常关闭broker而引起的分区leader选举策略
final case object ControlledShutdownPartitionLeaderElectionStrategy extends PartitionLeaderElectionStrategy

// 分区状态接口
sealed trait PartitionState {
  // 定义状态序号
  def state: Byte
  // 定义合法的前置状态
  def validPreviousStates: Set[PartitionState]
}

// 分区刚被创建后的状态，他表示一个全新的分区对象，处于这个状态的分区不能选举分区leader
case object NewPartition extends PartitionState {
  val state: Byte = 0
  val validPreviousStates: Set[PartitionState] = Set(NonExistentPartition)
}

// 分区正常提供服务时的状态
case object OnlinePartition extends PartitionState {
  val state: Byte = 1
  // 只有 NewPartition, OnlinePartition, OfflinePartition 这三种状态才能流转到 OnlinePartition
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

// 分区下线时的状态
case object OfflinePartition extends PartitionState {
  val state: Byte = 2
  val validPreviousStates: Set[PartitionState] = Set(NewPartition, OnlinePartition, OfflinePartition)
}

// 分区从分区状态机移除后的状态
case object NonExistentPartition extends PartitionState {
  val state: Byte = 3
  val validPreviousStates: Set[PartitionState] = Set(OfflinePartition)
}
