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

package kafka.controller

import java.util.ArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{CountDownLatch, LinkedBlockingQueue, TimeUnit}
import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.utils.Time

import scala.collection._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
  val EventQueueTimeMetricName = "EventQueueTimeMs"
  val EventQueueSizeMetricName = "EventQueueSize"
}

// controller事件处理器，他的唯一实现类就是 KafkaController
trait ControllerEventProcessor {
  // 处理事件
  def process(event: ControllerEvent): Unit
  // 处理抢占事件
  def preempt(event: ControllerEvent): Unit
}

// controller队列里的事件Class
class QueuedEvent(val event: ControllerEvent,
                  val enqueueTimeMs: Long) {
  // 标识该事件是否已经被处理
  val processingStarted = new CountDownLatch(1)
  // 标识该事件是否已经被处理
  val spent = new AtomicBoolean(false)

  // 处理事件，可以看到 QueuedEvent 里包含了事件本身以及对该事件的处理方法
  def process(processor: ControllerEventProcessor): Unit = {
    // 如果事件被处理过了就不处理了
    if (spent.getAndSet(true))
      return
    // 唤醒等待线程
    processingStarted.countDown()
    // 通过ControllerEventProcessor来处理事件（实现类是KafkaController）
    processor.process(event)
  }

  // 被抢占后的回调方法
  def preempt(processor: ControllerEventProcessor): Unit = {
    if (spent.getAndSet(true))
      return
    processor.preempt(event)
  }

  // 调用该方法的线程会阻塞等待，直到processingStarted等于0
  def awaitProcessing(): Unit = {
    processingStarted.await()
  }

  override def toString: String = {
    s"QueuedEvent(event=$event, enqueueTimeMs=$enqueueTimeMs)"
  }
}

// controller事件管理器，负责管理事件的添加、移除等
class ControllerEventManager(controllerId: Int, // controller的ID
                             processor: ControllerEventProcessor,// controller事件处理器
                             time: Time,
                             rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventQueueTimeTimeoutMs: Long = 300000) extends KafkaMetricsGroup {
  import ControllerEventManager._

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  private val queue = new LinkedBlockingQueue[QueuedEvent]
  // Visible for test 注意该线程的类型是 ControllerEventThread ，线程核心逻辑在他的内部
  private[controller] var thread = new ControllerEventThread(ControllerEventThreadName)

  private val eventQueueTimeHist = newHistogram(EventQueueTimeMetricName)

  newGauge(EventQueueSizeMetricName, () => queue.size)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    try {
      thread.initiateShutdown()
      // 抢占式的执行关闭事件
      clearAndPut(ShutdownEventThread)
      thread.awaitShutdown()
    } finally {
      removeMetric(EventQueueTimeMetricName)
      removeMetric(EventQueueSizeMetricName)
    }
  }

  // 添加一个事件（那么这些事件是由谁写入的呢？由controller监听到订阅的zk的对应目录的变化产生不同的事件，然后写入这个队列的）
  def put(event: ControllerEvent): QueuedEvent = inLock(putLock) {
    // 将ControllerEvent包装成QueuedEvent
    val queuedEvent = new QueuedEvent(event, time.milliseconds())
    // 入队
    queue.put(queuedEvent)
    queuedEvent
  }

  def clearAndPut(event: ControllerEvent): QueuedEvent = inLock(putLock){
    val preemptedEvents = new ArrayList[QueuedEvent]()
    // 将队列里的所有事件出队并添加到列表中
    queue.drainTo(preemptedEvents)
    // 回调这些事件的被抢占方法
    preemptedEvents.forEach(_.preempt(processor))
    // 添加抢占事件到队列
    put(event)
  }

  def isEmpty: Boolean = queue.isEmpty

  // 处理controller事件的线程，继承了JDK中的thread
  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    // 线程核心方法，可以看到就是不断的从队列里取事件，然后调用事件的process方法处理事件
    override def doWork(): Unit = {
      // 从队列里出队一个事件
      val dequeued = pollFromEventQueue()
      // 根据事件类型匹配处理方法
      dequeued.event match {
        case ShutdownEventThread => // The shutting down of the thread has been initiated at this point. Ignore this event.
        case controllerEvent =>
          _state = controllerEvent.state

          // 更新事件在队列里待的时间
          eventQueueTimeHist.update(time.milliseconds() - dequeued.enqueueTimeMs)

          try {
            // 定义事件的处理方法，可以看到事件里其实就包含了处理该事件的方法
            def process(): Unit = dequeued.process(processor)

            // 统计事件处理时间并调用processor方法处理事件
            rateAndTimeMetrics.get(state) match {
              // 执行事件处理
              case Some(timer) => timer.time { process() }
              case None => process()
            }
          } catch {
            case e: Throwable => error(s"Uncaught error processing event $controllerEvent", e)
          }

          _state = ControllerState.Idle
      }
    }
  }

  // 从队列里出队一个事件
  private def pollFromEventQueue(): QueuedEvent = {
    val count = eventQueueTimeHist.count()
    // 如果队列里有事件
    if (count != 0) {
      // 出队一个事件
      val event  = queue.poll(eventQueueTimeTimeoutMs, TimeUnit.MILLISECONDS)
      if (event == null) {
        eventQueueTimeHist.clear()
        // 阻塞式获取
        queue.take()
      } else {
        // 返回出队的事件
        event
      }
    } else {
      // 队列里没有事件，则阻塞式获取
      queue.take()
    }
  }

}
