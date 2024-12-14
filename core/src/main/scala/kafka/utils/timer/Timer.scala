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
package kafka.utils.timer

import java.util.concurrent.{DelayQueue, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.{KafkaThread, Time}

trait Timer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    * @param timerTask the task to add
    */
  def add(timerTask: TimerTask): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    * @return the number of tasks
    */
  def size: Int

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                 // 开始时间去系统当前时间
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // timeout timer
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))

  // 时间轮上的延时队列（底层是一个堆实现），用处存储时间轮上的每一个桶（以过期时间有序）
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  // 任务总数
  private[this] val taskCounter = new AtomicInteger(0)
  // 时间轮
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // 读写锁，用于操作时间轮时加锁保证往时间轮上添加任务以及推进时间轮时的线程安全
  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  // 添加任务到时间轮上
  def add(timerTask: TimerTask): Unit = {
    // 可以看到往时间轮上添加任务的时候用的是读锁，推进时间轮的时候用的是写锁，
    // 保证在推进时间轮的时候不会有任何的写入操作，在写入的时候不能做推进以免遗漏任务
    readLock.lock()
    try {
      // 【重要】从这里可以看到任务里的过期时间 expirationMs 的值到底是什么，外部传入的是 10ms这种形式的值，
      // 在这里会使用当前时间 + 外部传入的延时时间（毫秒）作为最终的expirationMs值
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  // 将节点插入到时间轮中，时间轮中的任务执行时机就是在这里体现的
  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    // 1、将节点插入时间轮中（如果加入时间轮失败，则说明任务已取消或已过期）
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      // 2、如果插入时间轮失败，则说明该任务已经过期或者已经被取消（这里只会有这两种情况会添加失败）
      if (!timerTaskEntry.cancelled) {
        // 3、【重要】如果该任务没有被取消，则说明该任务过期了，将过期的任务提交到线程池处理，这也就是真正调用延时任务的地方了
        taskExecutor.submit(timerTaskEntry.timerTask)
      }
    }
  }

  // 将该节点重新插入时间轮
  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   * 推进时间轮
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    // 1、从延时队列里拿出一个过期的bucket
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      // 加写锁，防止其他线程在推进时间轮的时候，bucket被其他线程删除了
      writeLock.lock()
      try {
        while (bucket != null) {
          // 2、通过调用时间轮的advanceClock方法，推进时间轮到bucket的过期时间点，这里只需要修改TimingWheel的对应轮盘上的currentTime
          timingWheel.advanceClock(bucket.getExpiration)
          // 3、【重要】将bucket下的所有定时任务重写回时间轮中（时间轮上的到期任务执行时机就是在这里）
          // 其顺序是：先将该桶里的元素从头节点开始逐个移除，移除后就调用addTimerTaskEntry方法重新添加到时间轮上或执行到期方法
          bucket.flush(reinsert)
          // 4、再次看看下延时队列里还有没有过期的bucket，如果有，就继续推进时间轮
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Int = taskCounter.get

  override def shutdown(): Unit = {
    taskExecutor.shutdown()
  }

}

