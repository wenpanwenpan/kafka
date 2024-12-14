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

import java.util.concurrent.{Delayed, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import kafka.utils.threadsafe
import org.apache.kafka.common.utils.Time

import scala.math._

@threadsafe
private[timer] class TimerTaskList(taskCounter: AtomicInteger) extends Delayed {

  // TimerTaskList forms a doubly linked cyclic list using a dummy root entry
  // root.next points to the head
  // root.prev points to the tail
  // 根节点，用于实现双向环形列表
  private[this] val root = new TimerTaskEntry(null, -1)
  // 根节点的双向指针
  root.next = root
  root.prev = root

  private[this] val expiration = new AtomicLong(-1L)

  // Set the bucket's expiration time
  // Returns true if the expiration time is changed
  def setExpiration(expirationMs: Long): Boolean = {
    expiration.getAndSet(expirationMs) != expirationMs
  }

  // Get the bucket's expiration time 时间轮上的每个桶的过期时间
  def getExpiration: Long = expiration.get

  // Apply the supplied function to each of tasks in this list
  def foreach(f: (TimerTask)=>Unit): Unit = {
    synchronized {
      var entry = root.next
      while (entry ne root) {
        val nextEntry = entry.next

        if (!entry.cancelled) f(entry.timerTask)

        entry = nextEntry
      }
    }
  }

  // Add a timer task entry to this list
  def add(timerTaskEntry: TimerTaskEntry): Unit = {
    var done = false
    while (!done) {
      // Remove the timer task entry if it is already in any other list
      // We do this outside of the sync block below to avoid deadlocking.
      // We may retry until timerTaskEntry.list becomes null.
      timerTaskEntry.remove()

      // 这里采用的是尾插法
      synchronized {
        timerTaskEntry.synchronized {
          // 当前节点所属的list为空，说明当前节点还没有添加到队列中
          if (timerTaskEntry.list == null) {
            // put the timer task entry to the end of the list. (root.prev points to the tail entry)
            val tail = root.prev
            // 当前节点的next指针指向root节点
            timerTaskEntry.next = root
            // 当前节点的pre指针指向tail节点（也就是之前的尾部节点）
            timerTaskEntry.prev = tail
            // 设置当前节点所属的list为当前list
            timerTaskEntry.list = this
            // 上一个最尾部的节点的next指针指向当前节点
            tail.next = timerTaskEntry
            // root节点的pre节点指向当前节点
            root.prev = timerTaskEntry
            // list中的元素个数加一
            taskCounter.incrementAndGet()
            done = true
          }
        }
      }
    }
  }

  // Remove the specified timer task entry from this list
  def remove(timerTaskEntry: TimerTaskEntry): Unit = {
    synchronized {
      timerTaskEntry.synchronized {
        // 当前entry所属的list是当前list时才进行移除操作
        if (timerTaskEntry.list eq this) {
          // 当前节点的下一个节点的pre指针指向当前节点的上一个节点
          timerTaskEntry.next.prev = timerTaskEntry.prev
          // 当前节点的上一个节点的next指针指向当前节点的下一个节点
          timerTaskEntry.prev.next = timerTaskEntry.next
          // 清空当前节点的指针信息
          timerTaskEntry.next = null
          timerTaskEntry.prev = null
          timerTaskEntry.list = null
          // 链表中的元素个数减一
          taskCounter.decrementAndGet()
        }
      }
    }
  }

  // Remove all task entries and apply the supplied function to each of them
  // 从头节点开始从链表上移除所有元素，每个节点移除后执行传入的f方法
  def flush(f: (TimerTaskEntry)=>Unit): Unit = {
    synchronized {
      // 先获取头节点
      var head = root.next
      // 从root节点开始遍历环形链表
      while (head ne root) {
        // 从list链表中移除该节点
        remove(head)
        // 对移除的节点执行传入的f方法
        f(head)
        // 继续取下一个节点
        head = root.next
      }
      expiration.set(-1L)
    }
  }

  def getDelay(unit: TimeUnit): Long = {
    unit.convert(max(getExpiration - Time.SYSTEM.hiResClockMs, 0), TimeUnit.MILLISECONDS)
  }

  def compareTo(d: Delayed): Int = {
    val other = d.asInstanceOf[TimerTaskList]
    java.lang.Long.compare(getExpiration, other.getExpiration)
  }

}

// 时间轮的链表桶里的节点元素
private[timer] class TimerTaskEntry(val timerTask: TimerTask,// 节点关联的任务
                                    val expirationMs: Long/**任务的过期时间戳*/) extends Ordered[TimerTaskEntry] {

  // 该节点所属的链表
  @volatile
  var list: TimerTaskList = null
  // 前后指针
  var next: TimerTaskEntry = null
  var prev: TimerTaskEntry = null

  // if this timerTask is already held by an existing timer task entry,
  // setTimerTaskEntry will remove it.
  if (timerTask != null) timerTask.setTimerTaskEntry(this)

  // 任务是否已取消
  def cancelled: Boolean = {
    timerTask.getTimerTaskEntry != this
  }

  // 从当前链表中移除当前节点
  def remove(): Unit = {
    var currentList = list
    // If remove is called when another thread is moving the entry from a task entry list to another,
    // this may fail to remove the entry due to the change of value of list. Thus, we retry until the list becomes null.
    // In a rare case, this thread sees null and exits the loop, but the other thread insert the entry to another list later.
    while (currentList != null) {
      currentList.remove(this)
      currentList = list
    }
  }

  override def compare(that: TimerTaskEntry): Int = {
    java.lang.Long.compare(expirationMs, that.expirationMs)
  }
}

