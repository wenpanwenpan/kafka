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

import kafka.utils.nonthreadsafe

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicInteger

/*
 * Hierarchical Timing Wheels
 *
 * A simple timing wheel is a circular list of buckets of timer tasks. Let u be the time unit.
 * A timing wheel with size n has n buckets and can hold timer tasks in n * u time interval.
 * Each bucket holds timer tasks that fall into the corresponding time range. At the beginning,
 * the first bucket holds tasks for [0, u), the second bucket holds tasks for [u, 2u), …,
 * the n-th bucket for [u * (n -1), u * n). Every interval of time unit u, the timer ticks and
 * moved to the next bucket then expire all timer tasks in it. So, the timer never insert a task
 * into the bucket for the current time since it is already expired. The timer immediately runs
 * the expired task. The emptied bucket is then available for the next round, so if the current
 * bucket is for the time t, it becomes the bucket for [t + u * n, t + (n + 1) * u) after a tick.
 * A timing wheel has O(1) cost for insert/delete (start-timer/stop-timer) whereas priority queue
 * based timers, such as java.util.concurrent.DelayQueue and java.util.Timer, have O(log n)
 * insert/delete cost.
 *
 * A major drawback of a simple timing wheel is that it assumes that a timer request is within
 * the time interval of n * u from the current time. If a timer request is out of this interval,
 * it is an overflow. A hierarchical timing wheel deals with such overflows. It is a hierarchically
 * organized timing wheels. The lowest level has the finest time resolution. As moving up the
 * hierarchy, time resolutions become coarser. If the resolution of a wheel at one level is u and
 * the size is n, the resolution of the next level should be n * u. At each level overflows are
 * delegated to the wheel in one level higher. When the wheel in the higher level ticks, it reinsert
 * timer tasks to the lower level. An overflow wheel can be created on-demand. When a bucket in an
 * overflow bucket expires, all tasks in it are reinserted into the timer recursively. The tasks
 * are then moved to the finer grain wheels or be executed. The insert (start-timer) cost is O(m)
 * where m is the number of wheels, which is usually very small compared to the number of requests
 * in the system, and the delete (stop-timer) cost is still O(1).
 *
 * Example
 * Let's say that u is 1 and n is 3. If the start time is c,
 * then the buckets at different levels are:
 *
 * level    buckets
 * 1        [c,c]   [c+1,c+1]  [c+2,c+2]
 * 2        [c,c+2] [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8] [c+9,c+17] [c+18,c+26]
 *
 * The bucket expiration is at the time of bucket beginning.
 * So at time = c+1, buckets [c,c], [c,c+2] and [c,c+8] are expired.
 * Level 1's clock moves to c+1, and [c+3,c+3] is created.
 * Level 2 and level3's clock stay at c since their clocks move in unit of 3 and 9, respectively.
 * So, no new buckets are created in level 2 and 3.
 *
 * Note that bucket [c,c+2] in level 2 won't receive any task since that range is already covered in level 1.
 * The same is true for the bucket [c,c+8] in level 3 since its range is covered in level 2.
 * This is a bit wasteful, but simplifies the implementation.
 *
 * 1        [c+1,c+1]  [c+2,c+2]  [c+3,c+3]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+26]
 *
 * At time = c+2, [c+1,c+1] is newly expired.
 * Level 1 moves to c+2, and [c+4,c+4] is created,
 *
 * 1        [c+2,c+2]  [c+3,c+3]  [c+4,c+4]
 * 2        [c,c+2]    [c+3,c+5]  [c+6,c+8]
 * 3        [c,c+8]    [c+9,c+17] [c+18,c+18]
 *
 * At time = c+3, [c+2,c+2] is newly expired.
 * Level 2 moves to c+3, and [c+5,c+5] and [c+9,c+11] are created.
 * Level 3 stay at c.
 *
 * 1        [c+3,c+3]  [c+4,c+4]  [c+5,c+5]
 * 2        [c+3,c+5]  [c+6,c+8]  [c+9,c+11]
 * 3        [c,c+8]    [c+9,c+17] [c+8,c+11]
 *
 * The hierarchical timing wheels works especially well when operations are completed before they time out.
 * Even when everything times out, it still has advantageous when there are many items in the timer.
 * Its insert cost (including reinsert) and delete cost are O(m) and O(1), respectively while priority
 * queue based timers takes O(log N) for both insert and delete where N is the number of items in the queue.
 *
 * This class is not thread-safe. There should not be any add calls while advanceClock is executing.
 * It is caller's responsibility to enforce it. Simultaneous add calls are thread-safe.
 * 每一层时间轮盘就是一个 TimingWheel 对象
 */
@nonthreadsafe
private[timer] class TimingWheel(tickMs: Long, // 时间轮盘上的可读
                                 wheelSize: Int,// 每层时间轮盘上有多少个刻度
                                 startMs: Long, // 开始时间
                                 taskCounter: AtomicInteger, // 时间轮上的任务总数
                                 queue: DelayQueue[TimerTaskList] // 时间轮上的延时队列，存储时间轮上的所有桶，以桶的过期时间来排序，底层是优先队列（也就是堆）
                                ) {

  private[this] val interval = tickMs * wheelSize
  // 时间轮盘上的每个桶，每个桶都是一个双向环形链表
  private[this] val buckets = Array.tabulate[TimerTaskList](wheelSize) { _ => new TimerTaskList(taskCounter) }
  // 时间轮盘上的当前时间
  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs

  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to be volatile due to the issue of Double-Checked Locking pattern with JVM
  // 下一层时间轮盘
  @volatile private[this] var overflowWheel: TimingWheel = null

  // 创建当前时间轮盘的下一层时间轮盘
  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new TimingWheel(
          // 下一层时间轮盘的刻度等于上一层时间轮盘的所有刻度之和
          tickMs = interval,
          // 时间轮盘上的刻度个数（每个轮盘保持一致）
          wheelSize = wheelSize,
          startMs = currentTime,
          // 这里可以看到整个时间轮共用的同一个任务个数计数器
          taskCounter = taskCounter,
          // 这里可以看到整个时间轮共用的同一个队列
          queue
        )
      }
    }
  }

  // 将节点加入到时间轮上
  def add(timerTaskEntry: TimerTaskEntry): Boolean = {
    // 获取任务的过期时间戳（当前时间 + 延时时间）
    val expiration = timerTaskEntry.expirationMs

    // 任务已经取消，直接返回添加失败
    if (timerTaskEntry.cancelled) {
      // Cancelled
      false
    } else if (expiration < currentTime + tickMs) {
      // Already expired 任务已经过期，直接返回添加失败
      false
      // 过期时间在当前轮盘内（也就是时间轮上的当前层）
    } else if (expiration < currentTime + interval) {
      // Put in its own bucket 计算bucket的虚拟ID
      val virtualId = expiration / tickMs
      // 从数组中获取对应的桶并将节点添加到桶中
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      bucket.add(timerTaskEntry)

      // Set the bucket expiration time 设置该桶的过期时间，这里特别，比如当前时间是T，要添加一个距当前时间25ms过期的任务
      // 那么应该是添加在第二层时间轮盘上的第0个刻度上，该刻度对应的桶的过期时间设置为T+20 ms，为什么不是T+25ms呢？因为当该桶过期后还会将
      // 该节点移动到上层的时间轮盘上刻度为4的桶内，以此来达到精确的时间延时
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        // 【重要】将该桶加入到延时队列（一个以过期时间来排序的堆）
        queue.offer(bucket)
      }
      true
    } else {
      // Out of the interval. Put it into the parent timer，如果任务的国旗时间不在当前层时间轮盘上，则继续向下层寻找
      if (overflowWheel == null) addOverflowWheel() // 如果下层时间轮盘为空，则创建一个时间轮盘
      // 将任务添加到下层时间轮盘上
      overflowWheel.add(timerTaskEntry)
    }
  }

  // Try to advance the clock 推进时间轮的时间
  def advanceClock(timeMs: Long): Unit = {
    // 这个判定很重要，这里决策出每层时间轮盘是否要推进时间
    // 【重要】要推进的时间大于等于当前时间轮的时间 + 当前时间轮每刻度的时间长度
    if (timeMs >= currentTime + tickMs) {
      // 推进当前时间轮盘的时间，这里可以看到将时间轮推进到当前轮盘的对应刻度的最小值位置
      // 比如：第一层轮盘，每个刻度值为1ms, 一共20个刻度，第二层轮盘，每个刻度值为20ms, 当前时间为T，当我们要添加一个延时时间为25ms的任务时
      // 此时currentTime = T + 20 而不是 T + 25
      // 【重要】 (timeMs % tickMs) : 表示当前时间除以刻度长度取余，也就是当前时间在刻度长度内的偏移量，比如：当前时间是T，刻度长度是20ms，那么T % 20 = 10
      currentTime = timeMs - (timeMs % tickMs)

      // Try to advance the clock of the overflow wheel if present
      // 如果有下一层时间轮盘，则继续推进下一层的时间轮盘的时间，注意这里传递的入参是：currentTime
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}
