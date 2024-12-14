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

// 延时任务
trait TimerTask extends Runnable {

  // 该任务延时的时间（毫秒）
  val delayMs: Long // timestamp in millisecond

  // 该任务关联的TimerTaskEntry节点
  private[this] var timerTaskEntry: TimerTaskEntry = null

  def cancel(): Unit = {
    synchronized {
      // 如果该任务关联的任务节点不为空，则取消前先将该entry从链表上移除
      if (timerTaskEntry != null) timerTaskEntry.remove()
      // 清空关联的entry
      timerTaskEntry = null
    }
  }

  // 设置该任务关联的TimerTaskEntry节点
  private[timer] def setTimerTaskEntry(entry: TimerTaskEntry): Unit = {
    synchronized {
      // if this timerTask is already held by an existing timer task entry,
      // we will remove such an entry first.
      // 如果该任务之前关联过任务节点并且不是当前节点，则先将原来关联的节点从链表移除
      if (timerTaskEntry != null && timerTaskEntry != entry)
        timerTaskEntry.remove()

      // 重新关联节点
      timerTaskEntry = entry
    }
  }

  private[timer] def getTimerTaskEntry: TimerTaskEntry = timerTaskEntry

}
