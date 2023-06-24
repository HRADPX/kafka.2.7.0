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

/**
 * 定时器类
 *  包含一个延迟队列和一个时间轮
 *
 *  延迟队列（delayQueue）中的每个元素是定时任务列表 [[TimerTaskList]]，一个定时任务列表可以存放多个定时任务条目 [[TimerTaskEntry]]，
 * 服务端创建的所有延迟任务都会被包装成定时任务条目，然后才会加入延迟队列指定的一个定时任务列表。
 *
 * 但是服务端创建的延迟操作并不是直接加入定时任务列表，而是加入到时间轮 [[TimingWheel]]。但是延迟队列会作为成员变量传递给时间轮，
 * 将延迟操作加入到定时器，实际流程是 延迟操作 --> 定时任务条目 --> 时间轮 --> 延迟队列，所以延迟操作实际上通过间接的方式加入到延迟队列中。
 *
 * todo 延迟队列、延迟操作（定时任务条目）
 */
@threadsafe
class SystemTimer(executorName: String,
                  tickMs: Long = 1,
                  wheelSize: Int = 20,
                  startMs: Long = Time.SYSTEM.hiResClockMs) extends Timer {

  // timeout timer
  private[this] val taskExecutor = Executors.newFixedThreadPool(1,
    (runnable: Runnable) => KafkaThread.nonDaemon("executor-" + executorName, runnable))

  // 延迟任务队列，按照失效时间排序
  private[this] val delayQueue = new DelayQueue[TimerTaskList]()
  // 任务计数器
  private[this] val taskCounter = new AtomicInteger(0)
  // 记时轮
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = taskCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(timerTask: TimerTask): Unit = {
    readLock.lock()
    try {
      // timerTask.delayMs + Time.SYSTEM.hiResClockMs: dead line time
      // 延迟操作 DelayedOperation 是一个定时任务类，并设置超时时间
      // 将延迟操作包装成定时任务条目，加入到时间轮中
      addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs))
    } finally {
      readLock.unlock()
    }
  }

  private def addTimerTaskEntry(timerTaskEntry: TimerTaskEntry): Unit = {
    if (!timingWheel.add(timerTaskEntry)) {
      // Already expired or cancelled
      // 执行到了过期时间立刻执行该任务
      if (!timerTaskEntry.cancelled)
        taskExecutor.submit(timerTaskEntry.timerTask)
    }
  }

  // 重新加入
  private[this] val reinsert = (timerTaskEntry: TimerTaskEntry) => addTimerTaskEntry(timerTaskEntry)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    // 延迟队列只会弹出超时的定时任务列表，队列中的每个元素都按照超时时间排序，如果第一个定时任务列表都没有过期，
    // 那么其他定时任务也一定不会超时。
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          timingWheel.advanceClock(bucket.getExpiration)
          // 执行 reinsert，这里本质上还是调用 insert 逻辑，如果发现延迟操作已经超时，会被立刻强制执行。
          // 这里需要说明下，延迟操作对应的定时任务，只有在定时器的 addTimerTaskEntry 方法中调用，所以
          // 该方法将定时任务列表弹出后，重新执行了 addTimerTaskEntry 方法，只有这样才有机会执行超时的定时任务。
          bucket.flush(reinsert)
          // 立即再轮询一次，如果没有超时，则返回空
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

