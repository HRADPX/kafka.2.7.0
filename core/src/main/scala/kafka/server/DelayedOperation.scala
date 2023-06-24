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

import java.util.concurrent._
import java.util.concurrent.atomic._
import java.util.concurrent.locks.{Lock, ReentrantLock}

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.CoreUtils.inLock
import kafka.utils._
import kafka.utils.timer._

import scala.collection._
import scala.collection.mutable.ListBuffer

/**
 * An operation whose processing needs to be delayed for at most the given delayMs. For example
 * a delayed produce operation could be waiting for specified number of acks; or
 * a delayed fetch operation could be waiting for a given number of bytes to accumulate.
 *
 * The logic upon completing a delayed operation is defined in onComplete() and will be called exactly once.
 * Once an operation is completed, isCompleted() will return true. onComplete() can be triggered by either
 * forceComplete(), which forces calling onComplete() after delayMs if the operation is not yet completed,
 * or tryComplete(), which first checks if the operation can be completed or not now, and if yes calls
 * forceComplete().
 *
 * A subclass of DelayedOperation needs to provide an implementation of both onComplete() and tryComplete().
 *
 * Noted that if you add a future delayed operation that calls ReplicaManager.appendRecords() in onComplete()
 * like DelayedJoin, you must be aware that this operation's onExpiration() needs to call actionQueue.tryCompleteAction().
 */
abstract class DelayedOperation(override val delayMs: Long,
                                lockOpt: Option[Lock] = None)
  extends TimerTask with Logging {

  // 控制 onComplete() 方法只会被调用一次
  private val completed = new AtomicBoolean(false)
  // Visible for testing
  private[server] val lock: Lock = lockOpt.getOrElse(new ReentrantLock)

  /*
   * Force completing the delayed operation, if not already completed.
   * This function can be triggered when
   *
   * 1. The operation has been verified to be completable inside tryComplete()
   * 2. The operation has expired and hence needs to be completed right now
   *
   * Return true iff the operation is completed by the caller: note that
   * concurrent threads can try to complete the same operation, but only
   * the first thread will succeed in completing the operation and return
   * true, others will still return false
   */
  def forceComplete(): Boolean = {
    if (completed.compareAndSet(false, true)) {
      // cancel the timeout timer
      cancel()
      onComplete()
      true
    } else {
      false
    }
  }

  /**
   * Check if the delayed operation is already completed
   */
  def isCompleted: Boolean = completed.get()

  /**
   * Call-back to execute when a delayed operation gets expired and hence forced to complete.
   */
  def onExpiration(): Unit

  /**
   * Process for completing an operation; This function needs to be defined
   * in subclasses and will be called exactly once in forceComplete()
   */
  def onComplete(): Unit

  /**
   * Try to complete the delayed operation by first checking if the operation
   * can be completed by now. If yes execute the completion logic by calling
   * forceComplete() and return true iff forceComplete returns true; otherwise return false
   *
   * This function needs to be defined in subclasses
   */
  def tryComplete(): Boolean

  /**
   * Thread-safe variant of tryComplete() and call extra function if first tryComplete returns false
   * @param f else function to be executed after first tryComplete returns false
   * @return result of tryComplete
   * 判断是否完成，如果没有执行 f 后最后再 check 一次
   */
  private[server] def safeTryCompleteOrElse(f: => Unit): Boolean = inLock(lock) {
    if (tryComplete()) true
    else {
      f
      // last completion check
      tryComplete()
    }
  }

  /**
   * Thread-safe variant of tryComplete()
   */
  private[server] def safeTryComplete(): Boolean = inLock(lock)(tryComplete())

  /*
   * run() method defines a task that is executed on timeout
   * 延迟操作超时被强制完成后，如果执行成功后，会调用 onExpiration 方法
   */
  override def run(): Unit = {
    if (forceComplete())
      onExpiration()
  }
}

object DelayedOperationPurgatory {

  private val Shards = 512 // Shard the watcher list to reduce lock contention

  def apply[T <: DelayedOperation](purgatoryName: String,
                                   brokerId: Int = 0,
                                   purgeInterval: Int = 1000,
                                   reaperEnabled: Boolean = true,
                                   timerEnabled: Boolean = true): DelayedOperationPurgatory[T] = {
    val timer = new SystemTimer(purgatoryName)
    new DelayedOperationPurgatory[T](purgatoryName, timer, brokerId, purgeInterval, reaperEnabled, timerEnabled)
  }

}

/**
 * A helper purgatory class for bookkeeping delayed operations with a timeout, and expiring timed out operations.
 */
final class DelayedOperationPurgatory[T <: DelayedOperation](purgatoryName: String,
                                                             timeoutTimer: Timer,
                                                             brokerId: Int = 0,
                                                             purgeInterval: Int = 1000,
                                                             reaperEnabled: Boolean = true,
                                                             timerEnabled: Boolean = true)
        extends Logging with KafkaMetricsGroup {
  /* a list of operation watching keys */
  private class WatcherList {
    val watchersByKey = new Pool[Any, Watchers](Some((key: Any) => new Watchers(key)))

    val watchersLock = new ReentrantLock()

    /*
     * Return all the current watcher lists,
     * note that the returned watchers may be removed from the list by other threads
     */
    def allWatchers = {
      watchersByKey.values
    }
  }

  private val watcherLists = Array.fill[WatcherList](DelayedOperationPurgatory.Shards)(new WatcherList)
  private def watcherList(key: Any): WatcherList = {
    watcherLists(Math.abs(key.hashCode() % watcherLists.length))
  }

  // the number of estimated total operations in the purgatory
  private[this] val estimatedTotalOperations = new AtomicInteger(0)

  /* background thread expiring operations that have timed out */
  private val expirationReaper = new ExpiredOperationReaper()

  private val metricsTags = Map("delayedOperation" -> purgatoryName)
  newGauge("PurgatorySize", () => watched, metricsTags)
  newGauge("NumDelayedOperations", () => numDelayed, metricsTags)

  if (reaperEnabled)
    expirationReaper.start()

  /**
   * Check if the operation can be completed, if not watch it based on the given watch keys
   *
   * Note that a delayed operation can be watched on multiple keys. It is possible that
   * an operation is completed after it has been added to the watch list for some, but
   * not all of the keys. In this case, the operation is considered completed and won't
   * be added to the watch list of the remaining keys. The expiration reaper thread will
   * remove this operation from any watcher list in which the operation exists.
   *
   * @param operation the delayed operation to be checked
   * @param watchKeys keys for bookkeeping the operation
   * @return true iff the delayed operations can be completed by the caller
   *
   * 创建延迟操作对象后，延迟缓存会首先尝试完成，如果不能完成延迟操作，才会被加入到监控中
   */
  def tryCompleteElseWatch(operation: T, watchKeys: Seq[Any]): Boolean = {
    assert(watchKeys.nonEmpty, "The watch key list can't be empty")

    // The cost of tryComplete() is typically proportional to the number of keys. Calling tryComplete() for each key is
    // going to be expensive if there are many keys. Instead, we do the check in the following way through safeTryCompleteOrElse().
    // If the operation is not completed, we just add the operation to all keys. Then we call tryComplete() again. At
    // this time, if the operation is still not completed, we are guaranteed that it won't miss any future triggering
    // event since the operation is already on the watcher list for all keys.
    //
    // ==============[story about lock]==============
    // Through safeTryCompleteOrElse(), we hold the operation's lock while adding the operation to watch list and doing
    // the tryComplete() check. This is to avoid a potential deadlock between the callers to tryCompleteElseWatch() and
    // checkAndComplete(). For example, the following deadlock can happen if the lock is only held for the final tryComplete()
    // 1) thread_a holds readlock of stateLock from TransactionStateManager
    // 2) thread_a is executing tryCompleteElseWatch()
    // 3) thread_a adds op to watch list
    // 4) thread_b requires writelock of stateLock from TransactionStateManager (blocked by thread_a)
    // 5) thread_c calls checkAndComplete() and holds lock of op
    // 6) thread_c is waiting readlock of stateLock to complete op (blocked by thread_b)
    // 7) thread_a is waiting lock of op to call the final tryComplete() (blocked by thread_c)
    //
    // Note that even with the current approach, deadlocks could still be introduced. For example,
    // 1) thread_a calls tryCompleteElseWatch() and gets lock of op
    // 2) thread_a adds op to watch list
    // 3) thread_a calls op#tryComplete and tries to require lock_b
    // 4) thread_b holds lock_b and calls checkAndComplete()
    // 5) thread_b sees op from watch list
    // 6) thread_b needs lock of op
    // To avoid the above scenario, we recommend DelayedOperationPurgatory.checkAndComplete() be called without holding
    // any exclusive lock. Since DelayedOperationPurgatory.checkAndComplete() completes delayed operations asynchronously,
    // holding a exclusive lock to make the call is often unnecessary.
    // 先 tryComplete 一下，如果失败，则执行入 watch 队列
    if (operation.safeTryCompleteOrElse {
      // 存储到 watcher 队列中
      watchKeys.foreach(key => watchForOperation(key, operation))
      // 增加计数
      if (watchKeys.nonEmpty) estimatedTotalOperations.incrementAndGet()
    }) return true

    // if it cannot be completed by now and hence is watched, add to the expire queue also
    if (!operation.isCompleted) {
      if (timerEnabled) {
        // SystemTimer
        // 存储到时间轮（timingWheel），当到达过期时候后，会强制执行延迟操作的 onComplete() 方法
        // 添加到时间轮目的是在延迟操作超时后，服务端可以强制返回响应结果给客户端，加入到时间轮无需 key，因为定时器和延迟操作的键无关
        timeoutTimer.add(operation)
      }
      if (operation.isCompleted) {
        // 添加后完成，取消这个延迟操作的定时线程
        // cancel the timer task
        operation.cancel()
      }
    }

    false
  }

  /**
   * Check if some delayed operations can be completed with the given watch key,
   * and if yes complete them.
   *
   * @return the number of completed operations during this process
   * 外部事件调用该方法，判断给定的延迟缓存的键对应的延迟操作是否可以完成
   */
  def checkAndComplete(key: Any): Int = {
    val wl = watcherList(key)
    val watchers = inLock(wl.watchersLock) { wl.watchersByKey.get(key) }
    val numCompleted = if (watchers == null)
      0
    else
      watchers.tryCompleteWatched()
    debug(s"Request key $key unblocked $numCompleted $purgatoryName operations")
    numCompleted
  }

  /**
   * Return the total size of watch lists the purgatory. Since an operation may be watched
   * on multiple lists, and some of its watched entries may still be in the watch lists
   * even when it has been completed, this number may be larger than the number of real operations watched
   */
  def watched: Int = {
    watcherLists.foldLeft(0) { case (sum, watcherList) => sum + watcherList.allWatchers.map(_.countWatched).sum }
  }

  /**
   * Return the number of delayed operations in the expiry queue
   */
  def numDelayed: Int = timeoutTimer.size

  /**
    * Cancel watching on any delayed operations for the given key. Note the operation will not be completed
    */
  def cancelForKey(key: Any): List[T] = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watchers = wl.watchersByKey.remove(key)
      if (watchers != null)
        watchers.cancel()
      else
        Nil
    }
  }

  /*
   * Return the watch list of the given key, note that we need to
   * grab the removeWatchersLock to avoid the operation being added to a removed watcher list
   */
  private def watchForOperation(key: Any, operation: T): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      val watcher = wl.watchersByKey.getAndMaybePut(key)
      watcher.watch(operation)
    }
  }

  /*
   * Remove the key from watcher lists if its list is empty
   */
  private def removeKeyIfEmpty(key: Any, watchers: Watchers): Unit = {
    val wl = watcherList(key)
    inLock(wl.watchersLock) {
      // if the current key is no longer correlated to the watchers to remove, skip
      if (wl.watchersByKey.get(key) != watchers)
        return

      if (watchers != null && watchers.isEmpty) {
        wl.watchersByKey.remove(key)
      }
    }
  }

  /**
   * Shutdown the expire reaper thread
   */
  def shutdown(): Unit = {
    if (reaperEnabled)
      expirationReaper.shutdown()
    timeoutTimer.shutdown()
    removeMetric("PurgatorySize", metricsTags)
    removeMetric("NumDelayedOperations", metricsTags)
  }

  /**
   * A linked list of watched delayed operations based on some key
   */
  private class Watchers(val key: Any) {
    private[this] val operations = new ConcurrentLinkedQueue[T]()

    // count the current number of watched operations. This is O(n), so use isEmpty() if possible
    def countWatched: Int = operations.size

    def isEmpty: Boolean = operations.isEmpty

    // add the element to watch
    def watch(t: T): Unit = {
      // 一个键对应一个监视器，它管理了这个键对应的所有的延迟操作
      operations.add(t)
    }

    // traverse the list and try to complete some watched elements
    // 当外部事件发生时，会对这个键的所有延迟操作执行尝试完成动作，如果可以完成，会从延迟
    // 操作链表中移除
    def tryCompleteWatched(): Int = {
      var completed = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // another thread has completed this operation, just remove it
          // 被其他线程完成
          iter.remove()
        } else if (curr.safeTryComplete()) {
          // 当前线程完成
          iter.remove()
          completed += 1
        }
      }

      // 所有都完成，清空键
      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      completed
    }

    def cancel(): List[T] = {
      val iter = operations.iterator()
      val cancelled = new ListBuffer[T]()
      while (iter.hasNext) {
        val curr = iter.next()
        curr.cancel()
        iter.remove()
        cancelled += curr
      }
      cancelled.toList
    }

    // traverse the list and purge elements that are already completed by others
    // 外部事件根据如果根据指定分区（并不是所有的延迟操作的键都是分区）尝试完成延迟的操作，如果延迟操作可以完成，只会从延迟缓存中删除这个
    // 分区中已经完成的延迟操作，并不会删除其他分区中已经完成的延迟操作。其他分区的延迟操作会被后台清理线程清理，最终会调用这个方法完成清理
    // 工作。
    def purgeCompleted(): Int = {
      var purged = 0

      val iter = operations.iterator()
      while (iter.hasNext) {
        val curr = iter.next()
        if (curr.isCompleted) {
          // 完成，从链表中删除
          iter.remove()
          purged += 1
        }
      }

      if (operations.isEmpty)
        removeKeyIfEmpty(key, this)

      purged
    }
  }

  def advanceClock(timeoutMs: Long): Unit = {
    timeoutTimer.advanceClock(timeoutMs)

    // Trigger a purge if the number of completed but still being watched operations is larger than
    // the purge threshold. That number is computed by the difference btw the estimated total number of
    // operations and the number of pending delayed operations.
    // 通常情况下，estimatedTotalOperations 表示延迟操作的个数，numDelayed 也是延迟操作的个数，但是在执行该判断前会先调用一次
    // 定时器的 advanceClock 方法，将定时器的时钟往前移动一次。定时器在运行时，如果延迟操作延超时了，就会将延迟操作从定时器的延迟
    // 队列中移除，从而就会破坏这个平衡关系，最终会满这足这个条件
    if (estimatedTotalOperations.get - numDelayed > purgeInterval) {
      // now set estimatedTotalOperations to delayed (the number of pending operations) since we are going to
      // clean up watchers. Note that, if more operations are completed during the clean up, we may end up with
      // a little overestimated total number of operations.
      // 重新设置 estimatedTotalOperations 的值
      estimatedTotalOperations.getAndSet(numDelayed)
      debug("Begin purging watch lists")
      val purged = watcherLists.foldLeft(0) {
            // 执行延迟操作的清理工作
        case (sum, watcherList) => sum + watcherList.allWatchers.map(_.purgeCompleted()).sum
      }
      debug("Purged %d elements from watch lists.".format(purged))
    }
  }

  /**
   * A background reaper to expire delayed operations that have timed out
   * 后台清理线程，用于清理那些已经完成的延迟操作，它是 DelayedOperationPurgatory 内部类。
   * 因为有些延迟操作已经完成，但是并没有从延迟缓存中移除，这个线程的作用就是将这些已经完成的延迟操作移除。例如，生产者向分区 P1，P2 发送了
   * 几条消息，此时会对 P1 和 P2 分别创建一个延迟生产的操作 D1、D2，当 P1 和 P2 的备份副本来拉取同步数据时，假设 P1 的备份副本先同步，
   * 但是此时还不能完成延迟操作，等到 P2 的备份副本完成同步，此时才能完成上面两个延迟操作。由于外部事件是 P2 的备份副本完成同步，所以会
   * 尝试完成 D2，可以完成，从延迟缓存中删除 D2，但是 D1 却没有被移除，D1 的移除就是通过这个后台清理线程完成的
   */
  private class ExpiredOperationReaper extends ShutdownableThread(
    "ExpirationReaper-%d-%s".format(brokerId, purgatoryName),
    false) {

    override def doWork(): Unit = {
      // 线程调度逻辑在父类，定时器时钟的滑动间隔，每隔 200 ms 前进一次
      advanceClock(200L)
    }
  }
}
