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

import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import kafka.common.OffsetAndMetadata
import kafka.log.LogConfig
import kafka.message.ProducerCompressionCodec
import kafka.server._
import kafka.utils.Logging
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.JoinGroupResponseData.JoinGroupResponseMember
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.metrics.stats.Meter
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.requests._
import org.apache.kafka.common.utils.Time

import scala.collection.{Map, Seq, Set, immutable, mutable}
import scala.math.max

/**
 * GroupCoordinator handles general group membership and offset management.
 *
 * Each Kafka server instantiates a coordinator which is responsible for a set of
 * groups. Groups are assigned to coordinators based on their group names.
 * <p>
 * <b>Delayed operation locking notes:</b>
 * Delayed operations in GroupCoordinator use `group` as the delayed operation
 * lock. ReplicaManager.appendRecords may be invoked while holding the group lock
 * used by its callback.  The delayed callback may acquire the group lock
 * since the delayed operation is completed only if the group lock can be acquired.
 */
class GroupCoordinator(val brokerId: Int,
                       val groupConfig: GroupConfig,
                       val offsetConfig: OffsetConfig,
                       val groupManager: GroupMetadataManager,
                       val heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
                       val joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
                       time: Time,
                       metrics: Metrics) extends Logging {
  import GroupCoordinator._

  type JoinCallback = JoinGroupResult => Unit
  type SyncCallback = SyncGroupResult => Unit

  /* setup metrics */
  val offsetDeletionSensor = metrics.sensor("OffsetDeletions")

  offsetDeletionSensor.add(new Meter(
    metrics.metricName("offset-deletion-rate",
      "group-coordinator-metrics",
      "The rate of administrative deleted offsets"),
    metrics.metricName("offset-deletion-count",
      "group-coordinator-metrics",
      "The total number of administrative deleted offsets")))

  val groupCompletedRebalanceSensor = metrics.sensor("CompletedRebalances")

  groupCompletedRebalanceSensor.add(new Meter(
    metrics.metricName("group-completed-rebalance-rate",
      "group-coordinator-metrics",
      "The rate of completed rebalance"),
    metrics.metricName("group-completed-rebalance-count",
      "group-coordinator-metrics",
      "The total number of completed rebalance")))

  this.logIdent = "[GroupCoordinator " + brokerId + "]: "

  private val isActive = new AtomicBoolean(false)

  def offsetsTopicConfigs: Properties = {
    val props = new Properties
    props.put(LogConfig.CleanupPolicyProp, LogConfig.Compact)
    props.put(LogConfig.SegmentBytesProp, offsetConfig.offsetsTopicSegmentBytes.toString)
    props.put(LogConfig.CompressionTypeProp, ProducerCompressionCodec.name)
    props
  }

  /**
   * NOTE: If a group lock and metadataLock are simultaneously needed,
   * be sure to acquire the group lock before metadataLock to prevent deadlock
   */

  /**
   * Startup logic executed at the same time when the server starts up.
   */
  def startup(enableMetadataExpiration: Boolean = true): Unit = {
    info("Starting up.")
    groupManager.startup(enableMetadataExpiration)
    isActive.set(true)
    info("Startup complete.")
  }

  /**
   * Shutdown logic executed at the same time when server shuts down.
   * Ordering of actions should be reversed from the startup process.
   */
  def shutdown(): Unit = {
    info("Shutting down.")
    isActive.set(false)
    groupManager.shutdown()
    heartbeatPurgatory.shutdown()
    joinPurgatory.shutdown()
    info("Shutdown complete.")
  }

  /**
   * Verify if the group has space to accept the joining member. The various
   * criteria are explained below.
   */
  private def acceptJoiningMember(group: GroupMetadata, member: String): Boolean = {
    group.currentState match {
      // Always accept the request when the group is empty or dead
      case Empty | Dead =>
        true

      // An existing member is accepted if it is already awaiting. New members are accepted
      // up to the max group size. Note that the number of awaiting members is used here
      // for two reasons:
      // 1) the group size is not reliable as it could already be above the max group size
      //    if the max group size was reduced.
      // 2) using the number of awaiting members allows to kick out the last rejoining
      //    members of the group.
      // 如果一个消费者已经存在，并且在等待中，这个加入的请求需要被处理（不会占用数量）
      // 新的消费者需要看当前正在等待加入的消费者是否已经达到了能最大容忍等待数量
      case PreparingRebalance =>
        (group.has(member) && group.get(member).isAwaitingJoin) ||
          group.numAwaiting < groupConfig.groupMaxSize

      // An existing member is accepted. New members are accepted up to the max group size.
      // Note that the group size is used here. When the group transitions to CompletingRebalance,
      // members which haven't rejoined are removed.
      case CompletingRebalance | Stable =>
        group.has(member) || group.size < groupConfig.groupMaxSize
    }
  }

  def handleJoinGroup(groupId: String,
                      memberId: String,
                      groupInstanceId: Option[String],
                      requireKnownMemberId: Boolean,
                      clientId: String,
                      clientHost: String,
                      rebalanceTimeoutMs: Int,
                      sessionTimeoutMs: Int,
                      protocolType: String,
                      protocols: List[(String, Array[Byte])],
                      responseCallback: JoinCallback): Unit = {
    // 执行校验规则，如 groupId 是否为空，当前节点是否是协调节点，如果没有通过，则通过回调函数返回对应失败的响应
    validateGroupStatus(groupId, ApiKeys.JOIN_GROUP).foreach { error =>
      responseCallback(JoinGroupResult(memberId, error))
      return
    }

    if (sessionTimeoutMs < groupConfig.groupMinSessionTimeoutMs ||
      sessionTimeoutMs > groupConfig.groupMaxSessionTimeoutMs) {
      responseCallback(JoinGroupResult(memberId, Errors.INVALID_SESSION_TIMEOUT))
    } else {
      // 首次加入 memberId = ""
      val isUnknownMember = memberId == JoinGroupRequest.UNKNOWN_MEMBER_ID
      // group is created if it does not exist and the member id is UNKNOWN. if member
      // is specified but group does not exist, request is rejected with UNKNOWN_MEMBER_ID
      // 如果 group 不存在，则为当前的 groupId 创建一个 GroupMetadata, state = Empty
      groupManager.getOrMaybeCreateGroup(groupId, isUnknownMember) match {
        case None =>
          responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
        case Some(group) =>
          group.inLock {
            // group 没有足够的空间
            if (!acceptJoiningMember(group, memberId)) {
              group.remove(memberId)
              group.removeStaticMember(groupInstanceId)
              responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.GROUP_MAX_SIZE_REACHED))
            } else if (isUnknownMember) {
              // 新消费者首次加入
              doUnknownJoinGroup(group, groupInstanceId, requireKnownMemberId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            } else {
              // 非首次加入，可能断联后重新连接或者发生再平衡事件
              doJoinGroup(group, memberId, groupInstanceId, clientId, clientHost, rebalanceTimeoutMs, sessionTimeoutMs, protocolType, protocols, responseCallback)
            }

            // attempt to complete JoinGroup
            // 如果是 PreparingRebalance 状态，尝试完成一下延迟操作
            // 这里为什么要检查是否完成，首先如果消费组是初始化状态（Empty）下消费者加入消费组，消费组创建的延迟操作是 InitialDelayedJoin,
            // 这里是检查完成的方法永远返回 false。
            // 但是如果是消费组发生再平衡，消费组创建的延迟操作是 DelayedJoin，而且协调者处理加入请求时并不是每次都会创建延迟操作，即并不会
            // 每次都调用创建延迟操作后的 tryCompleteElseWatch() 方法，并且延迟操作相关的外部事件可能会完成它（即加入消费组这个动作），
            // 所以只要有触发的外部事件，就应该调用该方式尝试尽快完成。
            if (group.is(PreparingRebalance)) {
              joinPurgatory.checkAndComplete(GroupKey(group.groupId))
            }
          }
      }
    }
  }

  private def doUnknownJoinGroup(group: GroupMetadata,
                                 groupInstanceId: Option[String],
                                 requireKnownMemberId: Boolean,
                                 clientId: String,
                                 clientHost: String,
                                 rebalanceTimeoutMs: Int,
                                 sessionTimeoutMs: Int,
                                 protocolType: String,
                                 protocols: List[(String, Array[Byte])],
                                 responseCallback: JoinCallback): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        responseCallback(JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else {
        // 生成一个新的 memberId（clientId-uuid）
        val newMemberId = group.generateMemberId(clientId, groupInstanceId)

        // 这个表示定义了 group.instance.id
        if (group.hasStaticMember(groupInstanceId)) {
          updateStaticMemberAndRebalance(group, newMemberId, groupInstanceId, protocols, responseCallback)
          // 如果这个方法但是没有给 memberId，放到 pending 队列，然后返回响应，需要重新创建要给新的 memberId 重新加入 group
        } else if (requireKnownMemberId) {
            // If member id required (dynamic membership), register the member in the pending member list
            // and send back a response to call for another join group request with allocated member id.
          debug(s"Dynamic member with unknown member id joins group ${group.groupId} in " +
              s"${group.currentState} state. Created a new member id $newMemberId and request the member to rejoin with this id.")
          // pending 队列
          group.addPendingMember(newMemberId)
          // 设置超时时间
          addPendingMemberExpiration(group, newMemberId, sessionTimeoutMs)
          // 返回响应
          responseCallback(JoinGroupResult(newMemberId, Errors.MEMBER_ID_REQUIRED))
        } else {
          info(s"${if (groupInstanceId.isDefined) "Static" else "Dynamic"} Member with unknown member id joins group ${group.groupId} in " +
            s"${group.currentState} state. Created a new member id $newMemberId for this member and add to the group.")
          // 正常流程在这里加入 group
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, newMemberId, groupInstanceId,
            clientId, clientHost, protocolType, protocols, group, responseCallback)
        }
      }
    }
  }

  private def doJoinGroup(group: GroupMetadata,
                          memberId: String,
                          groupInstanceId: Option[String],
                          clientId: String,
                          clientHost: String,
                          rebalanceTimeoutMs: Int,
                          sessionTimeoutMs: Int,
                          protocolType: String,
                          protocols: List[(String, Array[Byte])],
                          responseCallback: JoinCallback): Unit = {
    group.inLock {
      // 表示有其他线程将消费组移除了
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(JoinGroupResult(memberId, Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (!group.supportsProtocols(protocolType, MemberMetadata.plainProtocolSet(protocols))) {
        responseCallback(JoinGroupResult(memberId, Errors.INCONSISTENT_GROUP_PROTOCOL))
        // pending 逻辑，先前如果已经在发送过了，需要等待完成
      } else if (group.isPendingMember(memberId)) {
        // A rejoining pending member will be accepted. Note that pending member will never be a static member.
        if (groupInstanceId.isDefined) {
          throw new IllegalStateException(s"the static member $groupInstanceId was not expected to be assigned " +
            s"into pending member bucket with member id $memberId")
        } else {
          debug(s"Dynamic Member with specific member id $memberId joins group ${group.groupId} in " +
            s"${group.currentState} state. Adding to the group now.")
          addMemberAndRebalance(rebalanceTimeoutMs, sessionTimeoutMs, memberId, groupInstanceId,
            clientId, clientHost, protocolType, protocols, group, responseCallback)
        }
      } else {
        // false
        val groupInstanceIdNotFound = groupInstanceId.isDefined && !group.hasStaticMember(groupInstanceId)
        if (group.isStaticMemberFenced(memberId, groupInstanceId, "join-group")) {
          // given member id doesn't match with the groupInstanceId. Inform duplicate instance to shut down immediately.
          responseCallback(JoinGroupResult(memberId, Errors.FENCED_INSTANCE_ID))
        } else if (!group.has(memberId) || groupInstanceIdNotFound) {
            // If the dynamic member trying to register with an unrecognized id, or
            // the static member joins with unknown group instance id, send the response to let
            // it reset its member id and retry.
          responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
        } else {
          val member = group.get(memberId)

          group.currentState match {
              // 正常消费组稳定状态 Stable，新的消费者加入将消费组状态变更为 PreparingRebalance
            case PreparingRebalance =>
              updateMemberAndRebalance(group, member, protocols, s"Member ${member.memberId} joining group during ${group.currentState}", responseCallback)

            case CompletingRebalance =>
              // 元数据没有变更，这种状态下可能是消费者客户端没有收到之前服务端返回的响应，触发了重试，这种情况直接返回和上次相同的数据即可
              if (member.matches(protocols)) {
                // member is joining with the same metadata (which could be because it failed to
                // receive the initial JoinGroup response), so just return current group information
                // for the current generation.
                responseCallback(JoinGroupResult(
                  members = if (group.isLeader(memberId)) {
                    group.currentMemberMetadata
                  } else {
                    List.empty
                  },
                  memberId = memberId,
                  generationId = group.generationId,
                  protocolType = group.protocolType,
                  protocolName = group.protocolName,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              } else {
                // member has changed metadata, so force a rebalance
                // 元数据发生了变更，强制执行 rebalance
                updateMemberAndRebalance(group, member, protocols, s"Updating metadata for member ${member.memberId} during ${group.currentState}", responseCallback)
              }

            // 可能是重新连接或者心跳超时等场景
            case Stable =>
              val member = group.get(memberId)
              if (group.isLeader(memberId)) {
                // force a rebalance if the leader sends JoinGroup;
                // This allows the leader to trigger rebalances for changes affecting assignment
                // which do not affect the member metadata (such as topic metadata changes for the consumer)
                updateMemberAndRebalance(group, member, protocols, s"leader ${member.memberId} re-joining group during ${group.currentState}", responseCallback)
              } else if (!member.matches(protocols)) {
                // 元数据发生变更
                updateMemberAndRebalance(group, member, protocols, s"Updating metadata for member ${member.memberId} during ${group.currentState}", responseCallback)
              } else {
                // for followers with no actual change to their metadata, just return group information
                // for the current generation which will allow them to issue SyncGroup
                // 非主消费者重复加入，可能触发重试等
                responseCallback(JoinGroupResult(
                  members = List.empty,
                  memberId = memberId,
                  generationId = group.generationId,
                  protocolType = group.protocolType,
                  protocolName = group.protocolName,
                  leaderId = group.leaderOrNull,
                  error = Errors.NONE))
              }

              // memberId 不可空，但是消费组状态是 Empty，这是不合法的状态
            case Empty | Dead =>
              // Group reaches unexpected state. Let the joining member reset their generation and rejoin.
              warn(s"Attempt to add rejoining member $memberId of group ${group.groupId} in " +
                s"unexpected group state ${group.currentState}")
              responseCallback(JoinGroupResult(memberId, Errors.UNKNOWN_MEMBER_ID))
          }
        }
      }
    }
  }

  def handleSyncGroup(groupId: String,
                      generation: Int,
                      memberId: String,
                      protocolType: Option[String],
                      protocolName: Option[String],
                      groupInstanceId: Option[String],
                      groupAssignment: Map[String, Array[Byte]],
                      responseCallback: SyncCallback): Unit = {
    validateGroupStatus(groupId, ApiKeys.SYNC_GROUP) match {
      case Some(error) if error == Errors.COORDINATOR_LOAD_IN_PROGRESS =>
        // The coordinator is loading, which means we've lost the state of the active rebalance and the
        // group will need to start over at JoinGroup. By returning rebalance in progress, the consumer
        // will attempt to rejoin without needing to rediscover the coordinator. Note that we cannot
        // return COORDINATOR_LOAD_IN_PROGRESS since older clients do not expect the error.
        responseCallback(SyncGroupResult(Errors.REBALANCE_IN_PROGRESS))

      case Some(error) => responseCallback(SyncGroupResult(error))

      case None =>
        groupManager.getGroup(groupId) match {
          case None => responseCallback(SyncGroupResult(Errors.UNKNOWN_MEMBER_ID))
          // 处理逻辑
          case Some(group) => doSyncGroup(group, generation, memberId, protocolType, protocolName,
            groupInstanceId, groupAssignment, responseCallback)
        }
    }
  }

  private def doSyncGroup(group: GroupMetadata,
                          generationId: Int,
                          memberId: String,
                          protocolType: Option[String],
                          protocolName: Option[String],
                          groupInstanceId: Option[String],
                          groupAssignment: Map[String, Array[Byte]],
                          responseCallback: SyncCallback): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; this is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(SyncGroupResult(Errors.COORDINATOR_NOT_AVAILABLE))
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "sync-group")) {
        responseCallback(SyncGroupResult(Errors.FENCED_INSTANCE_ID))
      } else if (!group.has(memberId)) {
        responseCallback(SyncGroupResult(Errors.UNKNOWN_MEMBER_ID))
      } else if (generationId != group.generationId) {
        responseCallback(SyncGroupResult(Errors.ILLEGAL_GENERATION))
      } else if (protocolType.isDefined && !group.protocolType.contains(protocolType.get)) {
        responseCallback(SyncGroupResult(Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else if (protocolName.isDefined && !group.protocolName.contains(protocolName.get)) {
        responseCallback(SyncGroupResult(Errors.INCONSISTENT_GROUP_PROTOCOL))
      } else {
        group.currentState match {
            // SYNC 阶段消费组状态不应该 Empty 或 PreparingRebalance
          case Empty =>
            responseCallback(SyncGroupResult(Errors.UNKNOWN_MEMBER_ID))

          case PreparingRebalance =>
            responseCallback(SyncGroupResult(Errors.REBALANCE_IN_PROGRESS))

          // 确认 leader 消费者后，消费者会调用 onJoinLeader 方法将分区的分配结果返回给协调节点。
          // 在 leader 消费者的 JOIN GROUP 请求返回时，会将消费组的状态变更为 CompletingRebalance（GroupCoordinator.onCompleteJoin()）
          case CompletingRebalance =>
            // 保存回调函数，标识消费者正在同步
            // 如果不是主消费者，仅仅保存回调，等待主消费组将分配的结果写入 Kafka 的内部主题后，协调者会调用所有消费者元数据里
            // 的回调函数将分区分配的结果返回给消费者客户端
            group.get(memberId).awaitingSyncCallback = responseCallback

            // if this is the leader, then we can attempt to persist state and transition to stable
            // 只有 leader 节点才能执行同步操作
            if (group.isLeader(memberId)) {
              info(s"Assignment received from leader for group ${group.groupId} for generation ${group.generationId}")

              // fill any missing members with an empty assignment
              val missing = group.allMembers.diff(groupAssignment.keySet)
              val assignment = groupAssignment ++ missing.map(_ -> Array.empty[Byte]).toMap

              if (missing.nonEmpty) {
                warn(s"Setting empty assignments for members $missing of ${group.groupId} for generation ${group.generationId}")
              }

              // 存储消费组的信息
              groupManager.storeGroup(group, assignment, (error: Errors) => {
                group.inLock {
                  // another member may have joined the group while we were awaiting this callback,
                  // so we must ensure we are still in the CompletingRebalance state and the same generation
                  // when it gets invoked. if we have transitioned to another state, then do nothing
                  if (group.is(CompletingRebalance) && generationId == group.generationId) {
                    if (error != Errors.NONE) {
                      resetAndPropagateAssignmentError(group, error)
                      maybePrepareRebalance(group, s"error when storing group assignment during SyncGroup (member: $memberId)")
                    } else {
                      // 设置并传播各个消费者的分区
                      setAndPropagateAssignment(group, assignment)
                      // 状态由 CompletingRebalance -> Stable
                      group.transitionTo(Stable)
                    }
                  }
                }
              })
              groupCompletedRebalanceSensor.record()
            }

          case Stable =>
            // if the group is stable, we just return the current assignment
            // 如果某个 follower 消费者发送 SYNC GROUP 请求迟了，leader 消费者已经将消费组的状态变更为 Stable，这里也没有关系，因为
            // 所有这个节点的分配的分区已经在 leader 消费者的 SYNC GROUP 请求设置了并保存到了元数据的 assignment 里了，
            // 这里直接使用回调函数返回分配的分区，并重置下心跳即可。
            val memberMetadata = group.get(memberId)
            responseCallback(SyncGroupResult(group.protocolType, group.protocolName, memberMetadata.assignment, Errors.NONE))
            // 重置心跳，因为在消费者返回 SYNC GROUP 请求时，这个消费者元数据里的同步回调函数为空，是不会完成心跳的。这里重置心跳是告诉协调者
            // 虽然发送 SYNC GROUP 请求迟了，但是还是存活状态。
            completeAndScheduleNextHeartbeatExpiration(group, group.get(memberId))

          case Dead =>
            throw new IllegalStateException(s"Reached unexpected condition for Dead group ${group.groupId}")
        }
      }
    }
  }

  def handleLeaveGroup(groupId: String,
                       leavingMembers: List[MemberIdentity],
                       responseCallback: LeaveGroupResult => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.LEAVE_GROUP) match {
      case Some(error) =>
        responseCallback(leaveError(error, List.empty))
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            responseCallback(leaveError(Errors.NONE, leavingMembers.map {leavingMember =>
              memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
            }))
          case Some(group) =>
            group.inLock {
              if (group.is(Dead)) {
                responseCallback(leaveError(Errors.COORDINATOR_NOT_AVAILABLE, List.empty))
              } else {
                val memberErrors = leavingMembers.map { leavingMember =>
                  val memberId = leavingMember.memberId
                  val groupInstanceId = Option(leavingMember.groupInstanceId)
                  if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID
                    && group.isStaticMemberFenced(memberId, groupInstanceId, "leave-group")) {
                    memberLeaveError(leavingMember, Errors.FENCED_INSTANCE_ID)
                  } else if (group.isPendingMember(memberId)) {
                    if (groupInstanceId.isDefined) {
                      throw new IllegalStateException(s"the static member $groupInstanceId was not expected to be leaving " +
                        s"from pending member bucket with member id $memberId")
                    } else {
                      // if a pending member is leaving, it needs to be removed from the pending list, heartbeat cancelled
                      // and if necessary, prompt a JoinGroup completion.
                      info(s"Pending member $memberId is leaving group ${group.groupId}.")
                      removePendingMemberAndUpdateGroup(group, memberId)
                      heartbeatPurgatory.checkAndComplete(MemberKey(group.groupId, memberId))
                      memberLeaveError(leavingMember, Errors.NONE)
                    }
                  } else if (!group.has(memberId) && !group.hasStaticMember(groupInstanceId)) {
                    memberLeaveError(leavingMember, Errors.UNKNOWN_MEMBER_ID)
                  } else {
                    val member = if (group.hasStaticMember(groupInstanceId))
                      group.get(group.getStaticMemberId(groupInstanceId))
                    else
                      group.get(memberId)
                    removeHeartbeatForLeavingMember(group, member)
                    info(s"Member[group.instance.id ${member.groupInstanceId}, member.id ${member.memberId}] " +
                      s"in group ${group.groupId} has left, removing it from the group")
                    removeMemberAndUpdateGroup(group, member, s"removing member $memberId on LeaveGroup")
                    memberLeaveError(leavingMember, Errors.NONE)
                  }
                }
                responseCallback(leaveError(Errors.NONE, memberErrors))
              }
            }
        }
    }
  }

  def handleDeleteGroups(groupIds: Set[String]): Map[String, Errors] = {
    val groupErrors = mutable.Map.empty[String, Errors]
    val groupsEligibleForDeletion = mutable.ArrayBuffer[GroupMetadata]()

    groupIds.foreach { groupId =>
      validateGroupStatus(groupId, ApiKeys.DELETE_GROUPS) match {
        case Some(error) =>
          groupErrors += groupId -> error

        case None =>
          groupManager.getGroup(groupId) match {
            case None =>
              groupErrors += groupId ->
                (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
            case Some(group) =>
              group.inLock {
                group.currentState match {
                  case Dead =>
                    groupErrors += groupId ->
                      (if (groupManager.groupNotExists(groupId)) Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR)
                  case Empty =>
                    group.transitionTo(Dead)
                    groupsEligibleForDeletion += group
                  case Stable | PreparingRebalance | CompletingRebalance =>
                    groupErrors(groupId) = Errors.NON_EMPTY_GROUP
                }
              }
          }
      }
    }

    if (groupsEligibleForDeletion.nonEmpty) {
      val offsetsRemoved = groupManager.cleanupGroupMetadata(groupsEligibleForDeletion, _.removeAllOffsets())
      groupErrors ++= groupsEligibleForDeletion.map(_.groupId -> Errors.NONE).toMap
      info(s"The following groups were deleted: ${groupsEligibleForDeletion.map(_.groupId).mkString(", ")}. " +
        s"A total of $offsetsRemoved offsets were removed.")
    }

    groupErrors
  }

  def handleDeleteOffsets(groupId: String, partitions: Seq[TopicPartition]): (Errors, Map[TopicPartition, Errors]) = {
    var groupError: Errors = Errors.NONE
    var partitionErrors: Map[TopicPartition, Errors] = Map()
    var partitionsEligibleForDeletion: Seq[TopicPartition] = Seq()

    validateGroupStatus(groupId, ApiKeys.OFFSET_DELETE) match {
      case Some(error) =>
        groupError = error

      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            groupError = if (groupManager.groupNotExists(groupId))
              Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR

          case Some(group) =>
            group.inLock {
              group.currentState match {
                case Dead =>
                  groupError = if (groupManager.groupNotExists(groupId))
                    Errors.GROUP_ID_NOT_FOUND else Errors.NOT_COORDINATOR

                case Empty =>
                  partitionsEligibleForDeletion = partitions

                case PreparingRebalance | CompletingRebalance | Stable if group.isConsumerGroup =>
                  val (consumed, notConsumed) =
                    partitions.partition(tp => group.isSubscribedToTopic(tp.topic()))

                  partitionsEligibleForDeletion = notConsumed
                  partitionErrors = consumed.map(_ -> Errors.GROUP_SUBSCRIBED_TO_TOPIC).toMap

                case _ =>
                  groupError = Errors.NON_EMPTY_GROUP
              }
            }

            if (partitionsEligibleForDeletion.nonEmpty) {
              val offsetsRemoved = groupManager.cleanupGroupMetadata(Seq(group), group => {
                group.removeOffsets(partitionsEligibleForDeletion)
              })

              partitionErrors ++= partitionsEligibleForDeletion.map(_ -> Errors.NONE).toMap

              offsetDeletionSensor.record(offsetsRemoved)

              info(s"The following offsets of the group $groupId were deleted: ${partitionsEligibleForDeletion.mkString(", ")}. " +
                s"A total of $offsetsRemoved offsets were removed.")
            }
        }
    }

    // If there is a group error, the partition errors is empty
    groupError -> partitionErrors
  }

  def handleHeartbeat(groupId: String,
                      memberId: String,
                      groupInstanceId: Option[String],
                      generationId: Int,
                      responseCallback: Errors => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.HEARTBEAT).foreach { error =>
      if (error == Errors.COORDINATOR_LOAD_IN_PROGRESS)
        // the group is still loading, so respond just blindly
        responseCallback(Errors.NONE)
      else
        responseCallback(error)
      return
    }

    groupManager.getGroup(groupId) match {
      case None =>
        responseCallback(Errors.UNKNOWN_MEMBER_ID)

      case Some(group) => group.inLock {
        if (group.is(Dead)) {
          // if the group is marked as dead, it means some other thread has just removed the group
          // from the coordinator metadata; this is likely that the group has migrated to some other
          // coordinator OR the group is in a transient unstable phase. Let the member retry
          // finding the correct coordinator and rejoin.
          responseCallback(Errors.COORDINATOR_NOT_AVAILABLE)
        } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "heartbeat")) {
          responseCallback(Errors.FENCED_INSTANCE_ID)
        } else if (!group.has(memberId)) {
          responseCallback(Errors.UNKNOWN_MEMBER_ID)
        } else if (generationId != group.generationId) {
          responseCallback(Errors.ILLEGAL_GENERATION)
        } else {
          group.currentState match {
            case Empty =>
              responseCallback(Errors.UNKNOWN_MEMBER_ID)

            // 消费者在收到 JOIN GROUP 响应后开始发送心跳，如果消费组此时的状态是 CompletingRebalance，表示消费组正在进行 re-balance，
            // 此时需要将心跳请求正常处理即可
            case CompletingRebalance =>
              // consumers may start sending heartbeat after join-group response, in which case
              // we should treat them as normal hb request and reset the timer
              val member = group.get(memberId)
              // 处理逻辑，重置下心跳
              completeAndScheduleNextHeartbeatExpiration(group, member)
              // 返回响应
              responseCallback(Errors.NONE)

            // 正在准备 re-balance
            case PreparingRebalance =>
                val member = group.get(memberId)
                completeAndScheduleNextHeartbeatExpiration(group, member)
                responseCallback(Errors.REBALANCE_IN_PROGRESS)

            // 消费组状态变为 Stable，re-balance 完成，正常处理心跳请求，重新心跳
            case Stable =>
                val member = group.get(memberId)
                completeAndScheduleNextHeartbeatExpiration(group, member)
                responseCallback(Errors.NONE)

            case Dead =>
              throw new IllegalStateException(s"Reached unexpected condition for Dead group $groupId")
          }
        }
      }
    }
  }

  def handleTxnCommitOffsets(groupId: String,
                             producerId: Long,
                             producerEpoch: Short,
                             memberId: String,
                             groupInstanceId: Option[String],
                             generationId: Int,
                             offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                             responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.TXN_OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.map { case (k, _) => k -> error })
      case None =>
        val group = groupManager.getGroup(groupId).getOrElse {
          groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
        }
        doTxnCommitOffsets(group, memberId, groupInstanceId, generationId, producerId, producerEpoch, offsetMetadata, responseCallback)
    }
  }

  def handleCommitOffsets(groupId: String,
                          memberId: String,
                          groupInstanceId: Option[String],
                          generationId: Int,
                          offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                          responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    validateGroupStatus(groupId, ApiKeys.OFFSET_COMMIT) match {
      case Some(error) => responseCallback(offsetMetadata.map { case (k, _) => k -> error })
      case None =>
        groupManager.getGroup(groupId) match {
          case None =>
            if (generationId < 0) {
              // the group is not relying on Kafka for group management, so allow the commit
              val group = groupManager.addGroup(new GroupMetadata(groupId, Empty, time))
              doCommitOffsets(group, memberId, groupInstanceId, generationId, offsetMetadata, responseCallback)
            } else {
              // or this is a request coming from an older generation. either way, reject the commit
              // 请求来自之前的 generation，本次提交需要拒绝
              responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
            }

            // 正常场景
          case Some(group) =>
            doCommitOffsets(group, memberId, groupInstanceId, generationId, offsetMetadata, responseCallback)
        }
    }
  }

  def scheduleHandleTxnCompletion(producerId: Long,
                                  offsetsPartitions: Iterable[TopicPartition],
                                  transactionResult: TransactionResult): Unit = {
    require(offsetsPartitions.forall(_.topic == Topic.GROUP_METADATA_TOPIC_NAME))
    val isCommit = transactionResult == TransactionResult.COMMIT
    groupManager.scheduleHandleTxnCompletion(producerId, offsetsPartitions.map(_.partition).toSet, isCommit)
  }

  private def doTxnCommitOffsets(group: GroupMetadata,
                                 memberId: String,
                                 groupInstanceId: Option[String],
                                 generationId: Int,
                                 producerId: Long,
                                 producerEpoch: Short,
                                 offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                                 responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.COORDINATOR_NOT_AVAILABLE })
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "txn-commit-offsets")) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.FENCED_INSTANCE_ID })
      } else if (memberId != JoinGroupRequest.UNKNOWN_MEMBER_ID && !group.has(memberId)) {
        // Enforce member id when it is set.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.UNKNOWN_MEMBER_ID })
      } else if (generationId >= 0 && generationId != group.generationId) {
        // Enforce generation check when it is set.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
      } else {
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback, producerId, producerEpoch)
      }
    }
  }

  private def doCommitOffsets(group: GroupMetadata,
                              memberId: String,
                              groupInstanceId: Option[String],
                              generationId: Int,
                              offsetMetadata: immutable.Map[TopicPartition, OffsetAndMetadata],
                              responseCallback: immutable.Map[TopicPartition, Errors] => Unit): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        // if the group is marked as dead, it means some other thread has just removed the group
        // from the coordinator metadata; it is likely that the group has migrated to some other
        // coordinator OR the group is in a transient unstable phase. Let the member retry
        // finding the correct coordinator and rejoin.
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.COORDINATOR_NOT_AVAILABLE })
      } else if (group.isStaticMemberFenced(memberId, groupInstanceId, "commit-offsets")) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.FENCED_INSTANCE_ID })
      } else if (generationId < 0 && group.is(Empty)) {
        // The group is only using Kafka to store offsets.
        // 使用 Kafka 存储 offset
        groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)
      } else if (!group.has(memberId)) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.UNKNOWN_MEMBER_ID })
      } else if (generationId != group.generationId) {
        responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.ILLEGAL_GENERATION })
      } else {
        // 消费组当前的状态，只有 Stable 和 PreparingRebalance 可以提交
        group.currentState match {
          case Stable | PreparingRebalance =>
            // During PreparingRebalance phase, we still allow a commit request since we rely
            // on heartbeat response to eventually notify the rebalance in progress signal to the consumer
            val member = group.get(memberId)
            // 执行心跳 todo huangran 这里为什么要执行心跳
            completeAndScheduleNextHeartbeatExpiration(group, member)
            groupManager.storeOffsets(group, memberId, offsetMetadata, responseCallback)

          case CompletingRebalance =>
            // We should not receive a commit request if the group has not completed rebalance;
            // but since the consumer's member.id and generation is valid, it means it has received
            // the latest group generation information from the JoinResponse.
            // So let's return a REBALANCE_IN_PROGRESS to let consumer handle it gracefully.
            responseCallback(offsetMetadata.map { case (k, _) => k -> Errors.REBALANCE_IN_PROGRESS })

          case _ =>
            throw new RuntimeException(s"Logic error: unexpected group state ${group.currentState}")
        }
      }
    }
  }

  def handleFetchOffsets(groupId: String, requireStable: Boolean, partitions: Option[Seq[TopicPartition]] = None):
  (Errors, Map[TopicPartition, OffsetFetchResponse.PartitionData]) = {

    validateGroupStatus(groupId, ApiKeys.OFFSET_FETCH) match {
      case Some(error) => error -> Map.empty
      case None =>
        // return offsets blindly regardless the current group state since the group may be using
        // Kafka commit storage without automatic group management
        (Errors.NONE, groupManager.getOffsets(groupId, requireStable, partitions))
    }
  }

  def handleListGroups(states: Set[String]): (Errors, List[GroupOverview]) = {
    if (!isActive.get) {
      (Errors.COORDINATOR_NOT_AVAILABLE, List[GroupOverview]())
    } else {
      val errorCode = if (groupManager.isLoading) Errors.COORDINATOR_LOAD_IN_PROGRESS else Errors.NONE
      // if states is empty, return all groups
      val groups = if (states.isEmpty)
        groupManager.currentGroups
      else
        groupManager.currentGroups.filter(g => states.contains(g.summary.state))
      (errorCode, groups.map(_.overview).toList)
    }
  }

  def handleDescribeGroup(groupId: String): (Errors, GroupSummary) = {
    validateGroupStatus(groupId, ApiKeys.DESCRIBE_GROUPS) match {
      case Some(error) => (error, GroupCoordinator.EmptyGroup)
      case None =>
        groupManager.getGroup(groupId) match {
          case None => (Errors.NONE, GroupCoordinator.DeadGroup)
          case Some(group) =>
            group.inLock {
              (Errors.NONE, group.summary)
            }
        }
    }
  }

  def handleDeletedPartitions(topicPartitions: Seq[TopicPartition]): Unit = {
    val offsetsRemoved = groupManager.cleanupGroupMetadata(groupManager.currentGroups, group => {
      group.removeOffsets(topicPartitions)
    })
    info(s"Removed $offsetsRemoved offsets associated with deleted partitions: ${topicPartitions.mkString(", ")}.")
  }

  private def isValidGroupId(groupId: String, api: ApiKeys): Boolean = {
    api match {
      case ApiKeys.OFFSET_COMMIT | ApiKeys.OFFSET_FETCH | ApiKeys.DESCRIBE_GROUPS | ApiKeys.DELETE_GROUPS =>
        // For backwards compatibility, we support the offset commit APIs for the empty groupId, and also
        // in DescribeGroups and DeleteGroups so that users can view and delete state of all groups.
        groupId != null
      case _ =>
        // The remaining APIs are groups using Kafka for group coordination and must have a non-empty groupId
        groupId != null && !groupId.isEmpty
    }
  }

  /**
   * Check that the groupId is valid, assigned to this coordinator and that the group has been loaded.
   */
  private def validateGroupStatus(groupId: String, api: ApiKeys): Option[Errors] = {
    if (!isValidGroupId(groupId, api))
      Some(Errors.INVALID_GROUP_ID)
    else if (!isActive.get)
      Some(Errors.COORDINATOR_NOT_AVAILABLE)
    else if (isCoordinatorLoadInProgress(groupId))
      Some(Errors.COORDINATOR_LOAD_IN_PROGRESS)
    else if (!isCoordinatorForGroup(groupId))
      Some(Errors.NOT_COORDINATOR)
    else
      None
  }

  // 卸载元数据，消费组的协调者发生了改变
  private def onGroupUnloaded(group: GroupMetadata): Unit = {
    group.inLock {
      info(s"Unloading group metadata for ${group.groupId} with generation ${group.generationId}")
      val previousState = group.currentState
      // 状态设置为失败
      group.transitionTo(Dead)

      previousState match {
        // 之前状态是准备再平衡（协调者还不知道有多少消费者），发送加入响应 给所有消费者，
        // 让消费者重新加入新的协调者节点，返回 NOT_COORDINATOR 的错误信息
        case Empty | Dead =>
        case PreparingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeJoinCallback(member, JoinGroupResult(member.memberId, Errors.NOT_COORDINATOR))
          }

          joinPurgatory.checkAndComplete(GroupKey(group.groupId))

        // 状态是 稳定 或 完成再平衡（协调者已经知道有多少消费者了），直接返回 同步组响应 给所有消费者即可
        case Stable | CompletingRebalance =>
          for (member <- group.allMemberMetadata) {
            group.maybeInvokeSyncCallback(member, SyncGroupResult(Errors.NOT_COORDINATOR))
            heartbeatPurgatory.checkAndComplete(MemberKey(member.groupId, member.memberId))
          }
      }
    }
  }

  // 加载消费组元数据，这是代理节点发生故障时，控制器重新选举内部主题分区的主副本，并发送 LAI 请求，新的主副本会加载原来
  // 主副本的消费组元数据信息
  private def onGroupLoaded(group: GroupMetadata): Unit = {
    group.inLock {
      info(s"Loading group metadata for ${group.groupId} with generation ${group.generationId}")
      // 确保消费组状态已经是稳定状态
      assert(group.is(Stable) || group.is(Empty))
      // 这里为什么要 rebalance
      if (groupIsOverCapacity(group)) {
        prepareRebalance(group, s"Freshly-loaded group is over capacity ($groupConfig.groupMaxSize). Rebalacing in order to give a chance for consumers to commit offsets")
      }

      //
      group.allMemberMetadata.foreach(completeAndScheduleNextHeartbeatExpiration(group, _))
    }
  }

  /**
   * Load cached state from the given partition and begin handling requests for groups which map to it.
   *
   * @param offsetTopicPartitionId The partition we are now leading
   */
  def onElection(offsetTopicPartitionId: Int): Unit = {
    groupManager.scheduleLoadGroupAndOffsets(offsetTopicPartitionId, onGroupLoaded)
  }

  /**
   * Unload cached state for the given partition and stop handling requests for groups which map to it.
   *
   * @param offsetTopicPartitionId The partition we are no longer leading
   */
  def onResignation(offsetTopicPartitionId: Int): Unit = {
    groupManager.removeGroupsForPartition(offsetTopicPartitionId, onGroupUnloaded)
  }

  private def setAndPropagateAssignment(group: GroupMetadata, assignment: Map[String, Array[Byte]]): Unit = {
    assert(group.is(CompletingRebalance))
    // 消费者设置拉取数据的分区
    group.allMemberMetadata.foreach(member => member.assignment = assignment(member.memberId))
    // 返回 SYNC GROUP 响应
    propagateAssignment(group, Errors.NONE)
  }

  private def resetAndPropagateAssignmentError(group: GroupMetadata, error: Errors): Unit = {
    assert(group.is(CompletingRebalance))
    group.allMemberMetadata.foreach(_.assignment = Array.empty)
    propagateAssignment(group, error)
  }

  private def propagateAssignment(group: GroupMetadata, error: Errors): Unit = {
    val (protocolType, protocolName) = if (error == Errors.NONE)
      (group.protocolType, group.protocolName)
    else
      (None, None)
    for (member <- group.allMemberMetadata) {
      if (member.assignment.isEmpty && error == Errors.NONE) {
        warn(s"Sending empty assignment to member ${member.memberId} of ${group.groupId} for generation ${group.generationId} with no errors")
      }

      // return false, follower 消费者还没有发送 SYNC GROUP 请求来服务端....
      // 对于发送 SYNC GROUP 请求迟了的 follower 消费者，会在上层发现消费组的状态已经变为 Stable，会直接调用回调函数并重置心跳
      if (group.maybeInvokeSyncCallback(member, SyncGroupResult(protocolType, protocolName, member.assignment, error))) {
        // reset the session timeout for members after propagating the member's assignment.
        // This is because if any member's session expired while we were still awaiting either
        // the leader sync group or the storage callback, its expiration will be ignored and no
        // future heartbeat expectations will not be scheduled.
        // 重置下心跳
        completeAndScheduleNextHeartbeatExpiration(group, member)
      }
    }
  }

  /**
   * Complete existing DelayedHeartbeats for the given member and schedule the next one
   * 这里心跳的超时时间是消费者级别的，而不是消费组级别。
   *
   * 考虑下面这个场景，如果消费组完成了所有消费者的加入请求并执行了所有消费者的加入回调（这个过程同样会完成所有消费者的延迟心跳并调度一下心跳），
   * 此时消费组状态为 CompletingRebalance，消费组在等待所有消费者发送 "同步消费组" 的请求，如果在等待过程中，某个消费者掉线无法发送请求，
   * 这个消费者的延迟心跳会超时，在执行超时方法里，在将这个消费者从消费组中剔除前，会判断当前消费者是否在加入/同步阶段，依据是加入/同步回调函
   * 数是否有不为空，但是由于消费组已经返回加入响应（加入回调函数为空），当前消费者还没有发送 "同步消费组" 的请求（同步回调函数为空），所以
   * 消费组会认为这个消费者已经掉线，会从消费组中移除。
   *
   * 再考虑另一个场景，如果消费组状态也是 CompletingRebalance，当前消费者也已经发送了 "同步消费组" 的请求（同步回调函数不为空），但是由于
   * 等待同步阶段，心跳是消费者级别，如果当前消费者的超时时间比较短（3s），而主消费者和其他消费者的超时时间较长（10s），那么在该消费者心跳
   * 超时之前主消费者还没有发送 "同步消费组" 的请求，那么在心跳超时的操作中，在移除当前消费者之前看到加入回调不为空，消费者知道消费者还存活，所以
   * 不会将它从消费组中移除。
   */
  private def completeAndScheduleNextHeartbeatExpiration(group: GroupMetadata, member: MemberMetadata): Unit = {
    completeAndScheduleNextExpiration(group, member, member.sessionTimeoutMs)
  }

  /**
   * 完成本次心跳，并调度下一次心跳，如果能完成，从延迟缓存中将其移除，并创建下一次的心跳保存到延迟缓存中。
   * 判断延迟心跳是否能够完成（tryComplete()）的条件：
   *  1. 如果是新的消费者（第一次加入消费组，并且 "加入消费组" 的响应还没有返回给消费者，isNew = true），
   *  根据 [[MemberMetadata.heartbeatSatisfied]]的值判断是否可以完成。
   *  2. 消费者元数据里的加入回调或同步回调不为空。
   *  3. 消费者是否离开了消费组（这里为什么要执行心跳）
   *  3. 其他情况看 [[MemberMetadata.heartbeatSatisfied]] 的值
   *
   * 对于一个消费者而言，任何时候，延迟缓存中只会有一个延迟心跳与之对应。
   */
  private def completeAndScheduleNextExpiration(group: GroupMetadata, member: MemberMetadata, timeoutMs: Long): Unit = {
    val memberKey = MemberKey(member.groupId, member.memberId)

    // complete current heartbeat expectation
    member.heartbeatSatisfied = true
    // 检查延迟缓存里是否有延迟心跳，如果有并尝试完成（无特殊情况下都可以完成），这个方法执行有两种结果
    // 1. 延迟缓存中有延迟心跳，可以完成，并将旧的延迟心跳从缓存中移除
    // 2. 延迟缓存中无延迟心跳，直接返回
    heartbeatPurgatory.checkAndComplete(memberKey)

    // reschedule the next heartbeat expiration deadline
    member.heartbeatSatisfied = false
    // 封装一个延迟任务，判断心跳是否超时间
    val delayedHeartbeat = new DelayedHeartbeat(this, group, member.memberId, isPending = false, timeoutMs)
    // 时间轮机制
    // 尝试完成延迟心跳，一定不能完成，因为这个延迟心跳是刚刚创建的，会将这个延迟心跳放到缓存中
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(memberKey))
  }

  /**
    * Add pending member expiration to heartbeat purgatory
    */
  private def addPendingMemberExpiration(group: GroupMetadata, pendingMemberId: String, timeoutMs: Long): Unit = {
    val pendingMemberKey = MemberKey(group.groupId, pendingMemberId)
    val delayedHeartbeat = new DelayedHeartbeat(this, group, pendingMemberId, isPending = true, timeoutMs)
    heartbeatPurgatory.tryCompleteElseWatch(delayedHeartbeat, Seq(pendingMemberKey))
  }

  private def removeHeartbeatForLeavingMember(group: GroupMetadata, member: MemberMetadata): Unit = {
    member.isLeaving = true
    val memberKey = MemberKey(member.groupId, member.memberId)
    heartbeatPurgatory.checkAndComplete(memberKey)
  }

  private def addMemberAndRebalance(rebalanceTimeoutMs: Int,
                                    sessionTimeoutMs: Int,
                                    memberId: String,
                                    groupInstanceId: Option[String],
                                    clientId: String,
                                    clientHost: String,
                                    protocolType: String,
                                    protocols: List[(String, Array[Byte])],
                                    group: GroupMetadata,
                                    callback: JoinCallback): Unit = {
    // 创建 member 元数据信息，包含这个消费者的客户端信息、组信息等
    val member = new MemberMetadata(memberId, group.groupId, groupInstanceId,
      clientId, clientHost, rebalanceTimeoutMs,
      sessionTimeoutMs, protocolType, protocols)

    // 标识是个新的消费者
    member.isNew = true

    // update the newMemberAdded flag to indicate that the join group can be further delayed
    // 这个条件成立表示在主消费者延迟操作等待时间有新的消费者加入消费组，主消费者的 Join Group 的延迟操作可以进一步延迟等待更多的消费者加入
    // Note: 此时 leader 消费者的响应还没有返回给消费者客户端，因为 leader 消费者的 Join Group 请求返回时
    // 会更新消费组的 generationId，同时将消费组的状态更新为 CompletingRebalance（无异常场景）
    if (group.is(PreparingRebalance) && group.generationId == 0) {
      // 这里修改的状态主要是影响 InitalDelayJoin 的 onComplete 方法里的操作
      group.newMemberAdded = true
    }

    // 添加逻辑
    group.add(member, callback)

    // The session timeout does not affect new members since they do not have their memberId and
    // cannot send heartbeats. Furthermore, we cannot detect disconnects because sockets are muted
    // while the JoinGroup is in purgatory. If the client does disconnect (e.g. because of a request
    // timeout during a long rebalance), they may simply retry which will lead to a lot of defunct
    // members in the rebalance. To prevent this going on indefinitely, we timeout JoinGroup requests
    // for new members. If the new member is still there, we expect it to retry.
    // 延迟心跳，用户检测会话超时，在 re-balance 时会停止心跳
    // 如果客户端确实断开连接（例如，由于长时间重新平衡期间的请求超时），他们可能会简单地重试，这将导致重新平衡中有很多已失效的成员。
    // 为防止这种情况无限期地发生，会暂停对新成员的 JoinGroup 请求。 如果新成员仍然存在，则让它重试。
    // 正常逻辑加入逻辑这里是消费者第一次创建延迟心跳，但是因为缓存中还没有这个消费者的延迟心跳来让它执行，所以第一次只是创建一个延迟心跳并保存
    // 到延迟缓存中。
    // ------------------------------------- 下面为消费者非首次加入消费组的场景 -------------------------------------
    // 在消费组再平衡的场景下，如果消费者再次加入消费组，此时的延迟缓存里延迟心跳就不为空，这是就可以完成并调度下一次心跳。
    // 考虑一种异常的情况，如果在消费者再平衡时，如果某个消费者长时间没有发送 "加入消费组" 的请求，这时延迟缓存中该消费者的心跳就可能超时，
    // 在超时方法里会判断是否还在加入/同步消费组的阶段，依据是消费者元数据里的加入/同步回调函数是否为空，由于是非首次加入消费组，并且该消费
    // 者还没有完成发送 "加入消费组" 的请求，所以这时发生超时，会将这个消费者从消费组里移除。
    completeAndScheduleNextExpiration(group, member, NewMemberJoinTimeoutMs)  // 300s

    if (member.isStaticMember) {
      info(s"Adding new static member $groupInstanceId to group ${group.groupId} with member id $memberId.")
      group.addStaticMember(groupInstanceId, memberId)
    } else {
      group.removePendingMember(memberId)
    }
    // 准备开始 rebalance，这个操作只有主消费者可以执行，因为主消费者会将消费组的状态从 Empty -> PreparingRebalance，后续的消费者会
    // 因为这个状态变更而无法完成初始化延迟操作的问题。
    // 非主消费者的 JOIN GROUP 是如何返回的？
    // 每个消费者在加入消费组都会构造一个 MemberMetadata 对象，然后将其保存到消费组的 members 集合中，在主消费者超时后强制完成调用
    // DelayedJoin 的 onComplete 里，会遍历 members 里所有的消费者，并返回对应消费者客户端的响应。如果是主消费者，还会将 leaderId
    // 和消费组里所有的消费者返回给客户端，主消费者客户端在收到响应后，会执行分区分配，然后将分配分配的结果发送给服务端，以完成后续的消费组
    // re-balance 的操作。
    maybePrepareRebalance(group, s"Adding new member $memberId with group instance id $groupInstanceId")
  }

  private def updateStaticMemberAndRebalance(group: GroupMetadata,
                                             newMemberId: String,
                                             groupInstanceId: Option[String],
                                             protocols: List[(String, Array[Byte])],
                                             responseCallback: JoinCallback): Unit = {
    val oldMemberId = group.getStaticMemberId(groupInstanceId)
    info(s"Static member $groupInstanceId of group ${group.groupId} with unknown member id rejoins, assigning new member id $newMemberId, while " +
      s"old member id $oldMemberId will be removed.")

    val currentLeader = group.leaderOrNull
    val member = group.replaceGroupInstance(oldMemberId, newMemberId, groupInstanceId)
    // Heartbeat of old member id will expire without effect since the group no longer contains that member id.
    // New heartbeat shall be scheduled with new member id.
    completeAndScheduleNextHeartbeatExpiration(group, member)

    val knownStaticMember = group.get(newMemberId)
    group.updateMember(knownStaticMember, protocols, responseCallback)
    val oldProtocols = knownStaticMember.supportedProtocols

    group.currentState match {
      case Stable =>
        // check if group's selectedProtocol of next generation will change, if not, simply store group to persist the
        // updated static member, if yes, rebalance should be triggered to let the group's assignment and selectProtocol consistent
        val selectedProtocolOfNextGeneration = group.selectProtocol
        if (group.protocolName.contains(selectedProtocolOfNextGeneration)) {
          info(s"Static member which joins during Stable stage and doesn't affect selectProtocol will not trigger rebalance.")
          val groupAssignment: Map[String, Array[Byte]] = group.allMemberMetadata.map(member => member.memberId -> member.assignment).toMap
          groupManager.storeGroup(group, groupAssignment, error => {
            if (error != Errors.NONE) {
              warn(s"Failed to persist metadata for group ${group.groupId}: ${error.message}")

              // Failed to persist member.id of the given static member, revert the update of the static member in the group.
              group.updateMember(knownStaticMember, oldProtocols, null)
              val oldMember = group.replaceGroupInstance(newMemberId, oldMemberId, groupInstanceId)
              completeAndScheduleNextHeartbeatExpiration(group, oldMember)
              responseCallback(JoinGroupResult(
                List.empty,
                memberId = JoinGroupRequest.UNKNOWN_MEMBER_ID,
                generationId = group.generationId,
                protocolType = group.protocolType,
                protocolName = group.protocolName,
                leaderId = currentLeader,
                error = error
              ))
            } else {
              group.maybeInvokeJoinCallback(member, JoinGroupResult(
                members = List.empty,
                memberId = newMemberId,
                generationId = group.generationId,
                protocolType = group.protocolType,
                protocolName = group.protocolName,
                // We want to avoid current leader performing trivial assignment while the group
                // is in stable stage, because the new assignment in leader's next sync call
                // won't be broadcast by a stable group. This could be guaranteed by
                // always returning the old leader id so that the current leader won't assume itself
                // as a leader based on the returned message, since the new member.id won't match
                // returned leader id, therefore no assignment will be performed.
                leaderId = currentLeader,
                error = Errors.NONE))
            }
          })
        } else {
          maybePrepareRebalance(group, s"Group's selectedProtocol will change because static member ${member.memberId} with instance id $groupInstanceId joined with change of protocol")
        }
      case CompletingRebalance =>
        // if the group is in after-sync stage, upon getting a new join-group of a known static member
        // we should still trigger a new rebalance, since the old member may already be sent to the leader
        // for assignment, and hence when the assignment gets back there would be a mismatch of the old member id
        // with the new replaced member id. As a result the new member id would not get any assignment.
        prepareRebalance(group, s"Updating metadata for static member ${member.memberId} with instance id $groupInstanceId")
      case Empty | Dead =>
        throw new IllegalStateException(s"Group ${group.groupId} was not supposed to be " +
          s"in the state ${group.currentState} when the unknown static member $groupInstanceId rejoins.")
      case PreparingRebalance =>
    }
  }

  private def updateMemberAndRebalance(group: GroupMetadata,
                                       member: MemberMetadata,
                                       protocols: List[(String, Array[Byte])],
                                       reason: String,
                                       callback: JoinCallback): Unit = {
    group.updateMember(member, protocols, callback)
    maybePrepareRebalance(group, reason)
  }

  private def maybePrepareRebalance(group: GroupMetadata, reason: String): Unit = {
    group.inLock {
      // state initial is Empty
      // 在初始化阶段，只有主消费者可以满足，因为消费组的状态为 Empty
      // 因为加入消费组的方法加了同步锁，所以不存在多个消费者并发的问题
      if (group.canRebalance)
        prepareRebalance(group, reason)
    }
  }

  // package private for testing
  private[group] def prepareRebalance(group: GroupMetadata, reason: String): Unit = {
    // if any members are awaiting sync, cancel their request and have them rejoin
    /**
     * 正常流程加入消费组这个条件不会满足，可以先 pass。
     *
     * 满足这个的条件：
     *  1）正常流程所有消费者已经都加入消费组，并且协调者也返回了响应，消费组状态变更为 CompletingRebalance，在消费者发送
     *  "同步消费组" 请求过程中，一个新的消费者此时加入了消费组。
     *  2）消费组的状态为 Stable，但是此时新的消费者加入，消费者开始再平衡操作，在再平衡过程中，到消费组状态为 CompletingRebalance 时，
     * 又出现了情况 1）的场景， 与场景1）不同的是，此时消费组里的消费者都已经分配到了分区。
     *
     * 如果状态是 CompletingRebalance（等待同步，表示消费组协调者在等待消费者发送同步请求），即所有的加入请求的响应已经返回，
     * 主消费在为这些消费者分配分区，此时执行这个方法，可能表示有消费者加入（需要给这个消费者分配分区），这里给消费组里的所有消费
     * 者发送 REBALANCE_IN_PROGRESS 的错误，同时清空所有消费者分配的结果，让所有消费者重新加入消费组。
     *
     * Note:
     * 1) 如果消费组状态是 CompletingRebalance，如果有消费者已经发送了 "同步消费组" 请求，元数据里的同步回调不为空，
     * 这个方法会执行回调返回 REBALANCE_IN_PROGRESS 的错误，然后回调函数被置为空。 如果还没有发送请求，回调函数也是为空，
     * 自然也不会执行回调。
     *
     * 2) 如果后续上面那些没有发送 "同步消费组" 请求的消费者发送了请求，在处理请求时，但是发现消费组状态为 PreparingRebalance，
     * 同样会返回 REBALANCE_IN_PROGRESS 错误，表示消费组在执行再平衡操作，这些消费者收到这个错误后，会重新发送 "加入消费组" 请求.
     *
     * 3) 如果消费组状态是 CompletingRebalance，下面创建的延迟操作是 [[DelayedJoin]]，与消费组首次初始化创建的延迟操作
     * [[InitialDelayedJoin]] 不同，[[DelayedJoin]] 判断是否可以完成的条件是消费组里消费者的个数和
     * [[GroupMetadata.numMembersAwaitingJoin]] 的值是否相同。其含义是已经发送 "加入消费组" 请求但是协调者还没有返回
     * 响应的消费者的数量，这个值在消费者加入消费组是自增1，在协调者返回 "加入消费组" 响应时减1，。由于此时消费组的状态已经是
     * CompletingRebalance，表示之前所有的消费者的 "加入消费组" 的响应都已经返回了，所以此时 numMembersAwaitingJoin = 1（1是
     * 这个新加入的消费者），所以条件显然不成立，延迟操作无法完成。
     *
     * 4）[[resetAndPropagateAssignmentError]] 这个方法会清空所有消费者分配的分区，并执行所有已经发送"同步消费组" 请求的消费者的回调：
     *  （1）新消费者加入时，消费组中原来的消费者有部分消费者已经发送 "同步消费组" 请求，这些消费者的元数据回调不为空，所以在下面方法执行
     * 时会返回 "同步消费组" 响应，不过返回的是错误的响应，消费者收到会重新加入消费组。
     *  （2）新消费者加入消费组之前消费组中原来的消费者没有一个发送 "同步消费组" 请求，此时下面的方法执行时不会执行同步回调，后续过程中原先的
     * 消费者陆续发送 "同步消费组" 请求，由于状态已经是 PreparingRebalance， 协调者返回 REBALANCE_IN_PROGRESS 错误，让消费者重新加入消费组。
     *  （3）新消费者加入消费组之前消费组中原来的消费者都发送了 "同步消费组" 请求，这个不可能，因为存在同步锁，如果原先所有消费者都发送
     * 了 "同步消费组" 请求，新的消费者因为锁互斥无法进入这里的处理逻辑。
     * 随着原先消费者发送 "加入消费组" 请求，[[GroupMetadata.numMembersAwaitingJoin]] 不断自增，直到最后一个消费者加入，此时消费组
     * 中消费者数量与该值相同时，这个延迟操作就可以完成了，所以协调者在处理 "加入消费组" 请求时，每次在处理后都会尝试完成一次延迟操作，因为
     * 并不是所有消费者加入都会创建延迟操作，就不会调用 tryCompletedElseWatch()，而加入消费组这个外部事件可以触发延迟操作完成，不然只能
     * 被动等到超时后强制执行。
     *
     * 5）协调者不管是处理 "加入消费组" 请求还是 "同步消费组" 请求，都会加同步锁，所以同一时间不存在同时处理多个请求的情况。
     */
    if (group.is(CompletingRebalance))
      resetAndPropagateAssignmentError(group, Errors.REBALANCE_IN_PROGRESS)

    /**
     * 消费组的初始状态是 Empty.
     * 这里需要注意的是，首个消费者加入消费组后，并没有直接执行 JOIN GROUP 的回调返回给消费者客户端，而是在加入消费组后将回调函数保存到
     * [[MemberMetadata.awaitingJoinCallback]] 属性中。这是因为服务端需要等待尽可能多的其他消费者加入消费组，来进行后面的 re-balance 操作，
     * 所以这里需要等待一会，主消费者的 JOIN GROUP 请求的返回的响应会把这段时间所有加入的消费组的消费者返回给消费者客户端，主消费者收到响应后
     * 会进行分区分配，然后再将分区分配的结果同步给服务端。
     * 所以，在第一个消费者（主消费者）加入消费组时，这里会创建一个 [[InitialDelayedJoin]] 延迟操作，这个延迟操作的 tryComplete 方法返回 false。
     * 默认超时时间是 3s，当达到超时时间后，延迟操作会被强制完成执行，并执行其 onComplete 方法。
     * 具体的执行流程：
     * 这个延迟操作会被放到时间轮（timeWheel）中的延迟队列中，在 DelayedOperationPurgatory 中会有一个后台线程 ExpiredOperationReaper，
     * 会不断从 timeWheel 的队列中轮询要超时的延迟操作，如果没有则等待一段时间，如果有超时的操作，会从队列中取出，然后执行 reinsert 事件，这
     * 本质上还是调用 insert 的逻辑，在 insert 逻辑中如果发现这个操作已经过期，会拒绝入队，并会有一个线程池（taskExecutor）来立刻强制执行
     * 这个延迟操作，即 InitialDelayedJoin 中的 run 方法。调用链 InitialDelayedJoin#run() -> forceComplete() -> onComplete()
     *
     * 在 InitialDelayedJoin 中的 onComplete() 的方法中， 有一个判断条件，默认是 group.newMemberAdded = false，但是如果在主消费者等待的
     * 时间内有其他消费者也加入了消费组，这个属性会被重置为 true（[[GroupCoordinator.addMemberAndRebalance()]]），这表示主消费者可以再等一
     * 会让更多的消费者加入消费组，所以这该方法里会重新初始化一个新的 InitialDelayedJoin 开始新的一轮等待，同理如果有新的消费者加入同样也会执行相同
     * 的操作，直到等待到了最大的超时时间（默认是 300s）。否则（在新的一轮等待中无新的消费者加入消费组）该方法实际是调用父类的方法。
     * 在父类方法中，调用协调器的 onCompleteJoin() 方法，在该方法中将 leader 消费者的响应返回给客户端。
     *
     * 如果当前消费组状态不是 Empty，表示发生了再平衡的场景（订阅的主题发生变化、心跳超时、分区数量变化，消费组中消费者数量变化），此时创建的延迟
     * 操作实现是 DelayedJoin，它的超时时间是 5 分钟，如果 5 分钟之内所有的消费者完成了加入消费组，协调者处理每个加入请求后都会尝试完成一下
     * 这个延迟操作，因为并不是非要等到超时时间才强制执行，延迟操作另一个完成方式是由它关联的外部事件触发完成，这也可以解释为什么每次加入消费组后都会
     * 尝试完成延迟操作。
     * [[DelayedOperationPurgatory.expirationReaper]] 后台线程轮询超时任务线程
     * [[DelayedOperation.onComplete()]]
     * [[GroupCoordinator.onCompleteJoin()]]
     */
    val delayedRebalance = if (group.is(Empty))
      new InitialDelayedJoin(this,
        joinPurgatory,
        group,
        groupConfig.groupInitialRebalanceDelayMs,   // 3s
        groupConfig.groupInitialRebalanceDelayMs,   // 3s
        max(group.rebalanceTimeoutMs - groupConfig.groupInitialRebalanceDelayMs, 0))
    else
      new DelayedJoin(this, group, group.rebalanceTimeoutMs)  // 300s

    // 设置 state = PreparingRebalance，准备开始 rebalance，即等待所有的消费者发送 JOIN GROUP 请求
    group.transitionTo(PreparingRebalance)

    info(s"Preparing to rebalance group ${group.groupId} in state ${group.currentState} with old generation " +
      s"${group.generationId} (${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)}) (reason: $reason)")

    val groupKey = GroupKey(group.groupId)
    // 首次是不会完成的，所以会被放到一个 watch 队列中
    joinPurgatory.tryCompleteElseWatch(delayedRebalance, Seq(groupKey))
  }

  private def removeMemberAndUpdateGroup(group: GroupMetadata, member: MemberMetadata, reason: String): Unit = {
    // New members may timeout with a pending JoinGroup while the group is still rebalancing, so we have
    // to invoke the callback before removing the member. We return UNKNOWN_MEMBER_ID so that the consumer
    // will retry the JoinGroup request if is still active.
    group.maybeInvokeJoinCallback(member, JoinGroupResult(JoinGroupRequest.UNKNOWN_MEMBER_ID, Errors.UNKNOWN_MEMBER_ID))

    group.remove(member.memberId)
    group.removeStaticMember(member.groupInstanceId)

    group.currentState match {
      case Dead | Empty =>
      case Stable | CompletingRebalance => maybePrepareRebalance(group, reason) // re-balance
      case PreparingRebalance => joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  private def removePendingMemberAndUpdateGroup(group: GroupMetadata, memberId: String): Unit = {
    group.removePendingMember(memberId)

    if (group.is(PreparingRebalance)) {
      joinPurgatory.checkAndComplete(GroupKey(group.groupId))
    }
  }

  def tryCompleteJoin(group: GroupMetadata, forceComplete: () => Boolean) = {
    group.inLock {
      if (group.hasAllMembersJoined)
        forceComplete()
      else false
    }
  }

  def onExpireJoin(): Unit = {
    // TODO: add metrics for restabilize timeouts
  }

  /**
   * 延迟操作完成分两种情况：
   *  1. 正常完成，由外部事件触发，此时消费组里的所有消费者都已经发送了 "加入消费组" 请求。
   *  2. 超时完成，延迟操作等待最大超时时间后，还不能完成，可能是有些消费由于网络等原因迟迟没有发送 "加入消费组" 请求，
   * 此时延迟操作被强制完成，在强制完成过程中，会从消费组里移除那些还没有发送 "加入消费组" 请求的消费者，并停止它们的心跳。
   *
   * 判断没有发送 "加入消费组" 请求的条件，消费者元数据中等待加入的回调函数为空，消费者加入消费组时，会将回调函数保存到
   * [[kafka.coordinator.group.MemberMetadata.awaitingJoinCallback]] 消费者元数据的这个字段中。
   *
   * 这个方法可能由消费组首次从 Empty 状态执行再平衡操作，也可能是在非首次执行再平衡操作，上述表述的是第二种场景。首次执行再平衡
   * 延迟操作是由超时触发的。
   */
  def onCompleteJoin(group: GroupMetadata): Unit = {
    group.inLock {
      val notYetRejoinedDynamicMembers = group.notYetRejoinedMembers.filterNot(_._2.isStaticMember)
      if (notYetRejoinedDynamicMembers.nonEmpty) {
        info(s"Group ${group.groupId} remove dynamic members " +
          s"who haven't joined: ${notYetRejoinedDynamicMembers.keySet}")

        // 从消费组中移除还没有发送 "加入消费组" 的请求
        // 停止心跳检测
        notYetRejoinedDynamicMembers.values foreach { failedMember =>
          removeHeartbeatForLeavingMember(group, failedMember)
          group.remove(failedMember.memberId)
          // TODO: cut the socket connection to the client
        }
      }

      if (group.is(Dead)) {
        info(s"Group ${group.groupId} is dead, skipping rebalance stage")
      } else if (!group.maybeElectNewJoinedLeader() && group.allMembers.nonEmpty) {
        // If all members are not rejoining, we will postpone the completion
        // of rebalance preparing stage, and send out another delayed operation
        // until session timeout removes all the non-responsive members.
        error(s"Group ${group.groupId} could not complete rebalance because no members rejoined")
        joinPurgatory.tryCompleteElseWatch(
          new DelayedJoin(this, group, group.rebalanceTimeoutMs),
          Seq(GroupKey(group.groupId)))
      } else {
        // 生成 generationId、设置消费组状态为 CompletingRebalance，等待其他消费者加入或重新加入消费组执行 re-balance
        group.initNextGeneration()
        if (group.is(Empty)) {
          info(s"Group ${group.groupId} with generation ${group.generationId} is now empty " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          groupManager.storeGroup(group, Map.empty, error => {
            if (error != Errors.NONE) {
              // we failed to write the empty group metadata. If the broker fails before another rebalance,
              // the previous generation written to the log will become active again (and most likely timeout).
              // This should be safe since there are no active members in an empty generation, so we just warn.
              warn(s"Failed to write empty metadata for group ${group.groupId}: ${error.message}")
            }
          })
        } else {
          info(s"Stabilized group ${group.groupId} generation ${group.generationId} " +
            s"(${Topic.GROUP_METADATA_TOPIC_NAME}-${partitionFor(group.groupId)})")

          // trigger the awaiting join group response callback for all the members after rebalancing
          // 执行回调函数返回 JOIN GROUP 的结果返回给消费者客户端
          // 这里消费组发送加入组响应是一次性发送给组里所有的消费者
          for (member <- group.allMemberMetadata) {
            val joinResult = JoinGroupResult(
              members = if (group.isLeader(member.memberId)) {
                group.currentMemberMetadata
              } else {
                List.empty
              },
              memberId = member.memberId,
              generationId = group.generationId,
              protocolType = group.protocolType,
              protocolName = group.protocolName,
              leaderId = group.leaderOrNull,
              error = Errors.NONE)

            // 执行回调
            group.maybeInvokeJoinCallback(member, joinResult)
            // 执行延迟心跳，实际上是完成延迟缓存中上次创建的心跳，并创建下次调度的心跳任务存到延迟缓存中
            completeAndScheduleNextHeartbeatExpiration(group, member)
            // 执行完心跳后，将 isNew 设置为 false，后续判断心跳是否可以完成就不会进入 isNew 的分支了
            member.isNew = false
          }
        }
      }
    }
  }

  def tryCompleteHeartbeat(group: GroupMetadata,
                           memberId: String,
                           isPending: Boolean,
                           forceComplete: () => Boolean): Boolean = {
    group.inLock {
      // The group has been unloaded and invalid, we should complete the heartbeat.
      if (group.is(Dead)) {
        forceComplete()
      } else if (isPending) {
        // complete the heartbeat if the member has joined the group
        if (group.has(memberId)) {
          forceComplete()
        } else false
      } else if (shouldCompleteNonPendingHeartbeat(group, memberId)) {
        forceComplete()
      } else false
    }
  }

  def shouldCompleteNonPendingHeartbeat(group: GroupMetadata, memberId: String): Boolean = {
    if (group.has(memberId)) {
      val member = group.get(memberId)
      // 判断延迟心跳标识
      member.hasSatisfiedHeartbeat || member.isLeaving
    } else {
      // memberId 不再消费组里，立刻执行完成方法
      info(s"Member id $memberId was not found in ${group.groupId} during heartbeat completion check")
      true
    }
  }

  // 心跳超时逻辑
  def onExpireHeartbeat(group: GroupMetadata, memberId: String, isPending: Boolean): Unit = {
    group.inLock {
      if (group.is(Dead)) {
        info(s"Received notification of heartbeat expiration for member $memberId after group ${group.groupId} had already been unloaded or deleted.")
      } else if (isPending) {
        info(s"Pending member $memberId in group ${group.groupId} has been removed after session timeout expiration.")
        removePendingMemberAndUpdateGroup(group, memberId)
      } else if (!group.has(memberId)) {
        debug(s"Member $memberId has already been removed from the group.")
      } else {
        val member = group.get(memberId)
        // 这个条件表示，如果消费者还存活，虽然发生了超时，协调者也不会把它从消费组中移除。
        // 1. awaitingJoinCallback != null，表示消费这发送了 "加入消费组" 的请求，协调者正在处理这个 "加入消费组" 的请求
        // 2. awaitingSyncCallback != null，消费这发送了 "同步消费组" 的请求，协调者正在处理这个 "同步消费组" 的请求
        if (!member.hasSatisfiedHeartbeat) {
          info(s"Member ${member.memberId} in group ${group.groupId} has failed, removing it from the group")
          removeMemberAndUpdateGroup(group, member, s"removing member ${member.memberId} on heartbeat expiration")
        }
      }
    }
  }

  def onCompleteHeartbeat(): Unit = {
    // TODO: add metrics for complete heartbeats
  }

  def partitionFor(group: String): Int = groupManager.partitionFor(group)

  private def groupIsOverCapacity(group: GroupMetadata): Boolean = {
    group.size > groupConfig.groupMaxSize
  }

  private def isCoordinatorForGroup(groupId: String) = groupManager.isGroupLocal(groupId)

  private def isCoordinatorLoadInProgress(groupId: String) = groupManager.isGroupLoading(groupId)
}

object GroupCoordinator {

  val NoState = ""
  val NoProtocolType = ""
  val NoProtocol = ""
  val NoLeader = ""
  val NoGeneration = -1
  val NoMembers = List[MemberSummary]()
  val EmptyGroup = GroupSummary(NoState, NoProtocolType, NoProtocol, NoMembers)
  val DeadGroup = GroupSummary(Dead.toString, NoProtocolType, NoProtocol, NoMembers)
  val NewMemberJoinTimeoutMs: Int = 5 * 60 * 1000

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            time: Time,
            metrics: Metrics): GroupCoordinator = {
    val heartbeatPurgatory = DelayedOperationPurgatory[DelayedHeartbeat]("Heartbeat", config.brokerId)
    val joinPurgatory = DelayedOperationPurgatory[DelayedJoin]("Rebalance", config.brokerId)
    apply(config, zkClient, replicaManager, heartbeatPurgatory, joinPurgatory, time, metrics)
  }

  private[group] def offsetConfig(config: KafkaConfig) = OffsetConfig(
    maxMetadataSize = config.offsetMetadataMaxSize,
    loadBufferSize = config.offsetsLoadBufferSize,
    offsetsRetentionMs = config.offsetsRetentionMinutes * 60L * 1000L,
    offsetsRetentionCheckIntervalMs = config.offsetsRetentionCheckIntervalMs,
    offsetsTopicNumPartitions = config.offsetsTopicPartitions,
    offsetsTopicSegmentBytes = config.offsetsTopicSegmentBytes,
    offsetsTopicReplicationFactor = config.offsetsTopicReplicationFactor,
    offsetsTopicCompressionCodec = config.offsetsTopicCompressionCodec,
    offsetCommitTimeoutMs = config.offsetCommitTimeoutMs,
    offsetCommitRequiredAcks = config.offsetCommitRequiredAcks
  )

  def apply(config: KafkaConfig,
            zkClient: KafkaZkClient,
            replicaManager: ReplicaManager,
            heartbeatPurgatory: DelayedOperationPurgatory[DelayedHeartbeat],
            joinPurgatory: DelayedOperationPurgatory[DelayedJoin],
            time: Time,
            metrics: Metrics): GroupCoordinator = {
    val offsetConfig = this.offsetConfig(config)
    val groupConfig = GroupConfig(groupMinSessionTimeoutMs = config.groupMinSessionTimeoutMs,
      groupMaxSessionTimeoutMs = config.groupMaxSessionTimeoutMs,
      groupMaxSize = config.groupMaxSize,
      groupInitialRebalanceDelayMs = config.groupInitialRebalanceDelay)

    val groupMetadataManager = new GroupMetadataManager(config.brokerId, config.interBrokerProtocolVersion,
      offsetConfig, replicaManager, zkClient, time, metrics)
    new GroupCoordinator(config.brokerId, groupConfig, offsetConfig, groupMetadataManager, heartbeatPurgatory, joinPurgatory, time, metrics)
  }

  private def memberLeaveError(memberIdentity: MemberIdentity,
                               error: Errors): LeaveMemberResponse = {
    LeaveMemberResponse(
      memberId = memberIdentity.memberId,
      groupInstanceId = Option(memberIdentity.groupInstanceId),
      error = error)
  }

  private def leaveError(topLevelError: Errors,
                         memberResponses: List[LeaveMemberResponse]): LeaveGroupResult = {
    LeaveGroupResult(
      topLevelError = topLevelError,
      memberResponses = memberResponses)
  }
}

case class GroupConfig(groupMinSessionTimeoutMs: Int,
                       groupMaxSessionTimeoutMs: Int,
                       groupMaxSize: Int,
                       groupInitialRebalanceDelayMs: Int)

case class JoinGroupResult(members: List[JoinGroupResponseMember],
                           memberId: String,
                           generationId: Int,
                           protocolType: Option[String],
                           protocolName: Option[String],
                           leaderId: String,
                           error: Errors)

object JoinGroupResult {
  def apply(memberId: String, error: Errors): JoinGroupResult = {
    JoinGroupResult(
      members = List.empty,
      memberId = memberId,
      generationId = GroupCoordinator.NoGeneration,
      protocolType = None,
      protocolName = None,
      leaderId = GroupCoordinator.NoLeader,
      error = error)
  }
}

case class SyncGroupResult(protocolType: Option[String],
                           protocolName: Option[String],
                           memberAssignment: Array[Byte],
                           error: Errors)

object SyncGroupResult {
  def apply(error: Errors): SyncGroupResult = {
    SyncGroupResult(None, None, Array.empty, error)
  }
}

case class LeaveMemberResponse(memberId: String,
                               groupInstanceId: Option[String],
                               error: Errors)

case class LeaveGroupResult(topLevelError: Errors,
                            memberResponses : List[LeaveMemberResponse])
