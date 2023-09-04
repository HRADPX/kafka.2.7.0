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

import java.util
import java.util.concurrent.TimeUnit

import kafka.admin.AdminOperationException
import kafka.api._
import kafka.common._
import kafka.controller.KafkaController.AlterIsrCallback
import kafka.cluster.Broker
import kafka.controller.KafkaController.{AlterReassignmentsCallback, ElectLeadersCallback, ListReassignmentsCallback, UpdateFeaturesCallback}
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.server._
import kafka.utils._
import kafka.utils.Implicits._
import kafka.zk.KafkaZkClient.UpdateLeaderAndIsrResult
import kafka.zk.{FeatureZNodeStatus, _}
import kafka.zookeeper.{StateChangeHandler, ZNodeChangeHandler, ZNodeChildChangeHandler}
import org.apache.kafka.common.ElectionType
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{BrokerNotAvailableException, ControllerMovedException, StaleBrokerEpochException}
import org.apache.kafka.common.message.{AlterIsrRequestData, AlterIsrResponseData}
import org.apache.kafka.common.feature.{Features, FinalizedVersionRange}
import org.apache.kafka.common.message.UpdateFeaturesRequestData
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AbstractControlRequest, ApiError, LeaderAndIsrResponse, UpdateFeaturesRequest, UpdateMetadataResponse}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.zookeeper.KeeperException
import org.apache.zookeeper.KeeperException.Code

import scala.collection.{Map, Seq, Set, immutable, mutable}
import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

sealed trait ElectionTrigger
final case object AutoTriggered extends ElectionTrigger
final case object ZkTriggered extends ElectionTrigger
final case object AdminClientTriggered extends ElectionTrigger

object KafkaController extends Logging {
  val InitialControllerEpoch = 0
  val InitialControllerEpochZkVersion = 0

  type ElectLeadersCallback = Map[TopicPartition, Either[ApiError, Int]] => Unit
  type ListReassignmentsCallback = Either[Map[TopicPartition, ReplicaAssignment], ApiError] => Unit
  type AlterReassignmentsCallback = Either[Map[TopicPartition, ApiError], ApiError] => Unit
  type AlterIsrCallback = Either[Map[TopicPartition, Either[Errors, LeaderAndIsr]], Errors] => Unit
  type UpdateFeaturesCallback = Either[ApiError, Map[String, ApiError]] => Unit
}

/**
 * Kafka 控制器，主要负责
 *  1）分区的分配、分区的选举
 *  2）代理节点的启动或下线时，处理代理的节点的故障转移
 *  3）新建或删除主题，或新增分区时，处理分区的重新分配
 *  4）管理所有分区的状态机和副本的状态机，处理状态机的变化事件
 *
 * 控制器、监听器和 zk 三者完成一次事件处理的步骤：
 *  1）控制器向 zk 节点上注册监听器，每种监听器都有具体的事件处理逻辑。
 *  2）管理员更新 zk 节点的数据，触发监听器调用不同的回调方法。
 *  3）控制器执行具体的事件处理逻辑，处理完成后，再次注册监听器，为下次事件触发做准备。
 *
 * 控制器上下文保存了上一次的数据，而 zk 保存了最新的数据，两者数据会不一致。zk 节点关联的监听器触发事件处理时，监听器要比对 zk 节点
 * 和上下文，找出新增和删除的数据。
 */
class KafkaController(val config: KafkaConfig,
                      zkClient: KafkaZkClient,
                      time: Time,
                      metrics: Metrics,
                      initialBrokerInfo: BrokerInfo,
                      initialBrokerEpoch: Long,
                      tokenManager: DelegationTokenManager,
                      brokerFeatures: BrokerFeatures,
                      featureCache: FinalizedFeatureCache,
                      threadNamePrefix: Option[String] = None)
  extends ControllerEventProcessor with Logging with KafkaMetricsGroup {

  this.logIdent = s"[Controller id=${config.brokerId}] "

  @volatile private var brokerInfo = initialBrokerInfo
  @volatile private var _brokerEpoch = initialBrokerEpoch

  private val isAlterIsrEnabled = config.interBrokerProtocolVersion.isAlterIsrSupported
  private val stateChangeLogger = new StateChangeLogger(config.brokerId, inControllerContext = true, None)
  // 控制器上下文，启动控制器时从 zk 初始化数据
  val controllerContext = new ControllerContext
  // 控制器通道管理器，其保存了代理节点编号到代理节点状态（ControllerBrokerStateInfo）的映射关系。
  // 控制器要发送请求给代理节点，就需要维护到服务端节点的网络连接，它通过代理节点监听器，在代理节点发生
  // 变更时，更新通道管理器里的代理节点状态信息（网络连接对象，请求队列，请求发送线程等）
  var controllerChannelManager = new ControllerChannelManager(controllerContext, config, time, metrics,
    stateChangeLogger, threadNamePrefix)

  // have a separate scheduler for the controller to be able to start and stop independently of the kafka server
  // visible for testing
  private[controller] val kafkaScheduler = new KafkaScheduler(1)

  // visible for testing
  private[controller] val eventManager = new ControllerEventManager(config.brokerId, this, time,
    controllerContext.stats.rateAndTimeMetrics)

  private val brokerRequestBatch = new ControllerBrokerRequestBatch(config, controllerChannelManager,
    eventManager, controllerContext, stateChangeLogger)
  // 副本状态机，管理副本状态
  val replicaStateMachine: ReplicaStateMachine = new ZkReplicaStateMachine(config, stateChangeLogger, controllerContext, zkClient,
    new ControllerBrokerRequestBatch(config, controllerChannelManager, eventManager, controllerContext, stateChangeLogger))
  // 分区状态机，管理分区状态
  val partitionStateMachine: PartitionStateMachine = new ZkPartitionStateMachine(config, stateChangeLogger, controllerContext, zkClient,
    new ControllerBrokerRequestBatch(config, controllerChannelManager, eventManager, controllerContext, stateChangeLogger))
  val topicDeletionManager = new TopicDeletionManager(config, controllerContext, replicaStateMachine,
    partitionStateMachine, new ControllerDeletionClient(this, zkClient))

  // 各种事件处理
  private val controllerChangeHandler = new ControllerChangeHandler(eventManager)
  private val brokerChangeHandler = new BrokerChangeHandler(eventManager)
  private val brokerModificationsHandlers: mutable.Map[Int, BrokerModificationsHandler] = mutable.Map.empty
  private val topicChangeHandler = new TopicChangeHandler(eventManager)
  private val topicDeletionHandler = new TopicDeletionHandler(eventManager)
  private val partitionModificationsHandlers: mutable.Map[String, PartitionModificationsHandler] = mutable.Map.empty
  private val partitionReassignmentHandler = new PartitionReassignmentHandler(eventManager)
  private val preferredReplicaElectionHandler = new PreferredReplicaElectionHandler(eventManager)
  private val isrChangeNotificationHandler = new IsrChangeNotificationHandler(eventManager)
  private val logDirEventNotificationHandler = new LogDirEventNotificationHandler(eventManager)

  @volatile private var activeControllerId = -1
  @volatile private var offlinePartitionCount = 0
  @volatile private var preferredReplicaImbalanceCount = 0
  @volatile private var globalTopicCount = 0
  @volatile private var globalPartitionCount = 0
  @volatile private var topicsToDeleteCount = 0
  @volatile private var replicasToDeleteCount = 0
  @volatile private var ineligibleTopicsToDeleteCount = 0
  @volatile private var ineligibleReplicasToDeleteCount = 0

  /* single-thread scheduler to clean expired tokens */
  private val tokenCleanScheduler = new KafkaScheduler(threads = 1, threadNamePrefix = "delegation-token-cleaner")

  newGauge("ActiveControllerCount", () => if (isActive) 1 else 0)
  newGauge("OfflinePartitionsCount", () => offlinePartitionCount)
  newGauge("PreferredReplicaImbalanceCount", () => preferredReplicaImbalanceCount)
  newGauge("ControllerState", () => state.value)
  newGauge("GlobalTopicCount", () => globalTopicCount)
  newGauge("GlobalPartitionCount", () => globalPartitionCount)
  newGauge("TopicsToDeleteCount", () => topicsToDeleteCount)
  newGauge("ReplicasToDeleteCount", () => replicasToDeleteCount)
  newGauge("TopicsIneligibleToDeleteCount", () => ineligibleTopicsToDeleteCount)
  newGauge("ReplicasIneligibleToDeleteCount", () => ineligibleReplicasToDeleteCount)

  /**
   * Returns true if this broker is the current controller.
   */
  def isActive: Boolean = activeControllerId == config.brokerId

  def brokerEpoch: Long = _brokerEpoch

  def epoch: Int = controllerContext.epoch

  /**
   * Invoked when the controller module of a Kafka server is started up. This does not assume that the current broker
   * is the controller. It merely registers the session expiration listener and starts the controller leader
   * elector
   */
  def startup() = {
    // 对 zk 上面的目录（controller-state-change-handler）注册了监听器
    // 订阅临时节点数据改变事件，选举器处理 /controller 节点被删除，以及控制器处理会话失效，都会调用选举器的 elect()
    // 方法重新选举主控制器。
    zkClient.registerStateChangeHandler(new StateChangeHandler {
      override val name: String = StateChangeHandlers.ControllerHandler
      override def afterInitializingSession(): Unit = {
        // 重新选举事件
        eventManager.put(RegisterBrokerAndReelect)
      }
      override def beforeInitializingSession(): Unit = {
        // 过期事件
        val queuedEvent = eventManager.clearAndPut(Expire)

        // Block initialization of the new session until the expiration event is being handled,
        // which ensures that all pending events have been processed before creating the new session
        queuedEvent.awaitProcessing()
      }
    })
    eventManager.put(Startup)
    // eventManager 里的线程启动
    eventManager.start()
  }

  /**
   * Invoked when the controller module of a Kafka server is shutting down. If the broker was the current controller,
   * it shuts down the partition and replica state machines. If not, those are a no-op. In addition to that, it also
   * shuts down the controller channel manager, if one exists (i.e. if it was the current controller)
   */
  def shutdown(): Unit = {
    eventManager.close()
    onControllerResignation()
  }

  /**
   * On controlled shutdown, the controller first determines the partitions that the
   * shutting down broker leads, and moves leadership of those partitions to another broker
   * that is in that partition's ISR.
   *
   * @param id Id of the broker to shutdown.
   * @param brokerEpoch The broker epoch in the controlled shutdown request
   * @return The number of partitions that the broker still leads.
   */
  def controlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownEvent = ControlledShutdown(id, brokerEpoch, controlledShutdownCallback)
    eventManager.put(controlledShutdownEvent)
  }

  private[kafka] def updateBrokerInfo(newBrokerInfo: BrokerInfo): Unit = {
    this.brokerInfo = newBrokerInfo
    zkClient.updateBrokerInfo(newBrokerInfo)
  }

  private[kafka] def enableDefaultUncleanLeaderElection(): Unit = {
    eventManager.put(UncleanLeaderElectionEnable)
  }

  private[kafka] def enableTopicUncleanLeaderElection(topic: String): Unit = {
    if (isActive) {
      eventManager.put(TopicUncleanLeaderElectionEnable(topic))
    }
  }

  private def state: ControllerState = eventManager.state

  /**
   * This callback is invoked by the zookeeper leader elector on electing the current broker as the new controller.
   * It does the following things on the become-controller state change -
   * 1. Initializes the controller's context object that holds cache objects for current topics, live brokers and
   *    leaders for all existing partitions.
   * 2. Starts the controller's channel manager
   * 3. Starts the replica state machine
   * 4. Starts the partition state machine
   * If it encounters any unexpected exception/error while becoming controller, it resigns as the current controller.
   * This ensures another controller election will be triggered and there will always be an actively serving controller
   *
   * 该方法是当前 Controller 成为 leader Controller 后调用，主要会完成:
   * 1) 实例化 controller 上下文对象，它持有当前 topic，存活的代理节点以及各个分区的主副本的缓存。
   * 2) 启动 controller 通道管理器
   * 3) 启动副本状态机和分区状态机
   * 如果在成为 controller 遇到任何错误或异常，当前 controller 会取消主 controller 身份，这会触发另一个 controller 选举
   */
  private def onControllerFailover(): Unit = {
    maybeSetupFeatureVersioning()

    info("Registering handlers")

    // before reading source of truth from zookeeper, register the listeners to get broker/topic callbacks
    // 注册一些子节点目录变更的监听器，用来监听 zk 目录的变化，通过监听这些目录的变化来管理 Kafka 集群的。
    // brokerChangeHandler [[BrokerChangeHandler]]: 监听 /brokers/ids 目录，用于感知其他 broker 的注册
    // topicChangeHandler [[TopicChangeHandler]]: 监听  /brokers/topics 目录，用于感知 topic 的变化
    // topicDeletionHandler: 监听 /admin/delete_topics 目录
    // logDirEventNotificationHandler: 监听 /log_dir_event_notification 目录
    // isrChangeNotificationHandler: 监听 /isr_change_notification 目录
    val childChangeHandlers = Seq(brokerChangeHandler, topicChangeHandler, topicDeletionHandler, logDirEventNotificationHandler,
      isrChangeNotificationHandler)
    childChangeHandlers.foreach(zkClient.registerZNodeChildChangeHandler)

    // 注册分区充分配的监听器
    val nodeChangeHandlers = Seq(preferredReplicaElectionHandler, partitionReassignmentHandler)
    nodeChangeHandlers.foreach(zkClient.registerZNodeChangeHandlerAndCheckExistence)

    info("Deleting log dir event notifications")
    zkClient.deleteLogDirEventNotifications(controllerContext.epochZkVersion)
    info("Deleting isr change notifications")
    zkClient.deleteIsrChangeNotifications(controllerContext.epochZkVersion)
    info("Initializing controller context")
    initializeControllerContext()
    info("Fetching topic deletions in progress")
    val (topicsToBeDeleted, topicsIneligibleForDeletion) = fetchTopicDeletionsInProgress()
    info("Initializing topic deletion manager")
    topicDeletionManager.init(topicsToBeDeleted, topicsIneligibleForDeletion)

    // We need to send UpdateMetadataRequest after the controller context is initialized and before the state machines
    // are started. The is because brokers need to receive the list of live brokers from UpdateMetadataRequest before
    // they can process the LeaderAndIsrRequests that are generated by replicaStateMachine.startup() and
    // partitionStateMachine.startup().
    info("Sending update metadata request")
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)

    /*
     * 启动时状态机的初始化和状态转换
     *  （1）副本状态机
     *    1）初始化状态
     *      存活的副本，初始化为 上线
     *      不存活的副本，初始化为 删除失败
     *    2）状态转换
     *      存活的副本，转为 上线
     *  （2）分区状态机
     *    1）初始化状态
     *      分区有主副本，初始化为 上线
     *      分区有主副本，但不存活，初始化为 下线
     *      分区没有主副本，初始化为 新建
     *    2）状态转换
     *      新建 或 下线 的分区，转换为 上线
     *
     * 状态 case: P429
     */
    replicaStateMachine.startup()
    partitionStateMachine.startup()

    info(s"Ready to serve as the new controller with epoch $epoch")

    initializePartitionReassignments()
    topicDeletionManager.tryTopicDeletion()
    val pendingPreferredReplicaElections = fetchPendingPreferredReplicaElections()
    onReplicaElection(pendingPreferredReplicaElections, ElectionType.PREFERRED, ZkTriggered)
    info("Starting the controller scheduler")
    kafkaScheduler.startup()
    if (config.autoLeaderRebalanceEnable) {
      scheduleAutoLeaderRebalanceTask(delay = 5, unit = TimeUnit.SECONDS)
    }

    if (config.tokenAuthEnabled) {
      info("starting the token expiry check scheduler")
      tokenCleanScheduler.startup()
      tokenCleanScheduler.schedule(name = "delete-expired-tokens",
        fun = () => tokenManager.expireTokens(),
        period = config.delegationTokenExpiryCheckIntervalMs,
        unit = TimeUnit.MILLISECONDS)
    }
  }

  private def createFeatureZNode(newNode: FeatureZNode): Int = {
    info(s"Creating FeatureZNode at path: ${FeatureZNode.path} with contents: $newNode")
    zkClient.createFeatureZNode(newNode)
    val (_, newVersion) = zkClient.getDataAndVersion(FeatureZNode.path)
    newVersion
  }

  private def updateFeatureZNode(updatedNode: FeatureZNode): Int = {
    info(s"Updating FeatureZNode at path: ${FeatureZNode.path} with contents: $updatedNode")
    zkClient.updateFeatureZNode(updatedNode)
  }

  /**
   * This method enables the feature versioning system (KIP-584).
   *
   * Development in Kafka (from a high level) is organized into features. Each feature is tracked by
   * a name and a range of version numbers. A feature can be of two types:
   *
   * 1. Supported feature:
   * A supported feature is represented by a name (string) and a range of versions (defined by a
   * SupportedVersionRange). It refers to a feature that a particular broker advertises support for.
   * Each broker advertises the version ranges of its own supported features in its own
   * BrokerIdZNode. The contents of the advertisement are specific to the particular broker and
   * do not represent any guarantee of a cluster-wide availability of the feature for any particular
   * range of versions.
   *
   * 2. Finalized feature:
   * A finalized feature is represented by a name (string) and a range of version levels (defined
   * by a FinalizedVersionRange). Whenever the feature versioning system (KIP-584) is
   * enabled, the finalized features are stored in the cluster-wide common FeatureZNode.
   * In comparison to a supported feature, the key difference is that a finalized feature exists
   * in ZK only when it is guaranteed to be supported by any random broker in the cluster for a
   * specified range of version levels. Also, the controller is the only entity modifying the
   * information about finalized features.
   *
   * This method sets up the FeatureZNode with enabled status, which means that the finalized
   * features stored in the FeatureZNode are active. The enabled status should be written by the
   * controller to the FeatureZNode only when the broker IBP config is greater than or equal to
   * KAFKA_2_7_IV0.
   *
   * There are multiple cases handled here:
   *
   * 1. New cluster bootstrap:
   *    A new Kafka cluster (i.e. it is deployed first time) is almost always started with IBP config
   *    setting greater than or equal to KAFKA_2_7_IV0. We would like to start the cluster with all
   *    the possible supported features finalized immediately. Assuming this is the case, the
   *    controller will start up and notice that the FeatureZNode is absent in the new cluster,
   *    it will then create a FeatureZNode (with enabled status) containing the entire list of
   *    supported features as its finalized features.
   *
   * 2. Broker binary upgraded, but IBP config set to lower than KAFKA_2_7_IV0:
   *    Imagine there was an existing Kafka cluster with IBP config less than KAFKA_2_7_IV0, and the
   *    broker binary has now been upgraded to a newer version that supports the feature versioning
   *    system (KIP-584). But the IBP config is still set to lower than KAFKA_2_7_IV0, and may be
   *    set to a higher value later. In this case, we want to start with no finalized features and
   *    allow the user to finalize them whenever they are ready i.e. in the future whenever the
   *    user sets IBP config to be greater than or equal to KAFKA_2_7_IV0, then the user could start
   *    finalizing the features. This process ensures we do not enable all the possible features
   *    immediately after an upgrade, which could be harmful to Kafka.
   *    This is how we handle such a case:
   *      - Before the IBP config upgrade (i.e. IBP config set to less than KAFKA_2_7_IV0), the
   *        controller will start up and check if the FeatureZNode is absent.
   *        - If the node is absent, it will react by creating a FeatureZNode with disabled status
   *          and empty finalized features.
   *        - Otherwise, if a node already exists in enabled status then the controller will just
   *          flip the status to disabled and clear the finalized features.
   *      - After the IBP config upgrade (i.e. IBP config set to greater than or equal to
   *        KAFKA_2_7_IV0), when the controller starts up it will check if the FeatureZNode exists
   *        and whether it is disabled.
   *         - If the node is in disabled status, the controller won’t upgrade all features immediately.
   *           Instead it will just switch the FeatureZNode status to enabled status. This lets the
   *           user finalize the features later.
   *         - Otherwise, if a node already exists in enabled status then the controller will leave
   *           the node umodified.
   *
   * 3. Broker binary upgraded, with existing cluster IBP config >= KAFKA_2_7_IV0:
   *    Imagine there was an existing Kafka cluster with IBP config >= KAFKA_2_7_IV0, and the broker
   *    binary has just been upgraded to a newer version (that supports IBP config KAFKA_2_7_IV0 and
   *    higher). The controller will start up and find that a FeatureZNode is already present with
   *    enabled status and existing finalized features. In such a case, the controller leaves the node
   *    unmodified.
   *
   * 4. Broker downgrade:
   *    Imagine that a Kafka cluster exists already and the IBP config is greater than or equal to
   *    KAFKA_2_7_IV0. Then, the user decided to downgrade the cluster by setting IBP config to a
   *    value less than KAFKA_2_7_IV0. This means the user is also disabling the feature versioning
   *    system (KIP-584). In this case, when the controller starts up with the lower IBP config, it
   *    will switch the FeatureZNode status to disabled with empty features.
   */
  private def enableFeatureVersioning(): Unit = {
    val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(FeatureZNode.path)
    if (version == ZkVersion.UnknownVersion) {
      val newVersion = createFeatureZNode(new FeatureZNode(FeatureZNodeStatus.Enabled,
                                          brokerFeatures.defaultFinalizedFeatures))
      featureCache.waitUntilEpochOrThrow(newVersion, config.zkConnectionTimeoutMs)
    } else {
      val existingFeatureZNode = FeatureZNode.decode(mayBeFeatureZNodeBytes.get)
      val newFeatures = existingFeatureZNode.status match {
        case FeatureZNodeStatus.Enabled => existingFeatureZNode.features
        case FeatureZNodeStatus.Disabled =>
          if (!existingFeatureZNode.features.empty()) {
            warn(s"FeatureZNode at path: ${FeatureZNode.path} with disabled status" +
                 s" contains non-empty features: ${existingFeatureZNode.features}")
          }
          Features.emptyFinalizedFeatures
      }
      val newFeatureZNode = new FeatureZNode(FeatureZNodeStatus.Enabled, newFeatures)
      if (!newFeatureZNode.equals(existingFeatureZNode)) {
        val newVersion = updateFeatureZNode(newFeatureZNode)
        featureCache.waitUntilEpochOrThrow(newVersion, config.zkConnectionTimeoutMs)
      }
    }
  }

  /**
   * Disables the feature versioning system (KIP-584).
   *
   * Sets up the FeatureZNode with disabled status. This status means the feature versioning system
   * (KIP-584) is disabled, and, the finalized features stored in the FeatureZNode are not relevant.
   * This status should be written by the controller to the FeatureZNode only when the broker
   * IBP config is less than KAFKA_2_7_IV0.
   *
   * NOTE:
   * 1. When this method returns, existing finalized features (if any) will be cleared from the
   *    FeatureZNode.
   * 2. This method, unlike enableFeatureVersioning() need not wait for the FinalizedFeatureCache
   *    to be updated, because, such updates to the cache (via FinalizedFeatureChangeListener)
   *    are disabled when IBP config is < than KAFKA_2_7_IV0.
   */
  private def disableFeatureVersioning(): Unit = {
    val newNode = FeatureZNode(FeatureZNodeStatus.Disabled, Features.emptyFinalizedFeatures())
    val (mayBeFeatureZNodeBytes, version) = zkClient.getDataAndVersion(FeatureZNode.path)
    if (version == ZkVersion.UnknownVersion) {
      createFeatureZNode(newNode)
    } else {
      val existingFeatureZNode = FeatureZNode.decode(mayBeFeatureZNodeBytes.get)
      if (existingFeatureZNode.status == FeatureZNodeStatus.Disabled &&
          !existingFeatureZNode.features.empty()) {
        warn(s"FeatureZNode at path: ${FeatureZNode.path} with disabled status" +
             s" contains non-empty features: ${existingFeatureZNode.features}")
      }
      if (!newNode.equals(existingFeatureZNode)) {
        updateFeatureZNode(newNode)
      }
    }
  }

  private def maybeSetupFeatureVersioning(): Unit = {
    if (config.isFeatureVersioningSupported) {
      enableFeatureVersioning()
    } else {
      disableFeatureVersioning()
    }
  }

  private def scheduleAutoLeaderRebalanceTask(delay: Long, unit: TimeUnit): Unit = {
    kafkaScheduler.schedule("auto-leader-rebalance-task", () => eventManager.put(AutoPreferredReplicaLeaderElection),
      delay = delay, unit = unit)
  }

  /**
   * This callback is invoked by the zookeeper leader elector when the current broker resigns as the controller. This is
   * required to clean up internal controller data structures
   *
   * 当前代理节点不再是 controller 时，需要取消一些之前注册的数据结构
   */
  private def onControllerResignation(): Unit = {
    debug("Resigning")
    // de-register listeners
    zkClient.unregisterZNodeChildChangeHandler(isrChangeNotificationHandler.path)
    zkClient.unregisterZNodeChangeHandler(partitionReassignmentHandler.path)
    zkClient.unregisterZNodeChangeHandler(preferredReplicaElectionHandler.path)
    zkClient.unregisterZNodeChildChangeHandler(logDirEventNotificationHandler.path)
    unregisterBrokerModificationsHandler(brokerModificationsHandlers.keySet)

    // shutdown leader rebalance scheduler
    kafkaScheduler.shutdown()
    offlinePartitionCount = 0
    preferredReplicaImbalanceCount = 0
    globalTopicCount = 0
    globalPartitionCount = 0
    topicsToDeleteCount = 0
    replicasToDeleteCount = 0
    ineligibleTopicsToDeleteCount = 0
    ineligibleReplicasToDeleteCount = 0

    // stop token expiry check scheduler
    if (tokenCleanScheduler.isStarted)
      tokenCleanScheduler.shutdown()

    // de-register partition ISR listener for on-going partition reassignment task
    unregisterPartitionReassignmentIsrChangeHandlers()
    // shutdown partition state machine
    partitionStateMachine.shutdown()
    zkClient.unregisterZNodeChildChangeHandler(topicChangeHandler.path)
    unregisterPartitionModificationsHandlers(partitionModificationsHandlers.keys.toSeq)
    zkClient.unregisterZNodeChildChangeHandler(topicDeletionHandler.path)
    // shutdown replica state machine
    replicaStateMachine.shutdown()
    zkClient.unregisterZNodeChildChangeHandler(brokerChangeHandler.path)

    controllerChannelManager.shutdown()
    controllerContext.resetContext()

    info("Resigned")
  }

  /*
   * This callback is invoked by the controller's LogDirEventNotificationListener with the list of broker ids who
   * have experienced new log directory failures. In response the controller should send LeaderAndIsrRequest
   * to all these brokers to query the state of their replicas. Replicas with an offline log directory respond with
   * KAFKA_STORAGE_ERROR, which will be handled by the LeaderAndIsrResponseReceived event.
   */
  private def onBrokerLogDirFailure(brokerIds: Seq[Int]): Unit = {
    // send LeaderAndIsrRequest for all replicas on those brokers to see if they are still online.
    info(s"Handling log directory failure for brokers ${brokerIds.mkString(",")}")
    val replicasOnBrokers = controllerContext.replicasOnBrokers(brokerIds.toSet)
    replicaStateMachine.handleStateChanges(replicasOnBrokers.toSeq, OnlineReplica)
  }

  /**
   * This callback is invoked by the replica state machine's broker change listener, with the list of newly started
   * brokers as input. It does the following -
   * 1. Sends update metadata request to all live and shutting down brokers
   * 2. Triggers the OnlinePartition state change for all new/offline partitions
   * 3. It checks whether there are reassigned replicas assigned to any newly started brokers. If
   *    so, it performs the reassignment logic for each topic/partition.
   *
   * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point for two reasons:
   * 1. The partition state machine, when triggering online state change, will refresh leader and ISR for only those
   *    partitions currently new or offline (rather than every partition this controller is aware of)
   * 2. Even if we do refresh the cache, there is no guarantee that by the time the leader and ISR request reaches
   *    every broker that it is still valid.  Brokers check the leader epoch to determine validity of the request.
   *
   * 代理节点上线有两种场景: 新启动、重新启动。新启动的代理节点之前不在集群中，没有分配到任何分区，控制器不会处理分区和副本的状态改变。
   * 重新启动的代理节点之前在集群中，并分配到分区，控制器需要处理分区和副本的状态改变。
   *
   * 代理节点下线、重新上线、选举最优副本过程: P426
   */
  private def onBrokerStartup(newBrokers: Seq[Int]): Unit = {
    info(s"New broker startup callback for ${newBrokers.mkString(",")}")
    newBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val newBrokersSet = newBrokers.toSet
    val existingBrokers = controllerContext.liveOrShuttingDownBrokerIds.diff(newBrokersSet)
    // Send update metadata request to all the existing brokers in the cluster so that they know about the new brokers
    // via this update. No need to include any partition states in the request since there are no partition state changes.
    // 发送一个元数据更新请求，用于同步 broker 之间的数据
    sendUpdateMetadataRequest(existingBrokers.toSeq, Set.empty)
    // Send update metadata request to all the new brokers in the cluster with a full set of partition states for initialization.
    // In cases of controlled shutdown leaders will not be elected when a new broker comes up. So at least in the
    // common controlled shutdown case, the metadata will reach the new brokers faster.
    sendUpdateMetadataRequest(newBrokers, controllerContext.partitionsWithLeaders)
    // the very first thing to do when a new broker comes up is send it the entire list of partitions that it is
    // supposed to host. Based on that the broker starts the high watermark threads for the input list of partitions
    val allReplicasOnNewBrokers = controllerContext.replicasOnBrokers(newBrokersSet)
    // 上线节点所有的【副本】状态转换为上线
    replicaStateMachine.handleStateChanges(allReplicasOnNewBrokers.toSeq, OnlineReplica)
    // when a new broker comes up, the controller needs to trigger leader election for all new and offline partitions
    // to see if these brokers can become leaders for some/all of those
    // 如果分区状态为 新建 或 下线，则重新选举主副本，并转为 上线状态，已经是 在线 状态则不变。
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // check if reassignment of some partitions need to be restarted
    maybeResumeReassignments { (_, assignment) =>
      assignment.targetReplicas.exists(newBrokersSet.contains)
    }
    // check if topic deletion needs to be resumed. If at least one replica that belongs to the topic being deleted exists
    // on the newly restarted brokers, there is a chance that topic deletion can resume
    val replicasForTopicsToBeDeleted = allReplicasOnNewBrokers.filter(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))
    if (replicasForTopicsToBeDeleted.nonEmpty) {
      info(s"Some replicas ${replicasForTopicsToBeDeleted.mkString(",")} for topics scheduled for deletion " +
        s"${controllerContext.topicsToBeDeleted.mkString(",")} are on the newly restarted brokers " +
        s"${newBrokers.mkString(",")}. Signaling restart of topic deletion for these topics")
      topicDeletionManager.resumeDeletionForTopics(replicasForTopicsToBeDeleted.map(_.topic))
    }
    registerBrokerModificationsHandler(newBrokers)
  }

  private def maybeResumeReassignments(shouldResume: (TopicPartition, ReplicaAssignment) => Boolean): Unit = {
    controllerContext.partitionsBeingReassigned.foreach { tp =>
      val currentAssignment = controllerContext.partitionFullReplicaAssignment(tp)
      if (shouldResume(tp, currentAssignment))
        onPartitionReassignment(tp, currentAssignment)
    }
  }

  private def registerBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Register BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      val brokerModificationsHandler = new BrokerModificationsHandler(eventManager, brokerId)
      zkClient.registerZNodeChangeHandlerAndCheckExistence(brokerModificationsHandler)
      brokerModificationsHandlers.put(brokerId, brokerModificationsHandler)
    }
  }

  private def unregisterBrokerModificationsHandler(brokerIds: Iterable[Int]): Unit = {
    debug(s"Unregister BrokerModifications handler for $brokerIds")
    brokerIds.foreach { brokerId =>
      brokerModificationsHandlers.remove(brokerId).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  /*
   * This callback is invoked by the replica state machine's broker change listener with the list of failed brokers
   * as input. It will call onReplicaBecomeOffline(...) with the list of replicas on those failed brokers as input.
   *
   * 分区对应的代理节点下线，副本状态机会将这个代理节点的副本状态修改为下线状态，但并不将这个副本从分区的副本集（AR）中删除，但是会将这个副本
   * 从分区的 ISR 集合中删除。
   */
  private def onBrokerFailure(deadBrokers: Seq[Int]): Unit = {
    info(s"Broker failure callback for ${deadBrokers.mkString(",")}")
    deadBrokers.foreach(controllerContext.replicasOnOfflineDirs.remove)
    val deadBrokersThatWereShuttingDown =
      deadBrokers.filter(id => controllerContext.shuttingDownBrokerIds.remove(id))
    if (deadBrokersThatWereShuttingDown.nonEmpty)
      info(s"Removed ${deadBrokersThatWereShuttingDown.mkString(",")} from list of shutting down brokers.")
    val allReplicasOnDeadBrokers = controllerContext.replicasOnBrokers(deadBrokers.toSet)
    // 下线逻辑
    onReplicasBecomeOffline(allReplicasOnDeadBrokers)

    unregisterBrokerModificationsHandler(deadBrokers)
  }

  private def onBrokerUpdate(updatedBrokerId: Int): Unit = {
    info(s"Broker info update callback for $updatedBrokerId")
    sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
  }

  /**
    * This method marks the given replicas as offline. It does the following -
    * 1. Marks the given partitions as offline
    * 2. Triggers the OnlinePartition state change for all new/offline partitions
    * 3. Invokes the OfflineReplica state change on the input list of newly offline replicas
    * 4. If no partitions are affected then send UpdateMetadataRequest to live or shutting down brokers
    *
    * Note that we don't need to refresh the leader/isr cache for all topic/partitions at this point. This is because
    * the partition state machine will refresh our cache for us when performing leader election for all new/offline
    * partitions coming online.
    */
  private def onReplicasBecomeOffline(newOfflineReplicas: Set[PartitionAndReplica]): Unit = {
    val (newOfflineReplicasForDeletion, newOfflineReplicasNotForDeletion) =
      newOfflineReplicas.partition(p => topicDeletionManager.isTopicQueuedUpForDeletion(p.topic))

    // 主副本在下线代理节点上的分区，代理节点下线后，这些分区就没有主副本了
    val partitionsWithOfflineLeader = controllerContext.partitionsWithOfflineLeader

    // trigger OfflinePartition state for all partitions whose current leader is one amongst the newOfflineReplicas
    // 没有主副本的分区状态改为下线
    partitionStateMachine.handleStateChanges(partitionsWithOfflineLeader.toSeq, OfflinePartition)
    // trigger OnlinePartition state changes for offline or new partitions
    // 重新选举分区的主副本，下线状态的分区会转换为上线状态
    partitionStateMachine.triggerOnlinePartitionStateChange()
    // trigger OfflineReplica state change for those newly offline replicas
    // 不需要删除的副本状态标记为下线
    replicaStateMachine.handleStateChanges(newOfflineReplicasNotForDeletion.toSeq, OfflineReplica)

    // fail deletion of topics that are affected by the offline replicas
    if (newOfflineReplicasForDeletion.nonEmpty) {
      // it is required to mark the respective replicas in TopicDeletionFailed state since the replica cannot be
      // deleted when its log directory is offline. This will prevent the replica from being in TopicDeletionStarted state indefinitely
      // since topic deletion cannot be retried until at least one replica is in TopicDeletionStarted state
      topicDeletionManager.failReplicaDeletion(newOfflineReplicasForDeletion)
    }

    // If replica failure did not require leader re-election, inform brokers of the offline brokers
    // Note that during leader re-election, brokers update their metadata
    if (partitionsWithOfflineLeader.isEmpty) {
      // 重新选举主副本后，发送请求给所有的代理节点
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set.empty)
    }
  }

  /**
   * This callback is invoked by the topic change callback with the list of failed brokers as input.
   * It does the following -
   * 1. Move the newly created partitions to the NewPartition state
   * 2. Move the newly created partitions from NewPartition->OnlinePartition state
   *
   * 新建分区会调用两次分区状态机的状态改变处理方法。
   *
   * 假设创建了一个分区 P1，它的副本分别在代理节点 [0,1,2] 上，用 [R0,R1,R2] 表示这三个副本。下面表示了新建分区 P1 的过程中，
   * 状态机以及上下文的变化：
   * 1）分区 P1 从 不存在状态 改为 新建状态，更新分区状态机: [P1 -> NewPartition]
   * 2）分区的所有副本从 不存在状态 改为 新建状态。这一步会判断是否存在主副本，但是由于是新建分区，分区还没有主副本，因此这一步也
   * 只是更新副本状态机: [P1R0 -> NewReplica],[P1R1 -> NewReplica],[P1R2 -> NewReplica]
   * 3）分区 P1 从 新建状态 改为 上线状态，控制器为分区选举主副本、创建 zk 节点（/brokers/topics/[topic]/[partition]/state）、
   * 更新上下文的 partitionLeadershipInfo 变量、更新分区状态机: [P1 -> OnlinePartition].
   * 4）分区 P1 的所有副本从 新建状态 改为 上线状态，更新上下文的 partitionReplicaAssignment 变量: P1 -> [P1R0,P1R1,P1R2],
   * 更新副本状态机: [P1R0 -> OnlineReplica],[P1R1 -> OnlineReplica],[P1R2 -> OnlineReplica]
   */
  private def onNewPartitionCreation(newPartitions: Set[TopicPartition]): Unit = {
    info(s"New partition creation callback for ${newPartitions.mkString(",")}")
    // 注册各种监听器
    // 分区状态从初始的 不存在状态 改为 新建状态
    partitionStateMachine.handleStateChanges(newPartitions.toSeq, NewPartition)
    // 副本状态从初始的 不存在状态 改为 新建状态
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, NewReplica)
    // 分区状态从 新建状态 改为 上线状态
    partitionStateMachine.handleStateChanges(
      newPartitions.toSeq,
      OnlinePartition,
      Some(OfflinePartitionLeaderElectionStrategy(false)) // 这里多了一个主副本选择策略
    )
    // 发送请求的逻辑在这里
    // 副本状态从 新建状态 改为 上线状态
    replicaStateMachine.handleStateChanges(controllerContext.replicasForPartition(newPartitions).toSeq, OnlineReplica)
  }

  /**
   * This callback is invoked:
   * 1. By the AlterPartitionReassignments API
   * 2. By the reassigned partitions listener which is triggered when the /admin/reassign/partitions znode is created
   * 3. When an ongoing reassignment finishes - this is detected by a change in the partition's ISR znode
   * 4. Whenever a new broker comes up which is part of an ongoing reassignment
   * 5. On controller startup/failover
   *
   * 方法调用的场景：
   * 1）AlterPartitionReassignments API
   * 2）通过监听 /admin/reassign/partitions 节点的创建，对应的回调 PartitionReassignmentHandler，
   *    在代理节点成为 Controller 时注册到 zk elect -> onControllerFailover）
   * 3）通过分区的 ISR 节点变更，表示分区重分配完成
   * 4）新的代理节点成为分配的一个节点
   * 5）控制器启动或失败
   *
   * Reassigning replicas for a partition goes through a few steps listed in the code.
   * RS = current assigned replica set
   * ORS = Original replica set for partition
   * TRS = Reassigned (target) replica set
   * AR = The replicas we are adding as part of this reassignment
   * RR = The replicas we are removing as part of this reassignment
   *
   * 先更新 zk，再更新内存（这样可以在控制器 fail-over 时从内容中恢复），如果要取消重分配，对 AR 集合里的所有副本发送
   * StopReplica（如果不在新分配的 TRS 中）
   *
   * U1: 更新 zk RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
   * U2: 更新内存 RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
   *
   * A reassignment may have up to three phases, each with its own steps:
   *
   * Phase U (Assignment update): Regardless of the trigger, the first step is in the reassignment process
   * is to update the existing assignment state. We always update the state in Zookeeper before
   * we update memory so that it can be resumed upon controller fail-over.
   *
   *   U1. Update ZK with RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
   *   U2. Update memory with RS = ORS + TRS, AR = TRS - ORS and RR = ORS - TRS
   *   U3. If we are cancelling or replacing an existing reassignment, send StopReplica to all members
   *       of AR in the original reassignment if they are not in TRS from the new assignment
   *
   * To complete the reassignment, we need to bring the new replicas into sync, so depending on the state
   * of the ISR, we will execute one of the following steps.
   *
   * Phase A (when TRS != ISR): The reassignment is not yet complete
   *
   *   A1. Bump the leader epoch for the partition and send LeaderAndIsr updates to RS.
   *   A2. Start new replicas AR by moving replicas in AR to NewReplica state.
   *
   * Phase B (when TRS = ISR): The reassignment is complete
   *
   *   B1. Move all replicas in AR to OnlineReplica state.
   *   B2. Set RS = TRS, AR = [], RR = [] in memory.
   *   B3. Send a LeaderAndIsr request with RS = TRS. This will prevent the leader from adding any replica in TRS - ORS back in the isr.
   *       If the current leader is not in TRS or isn't alive, we move the leader to a new replica in TRS.
   *       We may send the LeaderAndIsr to more than the TRS replicas due to the
   *       way the partition state machine works (it reads replicas from ZK)
   *   B4. Move all replicas in RR to OfflineReplica state. As part of OfflineReplica state change, we shrink the
   *       isr to remove RR in ZooKeeper and send a LeaderAndIsr ONLY to the Leader to notify it of the shrunk isr.
   *       After that, we send a StopReplica (delete = false) to the replicas in RR.
   *   B5. Move all replicas in RR to NonExistentReplica state. This will send a StopReplica (delete = true) to
   *       the replicas in RR to physically delete the replicas on disk.
   *   B6. Update ZK with RS=TRS, AR=[], RR=[].
   *   B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it if present.
   *   B8. After electing leader, the replicas and isr information changes. So resend the update metadata request to every broker.
   *
   * 1.LeaderAndIsrRequest 副本集里的每个副本都会收到发送给它们的请求。
   * 2.从分区分配中删除的副本会收到发送给它们的 StopReplica。
   *
   * In general, there are two goals we want to aim for:
   * 1. Every replica present in the replica set of a LeaderAndIsrRequest gets the request sent to it
   * 2. Replicas that are removed from a partition's assignment get StopReplica sent to them
   *
   * RS: 当前分配的副本集合
   * ORS: 分区原始的副本集合
   * TRS: 重分配后的副本的集合
   * AR: 重分配后增加的副本集合 （TRS - ORS）
   * RR: 重分配后需要删除的副本集合 (ORS - TRS)
   *
   * U1: 更新 zk RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
   * U2: 更新内存 RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
   *
   * 阶段一（TRS != ISR）
   *  A1: 更新分区的 epoch 并且向 RS 集合里的副本发送 LeaderAndIsr（LAI）请求
   *  A2: 将 AR 里的副本状态更新为 NewReplica 来启动新的副本
   *
   * 阶段二（TRS = ISR）
   *  B1: 将 AR 里的所有副本状态设置为 OnlineReplica 状态
   *  B2: 设置内存里的 RS = TRS， AR = [], RR = []
   *  B3: 发送带有（RS = TRS）的 LAI 请求，这会阻止主副本将任何在（TRS - ORS）里的副本添加到 ISR 列表里。
   *  如果当前的主副本不在 TRS 中或者非存活状态，会从 TRS 中重新选举一个新的主副本。
   *  B4: 将 RR 里的所有副本状态设置为 OfflineReplica 状态，收缩 ISR 以删除 zk 中的 RR，并仅向主副本发送 LAI 请求通知其收缩的 ISR。
   *  之后，会发送 StopReplica（delete = false）给 RR 里的所有副本。
   *  B5: 将 RR 里的所有副本设置设置为 NonExistentReplica 状态，这回发送 StopReplica（delete = true） 给 RR 里的所有副本执行下线操作，
   *  这会物理删除磁盘上的副本信息。
   *  B6: 更新 zk RS = TRS, AR = [], RR = []
   *  B7: 移除 ISR 重分配监听器，更新 /admin/reassign_partitions 节点，移除当前这个重分配的分区。
   *  B8: 在选举完主副本后，副本和 ISR 信息已变更，会发送更新元数据的请求给各个代理节点。
   *
   * Note: 下面的是 ZK 节点的变化，而不是内存数据的变化
   * For example, if ORS = {1,2,3} and TRS = {4,5,6}, the values in the topic and leader/isr paths in ZK
   * may go through the following transitions.
   * RS                AR          RR          leader     isr
   * {1,2,3}           {}          {}          1          {1,2,3}           (initial state)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3}           (step A2)           这一步已经包含了 U2
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     1          {1,2,3,4,5,6}     (phase B)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {1,2,3,4,5,6}     (step B3)
   * {4,5,6,1,2,3}     {4,5,6}     {1,2,3}     4          {4,5,6}           (step B4)           收缩 ISR
   * {4,5,6}           {}          {}          4          {4,5,6}           (step B6)
   *
   * Note that we have to update RS in ZK with TRS last since it's the only place where we store ORS persistently.
   * This way, if the controller crashes before that step, we can still recover.
   *
   * 调用链： processPartitionReassignmentIsrChange --> maybeCompleteReassignment
   *
   * 因为 TRS 的所有副本都要和分区旧的主副本同步，并同步到 ISR 中，而分区数据同步比较慢。如果要一次性执行完成分区的重新分配，这个过程会
   * 比较长。所以这个方法分两个阶段执行。
   * 在执行重新分配之前，会在分区对应的状态节点（/topics/[topic]/[partition]/state 上）注册监听事件，监听分区的 ISR 节点的变更，
   * 对应回调 PartitionReassignmentIsrChangeHandler。
   *
   */
  private def onPartitionReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
    // While a reassignment is in progress, deletion is not allowed
    topicDeletionManager.markTopicIneligibleForDeletion(Set(topicPartition.topic), reason = "topic reassignment in progress")

    // U1, U2，更新内存和 ZK 的数据
    updateCurrentReassignment(topicPartition, reassignment)

    // AR = TRS - ORS: 重分配后增加的副本集合
    val addingReplicas = reassignment.addingReplicas
    // RR = ORS - TRS: 重分配后删除的副本集合
    val removingReplicas = reassignment.removingReplicas

    // 这个方法会在分区重分配过程中会被分阶段调用：
    // 阶段一：AR 和 RR 都不为空，且 TRS 不是 ISR 的子集，方法返回 false 满足进入这个方法
    if (!isReassignmentComplete(topicPartition, reassignment)) {
      // A1. Send LeaderAndIsr request to every replica in ORS + TRS (with the new RS, AR and RR).
      // A1. 实际发送 LAI 请求的逻辑在 A2，这里仅保存到 leaderAndIsrRequestMap 中
      updateLeaderEpochAndSendRequest(topicPartition, reassignment)
      // A2. replicas in AR -> NewReplica
      // A2. 将新增的副本状态设置为新副本，并且控制器会发送 LAI 请求给新副本，新副本只作为分区的备份副本，
      // 并同步主副本的数据，当数据赶上主副本时，就会放到 ISR 中（这个逻辑是服务端完成的，可以参考备份副本同步主副本那块的逻辑）
      // 控制器必须发送 LAI 请求给新副本，这样新副本才有可能加入到 ISR 中，否则，如果控制器只是更新了 AR，那么新副本不能同步数据，
      // 就没有机会加入到 ISR 中，就无法进入阶段二。
      startNewReplicasForReassignedPartition(topicPartition, addingReplicas)
    } else {
      // 阶段二：AR 和 RR 都为空，且 TRS 是 ISR 的子集，方法返回 true 满足进入这个方法
      // B1. replicas in AR -> OnlineReplica
      // B1. 将 AR 里的副本设置为上线状态（这个步骤没有 LAI 请求）
      replicaStateMachine.handleStateChanges(addingReplicas.map(PartitionAndReplica(topicPartition, _)), OnlineReplica)
      // B2. Set RS = TRS, AR = [], RR = [] in memory.
      // B2. 更新内存 RS = TRS， AR = [], RR = []，
      val completedReassignment = ReplicaAssignment(reassignment.targetReplicas)
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, completedReassignment)
      // B3. Send LeaderAndIsr request with a potential new leader (if current leader not in TRS) and
      //   a new RS (using TRS) and same isr to every broker in ORS + TRS or TRS
      moveReassignedPartitionLeaderIfRequired(topicPartition, completedReassignment)
      // B4. replicas in RR -> Offline (force those replicas out of isr)
      // B5. replicas in RR -> NonExistentReplica (force those replicas to be deleted)
      // B4-5: 下线 RR 集合里的副本
      stopRemovedReplicasOfReassignedPartition(topicPartition, removingReplicas)
      // B6. Update ZK with RS = TRS, AR = [], RR = [].
      // B6. 更新 ZK, RS = TRS， AR = [], RR = []
      updateReplicaAssignmentForPartition(topicPartition, completedReassignment)
      // B7. Remove the ISR reassign listener and maybe update the /admin/reassign_partitions path in ZK to remove this partition from it.
      // B7. 移除 ISR 重分配监听器，更新 /admin/reassign_partitions 节点，移除当前这个重分配的分区。
      removePartitionFromReassigningPartitions(topicPartition, completedReassignment)
      // B8. After electing a leader in B3, the replicas and isr information changes, so resend the update metadata request to every broker
      // B8. 在选举完主副本后，副本和 ISR 信息已变更，会发送更新元数据的请求给各个代理节点。
      sendUpdateMetadataRequest(controllerContext.liveOrShuttingDownBrokerIds.toSeq, Set(topicPartition))
      // signal delete topic thread if reassignment for some partitions belonging to topics being deleted just completed
      topicDeletionManager.resumeDeletionForTopics(Set(topicPartition.topic))
    }
  }

  /**
   * Update the current assignment state in Zookeeper and in memory. If a reassignment is already in
   * progress, then the new reassignment will supplant it and some replicas will be shutdown.
   *
   * Note that due to the way we compute the original replica set, we cannot guarantee that a
   * cancellation will restore the original replica order. Target replicas are always listed
   * first in the replica set in the desired order, which means we have no way to get to the
   * original order if the reassignment overlaps with the current assignment. For example,
   * with an initial assignment of [1, 2, 3] and a reassignment of [3, 4, 2], then the replicas
   * will be encoded as [3, 4, 2, 1] while the reassignment is in progress. If the reassignment
   * is cancelled, there is no way to restore the original order.
   *
   *
   * 更新内存和 zk 当前的分配状态，如果重分配正在进行中，新的重分配将会被取代，一些副本会被下线。
   * 由于 ORS 的计算方式，不能保证恢复原来的副本如果重分配被取消。TRS 总是被放在副本集的前面，这意味着无法恢复原始副本集如果重分配被覆盖。
   *
   * @param topicPartition The reassigning partition
   * @param reassignment The new reassignment
   */
  private def updateCurrentReassignment(topicPartition: TopicPartition, reassignment: ReplicaAssignment): Unit = {
    // 当前的分配的副本集
    val currentAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)

    // 表示有重分配和当前的一样，就没必要执行了
    if (currentAssignment != reassignment) {
      debug(s"Updating assignment of partition $topicPartition from $currentAssignment to $reassignment")

      // U1. Update assignment state in zookeeper
      // U1. 更新 zk RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
      updateReplicaAssignmentForPartition(topicPartition, reassignment)
      // U2. Update assignment state in memory
      // U2. 更新内存 RS = ORS + TRS, AR = TRS - ORS, RR = ORS - TRS.
      controllerContext.updatePartitionFullReplicaAssignment(topicPartition, reassignment)

      // If there is a reassignment already in progress, then some of the currently adding replicas
      // may be eligible for immediate removal, in which case we need to stop the replicas.
      val unneededReplicas = currentAssignment.replicas.diff(reassignment.replicas)
      if (unneededReplicas.nonEmpty)
        stopRemovedReplicasOfReassignedPartition(topicPartition, unneededReplicas)
    }

    // 这个属性是 true，这里不注册 ？？？
    if (!isAlterIsrEnabled) {
      val reassignIsrChangeHandler = new PartitionReassignmentIsrChangeHandler(eventManager, topicPartition)
      zkClient.registerZNodeChangeHandler(reassignIsrChangeHandler)
    }

    // 保存一下正在执行重分配的分区
    controllerContext.partitionsBeingReassigned.add(topicPartition)
  }

  /**
   * Trigger a partition reassignment provided that the topic exists and is not being deleted.
   *
   * This is called when a reassignment is initially received either through Zookeeper or through the
   * AlterPartitionReassignments API
   *
   * The `partitionsBeingReassigned` field in the controller context will be updated by this
   * call after the reassignment completes validation and is successfully stored in the topic
   * assignment zNode.
   *
   * @param reassignments The reassignments to begin processing
   * @return A map of any errors in the reassignment. If the error is NONE for a given partition,
   *         then the reassignment was submitted successfully.
   */
  private def maybeTriggerPartitionReassignment(reassignments: Map[TopicPartition, ReplicaAssignment]): Map[TopicPartition, ApiError] = {
    reassignments.map { case (tp, reassignment) =>
      val topic = tp.topic

      val apiError = if (topicDeletionManager.isTopicQueuedUpForDeletion(topic)) {
        info(s"Skipping reassignment of $tp since the topic is currently being deleted")
        new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist.")
      } else {
        val assignedReplicas = controllerContext.partitionReplicaAssignment(tp)
        if (assignedReplicas.nonEmpty) {
          try {
            onPartitionReassignment(tp, reassignment)
            ApiError.NONE
          } catch {
            case e: ControllerMovedException =>
              info(s"Failed completing reassignment of partition $tp because controller has moved to another broker")
              throw e
            case e: Throwable =>
              error(s"Error completing reassignment of partition $tp", e)
              new ApiError(Errors.UNKNOWN_SERVER_ERROR)
          }
        } else {
          new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist.")
        }
      }

      tp -> apiError
    }
  }

  /**
    * Attempt to elect a replica as leader for each of the given partitions.
    * @param partitions The partitions to have a new leader elected
    * @param electionType The type of election to perform
    * @param electionTrigger The reason for tigger this election
    * @return A map of failed and successful elections. The keys are the topic partitions and the corresponding values are
    *         either the exception that was thrown or new leader & ISR.
    */
  private[this] def onReplicaElection(
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    electionTrigger: ElectionTrigger
  ): Map[TopicPartition, Either[Throwable, LeaderAndIsr]] = {
    info(s"Starting replica leader election ($electionType) for partitions ${partitions.mkString(",")} triggered by $electionTrigger")
    try {
      val strategy = electionType match {
        case ElectionType.PREFERRED => PreferredReplicaPartitionLeaderElectionStrategy
        case ElectionType.UNCLEAN =>
          /* Let's be conservative and only trigger unclean election if the election type is unclean and it was
           * triggered by the admin client
           */
          OfflinePartitionLeaderElectionStrategy(allowUnclean = electionTrigger == AdminClientTriggered)
      }

      val results = partitionStateMachine.handleStateChanges(
        partitions.toSeq,
        OnlinePartition,
        Some(strategy)
      )
      if (electionTrigger != AdminClientTriggered) {
        results.foreach {
          case (tp, Left(throwable)) =>
            if (throwable.isInstanceOf[ControllerMovedException]) {
              info(s"Error completing replica leader election ($electionType) for partition $tp because controller has moved to another broker.", throwable)
              throw throwable
            } else {
              error(s"Error completing replica leader election ($electionType) for partition $tp", throwable)
            }
          case (_, Right(_)) => // Ignored; No need to log or throw exception for the success cases
        }
      }

      results
    } finally {
      if (electionTrigger != AdminClientTriggered) {
        removePartitionsFromPreferredReplicaElection(partitions, electionTrigger == AutoTriggered)
      }
    }
  }

  private def initializeControllerContext(): Unit = {
    // update controller cache with delete topic information
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    val (compatibleBrokerAndEpochs, incompatibleBrokerAndEpochs) = partitionOnFeatureCompatibility(curBrokerAndEpochs)
    if (!incompatibleBrokerAndEpochs.isEmpty) {
      warn("Ignoring registration of new brokers due to incompatibilities with finalized features: " +
        incompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
    }
    controllerContext.setLiveBrokers(compatibleBrokerAndEpochs)
    info(s"Initialized broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
    controllerContext.setAllTopics(zkClient.getAllTopicsInCluster(true))
    registerPartitionModificationsHandlers(controllerContext.allTopics.toSeq)
    zkClient.getFullReplicaAssignmentForTopics(controllerContext.allTopics.toSet).foreach {
      case (topicPartition, replicaAssignment) =>
        // 代理节点被选举为 Controller
        controllerContext.updatePartitionFullReplicaAssignment(topicPartition, replicaAssignment)
        if (replicaAssignment.isBeingReassigned)
          controllerContext.partitionsBeingReassigned.add(topicPartition)
    }
    controllerContext.clearPartitionLeadershipInfo()
    controllerContext.shuttingDownBrokerIds.clear()
    // register broker modifications handlers
    registerBrokerModificationsHandler(controllerContext.liveOrShuttingDownBrokerIds)
    // update the leader and isr cache for all existing partitions from Zookeeper
    updateLeaderAndIsrCache()
    // start the channel manager
    controllerChannelManager.startup()
    info(s"Currently active brokers in the cluster: ${controllerContext.liveBrokerIds}")
    info(s"Currently shutting brokers in the cluster: ${controllerContext.shuttingDownBrokerIds}")
    info(s"Current list of topics in the cluster: ${controllerContext.allTopics}")
  }

  private def fetchPendingPreferredReplicaElections(): Set[TopicPartition] = {
    val partitionsUndergoingPreferredReplicaElection = zkClient.getPreferredReplicaElection
    // check if they are already completed or topic was deleted
    val partitionsThatCompletedPreferredReplicaElection = partitionsUndergoingPreferredReplicaElection.filter { partition =>
      val replicas = controllerContext.partitionReplicaAssignment(partition)
      val topicDeleted = replicas.isEmpty
      val successful =
        if (!topicDeleted) controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader == replicas.head else false
      successful || topicDeleted
    }
    val pendingPreferredReplicaElectionsIgnoringTopicDeletion = partitionsUndergoingPreferredReplicaElection -- partitionsThatCompletedPreferredReplicaElection
    val pendingPreferredReplicaElectionsSkippedFromTopicDeletion = pendingPreferredReplicaElectionsIgnoringTopicDeletion.filter(partition => topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
    val pendingPreferredReplicaElections = pendingPreferredReplicaElectionsIgnoringTopicDeletion -- pendingPreferredReplicaElectionsSkippedFromTopicDeletion
    info(s"Partitions undergoing preferred replica election: ${partitionsUndergoingPreferredReplicaElection.mkString(",")}")
    info(s"Partitions that completed preferred replica election: ${partitionsThatCompletedPreferredReplicaElection.mkString(",")}")
    info(s"Skipping preferred replica election for partitions due to topic deletion: ${pendingPreferredReplicaElectionsSkippedFromTopicDeletion.mkString(",")}")
    info(s"Resuming preferred replica election for partitions: ${pendingPreferredReplicaElections.mkString(",")}")
    pendingPreferredReplicaElections
  }

  /**
   * Initialize pending reassignments. This includes reassignments sent through /admin/reassign_partitions,
   * which will supplant any API reassignments already in progress.
   */
  private def initializePartitionReassignments(): Unit = {
    // New reassignments may have been submitted through Zookeeper while the controller was failing over
    val zkPartitionsResumed = processZkPartitionReassignment()
    // We may also have some API-based reassignments that need to be restarted
    maybeResumeReassignments { (tp, _) =>
      !zkPartitionsResumed.contains(tp)
    }
  }

  private def fetchTopicDeletionsInProgress(): (Set[String], Set[String]) = {
    val topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    val topicsWithOfflineReplicas = controllerContext.allTopics.filter { topic => {
      val replicasForTopic = controllerContext.replicasForTopic(topic)
      replicasForTopic.exists(r => !controllerContext.isReplicaOnline(r.replica, r.topicPartition))
    }}
    val topicsForWhichPartitionReassignmentIsInProgress = controllerContext.partitionsBeingReassigned.map(_.topic)
    val topicsIneligibleForDeletion = topicsWithOfflineReplicas | topicsForWhichPartitionReassignmentIsInProgress
    info(s"List of topics to be deleted: ${topicsToBeDeleted.mkString(",")}")
    info(s"List of topics ineligible for deletion: ${topicsIneligibleForDeletion.mkString(",")}")
    (topicsToBeDeleted, topicsIneligibleForDeletion)
  }

  private def updateLeaderAndIsrCache(partitions: Seq[TopicPartition] = controllerContext.allPartitions.toSeq): Unit = {
    val leaderIsrAndControllerEpochs = zkClient.getTopicPartitionStates(partitions)
    leaderIsrAndControllerEpochs.forKeyValue { (partition, leaderIsrAndControllerEpoch) =>
      controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
    }
  }

  // 判断重分配是否已经完成
  // 1）addingReplicas 和 removingReplicas 都为空
  // 2）TRS 已经是 ISR 的子集
  private def isReassignmentComplete(partition: TopicPartition, assignment: ReplicaAssignment): Boolean = {
    if (!assignment.isBeingReassigned) {
      true
    } else {
      zkClient.getTopicPartitionStates(Seq(partition)).get(partition).exists { leaderIsrAndControllerEpoch =>
        val isr = leaderIsrAndControllerEpoch.leaderAndIsr.isr.toSet
        val targetReplicas = assignment.targetReplicas.toSet
        // targetReplicas 是否是 isr 的一部分
        // 阶段 B，重分配已经完成 TRS = ISR
        targetReplicas.subsetOf(isr)
      }
    }
  }

  // 重新分配分区从 RS 中选举一个主副本，分区的 ISR 中一定包含 TRS 的所有副本，但是不一定包含 ORA 的全部副本。
  // 在 RS 同步副本的过程，OAR 的副本可能会因为落后太多从 ISR 中移除。但是可以保证的是，TRS 的所有副本一定在
  // RS 中。
  private def moveReassignedPartitionLeaderIfRequired(topicPartition: TopicPartition,
                                                      newAssignment: ReplicaAssignment): Unit = {
    val reassignedReplicas = newAssignment.replicas
    val currentLeader = controllerContext.partitionLeadershipInfo(topicPartition).get.leaderAndIsr.leader

    // 当前主副本不在 TRS 中，上线执行选举
    if (!reassignedReplicas.contains(currentLeader)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is not in the new list of replicas ${reassignedReplicas.mkString(",")}. Re-electing leader")
      // move the leader to one of the alive and caught up new replicas
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
      // 当前副本已经在 TRS 中，并且在线，更新下元数据
    } else if (controllerContext.isReplicaOnline(currentLeader, topicPartition)) {
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} and is alive")
      // shrink replication factor and update the leader epoch in zookeeper to use on the next LeaderAndIsrRequest
      updateLeaderEpochAndSendRequest(topicPartition, newAssignment)
    } else {
      // 当前副本已经在 TRS 中，但是下线，重新选举
      info(s"Leader $currentLeader for partition $topicPartition being reassigned, " +
        s"is already in the new list of replicas ${reassignedReplicas.mkString(",")} but is dead")
      partitionStateMachine.handleStateChanges(Seq(topicPartition), OnlinePartition, Some(ReassignPartitionLeaderElectionStrategy))
    }
  }

  private def stopRemovedReplicasOfReassignedPartition(topicPartition: TopicPartition,
                                                       removedReplicas: Seq[Int]): Unit = {
    // first move the replica to offline state (the controller removes it from the ISR)
    val replicasToBeDeleted = removedReplicas.map(PartitionAndReplica(topicPartition, _))
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, OfflineReplica)
    // send stop replica command to the old replicas
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionStarted)
    // TODO: Eventually partition reassignment could use a callback that does retries if deletion failed
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, ReplicaDeletionSuccessful)
    replicaStateMachine.handleStateChanges(replicasToBeDeleted, NonExistentReplica)
  }

  private def updateReplicaAssignmentForPartition(topicPartition: TopicPartition, assignment: ReplicaAssignment): Unit = {
    val topicAssignment = mutable.Map() ++=
      controllerContext.partitionFullReplicaAssignmentForTopic(topicPartition.topic) +=
      (topicPartition -> assignment)

    // 更新 zk
    val setDataResponse = zkClient.setTopicAssignmentRaw(topicPartition.topic, topicAssignment, controllerContext.epochZkVersion)
    setDataResponse.resultCode match {
      case Code.OK =>
        info(s"Successfully updated assignment of partition $topicPartition to $assignment")
      case Code.NONODE =>
        throw new IllegalStateException(s"Failed to update assignment for $topicPartition since the topic " +
          "has no current assignment")
      case _ => throw new KafkaException(setDataResponse.resultException.get)
    }
  }

  private def startNewReplicasForReassignedPartition(topicPartition: TopicPartition, newReplicas: Seq[Int]): Unit = {
    // send the start replica request to the brokers in the reassigned replicas list that are not in the assigned
    // replicas list
    newReplicas.foreach { replica =>
      replicaStateMachine.handleStateChanges(Seq(PartitionAndReplica(topicPartition, replica)), NewReplica)
    }
  }

  private def updateLeaderEpochAndSendRequest(topicPartition: TopicPartition,
                                              assignment: ReplicaAssignment): Unit = {
    val stateChangeLog = stateChangeLogger.withControllerEpoch(controllerContext.epoch)
    updateLeaderEpoch(topicPartition) match {
      case Some(updatedLeaderIsrAndControllerEpoch) =>
        try {
          brokerRequestBatch.newBatch()
          // the isNew flag, when set to true, makes sure that when a replica possibly resided
          // in a logDir that is offline, we refrain from just creating a new replica in a good
          // logDir. This is exactly the behavior we want for the original replicas, but not
          // for the replicas we add in this reassignment. For new replicas, want to be able
          // to assign to one of the good logDirs.
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(assignment.originReplicas, topicPartition,
            updatedLeaderIsrAndControllerEpoch, assignment, isNew = false)
          brokerRequestBatch.addLeaderAndIsrRequestForBrokers(assignment.addingReplicas, topicPartition,
            updatedLeaderIsrAndControllerEpoch, assignment, isNew = true)
          brokerRequestBatch.sendRequestsToBrokers(controllerContext.epoch)
        } catch {
          case e: IllegalStateException =>
            handleIllegalState(e)
        }
        stateChangeLog.info(s"Sent LeaderAndIsr request $updatedLeaderIsrAndControllerEpoch with " +
          s"new replica assignment $assignment to leader ${updatedLeaderIsrAndControllerEpoch.leaderAndIsr.leader} " +
          s"for partition being reassigned $topicPartition")

      case None => // fail the reassignment
        stateChangeLog.error(s"Failed to send LeaderAndIsr request with new replica assignment " +
          s"$assignment to leader for partition being reassigned $topicPartition")
    }
  }

  private def registerPartitionModificationsHandlers(topics: Seq[String]) = {
    topics.foreach { topic =>
      val partitionModificationsHandler = new PartitionModificationsHandler(eventManager, topic)
      partitionModificationsHandlers.put(topic, partitionModificationsHandler)
    }
    partitionModificationsHandlers.values.foreach(zkClient.registerZNodeChangeHandler)
  }

  private[controller] def unregisterPartitionModificationsHandlers(topics: Seq[String]) = {
    topics.foreach { topic =>
      partitionModificationsHandlers.remove(topic).foreach(handler => zkClient.unregisterZNodeChangeHandler(handler.path))
    }
  }

  private def unregisterPartitionReassignmentIsrChangeHandlers(): Unit = {
    if (!isAlterIsrEnabled) {
      controllerContext.partitionsBeingReassigned.foreach { tp =>
        val path = TopicPartitionStateZNode.path(tp)
        zkClient.unregisterZNodeChangeHandler(path)
      }
    }
  }

  private def removePartitionFromReassigningPartitions(topicPartition: TopicPartition,
                                                       assignment: ReplicaAssignment): Unit = {
    if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
      if (!isAlterIsrEnabled) {
        val path = TopicPartitionStateZNode.path(topicPartition)
        zkClient.unregisterZNodeChangeHandler(path)
      }
      maybeRemoveFromZkReassignment((tp, replicas) => tp == topicPartition && replicas == assignment.replicas)
      controllerContext.partitionsBeingReassigned.remove(topicPartition)
    } else {
      throw new IllegalStateException("Cannot remove a reassigning partition because it is not present in memory")
    }
  }

  /**
   * Remove partitions from an active zk-based reassignment (if one exists).
   *
   * @param shouldRemoveReassignment Predicate indicating which partition reassignments should be removed
   */
  private def maybeRemoveFromZkReassignment(shouldRemoveReassignment: (TopicPartition, Seq[Int]) => Boolean): Unit = {
    if (!zkClient.reassignPartitionsInProgress)
      return

    val reassigningPartitions = zkClient.getPartitionReassignment
    val (removingPartitions, updatedPartitionsBeingReassigned) = reassigningPartitions.partition { case (tp, replicas) =>
      shouldRemoveReassignment(tp, replicas)
    }
    info(s"Removing partitions $removingPartitions from the list of reassigned partitions in zookeeper")

    // write the new list to zookeeper
    if (updatedPartitionsBeingReassigned.isEmpty) {
      info(s"No more partitions need to be reassigned. Deleting zk path ${ReassignPartitionsZNode.path}")
      zkClient.deletePartitionReassignment(controllerContext.epochZkVersion)
      // Ensure we detect future reassignments
      eventManager.put(ZkPartitionReassignment)
    } else {
      try {
        zkClient.setOrCreatePartitionReassignment(updatedPartitionsBeingReassigned, controllerContext.epochZkVersion)
      } catch {
        case e: KeeperException => throw new AdminOperationException(e)
      }
    }
  }

  private def removePartitionsFromPreferredReplicaElection(partitionsToBeRemoved: Set[TopicPartition],
                                                           isTriggeredByAutoRebalance : Boolean): Unit = {
    for (partition <- partitionsToBeRemoved) {
      // check the status
      val currentLeader = controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader
      val preferredReplica = controllerContext.partitionReplicaAssignment(partition).head
      if (currentLeader == preferredReplica) {
        info(s"Partition $partition completed preferred replica leader election. New leader is $preferredReplica")
      } else {
        warn(s"Partition $partition failed to complete preferred replica leader election to $preferredReplica. " +
          s"Leader is still $currentLeader")
      }
    }
    if (!isTriggeredByAutoRebalance) {
      zkClient.deletePreferredReplicaElection(controllerContext.epochZkVersion)
      // Ensure we detect future preferred replica leader elections
      eventManager.put(ReplicaLeaderElection(None, ElectionType.PREFERRED, ZkTriggered))
    }
  }

  /**
   * Send the leader information for selected partitions to selected brokers so that they can correctly respond to
   * metadata requests
   *
   * @param brokers The brokers that the update metadata request should be sent to
   */
  private[controller] def sendUpdateMetadataRequest(brokers: Seq[Int], partitions: Set[TopicPartition]): Unit = {
    try {
      brokerRequestBatch.newBatch()
      brokerRequestBatch.addUpdateMetadataRequestForBrokers(brokers, partitions)
      // 发送元数据更新请求给其他 broker
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
  }

  /**
   * Does not change leader or isr, but just increments the leader epoch
   *
   * @param partition partition
   * @return the new leaderAndIsr with an incremented leader epoch, or None if leaderAndIsr is empty.
   */
  private def updateLeaderEpoch(partition: TopicPartition): Option[LeaderIsrAndControllerEpoch] = {
    debug(s"Updating leader epoch for partition $partition")
    var finalLeaderIsrAndControllerEpoch: Option[LeaderIsrAndControllerEpoch] = None
    var zkWriteCompleteOrUnnecessary = false
    while (!zkWriteCompleteOrUnnecessary) {
      // refresh leader and isr from zookeeper again
      zkWriteCompleteOrUnnecessary = zkClient.getTopicPartitionStates(Seq(partition)).get(partition) match {
        case Some(leaderIsrAndControllerEpoch) =>
          val leaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
          val controllerEpoch = leaderIsrAndControllerEpoch.controllerEpoch
          if (controllerEpoch > epoch)
            throw new StateChangeFailedException("Leader and isr path written by another controller. This probably " +
              s"means the current controller with epoch $epoch went through a soft failure and another " +
              s"controller was elected with epoch $controllerEpoch. Aborting state change by this controller")
          // increment the leader epoch even if there are no leader or isr changes to allow the leader to cache the expanded
          // assigned replica list
          val newLeaderAndIsr = leaderAndIsr.newEpochAndZkVersion
          // update the new leadership decision in zookeeper or retry
          val UpdateLeaderAndIsrResult(finishedUpdates, _) =
            zkClient.updateLeaderAndIsr(immutable.Map(partition -> newLeaderAndIsr), epoch, controllerContext.epochZkVersion)

          finishedUpdates.get(partition) match {
            case Some(Right(leaderAndIsr)) =>
              val leaderIsrAndControllerEpoch = LeaderIsrAndControllerEpoch(leaderAndIsr, epoch)
              controllerContext.putPartitionLeadershipInfo(partition, leaderIsrAndControllerEpoch)
              finalLeaderIsrAndControllerEpoch = Some(leaderIsrAndControllerEpoch)
              info(s"Updated leader epoch for partition $partition to ${leaderAndIsr.leaderEpoch}")
              true
            case Some(Left(e)) => throw e
            case None => false
          }
        case None =>
          throw new IllegalStateException(s"Cannot update leader epoch for partition $partition as " +
            "leaderAndIsr path is empty. This could mean we somehow tried to reassign a partition that doesn't exist")
      }
    }
    finalLeaderIsrAndControllerEpoch
  }

  private def checkAndTriggerAutoLeaderRebalance(): Unit = {
    trace("Checking need to trigger auto leader balancing")
    val preferredReplicasForTopicsByBrokers: Map[Int, Map[TopicPartition, Seq[Int]]] =
      controllerContext.allPartitions.filterNot {
        tp => topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic)
      }.map { tp =>
        (tp, controllerContext.partitionReplicaAssignment(tp) )
      }.toMap.groupBy { case (_, assignedReplicas) => assignedReplicas.head }

    // for each broker, check if a preferred replica election needs to be triggered
    preferredReplicasForTopicsByBrokers.forKeyValue { (leaderBroker, topicPartitionsForBroker) =>
      val topicsNotInPreferredReplica = topicPartitionsForBroker.filter { case (topicPartition, _) =>
        val leadershipInfo = controllerContext.partitionLeadershipInfo(topicPartition)
        leadershipInfo.exists(_.leaderAndIsr.leader != leaderBroker)
      }
      debug(s"Topics not in preferred replica for broker $leaderBroker $topicsNotInPreferredReplica")

      val imbalanceRatio = topicsNotInPreferredReplica.size.toDouble / topicPartitionsForBroker.size
      trace(s"Leader imbalance ratio for broker $leaderBroker is $imbalanceRatio")

      // check ratio and if greater than desired ratio, trigger a rebalance for the topic partitions
      // that need to be on this broker
      if (imbalanceRatio > (config.leaderImbalancePerBrokerPercentage.toDouble / 100)) {
        // do this check only if the broker is live and there are no partitions being reassigned currently
        // and preferred replica election is not in progress
        val candidatePartitions = topicsNotInPreferredReplica.keys.filter(tp =>
          controllerContext.partitionsBeingReassigned.isEmpty &&
          !topicDeletionManager.isTopicQueuedUpForDeletion(tp.topic) &&
          controllerContext.allTopics.contains(tp.topic) &&
          canPreferredReplicaBeLeader(tp)
       )
        onReplicaElection(candidatePartitions.toSet, ElectionType.PREFERRED, AutoTriggered)
      }
    }
  }

  private def canPreferredReplicaBeLeader(tp: TopicPartition): Boolean = {
    val assignment = controllerContext.partitionReplicaAssignment(tp)
    val liveReplicas = assignment.filter(replica => controllerContext.isReplicaOnline(replica, tp))
    val isr = controllerContext.partitionLeadershipInfo(tp).get.leaderAndIsr.isr
    PartitionLeaderElectionAlgorithms
      .preferredReplicaPartitionLeaderElection(assignment, isr, liveReplicas.toSet)
      .nonEmpty
  }

  private def processAutoPreferredReplicaLeaderElection(): Unit = {
    if (!isActive) return
    try {
      info("Processing automatic preferred replica leader election")
      checkAndTriggerAutoLeaderRebalance()
    } finally {
      scheduleAutoLeaderRebalanceTask(delay = config.leaderImbalanceCheckIntervalSeconds, unit = TimeUnit.SECONDS)
    }
  }

  private def processUncleanLeaderElectionEnable(): Unit = {
    if (!isActive) return
    info("Unclean leader election has been enabled by default")
    partitionStateMachine.triggerOnlinePartitionStateChange()
  }

  private def processTopicUncleanLeaderElectionEnable(topic: String): Unit = {
    if (!isActive) return
    info(s"Unclean leader election has been enabled for topic $topic")
    partitionStateMachine.triggerOnlinePartitionStateChange(topic)
  }

  private def processControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit): Unit = {
    val controlledShutdownResult = Try { doControlledShutdown(id, brokerEpoch) }
    controlledShutdownCallback(controlledShutdownResult)
  }

  private def doControlledShutdown(id: Int, brokerEpoch: Long): Set[TopicPartition] = {
    if (!isActive) {
      throw new ControllerMovedException("Controller moved to another broker. Aborting controlled shutdown")
    }

    // broker epoch in the request is unknown if the controller hasn't been upgraded to use KIP-380
    // so we will keep the previous behavior and don't reject the request
    if (brokerEpoch != AbstractControlRequest.UNKNOWN_BROKER_EPOCH) {
      val cachedBrokerEpoch = controllerContext.liveBrokerIdAndEpochs(id)
      if (brokerEpoch < cachedBrokerEpoch) {
        val stateBrokerEpochErrorMessage = "Received controlled shutdown request from an old broker epoch " +
          s"$brokerEpoch for broker $id. Current broker epoch is $cachedBrokerEpoch."
        info(stateBrokerEpochErrorMessage)
        throw new StaleBrokerEpochException(stateBrokerEpochErrorMessage)
      }
    }

    info(s"Shutting down broker $id")

    if (!controllerContext.liveOrShuttingDownBrokerIds.contains(id))
      throw new BrokerNotAvailableException(s"Broker id $id does not exist.")

    controllerContext.shuttingDownBrokerIds.add(id)
    debug(s"All shutting down brokers: ${controllerContext.shuttingDownBrokerIds.mkString(",")}")
    debug(s"Live brokers: ${controllerContext.liveBrokerIds.mkString(",")}")

    val partitionsToActOn = controllerContext.partitionsOnBroker(id).filter { partition =>
      controllerContext.partitionReplicaAssignment(partition).size > 1 &&
        controllerContext.partitionLeadershipInfo(partition).isDefined &&
        !topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic)
    }
    val (partitionsLedByBroker, partitionsFollowedByBroker) = partitionsToActOn.partition { partition =>
      controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader == id
    }
    partitionStateMachine.handleStateChanges(partitionsLedByBroker.toSeq, OnlinePartition, Some(ControlledShutdownPartitionLeaderElectionStrategy))
    try {
      brokerRequestBatch.newBatch()
      partitionsFollowedByBroker.foreach { partition =>
        brokerRequestBatch.addStopReplicaRequestForBrokers(Seq(id), partition, deletePartition = false)
      }
      brokerRequestBatch.sendRequestsToBrokers(epoch)
    } catch {
      case e: IllegalStateException =>
        handleIllegalState(e)
    }
    // If the broker is a follower, updates the isr in ZK and notifies the current leader
    replicaStateMachine.handleStateChanges(partitionsFollowedByBroker.map(partition =>
      PartitionAndReplica(partition, id)).toSeq, OfflineReplica)
    trace(s"All leaders = ${controllerContext.partitionsLeadershipInfo.mkString(",")}")
    controllerContext.partitionLeadersOnBroker(id)
  }

  private def processUpdateMetadataResponseReceived(updateMetadataResponse: UpdateMetadataResponse, brokerId: Int): Unit = {
    if (!isActive) return

    if (updateMetadataResponse.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${updateMetadataResponse.error} in UpdateMetadata " +
        s"response $updateMetadataResponse from broker $brokerId")
    }
  }

  private def processLeaderAndIsrResponseReceived(leaderAndIsrResponse: LeaderAndIsrResponse, brokerId: Int): Unit = {
    if (!isActive) return

    if (leaderAndIsrResponse.error != Errors.NONE) {
      stateChangeLogger.error(s"Received error ${leaderAndIsrResponse.error} in LeaderAndIsr " +
        s"response $leaderAndIsrResponse from broker $brokerId")
      return
    }

    val offlineReplicas = new ArrayBuffer[TopicPartition]()
    val onlineReplicas = new ArrayBuffer[TopicPartition]()

    leaderAndIsrResponse.partitions.forEach { partition =>
      val tp = new TopicPartition(partition.topicName, partition.partitionIndex)
      if (partition.errorCode == Errors.KAFKA_STORAGE_ERROR.code)
        offlineReplicas += tp
      else if (partition.errorCode == Errors.NONE.code)
        onlineReplicas += tp
    }

    val previousOfflineReplicas = controllerContext.replicasOnOfflineDirs.getOrElse(brokerId, Set.empty[TopicPartition])
    val currentOfflineReplicas = mutable.Set() ++= previousOfflineReplicas --= onlineReplicas ++= offlineReplicas
    controllerContext.replicasOnOfflineDirs.put(brokerId, currentOfflineReplicas)
    val newOfflineReplicas = currentOfflineReplicas.diff(previousOfflineReplicas)

    if (newOfflineReplicas.nonEmpty) {
      stateChangeLogger.info(s"Mark replicas ${newOfflineReplicas.mkString(",")} on broker $brokerId as offline")
      onReplicasBecomeOffline(newOfflineReplicas.map(PartitionAndReplica(_, brokerId)))
    }
  }

  private def processTopicDeletionStopReplicaResponseReceived(replicaId: Int,
                                                              requestError: Errors,
                                                              partitionErrors: Map[TopicPartition, Errors]): Unit = {
    if (!isActive) return
    debug(s"Delete topic callback invoked on StopReplica response received from broker $replicaId: " +
      s"request error = $requestError, partition errors = $partitionErrors")

    val partitionsInError = if (requestError != Errors.NONE)
      partitionErrors.keySet
    else
      partitionErrors.filter { case (_, error) => error != Errors.NONE }.keySet

    val replicasInError = partitionsInError.map(PartitionAndReplica(_, replicaId))
    // move all the failed replicas to ReplicaDeletionIneligible
    topicDeletionManager.failReplicaDeletion(replicasInError)
    if (replicasInError.size != partitionErrors.size) {
      // some replicas could have been successfully deleted
      val deletedReplicas = partitionErrors.keySet.diff(partitionsInError)
      topicDeletionManager.completeReplicaDeletion(deletedReplicas.map(PartitionAndReplica(_, replicaId)))
    }
  }

  private def processStartup(): Unit = {
    // 在执行选举之前会在向 zk 的 controller 目录下注册一个 controller 变更事件，
    // 订阅临时节点上数据改变事件。选举器在处理 /controller 节点被删除，以及控制器
    // 处理会话失效，都会调用选举器的 elect 方法重新选举主控制器。
    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
    // 执行选举过程
    elect()
  }

  private def updateMetrics(): Unit = {
    offlinePartitionCount =
      if (!isActive) {
        0
      } else {
        controllerContext.offlinePartitionCount
      }

    preferredReplicaImbalanceCount =
      if (!isActive) {
        0
      } else {
        controllerContext.preferredReplicaImbalanceCount
      }

    globalTopicCount = if (!isActive) 0 else controllerContext.allTopics.size

    globalPartitionCount = if (!isActive) 0 else controllerContext.partitionWithLeadersCount

    topicsToDeleteCount = if (!isActive) 0 else controllerContext.topicsToBeDeleted.size

    replicasToDeleteCount = if (!isActive) 0 else controllerContext.topicsToBeDeleted.map { topic =>
      // For each enqueued topic, count the number of replicas that are not yet deleted
      controllerContext.replicasForTopic(topic).count { replica =>
        controllerContext.replicaState(replica) != ReplicaDeletionSuccessful
      }
    }.sum

    ineligibleTopicsToDeleteCount = if (!isActive) 0 else controllerContext.topicsIneligibleForDeletion.size

    ineligibleReplicasToDeleteCount = if (!isActive) 0 else controllerContext.topicsToBeDeleted.map { topic =>
      // For each enqueued topic, count the number of replicas that are ineligible
      controllerContext.replicasForTopic(topic).count { replica =>
        controllerContext.replicaState(replica) == ReplicaDeletionIneligible
      }
    }.sum
  }

  // visible for testing
  private[controller] def handleIllegalState(e: IllegalStateException): Nothing = {
    // Resign if the controller is in an illegal state
    error("Forcing the controller to resign")
    brokerRequestBatch.clear()
    triggerControllerMove()
    throw e
  }

  private def triggerControllerMove(): Unit = {
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    if (!isActive) {
      warn("Controller has already moved when trying to trigger controller movement")
      return
    }
    try {
      val expectedControllerEpochZkVersion = controllerContext.epochZkVersion
      activeControllerId = -1
      onControllerResignation()
      zkClient.deleteController(expectedControllerEpochZkVersion)
    } catch {
      case _: ControllerMovedException =>
        warn("Controller has already moved when trying to trigger controller movement")
    }
  }

  private def maybeResign(): Unit = {
    val wasActiveBeforeChange = isActive
    zkClient.registerZNodeChangeHandlerAndCheckExistence(controllerChangeHandler)
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    if (wasActiveBeforeChange && !isActive) {
      onControllerResignation()
    }
  }

  /**
   * 主控制器选举机制
   *  利用了 zk 的临时节点，每隔代理节点都会参与竞选主控制器，但只有一个代理节点可以成为主控制器，其他代理节点只有在主控制器出现故障或
   * 会话失效时参与选举。每个代理节点都会作为 zk 的客户端，向 zk 服务端尝试创建 /controller 临时节点，但最终于只有一个代理节点可以
   * 成功创建，创建成功的即为主控制器。
   *  由于主控制器创建的 zk 临时节点，因此当主控制器故障或会话失效，临时节点就会被删除。这时所有的其他代理节点都会重新尝试创建
   *  /controller 节点，并选举出新的主控制器。
   *
   * 调用路径 [[KafkaServer.startup()]] --> [[KafkaController.startup()]] --> 向 eventManager 中注册了 [[Startup]] 事件
   * （保存到事件队列中）并启动 ControllerEventThread 线程  --> 从 [[ControllerEventManager.queue]] 队列中取出事件并执行
   * --> [[KafkaController.process()]] --> [[KafkaController.processStartup()]]
   * 此外，除了上面的正常选举，在会话失效或者临时节点被删除时，代理节点都会重新选举主控制器。
   */
  private def elect(): Unit = {
    // 从 controller 目录下获取数，如果获取到了数据，返回一个 id 号，这个 id 号就是某个 broker 的 id 号，也就是这个 broker 就是 controller
    activeControllerId = zkClient.getControllerId.getOrElse(-1)
    /*
     * We can get here during the initial startup and the handleDeleted ZK callback. Because of the potential race condition,
     * it's possible that the controller has already been elected when we get here. This check will prevent the following
     * createEphemeralPath method from getting into an infinite loop if this broker is already the controller.
     */
    // 之前有 broker 已经在 zk 上写过，直接返回
    if (activeControllerId != -1) {
      debug(s"Broker $activeControllerId has been elected as the controller, so stopping the election process.")
      return
    }

    // 首次启动
    try {
      // 在 zk 上创建目录，这里可能会有多个代理节点来竞争，但是只有一个会成功，失败的会抛出异常被捕获
      val (epoch, epochZkVersion) = zkClient.registerControllerAndIncrementControllerEpoch(config.brokerId)
      controllerContext.epoch = epoch
      controllerContext.epochZkVersion = epochZkVersion
      // 当前的 broker 就是 controller
      activeControllerId = config.brokerId

      info(s"${config.brokerId} successfully elected as the controller. Epoch incremented to ${controllerContext.epoch} " +
        s"and epoch zk version is now ${controllerContext.epochZkVersion}")

      // 初始化方法执行的一些准备工作，只有成功创建临时节点的代理节点才会调用该方法
      onControllerFailover()
    } catch {
      case e: ControllerMovedException =>
        maybeResign()

        if (activeControllerId != -1)
          debug(s"Broker $activeControllerId was elected as controller instead of broker ${config.brokerId}", e)
        else
          warn("A controller has been elected but just resigned, this will result in another round of election", e)
      case t: Throwable =>
        error(s"Error while electing or becoming controller on broker ${config.brokerId}. " +
          s"Trigger controller movement immediately", t)
        triggerControllerMove()
    }
  }

  /**
   * Partitions the provided map of brokers and epochs into 2 new maps:
   *  - The first map contains only those brokers whose features were found to be compatible with
   *    the existing finalized features.
   *  - The second map contains only those brokers whose features were found to be incompatible with
   *    the existing finalized features.
   *
   * @param brokersAndEpochs   the map to be partitioned
   * @return                   two maps: first contains compatible brokers and second contains
   *                           incompatible brokers as explained above
   */
  private def partitionOnFeatureCompatibility(brokersAndEpochs: Map[Broker, Long]): (Map[Broker, Long], Map[Broker, Long]) = {
    // There can not be any feature incompatibilities when the feature versioning system is disabled
    // or when the finalized feature cache is empty. Otherwise, we check if the non-empty contents
    //  of the cache are compatible with the supported features of each broker.
    brokersAndEpochs.partition {
      case (broker, _) =>
        !config.isFeatureVersioningSupported ||
        !featureCache.get.exists(
          latestFinalizedFeatures =>
            BrokerFeatures.hasIncompatibleFeatures(broker.features, latestFinalizedFeatures.features))
    }
  }

  /**
   * 处理 broker 变更，如 broker 新加入集群或退出集群
   */
  private def processBrokerChange(): Unit = {
    if (!isActive) return
    // 获取当前集群所有的 broker
    val curBrokerAndEpochs = zkClient.getAllBrokerAndEpochsInCluster
    // 获取当前集群所有的 brokerId
    val curBrokerIdAndEpochs = curBrokerAndEpochs map { case (broker, epoch) => (broker.id, epoch) }
    val curBrokerIds = curBrokerIdAndEpochs.keySet
    // 获取到所有 live 的 broker
    val liveOrShuttingDownBrokerIds = controllerContext.liveOrShuttingDownBrokerIds
    // 获取所有新加入的 broker
    val newBrokerIds = curBrokerIds.diff(liveOrShuttingDownBrokerIds)
    // 获取所有宕机的 broker
    val deadBrokerIds = liveOrShuttingDownBrokerIds.diff(curBrokerIds)
    val bouncedBrokerIds = (curBrokerIds & liveOrShuttingDownBrokerIds)
      .filter(brokerId => curBrokerIdAndEpochs(brokerId) > controllerContext.liveBrokerIdAndEpochs(brokerId))
    val newBrokerAndEpochs = curBrokerAndEpochs.filter { case (broker, _) => newBrokerIds.contains(broker.id) }
    val bouncedBrokerAndEpochs = curBrokerAndEpochs.filter { case (broker, _) => bouncedBrokerIds.contains(broker.id) }
    val newBrokerIdsSorted = newBrokerIds.toSeq.sorted
    val deadBrokerIdsSorted = deadBrokerIds.toSeq.sorted
    val liveBrokerIdsSorted = curBrokerIds.toSeq.sorted
    val bouncedBrokerIdsSorted = bouncedBrokerIds.toSeq.sorted
    info(s"Newly added brokers: ${newBrokerIdsSorted.mkString(",")}, " +
      s"deleted brokers: ${deadBrokerIdsSorted.mkString(",")}, " +
      s"bounced brokers: ${bouncedBrokerIdsSorted.mkString(",")}, " +
      s"all live brokers: ${liveBrokerIdsSorted.mkString(",")}")

    // 更新网络通道管理器管理代理节点的信息，用于发送网络请求给服务端节点
    newBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)
    bouncedBrokerIds.foreach(controllerChannelManager.removeBroker)
    bouncedBrokerAndEpochs.keySet.foreach(controllerChannelManager.addBroker)
    deadBrokerIds.foreach(controllerChannelManager.removeBroker)

    // newBrokerIds 新注册的 id 不为空表示有集群里有新 broker 注册
    if (newBrokerIds.nonEmpty) {
      val (newCompatibleBrokerAndEpochs, newIncompatibleBrokerAndEpochs) =
        partitionOnFeatureCompatibility(newBrokerAndEpochs)
      if (!newIncompatibleBrokerAndEpochs.isEmpty) {
        warn("Ignoring registration of new brokers due to incompatibilities with finalized features: " +
          newIncompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
      }
      controllerContext.addLiveBrokers(newCompatibleBrokerAndEpochs)
      // 处理刚注册的 broker
      onBrokerStartup(newBrokerIdsSorted)
    }
    if (bouncedBrokerIds.nonEmpty) {
      controllerContext.removeLiveBrokers(bouncedBrokerIds)
      onBrokerFailure(bouncedBrokerIdsSorted)
      val (bouncedCompatibleBrokerAndEpochs, bouncedIncompatibleBrokerAndEpochs) =
        partitionOnFeatureCompatibility(bouncedBrokerAndEpochs)
      if (!bouncedIncompatibleBrokerAndEpochs.isEmpty) {
        warn("Ignoring registration of bounced brokers due to incompatibilities with finalized features: " +
          bouncedIncompatibleBrokerAndEpochs.map { case (broker, _) => broker.id }.toSeq.sorted.mkString(","))
      }
      controllerContext.addLiveBrokers(bouncedCompatibleBrokerAndEpochs)
      onBrokerStartup(bouncedBrokerIdsSorted)
    }
    // deadBrokerIds 宕机的 id 不为空，表示有 broker 离开集群
    if (deadBrokerIds.nonEmpty) {
      controllerContext.removeLiveBrokers(deadBrokerIds)
      onBrokerFailure(deadBrokerIdsSorted)
    }

    if (newBrokerIds.nonEmpty || deadBrokerIds.nonEmpty || bouncedBrokerIds.nonEmpty) {
      info(s"Updated broker epochs cache: ${controllerContext.liveBrokerIdAndEpochs}")
    }
  }

  private def processBrokerModification(brokerId: Int): Unit = {
    if (!isActive) return
    val newMetadataOpt = zkClient.getBroker(brokerId)
    val oldMetadataOpt = controllerContext.liveOrShuttingDownBroker(brokerId)
    if (newMetadataOpt.nonEmpty && oldMetadataOpt.nonEmpty) {
      val oldMetadata = oldMetadataOpt.get
      val newMetadata = newMetadataOpt.get
      if (newMetadata.endPoints != oldMetadata.endPoints || !oldMetadata.features.equals(newMetadata.features)) {
        info(s"Updated broker metadata: $oldMetadata -> $newMetadata")
        controllerContext.updateBrokerMetadata(oldMetadata, newMetadata)
        onBrokerUpdate(brokerId)
      }
    }
  }

  // 处理主题变更，这可能是分区重分配的驱动之一
  private def processTopicChange(): Unit = {
    if (!isActive) return
    // 从 zk 中获取所有 topic，zk 里的数据比内容中的新
    val topics = zkClient.getAllTopicsInCluster(true)
    // 新增的 topic
    val newTopics = topics -- controllerContext.allTopics
    // 删除的 topic
    val deletedTopics = controllerContext.allTopics.diff(topics)
    controllerContext.setAllTopics(topics)

    registerPartitionModificationsHandlers(newTopics.toSeq)
    // 从 ZK 读取 topic 的分配方案，其实就是元数据信息
    val addedPartitionReplicaAssignment = zkClient.getFullReplicaAssignmentForTopics(newTopics)
    // 清触被删除的 topic
    deletedTopics.foreach(controllerContext.removeTopic)
    addedPartitionReplicaAssignment.foreach {
      case (topicAndPartition, newReplicaAssignment) => controllerContext.updatePartitionFullReplicaAssignment(topicAndPartition, newReplicaAssignment)
    }
    info(s"New topics: [$newTopics], deleted topics: [$deletedTopics], new partition replica assignment " +
      s"[$addedPartitionReplicaAssignment]")
    if (addedPartitionReplicaAssignment.nonEmpty) {
      // 新 topic 注册了，元数据发生了变化
      onNewPartitionCreation(addedPartitionReplicaAssignment.keySet)
    }
  }

  private def processLogDirEventNotification(): Unit = {
    if (!isActive) return
    val sequenceNumbers = zkClient.getAllLogDirEventNotifications
    try {
      val brokerIds = zkClient.getBrokerIdsFromLogDirEvents(sequenceNumbers)
      onBrokerLogDirFailure(brokerIds)
    } finally {
      // delete processed children
      zkClient.deleteLogDirEventNotifications(sequenceNumbers, controllerContext.epochZkVersion)
    }
  }

  private def processPartitionModifications(topic: String): Unit = {
    def restorePartitionReplicaAssignment(
      topic: String,
      newPartitionReplicaAssignment: Map[TopicPartition, ReplicaAssignment]
    ): Unit = {
      info("Restoring the partition replica assignment for topic %s".format(topic))

      val existingPartitions = zkClient.getChildren(TopicPartitionsZNode.path(topic))
      val existingPartitionReplicaAssignment = newPartitionReplicaAssignment
        .filter(p => existingPartitions.contains(p._1.partition.toString))
        .map { case (tp, _) =>
          tp -> controllerContext.partitionFullReplicaAssignment(tp)
      }.toMap

      zkClient.setTopicAssignment(topic,
        existingPartitionReplicaAssignment,
        controllerContext.epochZkVersion)
    }

    if (!isActive) return
    val partitionReplicaAssignment = zkClient.getFullReplicaAssignmentForTopics(immutable.Set(topic))
    val partitionsToBeAdded = partitionReplicaAssignment.filter { case (topicPartition, _) =>
      controllerContext.partitionReplicaAssignment(topicPartition).isEmpty
    }

    if (topicDeletionManager.isTopicQueuedUpForDeletion(topic)) {
      if (partitionsToBeAdded.nonEmpty) {
        warn("Skipping adding partitions %s for topic %s since it is currently being deleted"
          .format(partitionsToBeAdded.map(_._1.partition).mkString(","), topic))

        restorePartitionReplicaAssignment(topic, partitionReplicaAssignment)
      } else {
        // This can happen if existing partition replica assignment are restored to prevent increasing partition count during topic deletion
        info("Ignoring partition change during topic deletion as no new partitions are added")
      }
    } else if (partitionsToBeAdded.nonEmpty) {
      info(s"New partitions to be added $partitionsToBeAdded")
      partitionsToBeAdded.forKeyValue { (topicPartition, assignedReplicas) =>
        controllerContext.updatePartitionFullReplicaAssignment(topicPartition, assignedReplicas)
      }
      onNewPartitionCreation(partitionsToBeAdded.keySet)
    }
  }

  private def processTopicDeletion(): Unit = {
    if (!isActive) return
    // 从 /admin/delete_topics/ 下获取要删除的主题
    var topicsToBeDeleted = zkClient.getTopicDeletions.toSet
    debug(s"Delete topics listener fired for topics ${topicsToBeDeleted.mkString(",")} to be deleted")
    // 删除不存在的主题
    val nonExistentTopics = topicsToBeDeleted -- controllerContext.allTopics
    if (nonExistentTopics.nonEmpty) {
      warn(s"Ignoring request to delete non-existing topics ${nonExistentTopics.mkString(",")}")
      // 因为主题不存在，直接将 /admin/delete_topics/ 下对应的节点删除即可，不需要做任何处理
      zkClient.deleteTopicDeletions(nonExistentTopics.toSeq, controllerContext.epochZkVersion)
    }
    // 要删除的主题
    topicsToBeDeleted --= nonExistentTopics
    if (config.deleteTopicEnable) {
      if (topicsToBeDeleted.nonEmpty) {
        info(s"Starting topic deletion for topics ${topicsToBeDeleted.mkString(",")}")
        // mark topic ineligible for deletion if other state changes are in progress
        topicsToBeDeleted.foreach { topic =>
          val partitionReassignmentInProgress =
            controllerContext.partitionsBeingReassigned.map(_.topic).contains(topic)
          if (partitionReassignmentInProgress)
            topicDeletionManager.markTopicIneligibleForDeletion(Set(topic),
              reason = "topic reassignment in progress")
        }
        // add topic to deletion list
        // 添加到待删除的主题集合中，执行后续删除操作
        topicDeletionManager.enqueueTopicsForDeletion(topicsToBeDeleted)
      }
    } else {
      // If delete topic is disabled remove entries under zookeeper path : /admin/delete_topics
      info(s"Removing $topicsToBeDeleted since delete topic is disabled")
      // 不允许删除主题，同样将 /admin/delete_topics/ 下对应的节点删除即可，不需要做任何处理
      zkClient.deleteTopicDeletions(topicsToBeDeleted.toSeq, controllerContext.epochZkVersion)
    }
  }

  private def processZkPartitionReassignment(): Set[TopicPartition] = {
    // We need to register the watcher if the path doesn't exist in order to detect future
    // reassignments and we get the `path exists` check for free
    if (isActive && zkClient.registerZNodeChangeHandlerAndCheckExistence(partitionReassignmentHandler)) {
      val reassignmentResults = mutable.Map.empty[TopicPartition, ApiError]
      val partitionsToReassign = mutable.Map.empty[TopicPartition, ReplicaAssignment]

      zkClient.getPartitionReassignment.forKeyValue { (tp, targetReplicas) =>
        maybeBuildReassignment(tp, Some(targetReplicas)) match {
          case Some(context) => partitionsToReassign.put(tp, context)
          case None => reassignmentResults.put(tp, new ApiError(Errors.NO_REASSIGNMENT_IN_PROGRESS))
        }
      }

      reassignmentResults ++= maybeTriggerPartitionReassignment(partitionsToReassign)
      val (partitionsReassigned, partitionsFailed) = reassignmentResults.partition(_._2.error == Errors.NONE)
      if (partitionsFailed.nonEmpty) {
        warn(s"Failed reassignment through zk with the following errors: $partitionsFailed")
        maybeRemoveFromZkReassignment((tp, _) => partitionsFailed.contains(tp))
      }
      partitionsReassigned.keySet
    } else {
      Set.empty
    }
  }

  /**
   * Process a partition reassignment from the AlterPartitionReassignment API. If there is an
   * existing reassignment through zookeeper for any of the requested partitions, they will be
   * cancelled prior to beginning the new reassignment. Any zk-based reassignment for partitions
   * which are NOT included in this call will not be affected.
   *
   * @param reassignments Map of reassignments passed through the AlterReassignments API. A null value
   *                      means that we should cancel an in-progress reassignment.
   * @param callback Callback to send AlterReassignments response
   */
  private def processApiPartitionReassignment(reassignments: Map[TopicPartition, Option[Seq[Int]]],
                                              callback: AlterReassignmentsCallback): Unit = {
    if (!isActive) {
      callback(Right(new ApiError(Errors.NOT_CONTROLLER)))
    } else {
      val reassignmentResults = mutable.Map.empty[TopicPartition, ApiError]
      val partitionsToReassign = mutable.Map.empty[TopicPartition, ReplicaAssignment]

      reassignments.forKeyValue { (tp, targetReplicas) =>
        val maybeApiError = targetReplicas.flatMap(validateReplicas(tp, _))
        maybeApiError match {
          case None =>
            maybeBuildReassignment(tp, targetReplicas) match {
              case Some(context) => partitionsToReassign.put(tp, context)
              case None => reassignmentResults.put(tp, new ApiError(Errors.NO_REASSIGNMENT_IN_PROGRESS))
            }
          case Some(err) =>
            reassignmentResults.put(tp, err)
        }
      }

      // The latest reassignment (whether by API or through zk) always takes precedence,
      // so remove from active zk reassignment (if one exists)
      maybeRemoveFromZkReassignment((tp, _) => partitionsToReassign.contains(tp))

      reassignmentResults ++= maybeTriggerPartitionReassignment(partitionsToReassign)
      callback(Left(reassignmentResults))
    }
  }

  private def validateReplicas(topicPartition: TopicPartition, replicas: Seq[Int]): Option[ApiError] = {
    val replicaSet = replicas.toSet
    if (replicas.isEmpty)
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
          s"Empty replica list specified in partition reassignment."))
    else if (replicas.size != replicaSet.size) {
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
          s"Duplicate replica ids in partition reassignment replica list: $replicas"))
    } else if (replicas.exists(_ < 0))
      Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
          s"Invalid broker id in replica list: $replicas"))
    else {
      // Ensure that any new replicas are among the live brokers
      val currentAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
      val newAssignment = currentAssignment.reassignTo(replicas)
      val areNewReplicasAlive = newAssignment.addingReplicas.toSet.subsetOf(controllerContext.liveBrokerIds)
      if (!areNewReplicasAlive)
        Some(new ApiError(Errors.INVALID_REPLICA_ASSIGNMENT,
          s"Replica assignment has brokers that are not alive. Replica list: " +
            s"${newAssignment.addingReplicas}, live broker list: ${controllerContext.liveBrokerIds}"))
      else None
    }
  }

  private def maybeBuildReassignment(topicPartition: TopicPartition,
                                     targetReplicasOpt: Option[Seq[Int]]): Option[ReplicaAssignment] = {
    val replicaAssignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
    if (replicaAssignment.isBeingReassigned) {
      val targetReplicas = targetReplicasOpt.getOrElse(replicaAssignment.originReplicas)
      Some(replicaAssignment.reassignTo(targetReplicas))
    } else {
      targetReplicasOpt.map { targetReplicas =>
        replicaAssignment.reassignTo(targetReplicas)
      }
    }
  }

  private def processPartitionReassignmentIsrChange(topicPartition: TopicPartition): Unit = {
    if (!isActive) return

    if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
      maybeCompleteReassignment(topicPartition)
    }
  }

  // processPartitionReassignmentIsrChange
  private def maybeCompleteReassignment(topicPartition: TopicPartition): Unit = {
    val reassignment = controllerContext.partitionFullReplicaAssignment(topicPartition)
    if (isReassignmentComplete(topicPartition, reassignment)) {
      // resume the partition reassignment process
      info(s"Target replicas ${reassignment.targetReplicas} have all caught up with the leader for " +
        s"reassigning partition $topicPartition")
      onPartitionReassignment(topicPartition, reassignment)
    }
  }

  private def processListPartitionReassignments(partitionsOpt: Option[Set[TopicPartition]], callback: ListReassignmentsCallback): Unit = {
    if (!isActive) {
      callback(Right(new ApiError(Errors.NOT_CONTROLLER)))
    } else {
      val results: mutable.Map[TopicPartition, ReplicaAssignment] = mutable.Map.empty
      val partitionsToList = partitionsOpt match {
        case Some(partitions) => partitions
        case None => controllerContext.partitionsBeingReassigned
      }

      partitionsToList.foreach { tp =>
        val assignment = controllerContext.partitionFullReplicaAssignment(tp)
        if (assignment.isBeingReassigned) {
          results += tp -> assignment
        }
      }

      callback(Left(results))
    }
  }

  /**
   * Returns the new FinalizedVersionRange for the feature, if there are no feature
   * incompatibilities seen with all known brokers for the provided feature update.
   * Otherwise returns an ApiError object containing Errors.INVALID_REQUEST.
   *
   * @param update   the feature update to be processed (this can not be meant to delete the feature)
   *
   * @return         the new FinalizedVersionRange or error, as described above.
   */
  private def newFinalizedVersionRangeOrIncompatibilityError(update: UpdateFeaturesRequestData.FeatureUpdateKey): Either[FinalizedVersionRange, ApiError] = {
    if (UpdateFeaturesRequest.isDeleteRequest(update)) {
      throw new IllegalArgumentException(s"Provided feature update can not be meant to delete the feature: $update")
    }

    val supportedVersionRange = brokerFeatures.supportedFeatures.get(update.feature)
    if (supportedVersionRange == null) {
      Right(new ApiError(Errors.INVALID_REQUEST,
                         "Could not apply finalized feature update because the provided feature" +
                         " is not supported."))
    } else {
      var newVersionRange: FinalizedVersionRange = null
      try {
        newVersionRange = new FinalizedVersionRange(supportedVersionRange.min, update.maxVersionLevel)
      } catch {
        case _: IllegalArgumentException => {
          // This exception means the provided maxVersionLevel is invalid. It is handled below
          // outside of this catch clause.
        }
      }
      if (newVersionRange == null) {
        Right(new ApiError(Errors.INVALID_REQUEST,
          "Could not apply finalized feature update because the provided" +
          s" maxVersionLevel:${update.maxVersionLevel} is lower than the" +
          s" supported minVersion:${supportedVersionRange.min}."))
      } else {
        val newFinalizedFeature =
          Features.finalizedFeatures(Utils.mkMap(Utils.mkEntry(update.feature, newVersionRange)))
        val numIncompatibleBrokers = controllerContext.liveOrShuttingDownBrokers.count(broker => {
          BrokerFeatures.hasIncompatibleFeatures(broker.features, newFinalizedFeature)
        })
        if (numIncompatibleBrokers == 0) {
          Left(newVersionRange)
        } else {
          Right(new ApiError(Errors.INVALID_REQUEST,
                             "Could not apply finalized feature update because" +
                             " brokers were found to have incompatible versions for the feature."))
        }
      }
    }
  }

  /**
   * Validates a feature update on an existing FinalizedVersionRange.
   * If the validation succeeds, then, the return value contains:
   * 1. the new FinalizedVersionRange for the feature, if the feature update was not meant to delete the feature.
   * 2. Option.empty, if the feature update was meant to delete the feature.
   *
   * If the validation fails, then returned value contains a suitable ApiError.
   *
   * @param update                 the feature update to be processed.
   * @param existingVersionRange   the existing FinalizedVersionRange which can be empty when no
   *                               FinalizedVersionRange exists for the associated feature
   *
   * @return                       the new FinalizedVersionRange to be updated into ZK or error
   *                               as described above.
   */
  private def validateFeatureUpdate(update: UpdateFeaturesRequestData.FeatureUpdateKey,
                                    existingVersionRange: Option[FinalizedVersionRange]): Either[Option[FinalizedVersionRange], ApiError] = {
    def newVersionRangeOrError(update: UpdateFeaturesRequestData.FeatureUpdateKey): Either[Option[FinalizedVersionRange], ApiError] = {
      newFinalizedVersionRangeOrIncompatibilityError(update)
        .fold(versionRange => Left(Some(versionRange)), error => Right(error))
    }

    if (update.feature.isEmpty) {
      // Check that the feature name is not empty.
      Right(new ApiError(Errors.INVALID_REQUEST, "Feature name can not be empty."))
    } else {
      // We handle deletion requests separately from non-deletion requests.
      if (UpdateFeaturesRequest.isDeleteRequest(update)) {
        if (existingVersionRange.isEmpty) {
          // Disallow deletion of a non-existing finalized feature.
          Right(new ApiError(Errors.INVALID_REQUEST,
                             "Can not delete non-existing finalized feature."))
        } else {
          Left(Option.empty)
        }
      } else if (update.maxVersionLevel() < 1) {
        // Disallow deletion of a finalized feature without allowDowngrade flag set.
        Right(new ApiError(Errors.INVALID_REQUEST,
                           s"Can not provide maxVersionLevel: ${update.maxVersionLevel} less" +
                           s" than 1 without setting the allowDowngrade flag to true in the request."))
      } else {
        existingVersionRange.map(existing =>
          if (update.maxVersionLevel == existing.max) {
            // Disallow a case where target maxVersionLevel matches existing maxVersionLevel.
            Right(new ApiError(Errors.INVALID_REQUEST,
                               s"Can not ${if (update.allowDowngrade) "downgrade" else "upgrade"}" +
                               s" a finalized feature from existing maxVersionLevel:${existing.max}" +
                               " to the same value."))
          } else if (update.maxVersionLevel < existing.max && !update.allowDowngrade) {
            // Disallow downgrade of a finalized feature without the allowDowngrade flag set.
            Right(new ApiError(Errors.INVALID_REQUEST,
                               s"Can not downgrade finalized feature from existing" +
                               s" maxVersionLevel:${existing.max} to provided" +
                               s" maxVersionLevel:${update.maxVersionLevel} without setting the" +
                               " allowDowngrade flag in the request."))
          } else if (update.allowDowngrade && update.maxVersionLevel > existing.max) {
            // Disallow a request that sets allowDowngrade flag without specifying a
            // maxVersionLevel that's lower than the existing maxVersionLevel.
            Right(new ApiError(Errors.INVALID_REQUEST,
                               s"When the allowDowngrade flag set in the request, the provided" +
                               s" maxVersionLevel:${update.maxVersionLevel} can not be greater than" +
                               s" existing maxVersionLevel:${existing.max}."))
          } else if (update.maxVersionLevel < existing.min) {
            // Disallow downgrade of a finalized feature below the existing finalized
            // minVersionLevel.
            Right(new ApiError(Errors.INVALID_REQUEST,
                               s"Can not downgrade finalized feature to maxVersionLevel:${update.maxVersionLevel}" +
                               s" because it's lower than the existing minVersionLevel:${existing.min}."))
          } else {
            newVersionRangeOrError(update)
          }
        ).getOrElse(newVersionRangeOrError(update))
      }
    }
  }

  private def processFeatureUpdates(request: UpdateFeaturesRequest,
                                    callback: UpdateFeaturesCallback): Unit = {
    if (isActive) {
      processFeatureUpdatesWithActiveController(request, callback)
    } else {
      callback(Left(new ApiError(Errors.NOT_CONTROLLER)))
    }
  }

  private def processFeatureUpdatesWithActiveController(request: UpdateFeaturesRequest,
                                                        callback: UpdateFeaturesCallback): Unit = {
    val updates = request.data.featureUpdates
    val existingFeatures = featureCache.get
      .map(featuresAndEpoch => featuresAndEpoch.features.features().asScala)
      .getOrElse(Map[String, FinalizedVersionRange]())
    // A map with key being feature name and value being FinalizedVersionRange.
    // This contains the target features to be eventually written to FeatureZNode.
    val targetFeatures = scala.collection.mutable.Map[String, FinalizedVersionRange]() ++ existingFeatures
    // A map with key being feature name and value being error encountered when the FeatureUpdate
    // was applied.
    val errors = scala.collection.mutable.Map[String, ApiError]()

    // Below we process each FeatureUpdate using the following logic:
    //  - If a FeatureUpdate is found to be valid, then:
    //    - The corresponding entry in errors map would be updated to contain Errors.NONE.
    //    - If the FeatureUpdate is an add or update request, then the targetFeatures map is updated
    //      to contain the new FinalizedVersionRange for the feature.
    //    - Otherwise if the FeatureUpdate is a delete request, then the feature is removed from the
    //      targetFeatures map.
    //  - Otherwise if a FeatureUpdate is found to be invalid, then:
    //    - The corresponding entry in errors map would be updated with the appropriate ApiError.
    //    - The entry in targetFeatures map is left untouched.
    updates.asScala.iterator.foreach { update =>
      validateFeatureUpdate(update, existingFeatures.get(update.feature())) match {
        case Left(newVersionRangeOrNone) =>
          newVersionRangeOrNone match {
            case Some(newVersionRange) => targetFeatures += (update.feature() -> newVersionRange)
            case None => targetFeatures -= update.feature()
          }
          errors += (update.feature() -> new ApiError(Errors.NONE))
        case Right(featureUpdateFailureReason) =>
          errors += (update.feature() -> featureUpdateFailureReason)
      }
    }

    // If the existing and target features are the same, then, we skip the update to the
    // FeatureZNode as no changes to the node are required. Otherwise, we replace the contents
    // of the FeatureZNode with the new features. This may result in partial or full modification
    // of the existing finalized features in ZK.
    try {
      if (!existingFeatures.equals(targetFeatures)) {
        val newNode = new FeatureZNode(FeatureZNodeStatus.Enabled, Features.finalizedFeatures(targetFeatures.asJava))
        val newVersion = updateFeatureZNode(newNode)
        featureCache.waitUntilEpochOrThrow(newVersion, request.data().timeoutMs())
      }
    } catch {
      // For all features that correspond to valid FeatureUpdate (i.e. error is Errors.NONE),
      // we set the error as Errors.FEATURE_UPDATE_FAILED since the FeatureZNode update has failed
      // for these. For the rest, the existing error is left untouched.
      case e: Exception =>
        warn(s"Processing of feature updates: $request failed due to error: $e")
        errors.foreach { case (feature, apiError) =>
          if (apiError.error() == Errors.NONE) {
            errors(feature) = new ApiError(Errors.FEATURE_UPDATE_FAILED)
          }
        }
    } finally {
      callback(Right(errors))
    }
  }

  private def processIsrChangeNotification(): Unit = {
    def processUpdateNotifications(partitions: Seq[TopicPartition]): Unit = {
      val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
      debug(s"Sending MetadataRequest to Brokers: $liveBrokers for TopicPartitions: $partitions")
      sendUpdateMetadataRequest(liveBrokers, partitions.toSet)
    }

    if (!isActive) return
    val sequenceNumbers = zkClient.getAllIsrChangeNotifications
    try {
      val partitions = zkClient.getPartitionsFromIsrChangeNotifications(sequenceNumbers)
      if (partitions.nonEmpty) {
        updateLeaderAndIsrCache(partitions)
        processUpdateNotifications(partitions)

        // During a partial upgrade, the controller may be on an IBP which assumes
        // ISR changes through the `AlterIsr` API while some brokers are on an older
        // IBP which assumes notification through Zookeeper. In this case, since the
        // controller will not have registered watches for reassigning partitions, we
        // can still rely on the batch ISR change notification path in order to
        // complete the reassignment.
        partitions.filter(controllerContext.partitionsBeingReassigned.contains).foreach { topicPartition =>
          maybeCompleteReassignment(topicPartition)
        }
      }
    } finally {
      // delete the notifications
      zkClient.deleteIsrChangeNotifications(sequenceNumbers, controllerContext.epochZkVersion)
    }
  }

  def electLeaders(
    partitions: Set[TopicPartition],
    electionType: ElectionType,
    callback: ElectLeadersCallback
  ): Unit = {
    eventManager.put(ReplicaLeaderElection(Some(partitions), electionType, AdminClientTriggered, callback))
  }

  def listPartitionReassignments(partitions: Option[Set[TopicPartition]],
                                 callback: ListReassignmentsCallback): Unit = {
    eventManager.put(ListPartitionReassignments(partitions, callback))
  }

  def updateFeatures(request: UpdateFeaturesRequest,
                     callback: UpdateFeaturesCallback): Unit = {
    eventManager.put(UpdateFeatures(request, callback))
  }

  def alterPartitionReassignments(partitions: Map[TopicPartition, Option[Seq[Int]]],
                                  callback: AlterReassignmentsCallback): Unit = {
    eventManager.put(ApiPartitionReassignment(partitions, callback))
  }

  private def processReplicaLeaderElection(
    partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
    electionType: ElectionType,
    electionTrigger: ElectionTrigger,
    callback: ElectLeadersCallback
  ): Unit = {
    if (!isActive) {
      callback(partitionsFromAdminClientOpt.fold(Map.empty[TopicPartition, Either[ApiError, Int]]) { partitions =>
        partitions.iterator.map(partition => partition -> Left(new ApiError(Errors.NOT_CONTROLLER, null))).toMap
      })
    } else {
      // We need to register the watcher if the path doesn't exist in order to detect future preferred replica
      // leader elections and we get the `path exists` check for free
      if (electionTrigger == AdminClientTriggered || zkClient.registerZNodeChangeHandlerAndCheckExistence(preferredReplicaElectionHandler)) {
        val partitions = partitionsFromAdminClientOpt match {
          case Some(partitions) => partitions
          case None => zkClient.getPreferredReplicaElection
        }

        val (knownPartitions, unknownPartitions) = partitions.partition(tp => controllerContext.allPartitions.contains(tp))
        unknownPartitions.foreach { p =>
          info(s"Skipping replica leader election ($electionType) for partition $p by $electionTrigger since it doesn't exist.")
        }

        val (partitionsBeingDeleted, livePartitions) = knownPartitions.partition(partition =>
            topicDeletionManager.isTopicQueuedUpForDeletion(partition.topic))
        if (partitionsBeingDeleted.nonEmpty) {
          warn(s"Skipping replica leader election ($electionType) for partitions $partitionsBeingDeleted " +
            s"by $electionTrigger since the respective topics are being deleted")
        }

        // partition those that have a valid leader
        val (electablePartitions, alreadyValidLeader) = livePartitions.partition { partition =>
          electionType match {
            case ElectionType.PREFERRED =>
              val assignedReplicas = controllerContext.partitionReplicaAssignment(partition)
              val preferredReplica = assignedReplicas.head
              val currentLeader = controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader
              currentLeader != preferredReplica

            case ElectionType.UNCLEAN =>
              val currentLeader = controllerContext.partitionLeadershipInfo(partition).get.leaderAndIsr.leader
              currentLeader == LeaderAndIsr.NoLeader || !controllerContext.liveBrokerIds.contains(currentLeader)
          }
        }

        val results = onReplicaElection(electablePartitions, electionType, electionTrigger).map {
          case (k, Left(ex)) =>
            if (ex.isInstanceOf[StateChangeFailedException]) {
              val error = if (electionType == ElectionType.PREFERRED) {
                Errors.PREFERRED_LEADER_NOT_AVAILABLE
              } else {
                Errors.ELIGIBLE_LEADERS_NOT_AVAILABLE
              }
              k -> Left(new ApiError(error, ex.getMessage))
            } else {
              k -> Left(ApiError.fromThrowable(ex))
            }
          case (k, Right(leaderAndIsr)) => k -> Right(leaderAndIsr.leader)
        } ++
        alreadyValidLeader.map(_ -> Left(new ApiError(Errors.ELECTION_NOT_NEEDED))) ++
        partitionsBeingDeleted.map(
          _ -> Left(new ApiError(Errors.INVALID_TOPIC_EXCEPTION, "The topic is being deleted"))
        ) ++
        unknownPartitions.map(
          _ -> Left(new ApiError(Errors.UNKNOWN_TOPIC_OR_PARTITION, "The partition does not exist."))
        )

        debug(s"Waiting for any successful result for election type ($electionType) by $electionTrigger for partitions: $results")
        callback(results)
      }
    }
  }

  def alterIsrs(alterIsrRequest: AlterIsrRequestData, callback: AlterIsrResponseData => Unit): Unit = {
    val isrsToAlter = mutable.Map[TopicPartition, LeaderAndIsr]()

    alterIsrRequest.topics.forEach { topicReq =>
      topicReq.partitions.forEach { partitionReq =>
        val tp = new TopicPartition(topicReq.name, partitionReq.partitionIndex)
        val newIsr = partitionReq.newIsr().asScala.toList.map(_.toInt)
        isrsToAlter.put(tp, new LeaderAndIsr(alterIsrRequest.brokerId, partitionReq.leaderEpoch, newIsr, partitionReq.currentIsrVersion))
      }
    }

    def responseCallback(results: Either[Map[TopicPartition, Either[Errors, LeaderAndIsr]], Errors]): Unit = {
      val resp = new AlterIsrResponseData()
      results match {
        case Right(error) =>
          resp.setErrorCode(error.code)
        case Left(partitionResults) =>
          resp.setTopics(new util.ArrayList())
          partitionResults
            .groupBy { case (tp, _) => tp.topic }   // Group by topic
            .foreach { case (topic, partitions) =>
              // Add each topic part to the response
              val topicResp = new AlterIsrResponseData.TopicData()
                .setName(topic)
                .setPartitions(new util.ArrayList())
              resp.topics.add(topicResp)
              partitions.foreach { case (tp, errorOrIsr) =>
                // Add each partition part to the response (new ISR or error)
                errorOrIsr match {
                  case Left(error) => topicResp.partitions.add(
                    new AlterIsrResponseData.PartitionData()
                      .setPartitionIndex(tp.partition)
                      .setErrorCode(error.code))
                  case Right(leaderAndIsr) => topicResp.partitions.add(
                    new AlterIsrResponseData.PartitionData()
                      .setPartitionIndex(tp.partition)
                      .setLeaderId(leaderAndIsr.leader)
                      .setLeaderEpoch(leaderAndIsr.leaderEpoch)
                      .setIsr(leaderAndIsr.isr.map(Integer.valueOf).asJava)
                      .setCurrentIsrVersion(leaderAndIsr.zkVersion))
                }
            }
          }
      }
      callback.apply(resp)
    }

    eventManager.put(AlterIsrReceived(alterIsrRequest.brokerId, alterIsrRequest.brokerEpoch, isrsToAlter, responseCallback))
  }

  private def processAlterIsr(brokerId: Int, brokerEpoch: Long,
                              isrsToAlter: Map[TopicPartition, LeaderAndIsr],
                              callback: AlterIsrCallback): Unit = {

    // Handle a few short-circuits
    if (!isActive) {
      callback.apply(Right(Errors.NOT_CONTROLLER))
      return
    }

    val brokerEpochOpt = controllerContext.liveBrokerIdAndEpochs.get(brokerId)
    if (brokerEpochOpt.isEmpty) {
      info(s"Ignoring AlterIsr due to unknown broker $brokerId")
      callback.apply(Right(Errors.STALE_BROKER_EPOCH))
      return
    }

    if (!brokerEpochOpt.contains(brokerEpoch)) {
      info(s"Ignoring AlterIsr due to stale broker epoch $brokerEpoch for broker $brokerId")
      callback.apply(Right(Errors.STALE_BROKER_EPOCH))
      return
    }

    val response = try {
      val partitionResponses = mutable.HashMap[TopicPartition, Either[Errors, LeaderAndIsr]]()

      // Determine which partitions we will accept the new ISR for
      val adjustedIsrs: Map[TopicPartition, LeaderAndIsr] = isrsToAlter.flatMap {
        case (tp: TopicPartition, newLeaderAndIsr: LeaderAndIsr) =>
          controllerContext.partitionLeadershipInfo(tp) match {
            case Some(leaderIsrAndControllerEpoch) =>
              val currentLeaderAndIsr = leaderIsrAndControllerEpoch.leaderAndIsr
              if (newLeaderAndIsr.leaderEpoch < currentLeaderAndIsr.leaderEpoch) {
                partitionResponses(tp) = Left(Errors.FENCED_LEADER_EPOCH)
                None
              } else if (newLeaderAndIsr.equalsIgnoreZk(currentLeaderAndIsr)) {
                // If a partition is already in the desired state, just return it
                partitionResponses(tp) = Right(currentLeaderAndIsr)
                None
              } else {
                Some(tp -> newLeaderAndIsr)
              }
            case None =>
              partitionResponses(tp) = Left(Errors.UNKNOWN_TOPIC_OR_PARTITION)
              None
          }
      }

      // Do the updates in ZK
      debug(s"Updating ISRs for partitions: ${adjustedIsrs.keySet}.")
      val UpdateLeaderAndIsrResult(finishedUpdates, badVersionUpdates) = zkClient.updateLeaderAndIsr(
        adjustedIsrs, controllerContext.epoch, controllerContext.epochZkVersion)

      val successfulUpdates: Map[TopicPartition, LeaderAndIsr] = finishedUpdates.flatMap {
        case (partition: TopicPartition, isrOrError: Either[Throwable, LeaderAndIsr]) =>
          isrOrError match {
            case Right(updatedIsr) =>
              debug(s"ISR for partition $partition updated to [${updatedIsr.isr.mkString(",")}] and zkVersion updated to [${updatedIsr.zkVersion}]")
              partitionResponses(partition) = Right(updatedIsr)
              Some(partition -> updatedIsr)
            case Left(error) =>
              warn(s"Failed to update ISR for partition $partition", error)
              partitionResponses(partition) = Left(Errors.forException(error))
              None
          }
      }

      badVersionUpdates.foreach(partition => {
        debug(s"Failed to update ISR for partition $partition, bad ZK version")
        partitionResponses(partition) = Left(Errors.INVALID_UPDATE_VERSION)
      })

      def processUpdateNotifications(partitions: Seq[TopicPartition]): Unit = {
        val liveBrokers: Seq[Int] = controllerContext.liveOrShuttingDownBrokerIds.toSeq
        sendUpdateMetadataRequest(liveBrokers, partitions.toSet)
      }

      // Update our cache and send out metadata updates
      updateLeaderAndIsrCache(successfulUpdates.keys.toSeq)
      processUpdateNotifications(isrsToAlter.keys.toSeq)

      Left(partitionResponses)
    } catch {
      case e: Throwable =>
        error(s"Error when processing AlterIsr for partitions: ${isrsToAlter.keys.toSeq}", e)
        Right(Errors.UNKNOWN_SERVER_ERROR)
    }

    callback.apply(response)

    // After we have returned the result of the `AlterIsr` request, we should check whether
    // there are any reassignments which can be completed by a successful ISR expansion.
    response.left.foreach { alterIsrResponses =>
      alterIsrResponses.forKeyValue { (topicPartition, partitionResponse) =>
        if (controllerContext.partitionsBeingReassigned.contains(topicPartition)) {
          val isSuccessfulUpdate = partitionResponse.isRight
          if (isSuccessfulUpdate) {
            maybeCompleteReassignment(topicPartition)
          }
        }
      }
    }
  }

  private def processControllerChange(): Unit = {
    maybeResign()
  }

  private def processReelect(): Unit = {
    maybeResign()
    elect()
  }

  private def processRegisterBrokerAndReelect(): Unit = {
    _brokerEpoch = zkClient.registerBroker(brokerInfo)
    processReelect()
  }

  private def processExpire(): Unit = {
    activeControllerId = -1
    onControllerResignation()
  }


  override def process(event: ControllerEvent): Unit = {
    try {
      event match {
        case event: MockEvent =>
          // Used only in test cases
          event.process()
        case ShutdownEventThread =>
          error("Received a ShutdownEventThread event. This type of event is supposed to be handle by ControllerEventThread")
        case AutoPreferredReplicaLeaderElection =>
          processAutoPreferredReplicaLeaderElection()
        case ReplicaLeaderElection(partitions, electionType, electionTrigger, callback) =>
          processReplicaLeaderElection(partitions, electionType, electionTrigger, callback)
        case UncleanLeaderElectionEnable =>
          processUncleanLeaderElectionEnable()
        case TopicUncleanLeaderElectionEnable(topic) =>
          processTopicUncleanLeaderElectionEnable(topic)
        case ControlledShutdown(id, brokerEpoch, callback) =>
          processControlledShutdown(id, brokerEpoch, callback)
        case LeaderAndIsrResponseReceived(response, brokerId) =>
          processLeaderAndIsrResponseReceived(response, brokerId)
        case UpdateMetadataResponseReceived(response, brokerId) =>
          processUpdateMetadataResponseReceived(response, brokerId)
        case TopicDeletionStopReplicaResponseReceived(replicaId, requestError, partitionErrors) =>
          processTopicDeletionStopReplicaResponseReceived(replicaId, requestError, partitionErrors)
        case BrokerChange =>
          // 处理 broker 变更事件
          processBrokerChange()
        case BrokerModifications(brokerId) =>
          processBrokerModification(brokerId)
        case ControllerChange =>
          processControllerChange()
        case Reelect =>
          processReelect()
        // 注册新的代理节点并重新选举
        case RegisterBrokerAndReelect =>
          processRegisterBrokerAndReelect()
        case Expire =>
          processExpire()
        // topic 变更事件
        case TopicChange =>
          // 处理 topic 变更事件，创建 Log，分区文件
          processTopicChange()
        case LogDirEventNotification =>
          processLogDirEventNotification()
        case PartitionModifications(topic) =>
          processPartitionModifications(topic)
        case TopicDeletion =>
          processTopicDeletion()
        case ApiPartitionReassignment(reassignments, callback) =>
          processApiPartitionReassignment(reassignments, callback)
          // 分区重分配
        case ZkPartitionReassignment =>
          processZkPartitionReassignment()
        case ListPartitionReassignments(partitions, callback) =>
          processListPartitionReassignments(partitions, callback)
        case UpdateFeatures(request, callback) =>
          processFeatureUpdates(request, callback)
        case PartitionReassignmentIsrChange(partition) =>
          processPartitionReassignmentIsrChange(partition)
        case IsrChangeNotification =>
          processIsrChangeNotification()
        case AlterIsrReceived(brokerId, brokerEpoch, isrsToAlter, callback) =>
          processAlterIsr(brokerId, brokerEpoch, isrsToAlter, callback)
        case Startup =>
          // 启动会执行选举过程
          processStartup()
      }
    } catch {
      case e: ControllerMovedException =>
        info(s"Controller moved to another broker when processing $event.", e)
        maybeResign()
      case e: Throwable =>
        error(s"Error processing event $event", e)
    } finally {
      updateMetrics()
    }
  }

  override def preempt(event: ControllerEvent): Unit = {
    event.preempt()
  }
}

class BrokerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = BrokerIdsZNode.path

  override def handleChildChange(): Unit = {
    // 把变更的事件添加到 eventManager 中，实际是在后续由 eventManager 处理中进行统一处理
    /** [[ ControllerEventManager.ControllerEventThread.doWork() ]]*/
    /** [[ KafkaController.process ]]*/
    eventManager.put(BrokerChange)
  }
}

class BrokerModificationsHandler(eventManager: ControllerEventManager, brokerId: Int) extends ZNodeChangeHandler {
  override val path: String = BrokerIdZNode.path(brokerId)

  override def handleDataChange(): Unit = {
    eventManager.put(BrokerModifications(brokerId))
  }
}

class TopicChangeHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = TopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(TopicChange)
}

class LogDirEventNotificationHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = LogDirEventNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(LogDirEventNotification)
}

object LogDirEventNotificationHandler {
  val Version: Long = 1L
}

class PartitionModificationsHandler(eventManager: ControllerEventManager, topic: String) extends ZNodeChangeHandler {
  override val path: String = TopicZNode.path(topic)

  override def handleDataChange(): Unit = eventManager.put(PartitionModifications(topic))
}

class TopicDeletionHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = DeleteTopicsZNode.path

  override def handleChildChange(): Unit = eventManager.put(TopicDeletion)
}

class PartitionReassignmentHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ReassignPartitionsZNode.path

  // Note that the event is also enqueued when the znode is deleted, but we do it explicitly instead of relying on
  // handleDeletion(). This approach is more robust as it doesn't depend on the watcher being re-registered after
  // it's consumed during data changes (we ensure re-registration when the znode is deleted).
  override def handleCreation(): Unit = eventManager.put(ZkPartitionReassignment)
}

class PartitionReassignmentIsrChangeHandler(eventManager: ControllerEventManager, partition: TopicPartition) extends ZNodeChangeHandler {
  override val path: String = TopicPartitionStateZNode.path(partition)

  override def handleDataChange(): Unit = eventManager.put(PartitionReassignmentIsrChange(partition))
}

class IsrChangeNotificationHandler(eventManager: ControllerEventManager) extends ZNodeChildChangeHandler {
  override val path: String = IsrChangeNotificationZNode.path

  override def handleChildChange(): Unit = eventManager.put(IsrChangeNotification)
}

object IsrChangeNotificationHandler {
  val Version: Long = 1L
}

class PreferredReplicaElectionHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = PreferredReplicaElectionZNode.path

  override def handleCreation(): Unit = eventManager.put(ReplicaLeaderElection(None, ElectionType.PREFERRED, ZkTriggered))
}

class ControllerChangeHandler(eventManager: ControllerEventManager) extends ZNodeChangeHandler {
  override val path: String = ControllerZNode.path

  override def handleCreation(): Unit = eventManager.put(ControllerChange)
  override def handleDeletion(): Unit = eventManager.put(Reelect)
  override def handleDataChange(): Unit = eventManager.put(ControllerChange)
}

case class PartitionAndReplica(topicPartition: TopicPartition, replica: Int) {
  def topic: String = topicPartition.topic
  def partition: Int = topicPartition.partition

  override def toString: String = {
    s"[Topic=$topic,Partition=$partition,Replica=$replica]"
  }
}

case class LeaderIsrAndControllerEpoch(leaderAndIsr: LeaderAndIsr, controllerEpoch: Int) {
  override def toString: String = {
    val leaderAndIsrInfo = new StringBuilder
    leaderAndIsrInfo.append("(Leader:" + leaderAndIsr.leader)
    leaderAndIsrInfo.append(",ISR:" + leaderAndIsr.isr.mkString(","))
    leaderAndIsrInfo.append(",LeaderEpoch:" + leaderAndIsr.leaderEpoch)
    leaderAndIsrInfo.append(",ControllerEpoch:" + controllerEpoch + ")")
    leaderAndIsrInfo.toString()
  }
}

private[controller] class ControllerStats extends KafkaMetricsGroup {
  val uncleanLeaderElectionRate = newMeter("UncleanLeaderElectionsPerSec", "elections", TimeUnit.SECONDS)

  val rateAndTimeMetrics: Map[ControllerState, KafkaTimer] = ControllerState.values.flatMap { state =>
    state.rateAndTimeMetricName.map { metricName =>
      state -> new KafkaTimer(newTimer(metricName, TimeUnit.MILLISECONDS, TimeUnit.SECONDS))
    }
  }.toMap

}

sealed trait ControllerEvent {
  def state: ControllerState
  // preempt() is not executed by `ControllerEventThread` but by the main thread.
  def preempt(): Unit
}

case object ControllerChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object Reelect extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object RegisterBrokerAndReelect extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object Expire extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object ShutdownEventThread extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerShutdown
  override def preempt(): Unit = {}
}

case object AutoPreferredReplicaLeaderElection extends ControllerEvent {
  override def state: ControllerState = ControllerState.AutoLeaderBalance
  override def preempt(): Unit = {}
}

case object UncleanLeaderElectionEnable extends ControllerEvent {
  override def state: ControllerState = ControllerState.UncleanLeaderElectionEnable
  override def preempt(): Unit = {}
}

case class TopicUncleanLeaderElectionEnable(topic: String) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicUncleanLeaderElectionEnable
  override def preempt(): Unit = {}
}

case class ControlledShutdown(id: Int, brokerEpoch: Long, controlledShutdownCallback: Try[Set[TopicPartition]] => Unit) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControlledShutdown
  override def preempt(): Unit = controlledShutdownCallback(Failure(new ControllerMovedException("Controller moved to another broker")))
}

case class LeaderAndIsrResponseReceived(leaderAndIsrResponse: LeaderAndIsrResponse, brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.LeaderAndIsrResponseReceived
  override def preempt(): Unit = {}
}

case class UpdateMetadataResponseReceived(updateMetadataResponse: UpdateMetadataResponse, brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.UpdateMetadataResponseReceived
  override def preempt(): Unit = {}
}

case class TopicDeletionStopReplicaResponseReceived(replicaId: Int,
                                                    requestError: Errors,
                                                    partitionErrors: Map[TopicPartition, Errors]) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicDeletion
  override def preempt(): Unit = {}
}

case object Startup extends ControllerEvent {
  override def state: ControllerState = ControllerState.ControllerChange
  override def preempt(): Unit = {}
}

case object BrokerChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.BrokerChange
  override def preempt(): Unit = {}
}

case class BrokerModifications(brokerId: Int) extends ControllerEvent {
  override def state: ControllerState = ControllerState.BrokerChange
  override def preempt(): Unit = {}
}

case object TopicChange extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicChange
  override def preempt(): Unit = {}
}

case object LogDirEventNotification extends ControllerEvent {
  override def state: ControllerState = ControllerState.LogDirChange
  override def preempt(): Unit = {}
}

case class PartitionModifications(topic: String) extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicChange
  override def preempt(): Unit = {}
}

case object TopicDeletion extends ControllerEvent {
  override def state: ControllerState = ControllerState.TopicDeletion
  override def preempt(): Unit = {}
}

case object ZkPartitionReassignment extends ControllerEvent {
  override def state: ControllerState = ControllerState.AlterPartitionReassignment
  override def preempt(): Unit = {}
}

case class ApiPartitionReassignment(reassignments: Map[TopicPartition, Option[Seq[Int]]],
                                    callback: AlterReassignmentsCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.AlterPartitionReassignment
  override def preempt(): Unit = callback(Right(new ApiError(Errors.NOT_CONTROLLER)))
}

case class PartitionReassignmentIsrChange(partition: TopicPartition) extends ControllerEvent {
  override def state: ControllerState = ControllerState.AlterPartitionReassignment
  override def preempt(): Unit = {}
}

case object IsrChangeNotification extends ControllerEvent {
  override def state: ControllerState = ControllerState.IsrChange
  override def preempt(): Unit = {}
}

case class AlterIsrReceived(brokerId: Int, brokerEpoch: Long, isrsToAlter: Map[TopicPartition, LeaderAndIsr],
                            callback: AlterIsrCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.IsrChange
  override def preempt(): Unit = {}
}

case class ReplicaLeaderElection(
  partitionsFromAdminClientOpt: Option[Set[TopicPartition]],
  electionType: ElectionType,
  electionTrigger: ElectionTrigger,
  callback: ElectLeadersCallback = _ => {}
) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ManualLeaderBalance

  override def preempt(): Unit = callback(
    partitionsFromAdminClientOpt.fold(Map.empty[TopicPartition, Either[ApiError, Int]]) { partitions =>
      partitions.iterator.map(partition => partition -> Left(new ApiError(Errors.NOT_CONTROLLER, null))).toMap
    }
  )
}

/**
  * @param partitionsOpt - an Optional set of partitions. If not present, all reassigning partitions are to be listed
  */
case class ListPartitionReassignments(partitionsOpt: Option[Set[TopicPartition]],
                                      callback: ListReassignmentsCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.ListPartitionReassignment
  override def preempt(): Unit = callback(Right(new ApiError(Errors.NOT_CONTROLLER, null)))
}

case class UpdateFeatures(request: UpdateFeaturesRequest,
                          callback: UpdateFeaturesCallback) extends ControllerEvent {
  override def state: ControllerState = ControllerState.UpdateFeatures
  override def preempt(): Unit = {}
}


// Used only in test cases
abstract class MockEvent(val state: ControllerState) extends ControllerEvent {
  def process(): Unit
  def preempt(): Unit
}
