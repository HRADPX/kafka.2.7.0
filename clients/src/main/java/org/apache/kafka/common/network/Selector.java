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
package org.apache.kafka.common.network;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.internals.IntGaugeSuite;
import org.apache.kafka.common.metrics.stats.*;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A nioSelector interface for doing non-blocking multi-connection network I/O.
 * <p>
 * This class works with {@link NetworkSend} and {@link NetworkReceive} to transmit size-delimited network requests and
 * responses.
 * <p>
 * A connection can be added to the nioSelector associated with an integer id by doing
 *
 * <pre>
 * nioSelector.connect(&quot;42&quot;, new InetSocketAddress(&quot;google.com&quot;, server.port), 64000, 64000);
 * </pre>
 *
 * The connect call does not block on the creation of the TCP connection, so the connect method only begins initiating
 * the connection. The successful invocation of this method does not mean a valid connection has been established.
 *
 * Sending requests, receiving responses, processing connection completions, and disconnections on the existing
 * connections are all done using the <code>poll()</code> call.
 *
 * <pre>
 * nioSelector.send(new NetworkSend(myDestination, myBytes));
 * nioSelector.send(new NetworkSend(myOtherDestination, myOtherBytes));
 * nioSelector.poll(TIMEOUT_MS);
 * </pre>
 *
 * The nioSelector maintains several lists that are reset by each call to <code>poll()</code> which are available via
 * various getters. These are reset by each call to <code>poll()</code>.
 *
 * This class is not thread safe!
 *
 * 这是 Kafka 自己封装的 Selector，基于 NIO 封装而成.
 */
public class Selector implements Selectable, AutoCloseable {

    public static final long NO_IDLE_TIMEOUT_MS = -1;
    public static final int NO_FAILED_AUTHENTICATION_DELAY = 0;

    private enum CloseMode {
        GRACEFUL(true),            // process outstanding buffered receives, notify disconnect
        NOTIFY_ONLY(true),         // discard any outstanding receives, notify disconnect
        DISCARD_NO_NOTIFY(false);  // discard any outstanding receives, no disconnect notification

        boolean notifyDisconnect;

        CloseMode(boolean notifyDisconnect) {
            this.notifyDisconnect = notifyDisconnect;
        }
    }

    private final Logger log;
    // Java NIO Selector
    // 负责发送网络请求和建立，处理实际的网络 I/O
    private final java.nio.channels.Selector nioSelector;
    // 维护主机节点和 KafkaChannel 的映射，KafkaChannel 是 SocketChannel 的简单封装
    private final Map<String, KafkaChannel> channels;
    private final Set<KafkaChannel> explicitlyMutedChannels;
    private boolean outOfMemory;
    // 已经完成发送的请求
    private final List<Send> completedSends;
    // 已经接收到并且处理完的响应
    private final LinkedHashMap<String, NetworkReceive> completedReceives;
    private final Set<SelectionKey> immediatelyConnectedKeys;
    private final Map<String, KafkaChannel> closingChannels;
    private Set<SelectionKey> keysWithBufferedRead;
    // 没有建立连接的主机
    private final Map<String, ChannelState> disconnected;
    // 完成建立的主机
    private final List<String> connected;
    // 建立连接失败的主机
    private final List<String> failedSends;
    private final Time time;
    private final SelectorMetrics sensors;
    private final ChannelBuilder channelBuilder;
    private final int maxReceiveSize;
    private final boolean recordTimePerConnection;
    private final IdleExpiryManager idleExpiryManager;
    private final LinkedHashMap<String, DelayedAuthenticationFailureClose> delayedClosingChannels;
    private final MemoryPool memoryPool;
    private final long lowMemThreshold;
    private final int failedAuthenticationDelayMs;

    //indicates if the previous call to poll was able to make progress in reading already-buffered data.
    //this is used to prevent tight loops when memory is not available to read any more data
    private boolean madeReadProgressLastPoll = true;

    /**
     * Create a new nioSelector
     * @param maxReceiveSize Max size in bytes of a single network receive (use {@link NetworkReceive#UNLIMITED} for no limit)
     * @param connectionMaxIdleMs Max idle connection time (use {@link #NO_IDLE_TIMEOUT_MS} to disable idle timeout)
     * @param failedAuthenticationDelayMs Minimum time by which failed authentication response and channel close should be delayed by.
     *                                    Use {@link #NO_FAILED_AUTHENTICATION_DELAY} to disable this delay.
     * @param metrics Registry for Selector metrics
     * @param time Time implementation
     * @param metricGrpPrefix Prefix for the group of metrics registered by Selector
     * @param metricTags Additional tags to add to metrics registered by Selector
     * @param metricsPerConnection Whether or not to enable per-connection metrics
     * @param channelBuilder Channel builder for every new connection
     * @param logContext Context for logging with additional info
     */
    public Selector(int maxReceiveSize,
            long connectionMaxIdleMs,
            int failedAuthenticationDelayMs,
            Metrics metrics,
            Time time,
            String metricGrpPrefix,
            Map<String, String> metricTags,
            boolean metricsPerConnection,
            boolean recordTimePerConnection,
            ChannelBuilder channelBuilder,
            MemoryPool memoryPool,
            LogContext logContext) {
        try {
            this.nioSelector = java.nio.channels.Selector.open();
        } catch (IOException e) {
            throw new KafkaException(e);
        }
        this.maxReceiveSize = maxReceiveSize;
        this.time = time;
        this.channels = new HashMap<>();
        this.explicitlyMutedChannels = new HashSet<>();
        this.outOfMemory = false;
        this.completedSends = new ArrayList<>();
        this.completedReceives = new LinkedHashMap<>();
        this.immediatelyConnectedKeys = new HashSet<>();
        this.closingChannels = new HashMap<>();
        this.keysWithBufferedRead = new HashSet<>();
        this.connected = new ArrayList<>();
        this.disconnected = new HashMap<>();
        this.failedSends = new ArrayList<>();
        this.log = logContext.logger(Selector.class);
        this.sensors = new SelectorMetrics(metrics, metricGrpPrefix, metricTags, metricsPerConnection);
        this.channelBuilder = channelBuilder;
        this.recordTimePerConnection = recordTimePerConnection;
        this.idleExpiryManager = connectionMaxIdleMs < 0 ? null : new IdleExpiryManager(time, connectionMaxIdleMs);
        this.memoryPool = memoryPool;
        this.lowMemThreshold = (long) (0.1 * this.memoryPool.size());
        this.failedAuthenticationDelayMs = failedAuthenticationDelayMs;
        this.delayedClosingChannels = (failedAuthenticationDelayMs > NO_FAILED_AUTHENTICATION_DELAY) ? new LinkedHashMap<String, DelayedAuthenticationFailureClose>() : null;
    }

    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    boolean recordTimePerConnection,
                    ChannelBuilder channelBuilder,
                    MemoryPool memoryPool,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags,
                metricsPerConnection, recordTimePerConnection, channelBuilder, memoryPool, logContext);
    }

    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    int failedAuthenticationDelayMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    ChannelBuilder channelBuilder,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, false, channelBuilder, MemoryPool.NONE, logContext);
    }

    public Selector(int maxReceiveSize,
                    long connectionMaxIdleMs,
                    Metrics metrics,
                    Time time,
                    String metricGrpPrefix,
                    Map<String, String> metricTags,
                    boolean metricsPerConnection,
                    ChannelBuilder channelBuilder,
                    LogContext logContext) {
        this(maxReceiveSize, connectionMaxIdleMs, NO_FAILED_AUTHENTICATION_DELAY, metrics, time, metricGrpPrefix, metricTags, metricsPerConnection, channelBuilder, logContext);
    }

    public Selector(long connectionMaxIdleMS, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, metrics, time, metricGrpPrefix, Collections.emptyMap(), true, channelBuilder, logContext);
    }

    public Selector(long connectionMaxIdleMS, int failedAuthenticationDelayMs, Metrics metrics, Time time, String metricGrpPrefix, ChannelBuilder channelBuilder, LogContext logContext) {
        this(NetworkReceive.UNLIMITED, connectionMaxIdleMS, failedAuthenticationDelayMs, metrics, time, metricGrpPrefix, Collections.<String, String>emptyMap(), true, channelBuilder, logContext);
    }

    /**
     * Begin connecting to the given address and add the connection to this nioSelector associated with the given id
     * number.
     * <p>
     * Note that this call only initiates the connection, which will be completed on a future {@link #poll(long)}
     * call. Check {@link #connected()} to see which (if any) connections have completed after a given poll call.
     * @param id The id for the new connection
     * @param address The address to connect to
     * @param sendBufferSize The send buffer for the new connection
     * @param receiveBufferSize The receive buffer for the new connection
     * @throws IllegalStateException if there is already a connection for that id
     * @throws IOException if DNS resolution fails on the hostname or if the broker is down
     */
    @Override
    public void connect(String id, InetSocketAddress address, int sendBufferSize, int receiveBufferSize) throws IOException {
        ensureNotRegistered(id);
        // 获取 SocketChannel
        SocketChannel socketChannel = SocketChannel.open();
        SelectionKey key = null;
        try {
            // 设置非阻塞等一些参数
            configureSocketChannel(socketChannel, sendBufferSize, receiveBufferSize);
            // 连接服务节点主机
            boolean connected = doConnect(socketChannel, address);
            // SocketChannel 向 Selector 上注册一个 OP_CONNECT 事件
            // 并将 SocketChannel 封装成 KafkaChannel，同时把 Key 和 KafkaChannel 关联起来，便于后续使用...
            key = registerChannel(id, socketChannel, SelectionKey.OP_CONNECT);

            // 正常情况下，网络不会立刻完成连接，除非是本地连接，这里是 true
            if (connected) {
                // OP_CONNECT won't trigger for immediately connected channels
                log.debug("Immediately connected to node {}", id);
                immediatelyConnectedKeys.add(key);
                // 立刻连接成功了，需要取消前面注册的 OP_CONNECT 事件
                key.interestOps(0);
            }
        } catch (IOException | RuntimeException e) {
            if (key != null)
                immediatelyConnectedKeys.remove(key);
            channels.remove(id);
            socketChannel.close();
            throw e;
        }
    }

    // Visible to allow test cases to override. In particular, we use this to implement a blocking connect
    // in order to simulate "immediately connected" sockets.
    protected boolean doConnect(SocketChannel channel, InetSocketAddress address) throws IOException {
        try {
            return channel.connect(address);
        } catch (UnresolvedAddressException e) {
            throw new IOException("Can't resolve address: " + address, e);
        }
    }

    private void configureSocketChannel(SocketChannel socketChannel, int sendBufferSize, int receiveBufferSize)
            throws IOException {
        socketChannel.configureBlocking(false);
        Socket socket = socketChannel.socket();
        socket.setKeepAlive(true);
        if (sendBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setSendBufferSize(sendBufferSize);
        if (receiveBufferSize != Selectable.USE_DEFAULT_BUFFER_SIZE)
            socket.setReceiveBufferSize(receiveBufferSize);
        // 默认值是 false，代表开启 Nagle 算法，表示它会把网络中的一些小的数据包收集起来，组合成一个大的数据包发送出去。
        // Kafka 需要把这个参数设置为 false，因为 Kafka 有的请求本身就是很小...
        socket.setTcpNoDelay(true);
    }

    /**
     * Register the nioSelector with an existing channel
     * Use this on server-side, when a connection is accepted by a different thread but processed by the Selector
     * <p>
     * If a connection already exists with the same connection id in `channels` or `closingChannels`,
     * an exception is thrown. Connection ids must be chosen to avoid conflict when remote ports are reused.
     * Kafka brokers add an incrementing index to the connection id to avoid reuse in the timing window
     * where an existing connection may not yet have been closed by the broker when a new connection with
     * the same remote host:port is processed.
     * </p><p>
     * If a `KafkaChannel` cannot be created for this connection, the `socketChannel` is closed
     * and its selection key cancelled.
     * </p>
     */
    public void register(String id, SocketChannel socketChannel) throws IOException {
        ensureNotRegistered(id);
        // 向 Selector 注册 OP_READ 事件
        registerChannel(id, socketChannel, SelectionKey.OP_READ);
        this.sensors.connectionCreated.record();
        // Default to empty client information as the ApiVersionsRequest is not
        // mandatory. In this case, we still want to account for the connection.
        ChannelMetadataRegistry metadataRegistry = this.channel(id).channelMetadataRegistry();
        if (metadataRegistry.clientInformation() == null)
            metadataRegistry.registerClientInformation(ClientInformation.EMPTY);
    }

    private void ensureNotRegistered(String id) {
        if (this.channels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id);
        if (this.closingChannels.containsKey(id))
            throw new IllegalStateException("There is already a connection for id " + id + " that is still being closed");
    }

    /**
     * 这里的封装关系为 SocketChannel + SelectionKey --> TransportLayer --> KafkaChannel
     * 同时将 KafkaChannel 绑定到 SelectionKey 中，这样后面就可以通过 SelectionKey 获取到 KafkaChannel
     * 这样就可以实现 SelectionKey -> KafkaChannel 和 KafkaChannel -> SelectionKey
     */
    protected SelectionKey registerChannel(String id, SocketChannel socketChannel, int interestedOps) throws IOException {
        // 向 Selector 上注册关注的事件
        SelectionKey key = socketChannel.register(nioSelector, interestedOps);
        // 将 SocketChannel 封装成 KafkaChannel.
        KafkaChannel channel = buildAndAttachKafkaChannel(socketChannel, id, key);
        // 保存节点和 KafkaChannel 的映射
        this.channels.put(id, channel);
        if (idleExpiryManager != null)
            idleExpiryManager.update(channel.id(), time.nanoseconds());
        return key;
    }

    private KafkaChannel buildAndAttachKafkaChannel(SocketChannel socketChannel, String id, SelectionKey key) throws IOException {
        try {
            KafkaChannel channel = channelBuilder.buildChannel(id, key, maxReceiveSize, memoryPool,
                new SelectorChannelMetadataRegistry());
            // 绑定 KafkaChannel 到 SelectionKey 中
            key.attach(channel);
            return channel;
        } catch (Exception e) {
            try {
                socketChannel.close();
            } finally {
                key.cancel();
            }
            throw new IOException("Channel could not be created for socket " + socketChannel, e);
        }
    }

    /**
     * Interrupt the nioSelector if it is blocked waiting to do I/O.
     *
     * 如果有其他线程阻塞在 {@link Selector#select(long)} 或 {@link Selector#select()} 方法上，调用该方法可以让其立刻返回。
     */
    @Override
    public void wakeup() {
        this.nioSelector.wakeup();
    }

    /**
     * Close this selector and all associated connections
     */
    @Override
    public void close() {
        List<String> connections = new ArrayList<>(channels.keySet());
        AtomicReference<Throwable> firstException = new AtomicReference<>();
        Utils.closeAllQuietly(firstException, "release connections",
                connections.stream().map(id -> (AutoCloseable) () -> close(id)).toArray(AutoCloseable[]::new));
        // If there is any exception thrown in close(id), we should still be able
        // to close the remaining objects, especially the sensors because keeping
        // the sensors may lead to failure to start up the ReplicaFetcherThread if
        // the old sensors with the same names has not yet been cleaned up.
        Utils.closeQuietly(nioSelector, "nioSelector", firstException);
        Utils.closeQuietly(sensors, "sensors", firstException);
        Utils.closeQuietly(channelBuilder, "channelBuilder", firstException);
        Throwable exception = firstException.get();
        if (exception instanceof RuntimeException && !(exception instanceof SecurityException)) {
            throw (RuntimeException) exception;
        }
    }

    /**
     * Queue the given request for sending in the subsequent {@link #poll(long)} calls
     * @param send The request to send
     *
     * 客户端发送的每个 Send 请求，都会被设置到 kafkaChannel 中，如果一个 KafkaChannel 上还有尚未发送的 Send 请求，
     * 则后面的请求就不能被发送。即客户端发送请求给服务端，在一个 kafkaChannel 中，一次只能发送一个 Send 请求。
     *
     * 每次发送 Send 请求都要给 SocketChannel 添加 OP_WRITE（写）事件，这样就可以往 SocketChannel 中写入数据，当 Send 请求发送成功后，
     * 都要将 OP_WRITE 事件移除。
     */
    public void send(Send send) {
        String connectionId = send.destination();
        // 获取 KafkaChannel，这里的 KafkaChannel 是从 channels 这个 Map 中获取的，
        // 之前在建立连接成功后，会将主机信息和 KafkaChannel 的映射保存到 channels 中。
        // 如果连接关闭但是还需要接收通道里的数据，如执行 close(channel, CloseMode.GRACEFUL) 方法，会将该
        // KafkaChannel 存到这个 closingChannels 集合中。
        KafkaChannel channel = openOrClosingChannelOrFail(connectionId);
        if (closingChannels.containsKey(connectionId)) {
            // ensure notification via `disconnected`, leave channel in the state in which closing was triggered
            // 表示连接已经断开，直接将 nodeId 放到失败的集合中，在下次执行 poll 方法时，会执行里 clear() 方法，将该集合里的
            // nodeId 存入到 Selector 里的 disconnected 数据结构中。最后在执行完成 poll 方法后，执行 handleDisconnections
            // 方法来处理这里所有发送失败的请求。
            this.failedSends.add(connectionId);
        } else {
            try {
                // 这里其实还没有真正发送，只是将请求保存到 KafkaChannel 中，并注册一个 OP_WRITE 事件，选择器轮询时监听到写事件后，
                // 会调用 KafkaChannel.write() 方法，将保存在 KafkaChannel 中保存的 Send 发送到传输层的 SocketChannel 中，
                // 数据发送还是在 poll 方法中执行，
                channel.setSend(send);
            } catch (Exception e) {
                // update the state for consistency, the channel will be discarded after `close`
                channel.state(ChannelState.FAILED_SEND);
                // ensure notification via `disconnected` when `failedSends` are processed in the next poll
                this.failedSends.add(connectionId);
                close(channel, CloseMode.DISCARD_NO_NOTIFY);
                if (!(e instanceof CancelledKeyException)) {
                    log.error("Unexpected exception during send, closing connection {} and rethrowing exception {}",
                            connectionId, e);
                    throw e;
                }
            }
        }
    }

    /**
     * Do whatever I/O can be done on each connection without blocking. This includes completing connections, completing
     * disconnections, initiating new sends, or making progress on in-progress sends or receives.
     *
     * When this call is completed the user can check for completed sends, receives, connections or disconnects using
     * {@link #completedSends()}, {@link #completedReceives()}, {@link #connected()}, {@link #disconnected()}. These
     * lists will be cleared at the beginning of each `poll` call and repopulated by the call if there is
     * any completed I/O.
     *
     * In the "Plaintext" setting, we are using socketChannel to read & write to the network. But for the "SSL" setting,
     * we encrypt the data before we use socketChannel to write data to the network, and decrypt before we return the responses.
     * This requires additional buffers to be maintained as we are reading from network, since the data on the wire is encrypted
     * we won't be able to read exact no.of bytes as kafka protocol requires. We read as many bytes as we can, up to SSLEngine's
     * application buffer size. This means we might be reading additional bytes than the requested size.
     * If there is no further data to read from socketChannel selector won't invoke that channel and we have additional bytes
     * in the buffer. To overcome this issue we added "keysWithBufferedRead" map which tracks channels which have data in the SSL
     * buffers. If there are channels with buffered data that can by processed, we set "timeout" to 0 and process the data even
     * if there is no more data to read from the socket.
     *
     * Atmost one entry is added to "completedReceives" for a channel in each poll. This is necessary to guarantee that
     * requests from a channel are processed on the broker in the order they are sent. Since outstanding requests added
     * by SocketServer to the request queue may be processed by different request handler threads, requests on each
     * channel must be processed one-at-a-time to guarantee ordering.
     *
     * @param timeout The amount of time to wait, in milliseconds, which must be non-negative
     * @throws IllegalArgumentException If `timeout` is negative
     * @throws IllegalStateException If a send is given for which we have no existing connection or for which there is
     *         already an in-progress send
     */
    @Override
    public void poll(long timeout) throws IOException {
        if (timeout < 0)
            throw new IllegalArgumentException("timeout should be >= 0");

        boolean madeReadProgressLastCall = madeReadProgressLastPoll;
        clear();

        boolean dataInBuffers = !keysWithBufferedRead.isEmpty();

        if (!immediatelyConnectedKeys.isEmpty() || (madeReadProgressLastCall && dataInBuffers))
            timeout = 0;

        if (!memoryPool.isOutOfMemory() && outOfMemory) {
            //we have recovered from memory pressure. unmute any channel not explicitly muted for other reasons
            log.trace("Broker no longer low on memory - unmuting incoming sockets");
            for (KafkaChannel channel : channels.values()) {
                if (channel.isInMutableState() && !explicitlyMutedChannels.contains(channel)) {
                    channel.maybeUnmute();
                }
            }
            outOfMemory = false;
        }

        /* check ready keys */
        long startSelect = time.nanoseconds();
        // 从 Selector 上找到有多少个 key 注册了
        int numReadyKeys = select(timeout);
        long endSelect = time.nanoseconds();
        this.sensors.selectTime.record(endSelect - startSelect, time.milliseconds());

        if (numReadyKeys > 0 || !immediatelyConnectedKeys.isEmpty() || dataInBuffers) {
            Set<SelectionKey> readyKeys = this.nioSelector.selectedKeys();

            // Poll from channels that have buffered data (but nothing more from the underlying socket)
            if (dataInBuffers) {
                keysWithBufferedRead.removeAll(readyKeys); //so no channel gets polled twice
                Set<SelectionKey> toPoll = keysWithBufferedRead;
                keysWithBufferedRead = new HashSet<>(); //poll() calls will repopulate if needed
                pollSelectionKeys(toPoll, false, endSelect);
            }

            // Poll from channels where the underlying socket has more data
            pollSelectionKeys(readyKeys, false, endSelect);
            // Clear all selected keys so that they are included in the ready count for the next select
            readyKeys.clear();

            pollSelectionKeys(immediatelyConnectedKeys, true, endSelect);
            immediatelyConnectedKeys.clear();
        } else {
            madeReadProgressLastPoll = true; //no work is also "progress"
        }

        long endIo = time.nanoseconds();
        this.sensors.ioTime.record(endIo - endSelect, time.milliseconds());

        // Close channels that were delayed and are now ready to be closed
        completeDelayedChannelClose(endIo);

        // we use the time at the end of select to ensure that we don't close any connections that
        // have just been processed in pollSelectionKeys
        maybeCloseOldestConnection(endSelect);
    }

    /**
     * handle any ready I/O on a set of selection keys
     * @param selectionKeys set of keys to handle
     * @param isImmediatelyConnected true if running over a set of keys for just-connected sockets
     * @param currentTimeNanos time at which set of keys was determined
     */
    // package-private for testing
    void pollSelectionKeys(Set<SelectionKey> selectionKeys,
                           boolean isImmediatelyConnected,
                           long currentTimeNanos) {
        // 遍历所有的 Key
        for (SelectionKey key : determineHandlingOrder(selectionKeys)) {
            KafkaChannel channel = channel(key);
            long channelStartTimeNanos = recordTimePerConnection ? time.nanoseconds() : 0;
            boolean sendFailed = false;
            String nodeId = channel.id();

            // register all per-connection metrics at once
            sensors.maybeRegisterConnectionMetrics(nodeId);
            if (idleExpiryManager != null)
                idleExpiryManager.update(nodeId, currentTimeNanos);

            try {
                /* complete any connections that have finished their handshake (either normally or immediately) */
                // 建立连接事件
                if (isImmediatelyConnected || key.isConnectable()) {
                    // 如果之前没有完成网络连接，这里完成...
                    // 完成连接建立后，会取消 OP_CONNECT 事件并给当前 KafkaChannel(SocketChannel) 增加 OP_READ 事件，可以接收服务端响应了
                    // 注册 OP_CONNECT 事件如果不是本地连接，后续需要通过 finishConnect() 方法判断是否完成连接建立
                    if (channel.finishConnect()) {
                        // 存储完成的网络连接
                        this.connected.add(nodeId);
                        this.sensors.connectionCreated.record();

                        SocketChannel socketChannel = (SocketChannel) key.channel();
                        log.debug("Created socket with SO_RCVBUF = {}, SO_SNDBUF = {}, SO_TIMEOUT = {} to node {}",
                                socketChannel.socket().getReceiveBufferSize(),
                                socketChannel.socket().getSendBufferSize(),
                                socketChannel.socket().getSoTimeout(),
                                nodeId);
                    } else {
                        continue;
                    }
                }

                /* if channel is not ready finish prepare */
                if (channel.isConnected() && !channel.ready()) {
                    channel.prepare();
                    if (channel.ready()) {
                        long readyTimeMs = time.milliseconds();
                        boolean isReauthentication = channel.successfulAuthentications() > 1;
                        if (isReauthentication) {
                            sensors.successfulReauthentication.record(1.0, readyTimeMs);
                            if (channel.reauthenticationLatencyMs() == null)
                                log.warn(
                                    "Should never happen: re-authentication latency for a re-authenticated channel was null; continuing...");
                            else
                                sensors.reauthenticationLatency
                                    .record(channel.reauthenticationLatencyMs().doubleValue(), readyTimeMs);
                        } else {
                            sensors.successfulAuthentication.record(1.0, readyTimeMs);
                            if (!channel.connectedClientSupportsReauthentication())
                                sensors.successfulAuthenticationNoReauth.record(1.0, readyTimeMs);
                        }
                        log.debug("Successfully {}authenticated with {}", isReauthentication ?
                            "re-" : "", channel.socketDescription());
                    }
                }
                if (channel.ready() && channel.state() == ChannelState.NOT_CONNECTED)
                    channel.state(ChannelState.READY);
                Optional<NetworkReceive> responseReceivedDuringReauthentication = channel.pollResponseReceivedDuringReauthentication();
                responseReceivedDuringReauthentication.ifPresent(receive -> {
                    long currentTimeMs = time.milliseconds();
                    addToCompletedReceives(channel, receive, currentTimeMs);
                });

                //if channel is ready and has bytes to read from socket or buffer, and has no
                //previous completed receive then read from it
                // 对于客户端是接收服务端发送回来的响应
                // 对于服务端是接收客户端发送过来的请求
                if (channel.ready() && (key.isReadable() || channel.hasBytesBuffered()) && !hasCompletedReceive(channel)
                        && !explicitlyMutedChannels.contains(channel)) {
                    attemptRead(channel);
                }

                if (channel.hasBytesBuffered()) {
                    //this channel has bytes enqueued in intermediary buffers that we could not read
                    //(possibly because no memory). it may be the case that the underlying socket will
                    //not come up in the next poll() and so we need to remember this channel for the
                    //next poll call otherwise data may be stuck in said buffers forever. If we attempt
                    //to process buffered data and no progress is made, the channel buffered status is
                    //cleared to avoid the overhead of checking every time.
                    keysWithBufferedRead.add(key);
                }

                /* if channel is ready write to any sockets that have space in their buffer and for which we have data */
                // 发送网络请求
                long nowNanos = channelStartTimeNanos != 0 ? channelStartTimeNanos : currentTimeNanos;
                try {
                    attemptWrite(key, channel, nowNanos);
                } catch (Exception e) {
                    sendFailed = true;
                    throw e;
                }

                /* cancel any defunct sockets */
                /* key cancel, channel close or selector close */
                if (!key.isValid())
                    close(channel, CloseMode.GRACEFUL);

            } catch (Exception e) {
                String desc = channel.socketDescription();
                if (e instanceof IOException) {
                    log.debug("Connection with {} disconnected", desc, e);
                } else if (e instanceof AuthenticationException) {
                    boolean isReauthentication = channel.successfulAuthentications() > 0;
                    if (isReauthentication)
                        sensors.failedReauthentication.record();
                    else
                        sensors.failedAuthentication.record();
                    String exceptionMessage = e.getMessage();
                    if (e instanceof DelayedResponseAuthenticationException)
                        exceptionMessage = e.getCause().getMessage();
                    log.info("Failed {}authentication with {} ({})", isReauthentication ? "re-" : "",
                        desc, exceptionMessage);
                } else {
                    log.warn("Unexpected error from {}; closing connection", desc, e);
                }

                if (e instanceof DelayedResponseAuthenticationException)
                    maybeDelayCloseOnAuthenticationFailure(channel);
                else
                    close(channel, sendFailed ? CloseMode.NOTIFY_ONLY : CloseMode.GRACEFUL);
            } finally {
                maybeRecordTimePerConnection(channel, channelStartTimeNanos);
            }
        }
    }

    /**
     *
     */
    private void attemptWrite(SelectionKey key, KafkaChannel channel, long nowNanos) throws IOException {
        if (channel.hasSend()
                && channel.ready()
                && key.isWritable()
                && !channel.maybeBeginClientReauthentication(() -> nowNanos)) {
            write(channel);
        }
    }

    // package-private for testing
    void write(KafkaChannel channel) throws IOException {
        String nodeId = channel.id();
        long bytesSent = channel.write();
        Send send = channel.maybeCompleteSend();
        // We may complete the send with bytesSent < 1 if `TransportLayer.hasPendingWrites` was true and `channel.write()`
        // caused the pending writes to be written to the socket channel buffer
        if (bytesSent > 0 || send != null) {
            long currentTimeMs = time.milliseconds();
            if (bytesSent > 0)
                this.sensors.recordBytesSent(nodeId, bytesSent, currentTimeMs);
            if (send != null) {
                // 保存已经完成发送的请求
                this.completedSends.add(send);
                this.sensors.recordCompletedSend(nodeId, send.size(), currentTimeMs);
            }
        }
    }

    private Collection<SelectionKey> determineHandlingOrder(Set<SelectionKey> selectionKeys) {
        //it is possible that the iteration order over selectionKeys is the same every invocation.
        //this may cause starvation of reads when memory is low. to address this we shuffle the keys if memory is low.
        if (!outOfMemory && memoryPool.availableMemory() < lowMemThreshold) {
            List<SelectionKey> shuffledKeys = new ArrayList<>(selectionKeys);
            Collections.shuffle(shuffledKeys);
            return shuffledKeys;
        } else {
            return selectionKeys;
        }
    }

    private void attemptRead(KafkaChannel channel) throws IOException {
        String nodeId = channel.id();

        // 读数据
        long bytesReceived = channel.read();
        if (bytesReceived != 0) {
            long currentTimeMs = time.milliseconds();
            sensors.recordBytesReceived(nodeId, bytesReceived, currentTimeMs);
            madeReadProgressLastPoll = true;

            // 是否完成读取
            NetworkReceive receive = channel.maybeCompleteReceive();
            if (receive != null) {
                // 保存响应（nodeId, receive）
                addToCompletedReceives(channel, receive, currentTimeMs);
            }
        }
        if (channel.isMuted()) {
            outOfMemory = true; //channel has muted itself due to memory pressure.
        } else {
            madeReadProgressLastPoll = true;
        }
    }

    private boolean maybeReadFromClosingChannel(KafkaChannel channel) {
        boolean hasPending;
        if (channel.state().state() != ChannelState.State.READY)
            hasPending = false;
        else if (explicitlyMutedChannels.contains(channel) || hasCompletedReceive(channel))
            hasPending = true;
        else {
            try {
                attemptRead(channel);
                hasPending = hasCompletedReceive(channel);
            } catch (Exception e) {
                log.trace("Read from closing channel failed, ignoring exception", e);
                hasPending = false;
            }
        }
        return hasPending;
    }

    // Record time spent in pollSelectionKeys for channel (moved into a method to keep checkstyle happy)
    private void maybeRecordTimePerConnection(KafkaChannel channel, long startTimeNanos) {
        if (recordTimePerConnection)
            channel.addNetworkThreadTimeNanos(time.nanoseconds() - startTimeNanos);
    }

    @Override
    public List<Send> completedSends() {
        return this.completedSends;
    }

    @Override
    public Collection<NetworkReceive> completedReceives() {
        return this.completedReceives.values();
    }

    @Override
    public Map<String, ChannelState> disconnected() {
        return this.disconnected;
    }

    @Override
    public List<String> connected() {
        return this.connected;
    }

    @Override
    public void mute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        mute(channel);
    }

    private void mute(KafkaChannel channel) {
        channel.mute();
        explicitlyMutedChannels.add(channel);
    }

    @Override
    public void unmute(String id) {
        KafkaChannel channel = openOrClosingChannelOrFail(id);
        unmute(channel);
    }

    private void unmute(KafkaChannel channel) {
        // Remove the channel from explicitlyMutedChannels only if the channel has been actually unmuted.
        if (channel.maybeUnmute()) {
            explicitlyMutedChannels.remove(channel);
        }
    }

    @Override
    public void muteAll() {
        for (KafkaChannel channel : this.channels.values())
            mute(channel);
    }

    @Override
    public void unmuteAll() {
        for (KafkaChannel channel : this.channels.values())
            unmute(channel);
    }

    // package-private for testing
    void completeDelayedChannelClose(long currentTimeNanos) {
        if (delayedClosingChannels == null)
            return;

        while (!delayedClosingChannels.isEmpty()) {
            DelayedAuthenticationFailureClose delayedClose = delayedClosingChannels.values().iterator().next();
            if (!delayedClose.tryClose(currentTimeNanos))
                break;
        }
    }

    private void maybeCloseOldestConnection(long currentTimeNanos) {
        if (idleExpiryManager == null)
            return;

        Map.Entry<String, Long> expiredConnection = idleExpiryManager.pollExpiredConnection(currentTimeNanos);
        if (expiredConnection != null) {
            String connectionId = expiredConnection.getKey();
            KafkaChannel channel = this.channels.get(connectionId);
            if (channel != null) {
                if (log.isTraceEnabled())
                    log.trace("About to close the idle connection from {} due to being idle for {} millis",
                            connectionId, (currentTimeNanos - expiredConnection.getValue()) / 1000 / 1000);
                channel.state(ChannelState.EXPIRED);
                close(channel, CloseMode.GRACEFUL);
            }
        }
    }

    /**
     * Clears completed receives. This is used by SocketServer to remove references to
     * receive buffers after processing completed receives, without waiting for the next
     * poll().
     */
    public void clearCompletedReceives() {
        this.completedReceives.clear();
    }

    /**
     * Clears completed sends. This is used by SocketServer to remove references to
     * send buffers after processing completed sends, without waiting for the next
     * poll().
     */
    public void clearCompletedSends() {
        this.completedSends.clear();
    }

    /**
     * Clears all the results from the previous poll. This is invoked by Selector at the start of
     * a poll() when all the results from the previous poll are expected to have been handled.
     * <p>
     * SocketServer uses {@link #clearCompletedSends()} and {@link #clearCompletedSends()} to
     * clear `completedSends` and `completedReceives` as soon as they are processed to avoid
     * holding onto large request/response buffers from multiple connections longer than necessary.
     * Clients rely on Selector invoking {@link #clear()} at the start of each poll() since memory usage
     * is less critical and clearing once-per-poll provides the flexibility to process these results in
     * any order before the next poll.
     *
     * 在执行 poll 方法之前需要将上次 poll 方法执行的结果清空。
     */
    private void clear() {
        this.completedSends.clear();
        this.completedReceives.clear();
        this.connected.clear();
        this.disconnected.clear();

        // Remove closed channels after all their buffered receives have been processed or if a send was requested
        // 处理那些关闭连接后还需要处理数据的 KafkaChannel，如调用 close(channel, CloseMode.GRACEFUL)，会将当前 KafkaChannel
        // 存到 closingChannels 中，后续发送的该节点的请求都会失败，通道会在这里被关闭...
        for (Iterator<Map.Entry<String, KafkaChannel>> it = closingChannels.entrySet().iterator(); it.hasNext(); ) {
            KafkaChannel channel = it.next().getValue();
            boolean sendFailed = failedSends.remove(channel.id());
            boolean hasPending = false;
            if (!sendFailed)
                hasPending = maybeReadFromClosingChannel(channel);
            if (!hasPending || sendFailed) {
                // Note: 这里的 notifyDisconnect 为 true，表示需要通知，在该方法里会将 nodeId 放到 disconnected 集合中
                doClose(channel, true);
                it.remove();
            }
        }

        // 将其他情况发送服务端节点失败的 nodeId 放到 disconnected 集合中，后续会在 NetworkClient#poll#handleDisconnections 方法中处理
        for (String channel : this.failedSends)
            this.disconnected.put(channel, ChannelState.FAILED_SEND);
        this.failedSends.clear();
        this.madeReadProgressLastPoll = false;
    }

    /**
     * Check for data, waiting up to the given timeout.
     *
     * @param timeoutMs Length of time to wait, in milliseconds, which must be non-negative
     * @return The number of keys ready
     */
    private int select(long timeoutMs) throws IOException {
        if (timeoutMs < 0L)
            throw new IllegalArgumentException("timeout should be >= 0");

        if (timeoutMs == 0L)
            return this.nioSelector.selectNow();
        else
            return this.nioSelector.select(timeoutMs);
    }

    /**
     * Close the connection identified by the given id
     */
    public void close(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel != null) {
            // There is no disconnect notification for local close, but updating
            // channel state here anyway to avoid confusion.
            // 将 channel 状态设置为 LOCAL_CLOSE
            channel.state(ChannelState.LOCAL_CLOSE);
            // CloseMode.DISCARD_NO_NOTIFY: 关闭连接并且不发送通知
            close(channel, CloseMode.DISCARD_NO_NOTIFY);
        } else {
            // 表示之前可能连接都没有建立或者连接被关闭了
            KafkaChannel closingChannel = this.closingChannels.remove(id);
            // Close any closing channel, leave the channel in the state in which closing was triggered
            if (closingChannel != null)
                doClose(closingChannel, false);
        }
    }

    private void maybeDelayCloseOnAuthenticationFailure(KafkaChannel channel) {
        DelayedAuthenticationFailureClose delayedClose = new DelayedAuthenticationFailureClose(channel, failedAuthenticationDelayMs);
        if (delayedClosingChannels != null)
            delayedClosingChannels.put(channel.id(), delayedClose);
        else
            delayedClose.closeNow();
    }

    private void handleCloseOnAuthenticationFailure(KafkaChannel channel) {
        try {
            channel.completeCloseOnAuthenticationFailure();
        } catch (Exception e) {
            log.error("Exception handling close on authentication failure node {}", channel.id(), e);
        } finally {
            close(channel, CloseMode.GRACEFUL);
        }
    }

    /**
     * Begin closing this connection.
     * If 'closeMode' is `CloseMode.GRACEFUL`, the channel is disconnected here, but outstanding receives
     * are processed. The channel is closed when there are no outstanding receives or if a send is
     * requested. For other values of `closeMode`, outstanding receives are discarded and the channel
     * is closed immediately.
     *
     * The channel will be added to disconnect list when it is actually closed if `closeMode.notifyDisconnect`
     * is true.
     *
     * 如果连接关闭且 closeMode 是 CloseMode.GRACEFUL，未处理的响应和请求会被处理。如果 closeMode 是其他，则连接关闭时不会进行任何出来。
     *
     * 如果连接关闭，并且 closeMode 的 notifyDisconnect 为 true，则会将这个连接加到 disconnect 列表里，会在后续通知该连接的断开状态。
     */
    private void close(KafkaChannel channel, CloseMode closeMode) {
        channel.disconnect();

        // Ensure that `connected` does not have closed channels. This could happen if `prepare` throws an exception
        // in the `poll` invocation when `finishConnect` succeeds
        // 从连接数据结构中移除
        connected.remove(channel.id());

        // Keep track of closed channels with pending receives so that all received records
        // may be processed. For example, when producer with acks=0 sends some records and
        // closes its connections, a single poll() in the broker may receive records and
        // handle close(). When the remote end closes its connection, the channel is retained until
        // a send fails or all outstanding receives are processed. Mute state of disconnected channels
        // are tracked to ensure that requests are processed one-by-one by the broker to preserve ordering.
        // 处理可能没有完成的请求和响应
        if (closeMode == CloseMode.GRACEFUL && maybeReadFromClosingChannel(channel)) {
            // 添加到关闭的 channel 数据结构中
            // 这里和下面分之的区别是没有关闭像 SocketChannel 等资源，只是把 Channel 放到了一个集合中。
            // 目的是在下次发送中如果发现当前的主机已经在关闭了，就不要再发送消息了。
            closingChannels.put(channel.id(), channel);
            log.debug("Tracking closing connection {} to process outstanding requests", channel.id());
        } else {
            // 关闭 channel
            doClose(channel, closeMode.notifyDisconnect);
        }
        // 从 channels 移除该主机（在建立连接后，会将该主机对应的 KafkaChannel 存储到这个数据结构中）
        this.channels.remove(channel.id());

        if (delayedClosingChannels != null)
            delayedClosingChannels.remove(channel.id());

        if (idleExpiryManager != null)
            idleExpiryManager.remove(channel.id());
    }

    private void doClose(KafkaChannel channel, boolean notifyDisconnect) {
        SelectionKey key = channel.selectionKey();
        try {
            immediatelyConnectedKeys.remove(key);
            keysWithBufferedRead.remove(key);
            // 执行 close 方法
            channel.close();
        } catch (IOException e) {
            log.error("Exception closing connection to node {}:", channel.id(), e);
        } finally {
            // 取消绑定
            key.cancel();
            // 移除绑定的 KafkaChannel
            key.attach(null);
        }

        this.sensors.connectionClosed.record();
        this.explicitlyMutedChannels.remove(channel);
        // 如果要通知连接关闭，则放到这个 disconnected 列表里
        if (notifyDisconnect)
            this.disconnected.put(channel.id(), channel.state());
    }

    /**
     * check if channel is ready
     */
    @Override
    public boolean isChannelReady(String id) {
        KafkaChannel channel = this.channels.get(id);
        return channel != null && channel.ready();
    }

    private KafkaChannel openOrClosingChannelOrFail(String id) {
        KafkaChannel channel = this.channels.get(id);
        if (channel == null)
            channel = this.closingChannels.get(id);
        if (channel == null)
            throw new IllegalStateException("Attempt to retrieve channel for which there is no connection. Connection id " + id + " existing connections " + channels.keySet());
        return channel;
    }

    /**
     * Return the selector channels.
     */
    public List<KafkaChannel> channels() {
        return new ArrayList<>(channels.values());
    }

    /**
     * Return the channel associated with this connection or `null` if there is no channel associated with the
     * connection.
     */
    public KafkaChannel channel(String id) {
        return this.channels.get(id);
    }

    /**
     * Return the channel with the specified id if it was disconnected, but not yet closed
     * since there are outstanding messages to be processed.
     */
    public KafkaChannel closingChannel(String id) {
        return closingChannels.get(id);
    }

    /**
     * Returns the lowest priority channel chosen using the following sequence:
     *   1) If one or more channels are in closing state, return any one of them
     *   2) If idle expiry manager is enabled, return the least recently updated channel
     *   3) Otherwise return any of the channels
     *
     * This method is used to close a channel to accommodate a new channel on the inter-broker listener
     * when broker-wide `max.connections` limit is enabled.
     */
    public KafkaChannel lowestPriorityChannel() {
        KafkaChannel channel = null;
        if (!closingChannels.isEmpty()) {
            channel = closingChannels.values().iterator().next();
        } else if (idleExpiryManager != null && !idleExpiryManager.lruConnections.isEmpty()) {
            String channelId = idleExpiryManager.lruConnections.keySet().iterator().next();
            channel = channel(channelId);
        } else if (!channels.isEmpty()) {
            channel = channels.values().iterator().next();
        }
        return channel;
    }

    /**
     * Get the channel associated with selectionKey
     */
    private KafkaChannel channel(SelectionKey key) {
        return (KafkaChannel) key.attachment();
    }

    /**
     * Check if given channel has a completed receive
     */
    private boolean hasCompletedReceive(KafkaChannel channel) {
        return completedReceives.containsKey(channel.id());
    }

    /**
     * adds a receive to completed receives
     */
    private void addToCompletedReceives(KafkaChannel channel, NetworkReceive networkReceive, long currentTimeMs) {
        if (hasCompletedReceive(channel))
            throw new IllegalStateException("Attempting to add second completed receive to channel " + channel.id());

        this.completedReceives.put(channel.id(), networkReceive);
        sensors.recordCompletedReceive(channel.id(), networkReceive.size(), currentTimeMs);
    }

    // only for testing
    public Set<SelectionKey> keys() {
        return new HashSet<>(nioSelector.keys());
    }


    class SelectorChannelMetadataRegistry implements ChannelMetadataRegistry {
        private CipherInformation cipherInformation;
        private ClientInformation clientInformation;

        @Override
        public void registerCipherInformation(final CipherInformation cipherInformation) {
            if (this.cipherInformation != null) {
                if (this.cipherInformation.equals(cipherInformation))
                    return;
                sensors.connectionsByCipher.decrement(this.cipherInformation);
            }

            this.cipherInformation = cipherInformation;
            sensors.connectionsByCipher.increment(cipherInformation);
        }

        @Override
        public CipherInformation cipherInformation() {
            return cipherInformation;
        }

        @Override
        public void registerClientInformation(final ClientInformation clientInformation) {
            if (this.clientInformation != null) {
                if (this.clientInformation.equals(clientInformation))
                    return;
                sensors.connectionsByClient.decrement(this.clientInformation);
            }

            this.clientInformation = clientInformation;
            sensors.connectionsByClient.increment(clientInformation);
        }

        @Override
        public ClientInformation clientInformation() {
            return clientInformation;
        }

        @Override
        public void close() {
            if (this.cipherInformation != null) {
                sensors.connectionsByCipher.decrement(this.cipherInformation);
                this.cipherInformation = null;
            }

            if (this.clientInformation != null) {
                sensors.connectionsByClient.decrement(this.clientInformation);
                this.clientInformation = null;
            }
        }
    }

    class SelectorMetrics implements AutoCloseable {
        private final Metrics metrics;
        private final Map<String, String> metricTags;
        private final boolean metricsPerConnection;
        private final String metricGrpName;
        private final String perConnectionMetricGrpName;

        public final Sensor connectionClosed;
        public final Sensor connectionCreated;
        public final Sensor successfulAuthentication;
        public final Sensor successfulReauthentication;
        public final Sensor successfulAuthenticationNoReauth;
        public final Sensor reauthenticationLatency;
        public final Sensor failedAuthentication;
        public final Sensor failedReauthentication;
        public final Sensor bytesTransferred;
        public final Sensor bytesSent;
        public final Sensor requestsSent;
        public final Sensor bytesReceived;
        public final Sensor responsesReceived;
        public final Sensor selectTime;
        public final Sensor ioTime;
        public final IntGaugeSuite<CipherInformation> connectionsByCipher;
        public final IntGaugeSuite<ClientInformation> connectionsByClient;

        /* Names of metrics that are not registered through sensors */
        private final List<MetricName> topLevelMetricNames = new ArrayList<>();
        private final List<Sensor> sensors = new ArrayList<>();

        public SelectorMetrics(Metrics metrics, String metricGrpPrefix, Map<String, String> metricTags, boolean metricsPerConnection) {
            this.metrics = metrics;
            this.metricTags = metricTags;
            this.metricsPerConnection = metricsPerConnection;
            this.metricGrpName = metricGrpPrefix + "-metrics";
            this.perConnectionMetricGrpName = metricGrpPrefix + "-node-metrics";
            StringBuilder tagsSuffix = new StringBuilder();

            for (Map.Entry<String, String> tag: metricTags.entrySet()) {
                tagsSuffix.append(tag.getKey());
                tagsSuffix.append("-");
                tagsSuffix.append(tag.getValue());
            }

            this.connectionClosed = sensor("connections-closed:" + tagsSuffix);
            this.connectionClosed.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-close", "connections closed"));

            this.connectionCreated = sensor("connections-created:" + tagsSuffix);
            this.connectionCreated.add(createMeter(metrics, metricGrpName, metricTags,
                    "connection-creation", "new connections established"));

            this.successfulAuthentication = sensor("successful-authentication:" + tagsSuffix);
            this.successfulAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "successful-authentication", "connections with successful authentication"));

            this.successfulReauthentication = sensor("successful-reauthentication:" + tagsSuffix);
            this.successfulReauthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "successful-reauthentication", "successful re-authentication of connections"));

            this.successfulAuthenticationNoReauth = sensor("successful-authentication-no-reauth:" + tagsSuffix);
            MetricName successfulAuthenticationNoReauthMetricName = metrics.metricName(
                    "successful-authentication-no-reauth-total", metricGrpName,
                    "The total number of connections with successful authentication where the client does not support re-authentication",
                    metricTags);
            this.successfulAuthenticationNoReauth.add(successfulAuthenticationNoReauthMetricName, new CumulativeSum());

            this.failedAuthentication = sensor("failed-authentication:" + tagsSuffix);
            this.failedAuthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "failed-authentication", "connections with failed authentication"));

            this.failedReauthentication = sensor("failed-reauthentication:" + tagsSuffix);
            this.failedReauthentication.add(createMeter(metrics, metricGrpName, metricTags,
                    "failed-reauthentication", "failed re-authentication of connections"));

            this.reauthenticationLatency = sensor("reauthentication-latency:" + tagsSuffix);
            MetricName reauthenticationLatencyMaxMetricName = metrics.metricName("reauthentication-latency-max",
                    metricGrpName, "The max latency observed due to re-authentication",
                    metricTags);
            this.reauthenticationLatency.add(reauthenticationLatencyMaxMetricName, new Max());
            MetricName reauthenticationLatencyAvgMetricName = metrics.metricName("reauthentication-latency-avg",
                    metricGrpName, "The average latency observed due to re-authentication",
                    metricTags);
            this.reauthenticationLatency.add(reauthenticationLatencyAvgMetricName, new Avg());

            this.bytesTransferred = sensor("bytes-sent-received:" + tagsSuffix);
            bytesTransferred.add(createMeter(metrics, metricGrpName, metricTags, new WindowedCount(),
                    "network-io", "network operations (reads or writes) on all connections"));

            this.bytesSent = sensor("bytes-sent:" + tagsSuffix, bytesTransferred);
            this.bytesSent.add(createMeter(metrics, metricGrpName, metricTags,
                    "outgoing-byte", "outgoing bytes sent to all servers"));

            this.requestsSent = sensor("requests-sent:" + tagsSuffix);
            this.requestsSent.add(createMeter(metrics, metricGrpName, metricTags, new WindowedCount(),
                    "request", "requests sent"));
            MetricName metricName = metrics.metricName("request-size-avg", metricGrpName, "The average size of requests sent.", metricTags);
            this.requestsSent.add(metricName, new Avg());
            metricName = metrics.metricName("request-size-max", metricGrpName, "The maximum size of any request sent.", metricTags);
            this.requestsSent.add(metricName, new Max());

            this.bytesReceived = sensor("bytes-received:" + tagsSuffix, bytesTransferred);
            this.bytesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    "incoming-byte", "bytes read off all sockets"));

            this.responsesReceived = sensor("responses-received:" + tagsSuffix);
            this.responsesReceived.add(createMeter(metrics, metricGrpName, metricTags,
                    new WindowedCount(), "response", "responses received"));

            this.selectTime = sensor("select-time:" + tagsSuffix);
            this.selectTime.add(createMeter(metrics, metricGrpName, metricTags,
                    new WindowedCount(), "select", "times the I/O layer checked for new I/O to perform"));
            metricName = metrics.metricName("io-wait-time-ns-avg", metricGrpName, "The average length of time the I/O thread spent waiting for a socket ready for reads or writes in nanoseconds.", metricTags);
            this.selectTime.add(metricName, new Avg());
            this.selectTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io-wait", "waiting"));

            this.ioTime = sensor("io-time:" + tagsSuffix);
            metricName = metrics.metricName("io-time-ns-avg", metricGrpName, "The average length of time for I/O per select call in nanoseconds.", metricTags);
            this.ioTime.add(metricName, new Avg());
            this.ioTime.add(createIOThreadRatioMeter(metrics, metricGrpName, metricTags, "io", "doing I/O"));

            this.connectionsByCipher = new IntGaugeSuite<>(log, "sslCiphers", metrics,
                cipherInformation -> {
                    Map<String, String> tags = new LinkedHashMap<>();
                    tags.put("cipher", cipherInformation.cipher());
                    tags.put("protocol", cipherInformation.protocol());
                    tags.putAll(metricTags);
                    return metrics.metricName("connections", metricGrpName, "The number of connections with this SSL cipher and protocol.", tags);
                }, 100);

            this.connectionsByClient = new IntGaugeSuite<>(log, "clients", metrics,
                clientInformation -> {
                    Map<String, String> tags = new LinkedHashMap<>();
                    tags.put("clientSoftwareName", clientInformation.softwareName());
                    tags.put("clientSoftwareVersion", clientInformation.softwareVersion());
                    tags.putAll(metricTags);
                    return metrics.metricName("connections", metricGrpName, "The number of connections with this client and version.", tags);
                }, 100);

            metricName = metrics.metricName("connection-count", metricGrpName, "The current number of active connections.", metricTags);
            topLevelMetricNames.add(metricName);
            this.metrics.addMetric(metricName, (config, now) -> channels.size());
        }

        private Meter createMeter(Metrics metrics, String groupName, Map<String, String> metricTags,
                SampledStat stat, String baseName, String descriptiveName) {
            MetricName rateMetricName = metrics.metricName(baseName + "-rate", groupName,
                            String.format("The number of %s per second", descriptiveName), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "-total", groupName,
                            String.format("The total number of %s", descriptiveName), metricTags);
            if (stat == null)
                return new Meter(rateMetricName, totalMetricName);
            else
                return new Meter(stat, rateMetricName, totalMetricName);
        }

        private Meter createMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                String baseName, String descriptiveName) {
            return createMeter(metrics, groupName, metricTags, null, baseName, descriptiveName);
        }

        private Meter createIOThreadRatioMeter(Metrics metrics, String groupName,  Map<String, String> metricTags,
                String baseName, String action) {
            MetricName rateMetricName = metrics.metricName(baseName + "-ratio", groupName,
                    String.format("The fraction of time the I/O thread spent %s", action), metricTags);
            MetricName totalMetricName = metrics.metricName(baseName + "time-total", groupName,
                    String.format("The total time the I/O thread spent %s", action), metricTags);
            return new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalMetricName);
        }

        private Sensor sensor(String name, Sensor... parents) {
            Sensor sensor = metrics.sensor(name, parents);
            sensors.add(sensor);
            return sensor;
        }

        public void maybeRegisterConnectionMetrics(String connectionId) {
            if (!connectionId.isEmpty() && metricsPerConnection) {
                // if one sensor of the metrics has been registered for the connection,
                // then all other sensors should have been registered; and vice versa
                String nodeRequestName = "node-" + connectionId + ".requests-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest == null) {
                    Map<String, String> tags = new LinkedHashMap<>(metricTags);
                    tags.put("node-id", "node-" + connectionId);

                    nodeRequest = sensor(nodeRequestName);
                    nodeRequest.add(createMeter(metrics, perConnectionMetricGrpName, tags, new WindowedCount(), "request", "requests sent"));
                    MetricName metricName = metrics.metricName("request-size-avg", perConnectionMetricGrpName, "The average size of requests sent.", tags);
                    nodeRequest.add(metricName, new Avg());
                    metricName = metrics.metricName("request-size-max", perConnectionMetricGrpName, "The maximum size of any request sent.", tags);
                    nodeRequest.add(metricName, new Max());

                    String bytesSentName = "node-" + connectionId + ".bytes-sent";
                    Sensor bytesSent = sensor(bytesSentName);
                    bytesSent.add(createMeter(metrics, perConnectionMetricGrpName, tags, "outgoing-byte", "outgoing bytes"));

                    String nodeResponseName = "node-" + connectionId + ".responses-received";
                    Sensor nodeResponse = sensor(nodeResponseName);
                    nodeResponse.add(createMeter(metrics, perConnectionMetricGrpName, tags, new WindowedCount(), "response", "responses received"));

                    String bytesReceivedName = "node-" + connectionId + ".bytes-received";
                    Sensor bytesReceive = sensor(bytesReceivedName);
                    bytesReceive.add(createMeter(metrics, perConnectionMetricGrpName, tags, "incoming-byte", "incoming bytes"));

                    String nodeTimeName = "node-" + connectionId + ".latency";
                    Sensor nodeRequestTime = sensor(nodeTimeName);
                    metricName = metrics.metricName("request-latency-avg", perConnectionMetricGrpName, tags);
                    nodeRequestTime.add(metricName, new Avg());
                    metricName = metrics.metricName("request-latency-max", perConnectionMetricGrpName, tags);
                    nodeRequestTime.add(metricName, new Max());
                }
            }
        }

        public void recordBytesSent(String connectionId, long bytes, long currentTimeMs) {
            this.bytesSent.record(bytes, currentTimeMs);
            if (!connectionId.isEmpty()) {
                String bytesSentName = "node-" + connectionId + ".bytes-sent";
                Sensor bytesSent = this.metrics.getSensor(bytesSentName);
                if (bytesSent != null)
                    bytesSent.record(bytes, currentTimeMs);
            }
        }

        public void recordCompletedSend(String connectionId, long totalBytes, long currentTimeMs) {
            requestsSent.record(totalBytes, currentTimeMs);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".requests-sent";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(totalBytes, currentTimeMs);
            }
        }

        public void recordBytesReceived(String connectionId, long bytes, long currentTimeMs) {
            this.bytesReceived.record(bytes, currentTimeMs);
            if (!connectionId.isEmpty()) {
                String bytesReceivedName = "node-" + connectionId + ".bytes-received";
                Sensor bytesReceived = this.metrics.getSensor(bytesReceivedName);
                if (bytesReceived != null)
                    bytesReceived.record(bytes, currentTimeMs);
            }
        }

        public void recordCompletedReceive(String connectionId, long totalBytes, long currentTimeMs) {
            responsesReceived.record(totalBytes, currentTimeMs);
            if (!connectionId.isEmpty()) {
                String nodeRequestName = "node-" + connectionId + ".responses-received";
                Sensor nodeRequest = this.metrics.getSensor(nodeRequestName);
                if (nodeRequest != null)
                    nodeRequest.record(totalBytes, currentTimeMs);
            }
        }

        public void close() {
            for (MetricName metricName : topLevelMetricNames)
                metrics.removeMetric(metricName);
            for (Sensor sensor : sensors)
                metrics.removeSensor(sensor.name());
            connectionsByCipher.close();
            connectionsByClient.close();
        }
    }

    /**
     * Encapsulate a channel that must be closed after a specific delay has elapsed due to authentication failure.
     */
    private class DelayedAuthenticationFailureClose {
        private final KafkaChannel channel;
        private final long endTimeNanos;
        private boolean closed;

        /**
         * @param channel The channel whose close is being delayed
         * @param delayMs The amount of time by which the operation should be delayed
         */
        public DelayedAuthenticationFailureClose(KafkaChannel channel, int delayMs) {
            this.channel = channel;
            this.endTimeNanos = time.nanoseconds() + (delayMs * 1000L * 1000L);
            this.closed = false;
        }

        /**
         * Try to close this channel if the delay has expired.
         * @param currentTimeNanos The current time
         * @return True if the delay has expired and the channel was closed; false otherwise
         */
        public final boolean tryClose(long currentTimeNanos) {
            if (endTimeNanos <= currentTimeNanos)
                closeNow();
            return closed;
        }

        /**
         * Close the channel now, regardless of whether the delay has expired or not.
         */
        public final void closeNow() {
            if (closed)
                throw new IllegalStateException("Attempt to close a channel that has already been closed");
            handleCloseOnAuthenticationFailure(channel);
            closed = true;
        }
    }

    // helper class for tracking least recently used connections to enable idle connection closing
    private static class IdleExpiryManager {
        private final Map<String, Long> lruConnections;
        private final long connectionsMaxIdleNanos;
        private long nextIdleCloseCheckTime;

        public IdleExpiryManager(Time time, long connectionsMaxIdleMs) {
            this.connectionsMaxIdleNanos = connectionsMaxIdleMs * 1000 * 1000;
            // initial capacity and load factor are default, we set them explicitly because we want to set accessOrder = true
            this.lruConnections = new LinkedHashMap<>(16, .75F, true);
            this.nextIdleCloseCheckTime = time.nanoseconds() + this.connectionsMaxIdleNanos;
        }

        public void update(String connectionId, long currentTimeNanos) {
            lruConnections.put(connectionId, currentTimeNanos);
        }

        public Map.Entry<String, Long> pollExpiredConnection(long currentTimeNanos) {
            if (currentTimeNanos <= nextIdleCloseCheckTime)
                return null;

            if (lruConnections.isEmpty()) {
                nextIdleCloseCheckTime = currentTimeNanos + connectionsMaxIdleNanos;
                return null;
            }

            Map.Entry<String, Long> oldestConnectionEntry = lruConnections.entrySet().iterator().next();
            Long connectionLastActiveTime = oldestConnectionEntry.getValue();
            nextIdleCloseCheckTime = connectionLastActiveTime + connectionsMaxIdleNanos;

            if (currentTimeNanos > nextIdleCloseCheckTime)
                return oldestConnectionEntry;
            else
                return null;
        }

        public void remove(String connectionId) {
            lruConnections.remove(connectionId);
        }
    }

    //package-private for testing
    boolean isOutOfMemory() {
        return outOfMemory;
    }

    //package-private for testing
    boolean isMadeReadProgressLastPoll() {
        return madeReadProgressLastPoll;
    }

    // package-private for testing
    Map<?, ?> delayedClosingChannels() {
        return delayedClosingChannels;
    }
}
