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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.clients.KafkaClient;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.RequestCompletionHandler;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.DisconnectException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.requests.AbstractRequest;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;
import org.slf4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Higher level consumer access to the network layer with basic support for request futures. This class
 * is thread-safe, but provides no synchronization for response callbacks. This guarantees that no locks
 * are held when they are invoked.
 */
public class ConsumerNetworkClient implements Closeable {
    // 一次拉取的最大超时时间 5 s
    private static final int MAX_POLL_TIMEOUT_MS = 5000;

    // the mutable state of this class is protected by the object's monitor (excluding the wakeup
    // flag and the request completion queue below).
    private final Logger log;
    // NetworkClient 对象，用于和broker进行通信
    private final KafkaClient client;
    // 消息发送缓冲队列，其中key是node节点，value是发往此node的ClientRequest集合
    private final UnsentRequests unsent = new UnsentRequests();
    // 消费者端kafka集群元数据
    private final Metadata metadata;
    private final Time time;
    // 重试退避时间
    private final long retryBackoffMs;
    // NetworkClient#poll操作的超时时间，默认5秒
    private final int maxPollTimeoutMs;
    // 请求超时时间，默认3s
    private final int requestTimeoutMs;
    // 是否禁用wakeup()，因为有的方法已经执行的情况下就没必要再唤醒Selector#poll() 方法的阻塞了，比如close() 方法，因为网络IO已经要关闭了，就没必要唤醒了
    private final AtomicBoolean wakeupDisabled = new AtomicBoolean();

    // We do not need high throughput, so use a fair lock to try to avoid starvation
    private final ReentrantLock lock = new ReentrantLock(true);

    // when requests complete, they are transferred to this queue prior to invocation. The purpose
    // is to avoid invoking them while holding this object's monitor which can open the door for deadlocks.
    // 用来存储请求的回调对象的队列集合。响应回来后并不是直接调用回调对象里的回调方法，而是放到队列中等待响应处理完后统一调用回调方法
    private final ConcurrentLinkedQueue<RequestFutureCompletionHandler> pendingCompletion = new ConcurrentLinkedQueue<>();
    // 用来存储待断开网络连接的节点集合
    private final ConcurrentLinkedQueue<Node> pendingDisconnects = new ConcurrentLinkedQueue<>();

    // this flag allows the client to be safely woken up without waiting on the lock above. It is
    // atomic to avoid the need to acquire the lock above in order to enable it concurrently.
    // 是否唤醒消费者网络客户端，当有唤醒selector#poll阻塞的操作时，就把这个值设置为true
    private final AtomicBoolean wakeup = new AtomicBoolean(false);

    public ConsumerNetworkClient(LogContext logContext,
                                 KafkaClient client,
                                 Metadata metadata,
                                 Time time,
                                 long retryBackoffMs,
                                 int requestTimeoutMs,
                                 int maxPollTimeoutMs) {
        this.log = logContext.logger(ConsumerNetworkClient.class);
        this.client = client;
        this.metadata = metadata;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
        this.maxPollTimeoutMs = Math.min(maxPollTimeoutMs, MAX_POLL_TIMEOUT_MS);
        this.requestTimeoutMs = requestTimeoutMs;
    }

    public int defaultRequestTimeoutMs() {
        return requestTimeoutMs;
    }

    /**
     * Send a request with the default timeout. See {@link #send(Node, AbstractRequest.Builder, int)}.
     * 给某个node发送一个请求（用默认的超时时间）
     */
    public RequestFuture<ClientResponse> send(Node node, AbstractRequest.Builder<?> requestBuilder) {
        return send(node, requestBuilder, requestTimeoutMs);
    }

    /**
     * Send a new request. Note that the request is not actually transmitted on the
     * network until one of the {@link #poll(Timer)} variants is invoked. At this
     * point the request will either be transmitted successfully or will fail.
     * Use the returned future to obtain the result of the send. Note that there is no
     * need to check for disconnects explicitly on the {@link ClientResponse} object;
     * instead, the future will be failed with a {@link DisconnectException}.
     *
     * @param node The destination of the request
     * @param requestBuilder A builder for the request payload
     * @param requestTimeoutMs Maximum time in milliseconds to await a response before disconnecting the socket and
     *                         cancelling the request. The request may be cancelled sooner if the socket disconnects
     *                         for any reason.
     * @return A future which indicates the result of the send.
     */
    public RequestFuture<ClientResponse> send(Node node,
                                              AbstractRequest.Builder<?> requestBuilder,
                                              int requestTimeoutMs) {
        long now = time.milliseconds();
        // 1、创建一个请求的回调对象，用于处理请求的结果
        RequestFutureCompletionHandler completionHandler = new RequestFutureCompletionHandler();
        // 2、构造请求对象，调用Client#newClientRequest方法创建一个ClientRequest对象
        ClientRequest clientRequest = client.newClientRequest(node.idString(), requestBuilder, now, true,
                requestTimeoutMs, completionHandler);
        // 3、将请求放入到该node对应的请求队列里
        unsent.put(node, clientRequest);

        // wakeup the client in case it is blocking in poll so that we can send the queued request
        // 4、唤醒处于阻塞过程中的selector#poll方法，为什么要唤醒呢？因为要尽快把请求发送出去
        client.wakeup();
        // 返回一个future个调用方
        return completionHandler.future;
    }

    // 找出负载最小的节点（也就是看发送中队列inFlightRequests里请求最少的node）
    public Node leastLoadedNode() {
        lock.lock();
        try {
            return client.leastLoadedNode(time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    public boolean hasReadyNodes(long now) {
        lock.lock();
        try {
            return client.hasReadyNodes(now);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block waiting on the metadata refresh with a timeout.
     *
     * @return true if update succeeded, false otherwise.
     */
    public boolean awaitMetadataUpdate(Timer timer) {
        int version = this.metadata.requestUpdate();
        do {
            poll(timer);
        } while (this.metadata.updateVersion() == version && timer.notExpired());
        return this.metadata.updateVersion() > version;
    }

    /**
     * Ensure our metadata is fresh (if an update is expected, this will block
     * until it has completed).
     */
    boolean ensureFreshMetadata(Timer timer) {
        if (this.metadata.updateRequested() || this.metadata.timeToNextUpdate(timer.currentTimeMs()) == 0) {
            return awaitMetadataUpdate(timer);
        } else {
            // the metadata is already fresh
            return true;
        }
    }

    /**
     * Wakeup an active poll. This will cause the polling thread to throw an exception either
     * on the current poll if one is active, or the next poll.
     */
    public void wakeup() {
        // wakeup should be safe without holding the client lock since it simply delegates to
        // Selector's wakeup, which is thread-safe
        log.debug("Received user wakeup");
        this.wakeup.set(true);
        this.client.wakeup();
    }

    /**
     * Block indefinitely until the given request future has finished.
     * @param future The request future to await.
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(RequestFuture<?> future) {
        while (!future.isDone())
            poll(time.timer(Long.MAX_VALUE), future);
    }

    /**
     * Block until the provided request future request has finished or the timeout has expired.
     * @param future The request future to wait for
     * @param timer Timer bounding how long this method can block
     * @return true if the future is done, false otherwise
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public boolean poll(RequestFuture<?> future, Timer timer) {
        do {
            poll(timer, future);
            // 只要请求没有响应并且没有超时就一直循环
        } while (!future.isDone() && timer.notExpired());
        return future.isDone();
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @throws WakeupException if {@link #wakeup()} is called from another thread
     * @throws InterruptException if the calling thread is interrupted
     */
    public void poll(Timer timer) {
        poll(timer, null);
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition
     */
    public void poll(Timer timer, PollCondition pollCondition) {
        poll(timer, pollCondition, false);
    }

    /**
     * Poll for any network IO.
     * @param timer Timer bounding how long this method can block
     * @param pollCondition Nullable blocking condition
     * @param disableWakeup If TRUE disable triggering wake-ups
     */
    public void poll(Timer timer, PollCondition pollCondition, boolean disableWakeup) {
        // there may be handlers which need to be invoked if we woke up the previous call to poll
        // 1、触发已经完成的请求的回调方法
        firePendingCompletedRequests();

        lock.lock();
        try {
            // Handle async disconnects prior to attempting any sends
            // 2、处理等待断开的连接
            handlePendingDisconnects();

            // send all the requests we can send now
            // 3、调用 trySend 方法将 unsent 中的请求取出，并与目标节点建立连接
            long pollDelayMs = trySend(timer.currentTimeMs());

            // check whether the poll is still needed by the caller. Note that if the expected completion
            // condition becomes satisfied after the call to shouldBlock() (because of a fired completion
            // handler), the client will be woken up.
            // 调用poll方法监听底层网络连接，并处理网络数据读写
            if (pendingCompletion.isEmpty() && (pollCondition == null || pollCondition.shouldBlock())) {
                // if there are no requests in flight, do not block longer than the retry backoff
                // poll 操作的阻塞时间
                long pollTimeout = Math.min(timer.remainingMs(), pollDelayMs);
                if (client.inFlightRequestCount() == 0)
                    pollTimeout = Math.min(pollTimeout, retryBackoffMs);
                // 带超时阻塞的poll（这里就会感知底层的Socket读写事件，如果有可写事件发生，就会将KafkaChannel里的send属性写入Socket的buffer里）
                client.poll(pollTimeout, timer.currentTimeMs());
            } else {
                // 非阻塞的poll
                client.poll(0, timer.currentTimeMs());
            }
            timer.update();

            // handle any disconnects by failing the active requests. note that disconnects must
            // be checked immediately following poll since any subsequent call to client.ready()
            // will reset the disconnect status
            // 4、处理断开连接的 node 的消息
            checkDisconnects(timer.currentTimeMs());
            if (!disableWakeup) {
                // trigger wakeups after checking for disconnects so that the callbacks will be ready
                // to be fired on the next call to poll()
                // 5、如果有selector#poll 阻塞中的请求，而且有方法标记不能中断，则抛出异常
                maybeTriggerWakeup();
            }
            // throw InterruptException if this thread is interrupted
            maybeThrowInterruptException();

            // try again to send requests since buffer space may have been
            // cleared or a connect finished in the poll
            // 6、【重要】上面调用了poll方法，KafkaChannel里的send对象就有可能被成功写入到底层socket的缓冲区了，
            // 所以再次从unsent 集合中取请求，尝试写入到KafkaChannel的send对象上
            trySend(timer.currentTimeMs());

            // fail requests that couldn't be sent if they have expired
            // 7、处理unsent中的超时请求
            failExpiredRequests(timer.currentTimeMs());

            // clean unsent requests collection to keep the map from growing indefinitely
            unsent.clean();
        } finally {
            lock.unlock();
        }

        // called without the lock to avoid deadlock potential if handlers need to acquire locks
        // 8、调用 firePendingCompletedRequests 方法回调上层请求的注册的发送完成回调处理器
        firePendingCompletedRequests();

        metadata.maybeThrowAnyException();
    }

    /**
     * Poll for network IO and return immediately. This will not trigger wakeups.
     */
    public void pollNoWakeup() {
        poll(time.timer(0), null, true);
    }

    /**
     * Poll for network IO in best-effort only trying to transmit the ready-to-send request
     * Do not check any pending requests or metadata errors so that no exception should ever
     * be thrown, also no wakeups be triggered and no interrupted exception either.
     */
    public void transmitSends() {
        Timer timer = time.timer(0);

        // do not try to handle any disconnects, prev request failures, metadata exception etc;
        // just try once and return immediately
        lock.lock();
        try {
            // send all the requests we can send now
            // 将所有节点发送队列里的请求发送出去（发送到哪里呢？发送到KafkaChannel的Send属性上，等待NIO channel有可写事件发生时才真正写入）
            trySend(timer.currentTimeMs());

            // 真实发送
            client.poll(0, timer.currentTimeMs());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Block until all pending requests from the given node have finished.
     * @param node The node to await requests from
     * @param timer Timer bounding how long this method can block
     * @return true If all requests finished, false if the timeout expired first
     */
    public boolean awaitPendingRequests(Node node, Timer timer) {
        while (hasPendingRequests(node) && timer.notExpired()) {
            poll(timer);
        }
        return !hasPendingRequests(node);
    }

    /**
     * Get the count of pending requests to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return The number of pending requests
     */
    public int pendingRequestCount(Node node) {
        lock.lock();
        try {
            return unsent.requestCount(node) + client.inFlightRequestCount(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request to the given node. This includes both request that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @param node The node in question
     * @return A boolean indicating whether there is pending request
     */
    public boolean hasPendingRequests(Node node) {
        if (unsent.hasRequests(node))
            return true;
        lock.lock();
        try {
            return client.hasInFlightRequests(node.idString());
        } finally {
            lock.unlock();
        }
    }

    /**
     * Get the total count of pending requests from all nodes. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return The total count of pending requests
     */
    public int pendingRequestCount() {
        lock.lock();
        try {
            return unsent.requestCount() + client.inFlightRequestCount();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check whether there is pending request. This includes both requests that
     * have been transmitted (i.e. in-flight requests) and those which are awaiting transmission.
     * @return A boolean indicating whether there is pending request
     * 是否有发送中的请求
     */
    public boolean hasPendingRequests() {
        if (unsent.hasRequests())
            return true;
        lock.lock();
        try {
            // 是否有发送中的请求
            return client.hasInFlightRequests();
        } finally {
            lock.unlock();
        }
    }

    // 触发请求完成的回调方法
    private void firePendingCompletedRequests() {
        boolean completedRequestsFired = false;
        for (;;) {
            // 从队列里取出已经响应完成等待回调的请求（可以看到消费者端的请求回调是在这里统一进行的）
            RequestFutureCompletionHandler completionHandler = pendingCompletion.poll();
            if (completionHandler == null)
                break;

            // 触发该handler的回调方法
            completionHandler.fireCompletion();
            completedRequestsFired = true;
        }

        // wakeup the client in case it is blocking in poll for this future's completion
        // 回调完成后唤醒client（client此时可能阻塞在selector上）
        if (completedRequestsFired)
            client.wakeup();
    }

    private void checkDisconnects(long now) {
        // any disconnects affecting requests that have already been transmitted will be handled
        // by NetworkClient, so we just need to check whether connections for any of the unsent
        // requests have been disconnected; if they have, then we complete the corresponding future
        // and set the disconnect flag in the ClientResponse
        for (Node node : unsent.nodes()) {
            // 连接是否失败
            if (client.connectionFailed(node)) {
                // Remove entry before invoking request callback to avoid callbacks handling
                // coordinator failures traversing the unsent list again.
                // 将该节点以及该节点的请求从unsent队列中移除
                Collection<ClientRequest> requests = unsent.remove(node);
                // 遍历发送请求
                for (ClientRequest request : requests) {
                    // 从发送请求里获取回调方法
                    RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
                    // 构建一个AuthenticationException异常
                    AuthenticationException authenticationException = client.authenticationException(node);
                    // 回调请求回调方法onComplete
                    handler.onComplete(new ClientResponse(request.makeHeader(request.requestBuilder().latestAllowedVersion()),
                            request.callback(), request.destination(), request.createdTimeMs(), now, true,
                            null, authenticationException, null));
                }
            }
        }
    }

    // 处理等待的断开的连接
    private void handlePendingDisconnects() {
        lock.lock();
        try {
            while (true) {
                Node node = pendingDisconnects.poll();
                if (node == null)
                    break;

                failUnsentRequests(node, DisconnectException.INSTANCE);
                // 断开连接
                client.disconnect(node.idString());
            }
        } finally {
            lock.unlock();
        }
    }

    public void disconnectAsync(Node node) {
        pendingDisconnects.offer(node);
        client.wakeup();
    }

    // 处理unsent集合里已经过期的请求
    private void failExpiredRequests(long now) {
        // clear all expired unsent requests and fail their corresponding futures
        // 从unsent队列中移除超时的请求，并返回已过期的请求集合
        Collection<ClientRequest> expiredRequests = unsent.removeExpiredRequests(now);
        // 遍历每个已经过期的请求，回调他们的回调方法
        for (ClientRequest request : expiredRequests) {
            // 获取请求的回调对象
            RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) request.callback();
            // 回调回调方法
            handler.onFailure(new TimeoutException("Failed to send request after " + request.requestTimeoutMs() + " ms."));
        }
    }

    // 将 node 从unsent 集合里移除，然后回调该node对应的请求队列里每个请求的失败回调方法
    private void failUnsentRequests(Node node, RuntimeException e) {
        // clear unsent requests to node and fail their corresponding futures
        lock.lock();
        try {
            Collection<ClientRequest> unsentRequests = unsent.remove(node);
            for (ClientRequest unsentRequest : unsentRequests) {
                RequestFutureCompletionHandler handler = (RequestFutureCompletionHandler) unsentRequest.callback();
                handler.onFailure(e);
            }
        } finally {
            lock.unlock();
        }
    }

    // Visible for testing
    long trySend(long now) {
        // poll操作的延时时间
        long pollDelayMs = maxPollTimeoutMs;

        // send any requests that can be sent now
        // 1、按节点顺序从unsent中取出node
        for (Node node : unsent.nodes()) {
            // 2、获取node的发送队列
            Iterator<ClientRequest> iterator = unsent.requestIterator(node);
            if (iterator.hasNext())
                // 3、poll操作的延迟时间
                pollDelayMs = Math.min(pollDelayMs, client.pollDelayMs(node, now));

            // 4、遍历队列里的每个请求，预发送
            while (iterator.hasNext()) {
                ClientRequest request = iterator.next();
                // 5、调用ready方法确保与目标节点建立了连接
                if (client.ready(node, now)) {
                    // 6、调用send方法，完成请求的预发送（即将请求存入channel里，等待socket可写时写入socket发送缓冲区）
                    client.send(request, now);
                    // 7、从发送队列里删除，避免重复发送
                    iterator.remove();
                } else {
                    // try next node when current node is not ready
                    break;
                }
            }
        }
        return pollDelayMs;
    }

    public void maybeTriggerWakeup() {
        // 通过 wakeupDisabled 检测是否在执行不可中断的方法，通过wakeup检测是否有中断请求
        // 没有执行不可中断的方法 并且 有中断请求，则将wakeup置为false（表示已经响应了该中断）
        if (!wakeupDisabled.get() && wakeup.get()) {
            log.debug("Raising WakeupException in response to user wakeup");
            // 重置中断标记
            wakeup.set(false);
            throw new WakeupException();
        }
    }

    private void maybeThrowInterruptException() {
        if (Thread.interrupted()) {
            throw new InterruptException(new InterruptedException());
        }
    }

    public void disableWakeups() {
        wakeupDisabled.set(true);
    }

    // 关闭网络客户端
    @Override
    public void close() throws IOException {
        lock.lock();
        try {
            client.close();
        } finally {
            lock.unlock();
        }
    }


    /**
     * Check if the code is disconnected and unavailable for immediate reconnection (i.e. if it is in
     * reconnect backoff window following the disconnect).
     */
    public boolean isUnavailable(Node node) {
        lock.lock();
        try {
            return client.connectionFailed(node) && client.connectionDelay(node, time.milliseconds()) > 0;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Check for an authentication error on a given node and raise the exception if there is one.
     */
    public void maybeThrowAuthFailure(Node node) {
        lock.lock();
        try {
            AuthenticationException exception = client.authenticationException(node);
            if (exception != null)
                throw exception;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Initiate a connection if currently possible. This is only really useful for resetting the failed
     * status of a socket. If there is an actual request to send, then {@link #send(Node, AbstractRequest.Builder)}
     * should be used.
     * @param node The node to connect to
     */
    public void tryConnect(Node node) {
        lock.lock();
        try {
            // 检查node是否已经连接，如果已连接则什么也不做，如果未连接则尝试连接
            client.ready(node, time.milliseconds());
        } finally {
            lock.unlock();
        }
    }

    // 请求完成处理器
    private class RequestFutureCompletionHandler implements RequestCompletionHandler {
        // future 对象，当请求完成时，会设置该对象里的值
        private final RequestFuture<ClientResponse> future;
        // 请求响应对象
        private ClientResponse response;
        // 请求异常
        private RuntimeException e;

        private RequestFutureCompletionHandler() {
            this.future = new RequestFuture<>();
        }

        // 任务结束后的回调方法（由谁吊起呢？当然是 ConsumerNetworkClient）
        public void fireCompletion() {
            // 有异常，则调用raise处理异常
            if (e != null) {
                future.raise(e);
            } else if (response.authenticationException() != null) {
                future.raise(response.authenticationException());
            } else if (response.wasDisconnected()) {
                log.debug("Cancelled request with header {} due to node {} being disconnected",
                        response.requestHeader(), response.destination());
                future.raise(DisconnectException.INSTANCE);
            } else if (response.versionMismatch() != null) {
                // 版本不匹配异常
                future.raise(response.versionMismatch());
            } else {
                // 正常的请求响应处理，调用注册在该future上的listeners的onSuccess方法
                future.complete(response);
            }
        }

        // 任务执行失败，则设置异常
        public void onFailure(RuntimeException e) {
            this.e = e;
            // 加入ConsumerNetworkClient的回调队列
            pendingCompletion.add(this);
        }

        @Override
        public void onComplete(ClientResponse response) {
            // 任务执行完成，则设置响应对象
            this.response = response;
            // 加入ConsumerNetworkClient的回调队列
            pendingCompletion.add(this);
        }
    }

    /**
     * When invoking poll from a multi-threaded environment, it is possible that the condition that
     * the caller is awaiting has already been satisfied prior to the invocation of poll. We therefore
     * introduce this interface to push the condition checking as close as possible to the invocation
     * of poll. In particular, the check will be done while holding the lock used to protect concurrent
     * access to {@link org.apache.kafka.clients.NetworkClient}, which means implementations must be
     * very careful about locking order if the callback must acquire additional locks.
     */
    public interface PollCondition {
        /**
         * Return whether the caller is still awaiting an IO event.
         * @return true if so, false otherwise.
         */
        boolean shouldBlock();
    }

    /*
     * A thread-safe helper class to hold requests per node that have not been sent yet
     */
    private final static class UnsentRequests {
        // Key是某个节点，value是该节点的请求队列
        private final ConcurrentMap<Node, ConcurrentLinkedQueue<ClientRequest>> unsent;

        private UnsentRequests() {
            unsent = new ConcurrentHashMap<>();
        }

        public void put(Node node, ClientRequest request) {
            // the lock protects the put from a concurrent removal of the queue for the node
            // 为什么要加锁呢? 因为这里是多个操作
            synchronized (unsent) {
                // 获取该节点的请求队列
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
                // 如果请求队列为空，则创建一个请求队列
                if (requests == null) {
                    requests = new ConcurrentLinkedQueue<>();
                    unsent.put(node, requests);
                }
                // 将请求添加到请求队列
                requests.add(request);
            }
        }

        public int requestCount(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? 0 : requests.size();
        }

        public int requestCount() {
            int total = 0;
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                total += requests.size();
            return total;
        }

        public boolean hasRequests(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests != null && !requests.isEmpty();
        }

        public boolean hasRequests() {
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values())
                if (!requests.isEmpty())
                    return true;
            return false;
        }

        // 移除过期的请求
        private Collection<ClientRequest> removeExpiredRequests(long now) {
            List<ClientRequest> expiredRequests = new ArrayList<>();
            // 遍历每个节点的请求队列，如果请求超时，则将该请求从请求队列中移除，并将该请求添加到过期请求列表中
            for (ConcurrentLinkedQueue<ClientRequest> requests : unsent.values()) {
                Iterator<ClientRequest> requestIterator = requests.iterator();
                // 遍历队列里的每个请求
                while (requestIterator.hasNext()) {
                    ClientRequest request = requestIterator.next();
                    // 获取请求创建时间到当前时间的时间间隔
                    long elapsedMs = Math.max(0, now - request.createdTimeMs());
                    // 如果时间间隔大于了请求指定的超时时间，则认为该请求已经超时，将该请求从请求队列中移除，并添加到过期请求列表中
                    if (elapsedMs > request.requestTimeoutMs()) {
                        expiredRequests.add(request);
                        requestIterator.remove();
                    } else
                        break;
                }
            }
            // 过期的请求集合
            return expiredRequests;
        }

        // 遍历每个节点的请求队列，如果请求队列为空，则从unsent中删除该节点
        public void clean() {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                Iterator<ConcurrentLinkedQueue<ClientRequest>> iterator = unsent.values().iterator();
                while (iterator.hasNext()) {
                    // 获取节点的请求队列
                    ConcurrentLinkedQueue<ClientRequest> requests = iterator.next();
                    // 如果请求队列为空，则从unsent中删除该节点
                    if (requests.isEmpty())
                        iterator.remove();
                }
            }
        }

        public Collection<ClientRequest> remove(Node node) {
            // the lock protects removal from a concurrent put which could otherwise mutate the
            // queue after it has been removed from the map
            synchronized (unsent) {
                // 将该node从unsent中删除，返回删除后的value
                ConcurrentLinkedQueue<ClientRequest> requests = unsent.remove(node);
                // 返回删除后的请求队列
                return requests == null ? Collections.<ClientRequest>emptyList() : requests;
            }
        }

        public Iterator<ClientRequest> requestIterator(Node node) {
            ConcurrentLinkedQueue<ClientRequest> requests = unsent.get(node);
            return requests == null ? Collections.<ClientRequest>emptyIterator() : requests.iterator();
        }

        public Collection<Node> nodes() {
            return unsent.keySet();
        }
    }

}
