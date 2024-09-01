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
package org.apache.kafka.clients;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The set of requests which have been sent or are being sent but haven't yet received a response
 */
final class InFlightRequests {

    // 每个连接中最大执行中的请求数，用于控制对broker的请求负载
    private final int maxInFlightRequestsPerConnection;
    // key：nodeID，value：待发送给该node的请求或等待该node返回结果的请求队列
    private final Map<String, Deque<NetworkClient.InFlightRequest>> requests = new HashMap<>();
    /** Thread safe total number of in flight requests. */
    // 发送中的请求计数器
    private final AtomicInteger inFlightRequestCount = new AtomicInteger(0);

    public InFlightRequests(int maxInFlightRequestsPerConnection) {
        // 设置每个连接最大执行中的请求数
        this.maxInFlightRequestsPerConnection = maxInFlightRequestsPerConnection;
    }

    /**
     * Add the given request to the queue for the connection it was directed to
     * 添加request到待发送或待server端响应的map集合
     */
    public void add(NetworkClient.InFlightRequest request) {
        // 请求要发送到哪个 broker 节点上
        String destination = request.destination;
        // 从 requests 集合中根据nodeID获取节点对应的双端队列
        Deque<NetworkClient.InFlightRequest> reqs = this.requests.get(destination);
        if (reqs == null) {
            reqs = new ArrayDeque<>();
            this.requests.put(destination, reqs);
        }
        // 将请求加入到双端队列的队首
        reqs.addFirst(request);
        // 增加计数
        inFlightRequestCount.incrementAndGet();
    }

    /**
     * Get the request queue for the given node
     */
    private Deque<NetworkClient.InFlightRequest> requestQueue(String node) {
        // 获取
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null || reqs.isEmpty())
            throw new IllegalStateException("There are no in-flight requests for node " + node);
        return reqs;
    }

    /**
     * Get the oldest request (the one that will be completed next) for the given node
     * 取出该连接对应的队列中最老的请求
     */
    public NetworkClient.InFlightRequest completeNext(String node) {
        // 获取该连接的双端队列的最后一个元素
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollLast();
        // 计数器-1
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * Get the last request we sent to the given node (but don't remove it from the queue)
     * @param node The node id
     *             获取某个连接最新的请求
     */
    public NetworkClient.InFlightRequest lastSent(String node) {
        return requestQueue(node).peekFirst();
    }

    /**
     * Complete the last request that was sent to a particular node.
     * 出队某个连接最新的请求
     * @param node The node the request was sent to
     * @return The request
     */
    public NetworkClient.InFlightRequest completeLastSent(String node) {
        // 从发送中的队列里取头节点
        NetworkClient.InFlightRequest inFlightRequest = requestQueue(node).pollFirst();
        // 发送中的请求数量-1
        inFlightRequestCount.decrementAndGet();
        return inFlightRequest;
    }

    /**
     * Can we send more requests to this node?
     * 判断该连接是否还能发送请求，用于控制节点负载
     * @param node Node in question
     * @return true iff we have no requests still being sent to the given node
     */
    public boolean canSendMore(String node) {
        // 获取节点对应的双端队列
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        // 队列为空 || （队首已经发送完成&& 队列中没有堆积更多的请求）
        // 队首的请求和kafkaChannel.send 字段指向的是同一个请求
        return queue == null || queue.isEmpty() ||
               (queue.peekFirst().send.completed() && queue.size() < this.maxInFlightRequestsPerConnection);
    }

    /**
     * Return the number of in-flight requests directed at the given node
     * @param node The node
     * @return The request count.
     */
    public int count(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null ? 0 : queue.size();
    }

    /**
     * Return true if there is no in-flight request directed at the given node and false otherwise
     */
    public boolean isEmpty(String node) {
        Deque<NetworkClient.InFlightRequest> queue = requests.get(node);
        return queue == null || queue.isEmpty();
    }

    /**
     * Count all in-flight requests for all nodes. This method is thread safe, but may lag the actual count.
     */
    public int count() {
        return inFlightRequestCount.get();
    }

    /**
     * Return true if there is no in-flight request and false otherwise
     */
    public boolean isEmpty() {
        for (Deque<NetworkClient.InFlightRequest> deque : this.requests.values()) {
            if (!deque.isEmpty())
                return false;
        }
        return true;
    }

    /**
     * Clear out all the in-flight requests for the given node and return them
     *
     * @param node The node
     * @return All the in-flight requests for that node that have been removed
     */
    public Iterable<NetworkClient.InFlightRequest> clearAll(String node) {
        Deque<NetworkClient.InFlightRequest> reqs = requests.get(node);
        if (reqs == null) {
            return Collections.emptyList();
        } else {
            final Deque<NetworkClient.InFlightRequest> clearedRequests = requests.remove(node);
            inFlightRequestCount.getAndAdd(-clearedRequests.size());
            return () -> clearedRequests.descendingIterator();
        }
    }

    // 队列里是否有超时的节点
    private Boolean hasExpiredRequest(long now, Deque<NetworkClient.InFlightRequest> deque) {
        for (NetworkClient.InFlightRequest request : deque) {
            long timeSinceSend = Math.max(0, now - request.sendTimeMs);
            // 节点队列请求是否超时，如果有一个请求超时，则认为这个broker超时了，默认30s
            if (timeSinceSend > request.requestTimeoutMs)
                return true;
        }
        return false;
    }

    /**
     * Returns a list of nodes with pending in-flight request, that need to be timed out
     * 收集超时请求的节点集合
     * @param now current time in milliseconds
     * @return list of nodes
     */
    public List<String> nodesWithTimedOutRequests(long now) {
        List<String> nodeIds = new ArrayList<>();
        // 遍历节点队列
        for (Map.Entry<String, Deque<NetworkClient.InFlightRequest>> requestEntry : requests.entrySet()) {
            // 获取节点ID
            String nodeId = requestEntry.getKey();
            // 获取队列数据
            Deque<NetworkClient.InFlightRequest> deque = requestEntry.getValue();
            // 判断是否超时
            if (hasExpiredRequest(now, deque))
                // 超时后将节点加入集合
                nodeIds.add(nodeId);
        }
        return nodeIds;
    }

}
