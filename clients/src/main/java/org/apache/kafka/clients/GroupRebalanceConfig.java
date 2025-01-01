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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.requests.JoinGroupRequest;

import java.util.Locale;
import java.util.Optional;

/**
 * Class to extract group rebalance related configs.
 */
public class GroupRebalanceConfig {

    public enum ProtocolType {
        CONSUMER,
        CONNECT;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public final int sessionTimeoutMs;
    // 重平衡超时时间（max.poll.interval.ms配置，默认5分钟），如果消费者两次poll请求时间间隔超过了该值，则认为消费能力不足
    // 将此消费者commit标记为失败，并将此消费者从消费者组中移除，并触发rebalance，将该消费者负责的分区分配给其他消费者
    public final int rebalanceTimeoutMs;
    // 心跳间隔时间
    public final int heartbeatIntervalMs;
    // 消费者组ID
    public final String groupId;
    // 消费者实例ID（由group.instance.id配置），默认为空字符串，如果设置了，则消费者被认为是静态成员，会分配较大的session超时时间，
    // 避免因成员临时不可用（比如重启）而触发rebalance，如果不设置，则消费者被认为是动态成员
    public final Optional<String> groupInstanceId;
    public final long retryBackoffMs;
    public final boolean leaveGroupOnClose;

    public GroupRebalanceConfig(AbstractConfig config, ProtocolType protocolType) {
        this.sessionTimeoutMs = config.getInt(CommonClientConfigs.SESSION_TIMEOUT_MS_CONFIG);

        // Consumer and Connect use different config names for defining rebalance timeout
        if (protocolType == ProtocolType.CONSUMER) {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.MAX_POLL_INTERVAL_MS_CONFIG);
        } else {
            this.rebalanceTimeoutMs = config.getInt(CommonClientConfigs.REBALANCE_TIMEOUT_MS_CONFIG);
        }

        this.heartbeatIntervalMs = config.getInt(CommonClientConfigs.HEARTBEAT_INTERVAL_MS_CONFIG);
        this.groupId = config.getString(CommonClientConfigs.GROUP_ID_CONFIG);

        // Static membership is only introduced in consumer API.
        if (protocolType == ProtocolType.CONSUMER) {
            String groupInstanceId = config.getString(CommonClientConfigs.GROUP_INSTANCE_ID_CONFIG);
            if (groupInstanceId != null) {
                JoinGroupRequest.validateGroupInstanceId(groupInstanceId);
                this.groupInstanceId = Optional.of(groupInstanceId);
            } else {
                this.groupInstanceId = Optional.empty();
            }
        } else {
            this.groupInstanceId = Optional.empty();
        }

        this.retryBackoffMs = config.getLong(CommonClientConfigs.RETRY_BACKOFF_MS_CONFIG);

        // Internal leave group config is only defined in Consumer.
        if (protocolType == ProtocolType.CONSUMER) {
            this.leaveGroupOnClose = config.getBoolean("internal.leave.group.on.close");
        } else {
            this.leaveGroupOnClose = true;
        }
    }

    // For testing purpose.
    public GroupRebalanceConfig(final int sessionTimeoutMs,
                                final int rebalanceTimeoutMs,
                                final int heartbeatIntervalMs,
                                String groupId,
                                Optional<String> groupInstanceId,
                                long retryBackoffMs,
                                boolean leaveGroupOnClose) {
        this.sessionTimeoutMs = sessionTimeoutMs;
        this.rebalanceTimeoutMs = rebalanceTimeoutMs;
        this.heartbeatIntervalMs = heartbeatIntervalMs;
        this.groupId = groupId;
        this.groupInstanceId = groupInstanceId;
        this.retryBackoffMs = retryBackoffMs;
        this.leaveGroupOnClose = leaveGroupOnClose;
    }
}
