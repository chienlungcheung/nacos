/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.nacos.naming.misc.GlobalExecutor;
import org.apache.commons.lang3.RandomUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * RaftPeer 是对 raft 算法描述的集群节点的抽象和实现。
 * <p>
 * 状态包括：
 * <li> 需要持久化的状态 currentTerm、votedFor、log[]，
 * <li> 易变的状态 commitIndex、lastApplied、nextIndex[]、matchIndex[]，其中后两者专属于 leader 节点。
 *  
 * @author nacos
 */
public class RaftPeer {

    /**
     * 该值可以作为节点的 ID 使用，在集群中唯一标识一个 raft 节点
     */
    public String ip;

    /**
     * votedFor: candidateId that received vote in current term (or null if none)
     */
    public String voteFor;

    /**
     * currentTerm: latest term server has seen (initialized to 0 on first boot, increases monotonically)
     */
    public AtomicLong term = new AtomicLong(0L);

    /**
     * 选举超时
     */
    public volatile long leaderDueMs = RandomUtils.nextLong(0, GlobalExecutor.LEADER_TIMEOUT_MS);

    /**
     * 心跳超时
     */
    public volatile long heartbeatDueMs = RandomUtils.nextLong(0, GlobalExecutor.HEARTBEAT_INTERVAL_MS);

    /**
     * 节点状态，初始为 FOLLOWER
     */
    public State state = State.FOLLOWER;

    /**
     * 重置选举超时（见 raft 协议 election-timeout），大小为 [15s, 20s)
     */
    public void resetLeaderDue() {
        leaderDueMs = GlobalExecutor.LEADER_TIMEOUT_MS + RandomUtils.nextLong(0, GlobalExecutor.RANDOM_MS);
    }

    /**
     * 重置心跳超时，大小为 5s
     */
    public void resetHeartbeatDue() {
        heartbeatDueMs = GlobalExecutor.HEARTBEAT_INTERVAL_MS;
    }

    /**
     * 每个节点三个可能的状态
     */
    public enum State {
        /**
         * Leader of the cluster, only one leader stands in a cluster
         */
        LEADER,
        /**
         * Follower of the cluster, report to and copy from leader
         */
        FOLLOWER,
        /**
         * Candidate leader to be elected
         */
        CANDIDATE
    }

    @Override
    public int hashCode() {
        return Objects.hash(ip);
    }

    /**
     * 两个节点的 IP 相同就视为同一个节点
     *
     * @param obj
     * @return
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (!(obj instanceof RaftPeer)) {
            return false;
        }

        RaftPeer other = (RaftPeer) obj;

        return StringUtils.equals(ip, other.ip);
    }
}
