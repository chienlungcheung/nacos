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
package com.alibaba.nacos.naming.cluster.servers;

import java.util.List;

/**
 * Nacos cluster member change event listener
 * <p>
 * ServerChangeListener 会被用于 Nacos 本身集群的成员管理（下线或新增）以及服务发现（更新 nacos 服务对应的最新的可达实例列表）。
 *
 * @author nkorange
 * @since 1.0.0
 */
public interface ServerChangeListener {

    /**
     * If member list changed, this method is invoked.
     *
     * 如果集群成员列表变化，该方法会被调用。
     * <p>
     * 目前仅被 nacos 本身使用（见 RaftPeerSet）。
     *
     * @param servers servers after change
     */
    void onChangeServerList(List<Server> servers);

    /**
     * If reachable member list changed, this method is invoked.
     *
     * 如果集群可达成员列表有变化，该方法会被调用。
     * <p>
     * 目前仅被用于服务发现（见 DistroMapper）。
     *
     * @param healthyServer reachable servers after change
     */
    void onChangeHealthyServerList(List<Server> healthyServer);
}
