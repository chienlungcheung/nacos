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
package com.alibaba.nacos.naming.cluster;

/**
 * Server running mode
 * <p>
 * 服务器的运行模式
 * <p>
 * We use CAP theory to set the server mode, users can select their preferred mode in running time.
 * <p>
 * 使用 CAP 理论来设置服务器模式，用户可以在运行时根据需要选择。
 * <p>
 * CP mode provides strong consistency, data persistence but network partition tolerance.
 * <p>
 * CP 模式提供了强一致性：网络分区发生后，可以保障数据一致性，这意味着期间将会拒绝服务。
 * <p>
 * AP mode provides eventual consistency and network partition tolerance but data persistence.
 * <p>
 * AP 模式提供了最终一致性：网络分区发生后，仍然可以对外提供服务，这意味着期间数据不一致（但网络恢复后将会进行数据同步达成最终一致性）
 * <p>
 * Mixed mode provides CP for some data and AP for some other data.
 * <p> 
 * 混合模式，针对某些数据提供 CP 模式，针对某些数据提供 AP 模式。
 * <p>
 * Service level information and cluster level information are always operated via CP protocol, so
 * in AP mode they cannot be edited.
 * <p>
 * 服务级别的信息和集群级别的信息总是运行在 CP 模式，所以如果使用 AP 模式，它们将拒绝写操作以确保各个分区数据一致性。
 *
 * @author nkorange
 * @since 1.0.0
 */
public enum ServerMode {
    /**
     * AP mode
     */
    AP,
    /**
     * CP mode
     */
    CP,
    /**
     * Mixed mode
     */
    MIXED
}
