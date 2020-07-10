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
package com.alibaba.nacos.naming.consistency.ephemeral;

import com.alibaba.nacos.naming.consistency.ConsistencyService;

/**
 * A type of consistency for ephemeral data.
 * <p>
 * This kind of consistency is not required to store data on disk or database, because the
 * ephemeral data always keeps a session with server and as long as the session still lives
 * the ephemeral data won't be lost.
 * <p>
 * What is required is that writing should always be successful even if network partition
 * happens. And when the network recovers, data of each partition is merged into one set,
 * so the cluster resumes to a consistent status.
 * <p>
 * 临时数据的一致性类型
 * <p>
 * 该一致性类型不要求把数据保存在磁盘或者数据库中，因为临时数据总是与服务实例保持一个会话，
 * 只要会话活着数据就不会丢失。
 * <p>
 * 唯一的要求就是，写操作应该总是能够成功，即使发生网络分区。当网络恢复后，每个分区的数据被合并，从而达成最终一致性。
 * 简单讲，就是针对临时数据要确保 AP。
 * @author nkorange
 * @since 1.0.0
 */
public interface EphemeralConsistencyService extends ConsistencyService {
}
