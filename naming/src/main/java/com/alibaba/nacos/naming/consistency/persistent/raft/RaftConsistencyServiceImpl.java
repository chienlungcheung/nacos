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

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.persistent.PersistentConsistencyService;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.pojo.Record;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Use simplified Raft protocol to maintain the consistency status of Nacos
 * cluster.
 *
 * RaftConsistencyServiceImpl 是一个 raft 实现，提供了若干读写数据方法，并使用简化的 raft
 * 算法确保数据的一致性（具体干活的是 RaftCore 类型成员）。
 * 
 * @author nkorange
 * @since 1.0.0
 */
@Service
public class RaftConsistencyServiceImpl implements PersistentConsistencyService {

  @Autowired
  private RaftCore raftCore;

  @Autowired
  private RaftPeerSet peers;

  @Autowired
  private SwitchDomain switchDomain;

  /**
   * 处理客户端的写请求
   */
  @Override
  public void put(String key, Record value) throws NacosException {
    try {
      raftCore.signalPublish(key, value);
    } catch (Exception e) {
      Loggers.RAFT.error("Raft put failed.", e);
      throw new NacosException(NacosException.SERVER_ERROR, "Raft put failed, key:" + key + ", value:" + value, e);
    }
  }

  /**
   * 响应客户端的删除请求
   */
  @Override
  public void remove(String key) throws NacosException {
    try {
      // 如果接收到 remove 请求的节点非 leader，则将相关数据从本地节点删除然后立即返回
      if (KeyBuilder.matchInstanceListKey(key) && !raftCore.isLeader()) {
        Datum datum = new Datum();
        datum.key = key;
        // 将数据从本地节点删除
        raftCore.onDelete(datum.key, peers.getLeader());
        // 将该数据对应的集群监听器列表也删除
        raftCore.unlistenAll(key);
        return;
      }
      // 如果接收到 remove 请求的是 leader，则需要将该删除操作广播到集群
      raftCore.signalDelete(key);
      // 将该数据对应的集群监听器列表也删除
      raftCore.unlistenAll(key);
    } catch (Exception e) {
      Loggers.RAFT.error("Raft remove failed.", e);
      throw new NacosException(NacosException.SERVER_ERROR, "Raft remove failed, key:" + key, e);
    }
  }

  @Override
  public Datum get(String key) throws NacosException {
    return raftCore.getDatum(key);
  }

  @Override
  public void listen(String key, RecordListener listener) throws NacosException {
    raftCore.listen(key, listener);
  }

  @Override
  public void unlisten(String key, RecordListener listener) throws NacosException {
    raftCore.unlisten(key, listener);
  }

  @Override
  public boolean isAvailable() {
    return raftCore.isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
  }

  /**
   * 将来自 source(必须是 leader, 只有 leader 发布数据以达成一致性)的数据写入本地存储.
   * 
   * @param datum
   * @param source
   * @throws NacosException
   */
  public void onPut(Datum datum, RaftPeer source) throws NacosException {
    try {
      raftCore.onPublish(datum, source);
    } catch (Exception e) {
      Loggers.RAFT.error("Raft onPut failed.", e);
      throw new NacosException(NacosException.SERVER_ERROR, "Raft onPut failed, datum:" + datum + ", source: " + source,
          e);
    }
  }

  public void onRemove(Datum datum, RaftPeer source) throws NacosException {
    try {
      raftCore.onDelete(datum.key, source);
    } catch (Exception e) {
      Loggers.RAFT.error("Raft onRemove failed.", e);
      throw new NacosException(NacosException.SERVER_ERROR,
          "Raft onRemove failed, datum:" + datum + ", source: " + source, e);
    }
  }
}
