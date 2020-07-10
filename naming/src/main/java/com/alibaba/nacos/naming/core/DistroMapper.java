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
package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

/**
 * DistroMapper 负责监听服务对应的健康实例列表变更
 * 
 * @author nkorange
 */
@Component("distroMapper")
public class DistroMapper implements ServerChangeListener {

  /**
   * healthyList 保存当前最新的可达实例列表，列表每个元素是实例的 key（即 IP:Port）
   */
  private List<String> healthyList = new ArrayList<>();

  public List<String> getHealthyList() {
    return healthyList;
  }

  @Autowired
  private SwitchDomain switchDomain;

  @Autowired
  private ServerListManager serverListManager;

  /**
   * init server list
   */
  @PostConstruct
  public void init() {
    serverListManager.listen(this);
  }

  public boolean responsible(Cluster cluster, Instance instance) {
    return switchDomain.isHealthCheckEnabled(cluster.getServiceName()) && !cluster.getHealthCheckTask().isCancelled()
        && responsible(cluster.getServiceName()) && cluster.contains(instance);
  }

  /**
   * 确认当前 nacos 实例是否负责管理名为 serviceName 的服务
   * 
   * @param serviceName
   * @return
   */
  public boolean responsible(String serviceName) {
    if (!switchDomain.isDistroEnabled() || SystemUtils.STANDALONE_MODE) {
      return true;
    }

    if (CollectionUtils.isEmpty(healthyList)) {
      // means distro config is not ready yet
      return false;
    }

    // 取出当前 nacos 实例在健康列表中的索引值
    int index = healthyList.indexOf(NetUtils.localServer());
    int lastIndex = healthyList.lastIndexOf(NetUtils.localServer());
    if (lastIndex < 0 || index < 0) {
      return true;
    }

    int target = distroHash(serviceName) % healthyList.size();
    return target >= index && target <= lastIndex;
  }

  /**
   * 确认 serviceName 对应的服务由哪个 nacos 实例管理，返回它的 ip:port
   * 
   * @param serviceName
   * @return
   */
  public String mapSrv(String serviceName) {
    if (CollectionUtils.isEmpty(healthyList) || !switchDomain.isDistroEnabled()) {
      return NetUtils.localServer();
    }

    try {
      return healthyList.get(distroHash(serviceName) % healthyList.size());
    } catch (Exception e) {
      Loggers.SRV_LOG.warn("distro mapper failed, return localhost: " + NetUtils.localServer(), e);

      return NetUtils.localServer();
    }
  }

  /**
   * 根据服务名称生成哈希值
   * 
   * @param serviceName
   * @return
   */
  public int distroHash(String serviceName) {
    return Math.abs(serviceName.hashCode() % Integer.MAX_VALUE);
  }

  @Override
  public void onChangeServerList(List<Server> latestMembers) {

  }

  /**
   * onChangeHealthyServerList 负责使用最新的可达实例列表替换本地保存的旧的实例列表。
   */
  @Override
  public void onChangeHealthyServerList(List<Server> latestReachableMembers) {

    // 用新的集群可达成员列表替换本地可达列表
    List<String> newHealthyList = new ArrayList<>();
    for (Server server : latestReachableMembers) {
      newHealthyList.add(server.getKey());
    }
    healthyList = newHealthyList;
  }
}
