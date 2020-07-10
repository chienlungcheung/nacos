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
package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Data replicator
 * <p>
 * DataSyncer 负责周期性地将当前实例负责的服务数据校验和同步给 nacos 集群其它全部可达的服务实例，实现 AP 的最终一致性；
 * 同时它还提供了一个 submit 接口供外部有同步需求的类调用。
 * 
 * @author nkorange
 * @since 1.0.0
 */
@Component
@DependsOn("serverListManager")
public class DataSyncer {

  @Autowired
  private DataStore dataStore;

  @Autowired
  private GlobalConfig partitionConfig;

  @Autowired
  private Serializer serializer;

  @Autowired
  private DistroMapper distroMapper;

  /**
   * 负责提供 nacos 集群全部可达实例，发送数据时进行使用。
   */
  @Autowired
  private ServerListManager serverListManager;

  private Map<String, String> taskMap = new ConcurrentHashMap<>();

  @PostConstruct
  public void init() {
    startTimedSync();
  }

  /**
   * 该方法供有同步需求的外部类调用，负责提交一个 one-shot（但是同步失败会自动重试）数据同步任务到全局调度器。
   * 
   * @param task
   * @param delay
   */
  public void submit(SyncTask task, long delay) {

    // If it's a new task:
    if (task.getRetryCount() == 0) {
      Iterator<String> iterator = task.getKeys().iterator();
      while (iterator.hasNext()) {
        String key = iterator.next();
        // 如果数据 key 正在同步给 server，则不再重复同步
        if (StringUtils.isNotBlank(taskMap.putIfAbsent(buildKey(key, task.getTargetServer()), key))) {
          // associated key already exist:
          if (Loggers.DISTRO.isDebugEnabled()) {
            Loggers.DISTRO.debug("sync already in process, key: {}", key);
          }
          iterator.remove();
        }
      }
    }

    if (task.getKeys().isEmpty()) {
      // all keys are removed:
      return;
    }

    GlobalExecutor.submitDataSync(new Runnable() {
      @Override
      public void run() {

        try {
          // 如果从当前实例到其它 nacos 服务实例均不可达，则不做同步操作了
          if (getServers() == null || getServers().isEmpty()) {
            Loggers.SRV_LOG.warn("try to sync data but server list is empty.");
            return;
          }

          List<String> keys = task.getKeys();

          if (Loggers.DISTRO.isDebugEnabled()) {
            Loggers.DISTRO.debug("sync keys: {}", keys);
          }

          // 获取待同步的数据
          Map<String, Datum> datumMap = dataStore.batchGet(keys);

          // 待同步数据为空，则全部键均无效，从同步任务删除，终止本次同步。
          if (datumMap == null || datumMap.isEmpty()) {
            // clear all flags of this task:
            for (String key : task.getKeys()) {
              taskMap.remove(buildKey(key, task.getTargetServer()));
            }
            return;
          }

          // 将待同步数据序列化
          byte[] data = serializer.serialize(datumMap);

          long timestamp = System.currentTimeMillis();
          boolean success = NamingProxy.syncData(data, task.getTargetServer());
          // 如果同步失败，则重新同步
          if (!success) {
            SyncTask syncTask = new SyncTask();
            syncTask.setKeys(task.getKeys());
            // 重试次数加一
            syncTask.setRetryCount(task.getRetryCount() + 1);
            // 记录上次同步时间
            syncTask.setLastExecuteTime(timestamp);
            syncTask.setTargetServer(task.getTargetServer());
            retrySync(syncTask);
          } else {
            // 如果同步成功，则将其对应的 keys 从暂存区删除避免重复同步
            // clear all flags of this task:
            for (String key : task.getKeys()) {
              taskMap.remove(buildKey(key, task.getTargetServer()));
            }
          }

        } catch (Exception e) {
          Loggers.DISTRO.error("sync data failed.", e);
        }
      }
    }, delay);
  }

  /**
   * 重新提交同步任务
   * 
   * @param syncTask
   */
  public void retrySync(SyncTask syncTask) {

    Server server = new Server();
    server.setIp(syncTask.getTargetServer().split(":")[0]);
    server.setServePort(Integer.parseInt(syncTask.getTargetServer().split(":")[1]));
    // 如果目标实例不可达了，则不重试了
    if (!getServers().contains(server)) {
      // if server is no longer in healthy server list, ignore this task:
      return;
    }

    // TODO may choose other retry policy.
    submit(syncTask, partitionConfig.getSyncRetryDelay());
  }

  /**
   * 提交一个定时同步任务 TimedSync 到全局调度器，该任务周期性被执行。
   */
  public void startTimedSync() {
    GlobalExecutor.schedulePartitionDataTimedSync(new TimedSync());
  }

  /**
   * 该任务被周期性调度，负责将本实例负责的全部服务信息的校验和同步给从当前实例可达的 nacos 集群其它全部服务实例。
   */
  public class TimedSync implements Runnable {

    @Override
    public void run() {

      try {

        if (Loggers.DISTRO.isDebugEnabled()) {
          Loggers.DISTRO.debug("server list is: {}", getServers());
        }

        // send local timestamps to other servers:
        Map<String, String> keyChecksums = new HashMap<>(64);
        // 获取本地存储的数据的校验和
        for (String key : dataStore.keys()) {
          if (!distroMapper.responsible(KeyBuilder.getServiceName(key))) {
            continue;
          }

          keyChecksums.put(key, dataStore.get(key).value.getChecksum());
        }

        if (keyChecksums.isEmpty()) {
          return;
        }

        if (Loggers.DISTRO.isDebugEnabled()) {
          Loggers.DISTRO.debug("sync checksums: {}", keyChecksums);
        }

        /**
         * 将本地存储的每个数据项的校验和发给其它可达的 nacos 服务实例
         */
        for (Server member : getServers()) {
          // 自己有，不用发给自己
          if (NetUtils.localServer().equals(member.getKey())) {
            continue;
          }
          NamingProxy.syncCheckSums(keyChecksums, member.getKey());
        }
      } catch (Exception e) {
        Loggers.DISTRO.error("timed sync task failed.", e);
      }
    }
  }

  /**
   * 返回从当前实例可达的 nacos 其它服务实例列表
   * 
   * @return
   */
  public List<Server> getServers() {
    return serverListManager.getHealthyServers();
  }

  public String buildKey(String key, String targetServer) {
    return key + UtilsAndCommons.CACHE_KEY_SPLITER + targetServer;
  }
}
