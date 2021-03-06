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
package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.boot.SpringContext;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.healthcheck.events.InstanceHeartbeatTimeoutEvent;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.push.PushService;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;

import java.net.HttpURLConnection;
import java.util.List;

/**
 * Check and update statues of ephemeral instances, remove them if they have
 * been expired.
 * <p>
 * ClientBeatCheckTask 负责根据心跳是否超时（根据其上次心跳时间确认其是否很长时间未发送过心跳给当前 nacos 实例了）
 * 更新某个服务的全部实例的状态，如果有心跳超时的实例，则将其移除（在一致性算法实现进行维护，从而广播给其它 nacos 实例）。
 * <p>
 * 每个服务都有一个专门的 ClientBeatCheckTask 实例负责，服务初始化时候启动周期性任务运行之。
 * 
 * @author nkorange
 */
public class ClientBeatCheckTask implements Runnable {

  /**
   * service 为被监测的服务
   */
  private Service service;

  public ClientBeatCheckTask(Service service) {
    this.service = service;
  }

  @JSONField(serialize = false)
  public PushService getPushService() {
    return SpringContext.getAppContext().getBean(PushService.class);
  }

  @JSONField(serialize = false)
  public DistroMapper getDistroMapper() {
    return SpringContext.getAppContext().getBean(DistroMapper.class);
  }

  public GlobalConfig getGlobalConfig() {
    return SpringContext.getAppContext().getBean(GlobalConfig.class);
  }

  public String taskKey() {
    return service.getName();
  }

  /**
   * 检查每个实例，查看是否存在长时间未发送心跳到当前 nacos 节点的实例。
   * <p>
   * 如果存在则将其置为不健康，同时根据配置确定是否将其从集群中删除。
   */
  @Override
  public void run() {
    try {
      // 不是当前 nacos 实例负责的服务跳过不管
      if (!getDistroMapper().responsible(service.getName())) {
        return;
      }

      // 获取该服务的全部实例（注意，服务实例都是 ephemeral 的）
      List<Instance> instances = service.allIPs(true);

      // 挨个检查每个实例，根据其上次心跳时间确认其是否很长时间未发送过心跳给当前 nacos 实例了。
      // 如果超时则将其标记为不健康。同时推送一个服务变更事件。
      // first set health status of instances:
      for (Instance instance : instances) {
        if (System.currentTimeMillis() - instance.getLastBeat() > instance.getInstanceHeartBeatTimeOut()) {
          // 如果实例被标记过无需做健康检查则跳过后面逻辑
          if (!instance.isMarked()) {
            // 如果以前是健康状态则将其标记为不健康
            if (instance.isHealthy()) {
              instance.setHealthy(false);
              Loggers.EVT_LOG.info(
                  "{POS} {IP-DISABLED} valid: {}:{}@{}@{}, region: {}, msg: client timeout after {}, last beat: {}",
                  instance.getIp(), instance.getPort(), instance.getClusterName(), service.getName(),
                  UtilsAndCommons.LOCALHOST_SITE, instance.getInstanceHeartBeatTimeOut(), instance.getLastBeat());
              // TODO 通知机制，是否通过网络？
              getPushService().serviceChanged(service);
              // TODO 通知机制，是否通过网络
              // 将实例心跳超时事件发布出去
              SpringContext.getAppContext().publishEvent(new InstanceHeartbeatTimeoutEvent(this, instance));
            }
          }
        }
      }

      // 检查全局配置，确认是否要终止掉不健康的实例
      if (!getGlobalConfig().isExpireInstance()) {
        return;
      }

      // 移除不健康实例
      // then remove obsolete instances:
      for (Instance instance : instances) {

        if (instance.isMarked()) {
          continue;
        }

        if (System.currentTimeMillis() - instance.getLastBeat() > instance.getIpDeleteTimeout()) {
          // delete instance
          Loggers.SRV_LOG.info("[AUTO-DELETE-IP] service: {}, ip: {}", service.getName(), JSON.toJSONString(instance));
          deleteIP(instance);
        }
      }

    } catch (Exception e) {
      Loggers.SRV_LOG.warn("Exception while processing client beat time out.", e);
    }

  }

  /**
   * 给当前实例自己的 instance 接口（具体见 InstanceController.deregister 方法）发送一个异步删除请求，
   * 从对应服务的对应集群中删除参数指定的实例。
   * 
   * @param instance
   */
  private void deleteIP(Instance instance) {

    try {
      NamingProxy.Request request = NamingProxy.Request.newRequest();
      request.appendParam("ip", instance.getIp()).appendParam("port", String.valueOf(instance.getPort()))
          .appendParam("ephemeral", "true").appendParam("clusterName", instance.getClusterName())
          .appendParam("serviceName", service.getName()).appendParam("namespaceId", service.getNamespaceId());

      String url = "http://127.0.0.1:" + RunningConfig.getServerPort() + RunningConfig.getContextPath()
          + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance?" + request.toUrl();

      // delete instance asynchronously:
      HttpClient.asyncHttpDelete(url, null, null, new AsyncCompletionHandler() {
        @Override
        public Object onCompleted(Response response) throws Exception {
          if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
            Loggers.SRV_LOG.error("[IP-DEAD] failed to delete ip automatically, ip: {}, caused {}, resp code: {}",
                instance.toJSON(), response.getResponseBody(), response.getStatusCode());
          }
          return null;
        }
      });

    } catch (Exception e) {
      Loggers.SRV_LOG.error("[IP-DEAD] failed to delete ip automatically, ip: {}, error: {}", instance.toJSON(), e);
    }
  }
}
