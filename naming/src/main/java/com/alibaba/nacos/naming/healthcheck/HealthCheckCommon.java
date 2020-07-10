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
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.push.PushService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.HttpURLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Health check public methods
 *
 * @author nkorange
 * @since 1.0.0
 */
@Component
public class HealthCheckCommon {

  @Autowired
  private DistroMapper distroMapper;

  @Autowired
  private SwitchDomain switchDomain;

  @Autowired
  private ServerListManager serverListManager;

  @Autowired
  private PushService pushService;

  private static LinkedBlockingDeque<HealthCheckResult> healthCheckResults = new LinkedBlockingDeque<>(1024 * 128);

  private static ScheduledExecutorService executorService = Executors
      .newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = new Thread(r);
          thread.setDaemon(true);
          thread.setName("com.taobao.health-check.notifier");
          return thread;
        }
      });

  public void init() {
    // 启动一个周期性任务，周期性的将当前 nacos 实例针对客户端（服务发现中维护的实例）的健康检查结果同步给
    // 其它可达 nacos 节点的 api/healthCheckResult 接口。
    executorService.schedule(new Runnable() {
      @Override
      public void run() {
        // 一次将本地存储的健康检查结果全部发送走，然后清空本地存储
        List list = Arrays.asList(healthCheckResults.toArray());
        healthCheckResults.clear();

        List<Server> sameSiteServers = serverListManager.getServers();

        if (sameSiteServers == null || sameSiteServers.size() <= 0) {
          return;
        }

        for (Server server : sameSiteServers) {
          if (server.getKey().equals(NetUtils.localServer())) {
            continue;
          }
          Map<String, String> params = new HashMap<>(10);
          params.put("result", JSON.toJSONString(list));
          if (Loggers.SRV_LOG.isDebugEnabled()) {
            Loggers.SRV_LOG.debug("[HEALTH-SYNC] server: {}, healthCheckResults: {}", server, JSON.toJSONString(list));
          }

          HttpClient.HttpResult httpResult = HttpClient.httpPost("http://" + server.getKey()
              + RunningConfig.getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/api/healthCheckResult", null,
              params);

          if (httpResult.code != HttpURLConnection.HTTP_OK) {
            Loggers.EVT_LOG.warn("[HEALTH-CHECK-SYNC] failed to send result to {}, result: {}", server,
                JSON.toJSONString(list));
          }

        }

      }
    }, 500, TimeUnit.MILLISECONDS);
  }

  /**
   * 重新计算各个 round-trip
   * 
   * @param checkRT 最近一次健康检查的 rount-trip 耗时
   * @param task 最近一次健康检查任务
   * @param params 与 round-trip 相关的参数，如计算指数移动平均用的系数
   */
  public void reEvaluateCheckRT(long checkRT, HealthCheckTask task, SwitchDomain.HealthParams params) {
    task.setCheckRTLast(checkRT);

    if (checkRT > task.getCheckRTWorst()) {
      task.setCheckRTWorst(checkRT);
    }

    if (checkRT < task.getCheckRTBest()) {
      task.setCheckRTBest(checkRT);
    }

    // 指数移动平均
    // https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average
    // 各数值的加权影响力随时间而指数式递减，越近期的数据加权影响力越重，但较旧的数据也给予一定的加权值。
    checkRT = (long) ((params.getFactor() * task.getCheckRTNormalized()) + (1 - params.getFactor()) * checkRT);

    if (checkRT > params.getMax()) {
      checkRT = params.getMax();
    }

    if (checkRT < params.getMin()) {
      checkRT = params.getMin();
    }

    task.setCheckRTNormalized(checkRT);
  }

  /**
   * 处理好的健康检查结果。将之前不可用状态置为健康，并广播一个服务变更事件出去，同时保留健康检查结果。
   * 
   * @param ip
   * @param task
   * @param msg
   */
  public void checkOK(Instance ip, HealthCheckTask task, String msg) {
    // 参数 task 唯一作用在这就是确定是哪个集群的健康检查
    Cluster cluster = task.getCluster();

    try {
      // 如果之前被检查实例被判为不可用
      if (!ip.isHealthy() || !ip.isMockValid()) {
        // 只有好的健康检查结果达到阈值才被判为健康（防止一时回光返照）
        if (ip.getOKCount().incrementAndGet() >= switchDomain.getCheckTimes()) {
          // 如果当前 nacos 节点负责该 cluster
          if (distroMapper.responsible(cluster, ip)) {
            // 将实例设置为健康
            ip.setHealthy(true);
            ip.setMockValid(true);

            Service service = cluster.getService();
            service.setLastModifiedMillis(System.currentTimeMillis());
            // 之前该节点不可用，目前恢复了，服务可用实例变多了，这个变化要广播一个事件出去
            pushService.serviceChanged(service);
            // 保存健康检查结果
            addResult(new HealthCheckResult(service.getName(), ip));

            Loggers.EVT_LOG.info("serviceName: {} {POS} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: {}",
                cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(),
                UtilsAndCommons.LOCALHOST_SITE, msg);
          } else {
            if (!ip.isMockValid()) {
              ip.setMockValid(true);
              Loggers.EVT_LOG.info("serviceName: {} {PROBE} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: {}",
                  cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(),
                  UtilsAndCommons.LOCALHOST_SITE, msg);
            }
          }
        } else {
          Loggers.EVT_LOG.info("serviceName: {} {OTHER} {IP-ENABLED} pre-valid: {}:{}@{} in {}, msg: {}",
              cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), ip.getOKCount(), msg);
        }
      }
    } catch (Throwable t) {
      Loggers.SRV_LOG.error("[CHECK-OK] error when close check task.", t);
    }

    ip.getFailCount().set(0);
    ip.setBeingChecked(false);
  }

  /**
   * 处理（可能是一时的）坏的健康检查结果。
   * <p>
   * 将对应的 instance 置为非健康状态，由于其服务对应的有实例不可用了，所以要广播一个事件出去。
   * 
   * @param ip
   * @param task
   * @param msg
   */
  public void checkFail(Instance ip, HealthCheckTask task, String msg) {
    Cluster cluster = task.getCluster();

    try {
      if (ip.isHealthy() || ip.isMockValid()) {
        // 确认不健康检查次数是否达到阈值，没达到阈值不会置为不健康（实例可能是一时不可用）
        if (ip.getFailCount().incrementAndGet() >= switchDomain.getCheckTimes()) {
          // 确认不健康实例对应服务属于当前 nacos 节点管理
          if (distroMapper.responsible(cluster, ip)) {
            ip.setHealthy(false);
            ip.setMockValid(false);

            Service service = cluster.getService();
            service.setLastModifiedMillis(System.currentTimeMillis());
            // 记录健康检查结果，回头会同步给其它可达的 nacos 节点
            addResult(new HealthCheckResult(service.getName(), ip));

            // 发广播昭告不健康实例对应的服务有变更
            pushService.serviceChanged(service);

            Loggers.EVT_LOG.info("serviceName: {} {POS} {IP-DISABLED} invalid: {}:{}@{}, region: {}, msg: {}",
                cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(),
                UtilsAndCommons.LOCALHOST_SITE, msg);
          } else {
            Loggers.EVT_LOG.info("serviceName: {} {PROBE} {IP-DISABLED} invalid: {}:{}@{}, region: {}, msg: {}",
                cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(),
                UtilsAndCommons.LOCALHOST_SITE, msg);
          }

        } else {
          Loggers.EVT_LOG.info("serviceName: {} {OTHER} {IP-DISABLED} pre-invalid: {}:{}@{} in {}, msg: {}",
              cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(), ip.getFailCount(), msg);
        }
      }
    } catch (Throwable t) {
      Loggers.SRV_LOG.error("[CHECK-FAIL] error when close check task.", t);
    }

    ip.getOKCount().set(0);

    ip.setBeingChecked(false);
  }

  /**
   * 直接将节点判为不健康（不受阈值限制），其它处理同 checkFail。
   */
  public void checkFailNow(Instance ip, HealthCheckTask task, String msg) {
    Cluster cluster = task.getCluster();
    try {
      if (ip.isHealthy() || ip.isMockValid()) {
        if (distroMapper.responsible(cluster, ip)) {
          ip.setHealthy(false);
          ip.setMockValid(false);

          Service service = cluster.getService();
          service.setLastModifiedMillis(System.currentTimeMillis());

          pushService.serviceChanged(service);
          addResult(new HealthCheckResult(service.getName(), ip));

          Loggers.EVT_LOG.info("serviceName: {} {POS} {IP-DISABLED} invalid-now: {}:{}@{}, region: {}, msg: {}",
              cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(),
              UtilsAndCommons.LOCALHOST_SITE, msg);
        } else {
          if (ip.isMockValid()) {
            ip.setMockValid(false);
            Loggers.EVT_LOG.info("serviceName: {} {PROBE} {IP-DISABLED} invalid-now: {}:{}@{}, region: {}, msg: {}",
                cluster.getService().getName(), ip.getIp(), ip.getPort(), cluster.getName(),
                UtilsAndCommons.LOCALHOST_SITE, msg);
          }

        }
      }
    } catch (Throwable t) {
      Loggers.SRV_LOG.error("[CHECK-FAIL-NOW] error when close check task.", t);
    }

    ip.getOKCount().set(0);
    ip.setBeingChecked(false);
  }

  /**
   * 将健康检查结果保存到本地（有后台线程会周期性将结果同步给其它可达的 nacos 节点）
   * 
   * @param result
   */
  private void addResult(HealthCheckResult result) {

    if (!switchDomain.getIncrementalList().contains(result.getServiceName())) {
      return;
    }

    if (!healthCheckResults.offer(result)) {
      Loggers.EVT_LOG.warn("[HEALTH-CHECK-SYNC] failed to add check result to queue, queue size: {}",
          healthCheckResults.size());
    }
  }

  static class HealthCheckResult {
    private String serviceName;
    private Instance instance;

    public HealthCheckResult(String serviceName, Instance instance) {
      this.serviceName = serviceName;
      this.instance = instance;
    }

    public String getServiceName() {
      return serviceName;
    }

    public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
    }

    public Instance getInstance() {
      return instance;
    }

    public void setInstance(Instance instance) {
      this.instance = instance;
    }
  }
}
