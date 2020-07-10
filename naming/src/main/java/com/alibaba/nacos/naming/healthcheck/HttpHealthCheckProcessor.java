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

import com.alibaba.nacos.api.naming.pojo.AbstractHealthChecker;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.AsyncHttpClientConfig;
import com.ning.http.client.Response;
import io.netty.channel.ConnectTimeoutException;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static com.alibaba.nacos.naming.misc.Loggers.SRV_LOG;

/**
 * HTTP health check processor
 * <p>
 * 封装了基于 HTTP 的健康检查请求和结果处理逻辑。
 * 
 * @author xuanyin.zy
 */
@Component
public class HttpHealthCheckProcessor implements HealthCheckProcessor {

  @Autowired
  private SwitchDomain switchDomain;

  @Autowired
  private HealthCheckCommon healthCheckCommon;

  private static AsyncHttpClient asyncHttpClient;

  private static final int CONNECT_TIMEOUT_MS = 500;

  static {
    try {
      AsyncHttpClientConfig.Builder builder = new AsyncHttpClientConfig.Builder();

      builder.setMaximumConnectionsTotal(-1);
      builder.setMaximumConnectionsPerHost(-1);
      builder.setAllowPoolingConnection(false);
      builder.setFollowRedirects(false);
      builder.setIdleConnectionTimeoutInMs(CONNECT_TIMEOUT_MS);
      builder.setConnectionTimeoutInMs(CONNECT_TIMEOUT_MS);
      builder.setCompressionEnabled(false);
      builder.setIOThreadMultiplier(1);
      builder.setMaxRequestRetry(0);
      builder.setUserAgent("VIPServer");
      asyncHttpClient = new AsyncHttpClient(builder.build());
    } catch (Throwable e) {
      SRV_LOG.error("[HEALTH-CHECK] Error while constructing HTTP asynchronous client", e);
    }
  }

  /**
   * 返回健康检查类型，http 型
   */
  @Override
  public String getType() {
    return "HTTP";
  }

  /**
   * 执行基于 http 的健康检查逻辑。
   */
  @Override
  public void process(HealthCheckTask task) {
    // 取出集群全部实例，注意这里获取的是持久性实例
    List<Instance> ips = task.getCluster().allIPs(false);
    if (CollectionUtils.isEmpty(ips)) {
      return;
    }

    // 检查是否开启了健康检查
    if (!switchDomain.isHealthCheckEnabled()) {
      return;
    }

    Cluster cluster = task.getCluster();

    // 遍历集群的全部实例，发送健康检查请求
    for (Instance ip : ips) {
      try {

        // 如果实例被标记过不用健康检查，则跳过
        if (ip.isMarked()) {
          if (SRV_LOG.isDebugEnabled()) {
            SRV_LOG.debug("http check, ip is marked as to skip health check, ip: {}" + ip.getIp());
          }
          continue;
        }

        // 标记实例正在被执行健康检查（标记失败说明其它任务抢先了，直接返回）
        if (!ip.markChecking()) {
          SRV_LOG.warn("http check started before last one finished, service: {}:{}:{}",
              task.getCluster().getService().getName(), task.getCluster().getName(), ip.getIp());

          // todo 使用两倍的指数移动平均值作为本次 round-trip 耗时（其实并未发生 round-trip），不晓得为何如此
          healthCheckCommon.reEvaluateCheckRT(task.getCheckRTNormalized() * 2, task,
              switchDomain.getHttpHealthParams());
          continue;
        }

        // 集群可以配置不同的检查器（目前有基于 http、tcp、mysql、none 四类）
        AbstractHealthChecker.Http healthChecker = (AbstractHealthChecker.Http) cluster.getHealthChecker();

        // 用服务实例对外提供的服务端口还是集群默认的端口作为健康检查目的端口
        int ckPort = cluster.isUseIPPort4Check() ? ip.getPort() : cluster.getDefCkport();
        URL host = new URL("http://" + ip.getIp() + ":" + ckPort);
        // 构造健康检查的完整 URL
        URL target = new URL(host, healthChecker.getPath());

        AsyncHttpClient.BoundRequestBuilder builder = asyncHttpClient.prepareGet(target.toString());
        Map<String, String> customHeaders = healthChecker.getCustomHeaders();
        for (Map.Entry<String, String> entry : customHeaders.entrySet()) {
          if ("Host".equals(entry.getKey())) {
            builder.setVirtualHost(entry.getValue());
            continue;
          }

          builder.setHeader(entry.getKey(), entry.getValue());
        }

        // 发起异步的健康检查请求，响应由 HttpHealthCheckCallback 负责处理
        builder.execute(new HttpHealthCheckCallback(ip, task));
        // 检查计数
        MetricsMonitor.getHttpHealthCheckMonitor().incrementAndGet();
      } catch (Throwable e) {
        // 针对当前 instance 的健康检查出现任何异常都当作返回了坏的健康检查结果进行处理
        ip.setCheckRT(switchDomain.getHttpHealthParams().getMax());
        healthCheckCommon.checkFail(ip, task, "http:error:" + e.getMessage());
        healthCheckCommon.reEvaluateCheckRT(switchDomain.getHttpHealthParams().getMax(), task,
            switchDomain.getHttpHealthParams());
      }
    }
  }

  /**
   * 负责在健康检查请求返回结果时候进行处理，针对好的、坏的结果分别进行处理。
   * <p>
   * 主要处理逻辑就是将对应实例置为健康或者非健康，并且广播其对应服务的变更更事件，同时记录健康检查结果并稍后同步给其它可达的 nacos 节点。
   */
  private class HttpHealthCheckCallback extends AsyncCompletionHandler<Integer> {
    private Instance ip;
    private HealthCheckTask task;

    private long startTime = System.currentTimeMillis();

    /**
     * 负责在健康检查请求返回结果时候进行处理，针对好的、坏的结果分别进行处理。
     * <p>
     * 主要处理逻辑就是将对应实例置为健康或者非健康，并且广播其对应服务的变更更事件，同时记录健康检查结果并稍后同步给其它可达的 nacos 节点。
     * 
     * @param ip   被进行健康检查的客户端（某个服务的实例）
     * @param task 对应的健康检查任务（其封装了实例对应的集群信息）
     */
    public HttpHealthCheckCallback(Instance ip, HealthCheckTask task) {
      this.ip = ip;
      this.task = task;
    }

    /**
     * 健康检查请求正常完成，处理返回结果。
     */
    @Override
    public Integer onCompleted(Response response) throws Exception {
      // 记录 rount-trip 耗时
      ip.setCheckRT(System.currentTimeMillis() - startTime);

      int httpCode = response.getStatusCode();
      // 针对不同的健康检查结果分别进行处理
      if (HttpURLConnection.HTTP_OK == httpCode) {
        healthCheckCommon.checkOK(ip, task, "http:" + httpCode);
        healthCheckCommon.reEvaluateCheckRT(System.currentTimeMillis() - startTime, task,
            switchDomain.getHttpHealthParams());
      } else if (HttpURLConnection.HTTP_UNAVAILABLE == httpCode || HttpURLConnection.HTTP_MOVED_TEMP == httpCode) {
        // server is busy, need verification later
        // 503 或者 302，实例可能是一时的不可用
        healthCheckCommon.checkFail(ip, task, "http:" + httpCode);
        healthCheckCommon.reEvaluateCheckRT(task.getCheckRTNormalized() * 2, task, switchDomain.getHttpHealthParams());
      } else {
        // probably means the state files has been removed by administrator
        // 其它情况直接将实例判为不健康（不受阈值限制）
        healthCheckCommon.checkFailNow(ip, task, "http:" + httpCode);
        healthCheckCommon.reEvaluateCheckRT(switchDomain.getHttpHealthParams().getMax(), task,
            switchDomain.getHttpHealthParams());
      }

      return httpCode;
    }

    /**
     * 健康检查请求出现异常，按照异常类型分别进行处理。
     */
    @Override
    public void onThrowable(Throwable t) {
      ip.setCheckRT(System.currentTimeMillis() - startTime);

      Throwable cause = t;
      int maxStackDepth = 50;
      // 探寻异常栈，看是否为网络超时导致，如果是，按照一时不健康处理
      for (int deepth = 0; deepth < maxStackDepth && cause != null; deepth++) {
        if (cause instanceof SocketTimeoutException || cause instanceof ConnectTimeoutException
            || cause instanceof org.jboss.netty.channel.ConnectTimeoutException || cause instanceof TimeoutException
            || cause.getCause() instanceof TimeoutException) {

          healthCheckCommon.checkFail(ip, task, "http:timeout:" + cause.getMessage());
          healthCheckCommon.reEvaluateCheckRT(task.getCheckRTNormalized() * 2, task,
              switchDomain.getHttpHealthParams());

          return;
        }

        cause = cause.getCause();
      }

      // connection error, probably not reachable
      // 如果时连接问题，按照不可达处理，直接判为不健康（不受阈值限制）
      if (t instanceof ConnectException) {
        healthCheckCommon.checkFailNow(ip, task, "http:unable2connect:" + t.getMessage());
        healthCheckCommon.reEvaluateCheckRT(switchDomain.getHttpHealthParams().getMax(), task,
            switchDomain.getHttpHealthParams());
      } else {
        // 其它异常，按照一时不健康处理
        healthCheckCommon.checkFail(ip, task, "http:error:" + t.getMessage());
        healthCheckCommon.reEvaluateCheckRT(switchDomain.getHttpHealthParams().getMax(), task,
            switchDomain.getHttpHealthParams());
      }
    }
  }
}
