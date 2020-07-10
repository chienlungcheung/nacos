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

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.nacos.naming.boot.SpringContext;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.apache.commons.lang3.RandomUtils;

/**
 * 每个集群(cluster)一个健康检查任务。集群初始化时创建健康检查任务，然后周期性调度之（周期性靠自己调度自己来实现，具体见 run 的
 * finally）。
 * <p>
 * 其内包含了集群相关信息，以及执行本 task 的 processor 是哪个（task 会在自己的 run 方法中调用
 * processor，后者再处理前者，设计有点别扭）。
 * 
 * @author nacos
 */
public class HealthCheckTask implements Runnable {

  private Cluster cluster;

  /**
   * 每次健康检查 Round-Trip 的移动加权平均值
   */
  private long checkRTNormalized = -1;
  private long checkRTBest = -1;
  private long checkRTWorst = -1;

  /**
   * 上一次健康检查 round-trip 耗时
   */
  private long checkRTLast = -1;
  private long checkRTLastLast = -1;

  private long startTime;

  private volatile boolean cancelled = false;

  @JSONField(serialize = false)
  private DistroMapper distroMapper;

  @JSONField(serialize = false)
  private SwitchDomain switchDomain;

  @JSONField(serialize = false)
  private HealthCheckProcessor healthCheckProcessor;

  public HealthCheckTask(Cluster cluster) {
    this.cluster = cluster;
    distroMapper = SpringContext.getAppContext().getBean(DistroMapper.class);
    switchDomain = SpringContext.getAppContext().getBean(SwitchDomain.class);
    // HealthCheckProcessor 目前有四种，可以根据集群特点进行配置
    healthCheckProcessor = SpringContext.getAppContext().getBean(HealthCheckProcessorDelegate.class);
    initCheckRT();
  }

  public void initCheckRT() {
    // first check time delay
    checkRTNormalized = 2000
        + RandomUtils.nextInt(0, RandomUtils.nextInt(0, switchDomain.getTcpHealthParams().getMax()));
    checkRTBest = Long.MAX_VALUE;
    checkRTWorst = 0L;
  }

  @Override
  public void run() {

    try {
      // 如果当前 nacos 实例负责维护该集群对应的服务，并且该集群开启了健康检查功能
      if (distroMapper.responsible(cluster.getService().getName())
          && switchDomain.isHealthCheckEnabled(cluster.getService().getName())) {
        // 则把当前健康检查任务添加到健康检查处理器中
        healthCheckProcessor.process(this);
        if (Loggers.EVT_LOG.isDebugEnabled()) {
          Loggers.EVT_LOG.debug("[HEALTH-CHECK] schedule health check task: {}", cluster.getService().getName());
        }
      }
    } catch (Throwable e) {
      Loggers.SRV_LOG.error("[HEALTH-CHECK] error while process health check for {}:{}", cluster.getService().getName(),
          cluster.getName(), e);
    } finally {
      if (!cancelled) {
        // 如果任务未被取消，则将自己放到线程池实现重复调度
        HealthCheckReactor.scheduleCheck(this);

        // worst == 0 means never checked
        if (this.getCheckRTWorst() > 0 && switchDomain.isHealthCheckEnabled(cluster.getService().getName())
            && distroMapper.responsible(cluster.getService().getName())) {
          // TLog doesn't support float so we must convert it into long
          long diff = ((this.getCheckRTLast() - this.getCheckRTLastLast()) * 10000) / this.getCheckRTLastLast();

          this.setCheckRTLastLast(this.getCheckRTLast());

          Cluster cluster = this.getCluster();

          if (Loggers.CHECK_RT.isDebugEnabled()) {
            Loggers.CHECK_RT.debug("{}:{}@{}->normalized: {}, worst: {}, best: {}, last: {}, diff: {}",
                cluster.getService().getName(), cluster.getName(), cluster.getHealthChecker().getType(),
                this.getCheckRTNormalized(), this.getCheckRTWorst(), this.getCheckRTBest(), this.getCheckRTLast(),
                diff);
          }
        }
      }
    }
  }

  public Cluster getCluster() {
    return cluster;
  }

  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }

  public long getCheckRTNormalized() {
    return checkRTNormalized;
  }

  public long getCheckRTBest() {
    return checkRTBest;
  }

  public long getCheckRTWorst() {
    return checkRTWorst;
  }

  public void setCheckRTWorst(long checkRTWorst) {
    this.checkRTWorst = checkRTWorst;
  }

  public void setCheckRTBest(long checkRTBest) {
    this.checkRTBest = checkRTBest;
  }

  public void setCheckRTNormalized(long checkRTNormalized) {
    this.checkRTNormalized = checkRTNormalized;
  }

  public boolean isCancelled() {
    return cancelled;
  }

  public void setCancelled(boolean cancelled) {
    this.cancelled = cancelled;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getCheckRTLast() {
    return checkRTLast;
  }

  public void setCheckRTLast(long checkRTLast) {
    this.checkRTLast = checkRTLast;
  }

  public long getCheckRTLastLast() {
    return checkRTLastLast;
  }

  public void setCheckRTLastLast(long checkRTLastLast) {
    this.checkRTLastLast = checkRTLastLast;
  }
}
