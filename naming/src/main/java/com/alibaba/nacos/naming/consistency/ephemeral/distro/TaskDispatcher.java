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

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Data sync task dispatcher 任务分配器，内含多个任务调度器，通过 addTask 采用近似均匀分布
 * 算法将任务追加到某个调度器的队列中等待被执行，执行过程就是取出一批数据然后调用 DataSyncer 提交同步任务。
 * <p>
 * 该类由 DistroConsistencyServiceImpl 一致性算法来使用，当新增一个服务发现时，就会调用该类的 addTask
 * 方法将对应的服务追加到某个任务调度器的队列等待被执行。
 * 
 * @author nkorange
 * @since 1.0.0
 */
@Component
public class TaskDispatcher {

  @Autowired
  private GlobalConfig partitionConfig;

  @Autowired
  private DataSyncer dataSyncer;

  private List<TaskScheduler> taskSchedulerList = new ArrayList<>();

  private final int cpuCoreCount = Runtime.getRuntime().availableProcessors();

  @PostConstruct
  public void init() {
    // 生成 cpu 个数个任务调度器
    for (int i = 0; i < cpuCoreCount; i++) {
      TaskScheduler taskScheduler = new TaskScheduler(i);
      taskSchedulerList.add(taskScheduler);
      GlobalExecutor.submitTaskDispatch(taskScheduler);
    }
  }

  /**
   * 新增一个任务，将采用近似均匀分布的算法将该任务追加到某个任务调度器的队列中。
   * 
   * @param key
   */
  public void addTask(String key) {
    taskSchedulerList.get(UtilsAndCommons.shakeUp(key, cpuCoreCount)).addTask(key);
  }

  /**
   * 任务调度器，具体就是取出一批数据然后调用 DataSyncer 提交同步任务。
   */
  public class TaskScheduler implements Runnable {

    private int index;

    private int dataSize = 0;

    private long lastDispatchTime = 0L;

    private BlockingQueue<String> queue = new LinkedBlockingQueue<>(128 * 1024);

    public TaskScheduler(int index) {
      this.index = index;
    }

    public void addTask(String key) {
      queue.offer(key);
    }

    public int getIndex() {
      return index;
    }

    @Override
    public void run() {

      List<String> keys = new ArrayList<>();
      while (true) {

        try {

          String key = queue.poll(partitionConfig.getTaskDispatchPeriod(), TimeUnit.MILLISECONDS);

          if (Loggers.DISTRO.isDebugEnabled() && StringUtils.isNotBlank(key)) {
            Loggers.DISTRO.debug("got key: {}", key);
          }

          if (dataSyncer.getServers() == null || dataSyncer.getServers().isEmpty()) {
            continue;
          }

          if (StringUtils.isBlank(key)) {
            continue;
          }

          if (dataSize == 0) {
            keys = new ArrayList<>();
          }

          keys.add(key);
          dataSize++;

          // 要同步的数据达到一定个数或者距离上次同步超过一定时间才触发一次新的同步过程
          if (dataSize == partitionConfig.getBatchSyncKeyCount()
              || (System.currentTimeMillis() - lastDispatchTime) > partitionConfig.getTaskDispatchPeriod()) {

            // 针对每个可达实例发起一个同步任务（只要当前实例认为某个实例可达，如果同步失败就会不停地重试）
            for (Server member : dataSyncer.getServers()) {
              // 没必要发给自己
              if (NetUtils.localServer().equals(member.getKey())) {
                continue;
              }
              // 针对一个可达实例，生成一个同步任务，放到调度器等待执行
              SyncTask syncTask = new SyncTask();
              // 只同步 keys 过去，对方会根据拉取自己感兴趣的（即非自己负责的） keys 对应的数据
              syncTask.setKeys(keys);
              syncTask.setTargetServer(member.getKey());

              if (Loggers.DISTRO.isDebugEnabled() && StringUtils.isNotBlank(key)) {
                Loggers.DISTRO.debug("add sync task: {}", JSON.toJSONString(syncTask));
              }

              dataSyncer.submit(syncTask, 0);
            }
            // 记录最近一次同步时间
            lastDispatchTime = System.currentTimeMillis();
            dataSize = 0;
          }

        } catch (Exception e) {
          Loggers.DISTRO.error("dispatch sync task failed.", e);
        }
      }
    }
  }
}
