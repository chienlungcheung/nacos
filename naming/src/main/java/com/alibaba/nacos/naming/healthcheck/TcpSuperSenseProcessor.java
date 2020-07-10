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

import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.*;

import static com.alibaba.nacos.naming.misc.Loggers.SRV_LOG;

/**
 * TCP health check processor
 * <p>
 * 基于 TCP 的健康检查，负责执行检查逻辑。
 * 
 * @author nacos
 */
@Component
public class TcpSuperSenseProcessor implements HealthCheckProcessor, Runnable {

  @Autowired
  private HealthCheckCommon healthCheckCommon;

  @Autowired
  private SwitchDomain switchDomain;

  public static final int CONNECT_TIMEOUT_MS = 500;

  private Map<String, BeatKey> keyMap = new ConcurrentHashMap<>();

  private BlockingQueue<Beat> taskQueue = new LinkedBlockingQueue<Beat>();

  /**
   * this value has been carefully tuned, do not modify unless you're confident
   */
  private static final int NIO_THREAD_COUNT = Runtime.getRuntime().availableProcessors() <= 1 ? 1
      : Runtime.getRuntime().availableProcessors() / 2;

  /**
   * because some hosts doesn't support keep-alive connections, disabled
   * temporarily
   */
  private static final long TCP_KEEP_ALIVE_MILLIS = 0;

  private static ScheduledExecutorService TCP_CHECK_EXECUTOR = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setName("nacos.naming.tcp.check.worker");
      t.setDaemon(true);
      return t;
    }
  });

  private static ScheduledExecutorService NIO_EXECUTOR = Executors.newScheduledThreadPool(NIO_THREAD_COUNT,
      new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread thread = new Thread(r);
          thread.setDaemon(true);
          thread.setName("nacos.supersense.checker");
          return thread;
        }
      });

  private Selector selector;

  public TcpSuperSenseProcessor() {
    try {
      selector = Selector.open();

      TCP_CHECK_EXECUTOR.submit(this);

    } catch (Exception e) {
      throw new IllegalStateException("Error while initializing SuperSense(TM).");
    }
  }

  @Override
  public void process(HealthCheckTask task) {
    // 获取集群对应的全部实例，注意这里获取的是持久性实例
    List<Instance> ips = task.getCluster().allIPs(false);

    if (CollectionUtils.isEmpty(ips)) {
      return;
    }

    // 遍历每个实例，发送心跳
    for (Instance ip : ips) {

      // 如果标记过无须检查，则跳过
      if (ip.isMarked()) {
        if (SRV_LOG.isDebugEnabled()) {
          SRV_LOG.debug("tcp check, ip is marked as to skip health check, ip:" + ip.getIp());
        }
        continue;
      }

      // 尝试为实例打正在检查中的标记（打不上说明其它线程抢先了）
      if (!ip.markChecking()) {
        SRV_LOG.warn("tcp check started before last one finished, service: " + task.getCluster().getService().getName()
            + ":" + task.getCluster().getName() + ":" + ip.getIp() + ":" + ip.getPort());

        healthCheckCommon.reEvaluateCheckRT(task.getCheckRTNormalized() * 2, task, switchDomain.getTcpHealthParams());
        continue;
      }

      // 创建一个心跳信息，放入 IO 线程的队列等待被发送
      Beat beat = new Beat(ip, task);
      taskQueue.add(beat);
      MetricsMonitor.getTcpHealthCheckMonitor().incrementAndGet();
    }
  }

  /**
   * 批量处理封装了心跳的 TaskProcessor 任务，后者负责建连接并注册 socket 事件（发送和收响应由 selector 负责）。
   * 
   * @throws Exception
   */
  private void processTask() throws Exception {
    Collection<Callable<Void>> tasks = new LinkedList<>();
    // 待发送心跳消息队列不为空时候，取够 NIO_THREAD_COUNT * 64 个消息就一批发走
    do {
      Beat beat = taskQueue.poll(CONNECT_TIMEOUT_MS / 2, TimeUnit.MILLISECONDS);
      if (beat == null) {
        return;
      }

      // 将每个心跳封装到 TaskProcessor 中，后者负责建立连接并在 selector 上注册相关事件，
      // selector 在监听到 CONNECT 事件会发送心跳，收到 READ 事件会读取响应。
      tasks.add(new TaskProcessor(beat));
    } while (taskQueue.size() > 0 && tasks.size() < NIO_THREAD_COUNT * 64);

    // 并行执行多个 TaskProcessor
    for (Future<?> f : NIO_EXECUTOR.invokeAll(tasks)) {
      f.get();
    }
  }

  /**
   * 基于 tcp 的 processor 不同于 http 那个，是一个长期运行的任务。
   */
  @Override
  public void run() {
    while (true) {
      try {
        processTask();

        // 没有就绪的立马返回，紧接着去处理下一批心跳
        int readyCount = selector.selectNow();
        if (readyCount <= 0) {
          continue;
        }

        Iterator<SelectionKey> iter = selector.selectedKeys().iterator();
        while (iter.hasNext()) {
          SelectionKey key = iter.next();
          iter.remove();

          NIO_EXECUTOR.execute(new PostProcessor(key));
        }
      } catch (Throwable e) {
        SRV_LOG.error("[HEALTH-CHECK] error while processing NIO task", e);
      }
    }
  }

  /**
   * 负责心跳响应处理
   */
  public class PostProcessor implements Runnable {
    SelectionKey key;

    public PostProcessor(SelectionKey key) {
      this.key = key;
    }

    @Override
    public void run() {
      Beat beat = (Beat) key.attachment();
      SocketChannel channel = (SocketChannel) key.channel();
      try {
        if (!beat.isHealthy()) {
          // invalid beat means this server is no longer responsible for the current
          // service
          key.cancel();
          key.channel().close();

          beat.finishCheck();
          return;
        }

        // 超时之前能建立连接说明就是健康的
        if (key.isValid() && key.isConnectable()) {
          // connected
          channel.finishConnect();
          beat.finishCheck(true, false, System.currentTimeMillis() - beat.getTask().getStartTime(), "tcp:ok+");
        }

        // 并不关心对端返回的数据，但是如果对端关闭连接也把几端关闭，否则维持连接啥也不干
        if (key.isValid() && key.isReadable()) {
          // disconnected
          ByteBuffer buffer = ByteBuffer.allocate(128);
          if (channel.read(buffer) == -1) {
            key.cancel();
            key.channel().close();
          } else {
            // not terminate request, ignore
          }
        }
      } catch (ConnectException e) {
        // unable to connect, possibly port not opened
        // 如果连接不上，则直接判为不健康
        beat.finishCheck(false, true, switchDomain.getTcpHealthParams().getMax(),
            "tcp:unable2connect:" + e.getMessage());
      } catch (Exception e) {
        // 其它异常，认为是一时的
        beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(), "tcp:error:" + e.getMessage());
        // 但是要关闭几端连接
        try {
          key.cancel();
          key.channel().close();
        } catch (Exception ignore) {
        }
      }
    }
  }

  /**
   * 封装了一个心跳消息
   */
  private class Beat {
    Instance ip;

    /**
     * 对应的健康检查任务，处理最后结果的时候要用到。
     */
    HealthCheckTask task;

    long startTime = System.currentTimeMillis();

    Beat(Instance ip, HealthCheckTask task) {
      this.ip = ip;
      this.task = task;
    }

    public void setStartTime(long time) {
      startTime = time;
    }

    public long getStartTime() {
      return startTime;
    }

    public Instance getIp() {
      return ip;
    }

    public HealthCheckTask getTask() {
      return task;
    }

    /**
     * 如果心跳超过阈值才返回，判为不健康
     */
    public boolean isHealthy() {
      return System.currentTimeMillis() - startTime < TimeUnit.SECONDS.toMillis(30L);
    }

    /**
     * finish check only, no ip state will be changed
     */
    public void finishCheck() {
      ip.setBeingChecked(false);
    }

    /**
     * 处理健康检查结果。
     * 
     * @param success
     * @param now 若为 true，则直接判为不健康；若为 false，则由阈值控制是否不健康。
     * @param rt
     * @param msg
     */
    public void finishCheck(boolean success, boolean now, long rt, String msg) {
      // 记录本次 round-trip 耗时
      ip.setCheckRT(System.currentTimeMillis() - startTime);

      if (success) {
        healthCheckCommon.checkOK(ip, task, msg);
      } else {
        if (now) {
          healthCheckCommon.checkFailNow(ip, task, msg);
        } else {
          healthCheckCommon.checkFail(ip, task, msg);
        }

        keyMap.remove(task.toString());
      }

      healthCheckCommon.reEvaluateCheckRT(rt, task, switchDomain.getTcpHealthParams());
    }

    @Override
    public String toString() {
      return task.getCluster().getService().getName() + ":" + task.getCluster().getName() + ":" + ip.getIp() + ":"
          + ip.getPort();
    }

    @Override
    public int hashCode() {
      return Objects.hash(ip.toJSON());
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null || !(obj instanceof Beat)) {
        return false;
      }

      return this.toString().equals(obj.toString());
    }
  }

  private static class BeatKey {
    public SelectionKey key;
    public long birthTime;

    public BeatKey(SelectionKey key) {
      this.key = key;
      this.birthTime = System.currentTimeMillis();
    }
  }

  /**
   * 负责控制心跳连接的超时
   */
  private static class TimeOutTask implements Runnable {
    SelectionKey key;

    public TimeOutTask(SelectionKey key) {
      this.key = key;
    }

    @Override
    public void run() {
      if (key != null && key.isValid()) {
        SocketChannel channel = (SocketChannel) key.channel();
        Beat beat = (Beat) key.attachment();

        if (channel.isConnected()) {
          return;
        }

        try {
          channel.finishConnect();
        } catch (Exception ignore) {
        }

        try {
          beat.finishCheck(false, false, beat.getTask().getCheckRTNormalized() * 2, "tcp:timeout");
          key.cancel();
          key.channel().close();
        } catch (Exception ignore) {
        }
      }
    }
  }

  /**
   * 负责创建 tcp 连接发送心跳，并把连接注册到 selector 上让后者等结果。
   */
  private class TaskProcessor implements Callable<Void> {

    private static final int MAX_WAIT_TIME_MILLISECONDS = 500;
    Beat beat;

    public TaskProcessor(Beat beat) {
      this.beat = beat;
    }

    @Override
    public Void call() {
      // 检查下这个心跳在队列中国等待多久了
      long waited = System.currentTimeMillis() - beat.getStartTime();
      if (waited > MAX_WAIT_TIME_MILLISECONDS) {
        Loggers.SRV_LOG.warn("beat task waited too long: " + waited + "ms");
      }

      SocketChannel channel = null;
      try {
        Instance instance = beat.getIp();
        Cluster cluster = beat.getTask().getCluster();

        BeatKey beatKey = keyMap.get(beat.toString());
        if (beatKey != null && beatKey.key.isValid()) {
          if (System.currentTimeMillis() - beatKey.birthTime < TCP_KEEP_ALIVE_MILLIS) {
            instance.setBeingChecked(false);
            return null;
          }

          beatKey.key.cancel();
          beatKey.key.channel().close();
        }

        // 创建连接
        channel = SocketChannel.open();
        // 连接设置为非阻塞
        channel.configureBlocking(false);
        // only by setting this can we make the socket close event asynchronous
        // 只有设置了下面四个属性，才能确保我们异步关闭连接
        // 关闭 SO_LINGER 避免发送 RST 给对方（这个可以避免己端 TIME_WAIT, 但不优雅，RST 严格来说是异常）
        channel.socket().setSoLinger(false, -1);
        // 设置本地地址即使处于 TIME_WAIT 也能进行重复绑定
        channel.socket().setReuseAddress(true);
        // 在该连接启用 TCP 保活定时器
        channel.socket().setKeepAlive(true);
        // 禁用 Nagle 算法，有数据不缓存立马发送
        channel.socket().setTcpNoDelay(true);

        int port = cluster.isUseIPPort4Check() ? instance.getPort() : cluster.getDefCkport();
        channel.connect(new InetSocketAddress(instance.getIp(), port));

        // 将该连接注册到 selector, 只对 CONNECT 和 READ 事件感兴趣
        SelectionKey key = channel.register(selector, SelectionKey.OP_CONNECT | SelectionKey.OP_READ);
        key.attach(beat);
        keyMap.put(beat.toString(), new BeatKey(key));

        beat.setStartTime(System.currentTimeMillis());

        // 启动一个异步任务，负责在 500ms 后检查连接是否成功，如果还没连上，就在超时任务中将连接关闭
        NIO_EXECUTOR.schedule(new TimeOutTask(key), CONNECT_TIMEOUT_MS, TimeUnit.MILLISECONDS);
      } catch (Exception e) {
        // 如果发生异常，则关闭连接
        beat.finishCheck(false, false, switchDomain.getTcpHealthParams().getMax(), "tcp:error:" + e.getMessage());

        if (channel != null) {
          try {
            channel.close();
          } catch (Exception ignore) {
          }
        }
      }

      return null;
    }
  }

  @Override
  public String getType() {
    return "TCP";
  }
}
