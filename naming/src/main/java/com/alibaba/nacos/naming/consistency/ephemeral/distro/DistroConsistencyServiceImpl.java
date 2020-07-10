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

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.ServerStatus;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.ephemeral.EphemeralConsistencyService;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.pojo.Record;
import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * A consistency protocol algorithm called <b>Partition</b>
 * <p>
 * Use a distro algorithm to divide data into many blocks. Each Nacos server
 * node takes responsibility for exactly one block of data. Each block of data
 * is generated, removed and synchronized by its responsible server. So every
 * Nacos server only handles writings for a subset of the total service data.
 * <p>
 * At mean time every Nacos server receives data sync of other Nacos server, so
 * every Nacos server will eventually have a complete set of data.
 *
 * <p>
 * DistroConsistencyServiceImpl 实现了一个叫做 Partition 的一致性算法，能够保证 AP（而 RaftConsistencyServiceImpl 负责保障 CP）。
 * <p>
 * 使用 distro 算法将数据分成许多块，每个 nacos 服务实例恰好负责一块。 每块数据的生成、移除以及同步全部由对应的 nacos
 * 服务实例负责，于是，每个 nacos 服务实例仅负责处理服务数据的一个 子集。
 * <p>
 * 除了自己负责的一块，每个 nacos 服务实例还会同步集群其它实例同步数据到本地，于是每个 nacos 实例将会用于一个完整的数据集。
 * 
 * @author nkorange
 * @since 1.0.0
 */
@org.springframework.stereotype.Service("distroConsistencyService")
public class DistroConsistencyServiceImpl implements EphemeralConsistencyService {

  private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);

      t.setDaemon(true);
      t.setName("com.alibaba.nacos.naming.distro.notifier");

      return t;
    }
  });

  @Autowired
  private DistroMapper distroMapper;

  @Autowired
  private DataStore dataStore;

  @Autowired
  private TaskDispatcher taskDispatcher;

  @Autowired
  private DataSyncer dataSyncer;

  @Autowired
  private Serializer serializer;

  @Autowired
  private ServerListManager serverListManager;

  @Autowired
  private SwitchDomain switchDomain;

  @Autowired
  private GlobalConfig globalConfig;

  private boolean initialized = false;

  public volatile Notifier notifier = new Notifier();

  private Map<String, CopyOnWriteArrayList<RecordListener>> listeners = new ConcurrentHashMap<>();

  private Map<String, String> syncChecksumTasks = new ConcurrentHashMap<>(16);

  @PostConstruct
  public void init() {
    // 启动时提交一个 one-shot 类型的服务信息拉取任务给全局调度器
    GlobalExecutor.submit(new Runnable() {
      @Override
      public void run() {
        try {
          load();
        } catch (Exception e) {
          Loggers.DISTRO.error("load data failed.", e);
        }
      }
    });

    /**
     * 启动通知器，当有服务出现变化时（CHANGE or REMOVE）就会做相应的处理
     */
    executor.submit(notifier);
  }

  /**
   * 负责在当前节点启动时从 nacos 集群任何一个其它可达节点拉取全量服务发现数据（服务及其实例列表信息）到本地初始化内存存储
   * 并将每个服务注册到一致性算法的监听器列表中（这样当服务有任何变更都会自动触发服务的相关方法进行处理）。
   * 
   * @throws Exception
   */
  public void load() throws Exception {
    if (SystemUtils.STANDALONE_MODE) {
      initialized = true;
      return;
    }
    // 其它可达节点不能小于 1，否则一直等
    // size = 1 means only myself in the list, we need at least one another server
    // alive:
    while (serverListManager.getHealthyServers().size() <= 1) {
      Thread.sleep(1000L);
      Loggers.DISTRO.info("waiting server list init...");
    }

    // 从其它可达节点拉取数据
    for (Server server : serverListManager.getHealthyServers()) {
      if (NetUtils.localServer().equals(server.getKey())) {
        continue;
      }
      if (Loggers.DISTRO.isDebugEnabled()) {
        Loggers.DISTRO.debug("sync from " + server);
      }
      // 有一个可达节点数据被拉过来，就结束，表示初始化完成
      // try sync data from remote server:
      if (syncAllDataFromRemote(server)) {
        initialized = true;
        return;
      }
    }
  }

  /**
   * 新增一个服务发现到一致性管理中（会自动广播给其它可达 nacos 实例）。
   */
  @Override
  public void put(String key, Record value) throws NacosException {
    onPut(key, value);
    // 新增一个服务，需要将其同步给 nacos 集群其它实例，这里会增加一个同步任务，稍后将被执行。
    taskDispatcher.addTask(key);
  }

  /**
   * 移除某个服务一致性管理及其相关监听器。
   */
  @Override
  public void remove(String key) throws NacosException {
    onRemove(key);
    listeners.remove(key);
  }

  /**
   * 获取某个服务对应的实例列表信息
   */
  @Override
  public Datum get(String key) throws NacosException {
    return dataStore.get(key);
  }

  /**
   * 将 key 对应的服务相关信息放到本地内存存储中，如果该服务加入前将自己注册到了一致性算法的监听列表里，则触发一个变更事件。
   * 
   * @param key
   * @param value
   */
  public void onPut(String key, Record value) {

    // 除了特殊的 SERVICE_META_KEY_PREFIX ，服务发现中的服务信息都是临时的
    if (KeyBuilder.matchEphemeralInstanceListKey(key)) {
      Datum<Instances> datum = new Datum<>();
      datum.value = (Instances) value;
      datum.key = key;
      datum.timestamp.incrementAndGet();
      dataStore.put(key, datum);
    }

    if (!listeners.containsKey(key)) {
      return;
    }

    // 触发一个 CHANGE 类型的通知任务，将会导致 ServiceManager.onChange（当为 SERVICE_META_KEY_PREFIX
    // 时）
    // 或 Service.onChange 执行
    notifier.addTask(key, ApplyAction.CHANGE);
  }

  /**
   * 移除对某个服务的一致性保证，如果该服务注册过监听器则触发之。
   * 
   * @param key
   */
  public void onRemove(String key) {

    dataStore.remove(key);

    if (!listeners.containsKey(key)) {
      return;
    }

    notifier.addTask(key, ApplyAction.DELETE);
  }

  /**
   * 负责处理从名为 server 的 nacos 节点发来的各个服务的校验和。
   * <p>
   * 处理结果有两种: 删除某些服务(不再被维护), 更新某些服务(有变更, 这会导致二次请求,即根据服务 key 拉取对应服务信息).
   * 
   * @param checksumMap
   * @param server
   */
  public void onReceiveChecksums(Map<String, String> checksumMap, String server) {

    // 检查下如果目前正在处理某个 nacos 实例发来的校验和，期间如果它再发校验和过来就跳过
    if (syncChecksumTasks.containsKey(server)) {
      // Already in process of this server:
      Loggers.DISTRO.warn("sync checksum task already in process with {}", server);
      return;
    }

    // 打个正在处理中的标记，key 为本次发来校验和的 nacos 实例
    syncChecksumTasks.put(server, "1");

    try {

      // 保存待更新的服务的键
      List<String> toUpdateKeys = new ArrayList<>();
      // 保存待删除的服务的键
      List<String> toRemoveKeys = new ArrayList<>();
      for (Map.Entry<String, String> entry : checksumMap.entrySet()) {
        // 自己负责的 service 不应该出现在这里，其它服务只会发送自己负责的 service 的校验和
        if (distroMapper.responsible(KeyBuilder.getServiceName(entry.getKey()))) {
          // this key should not be sent from remote server:
          Loggers.DISTRO.error("receive responsible key timestamp of " + entry.getKey() + " from " + server);
          // abort the procedure:
          return;
        }
        // 如果本地不存在对应服务的信息，或者只有 key 没有 value，或者 value 校验和不等于刚发来的校验和，
        // 则用收到的数据进行更新。
        if (!dataStore.contains(entry.getKey()) || dataStore.get(entry.getKey()).value == null
            || !dataStore.get(entry.getKey()).value.getChecksum().equals(entry.getValue())) {
          toUpdateKeys.add(entry.getKey());
        }
      }

      // 遍历当前 nacos 实例内存存储中的全部服务
      for (String key : dataStore.keys()) {

        // 只处理发来校验和的 nacos 实例负责的服务
        if (!server.equals(distroMapper.mapSrv(KeyBuilder.getServiceName(key)))) {
          continue;
        }

        // 如果本地有的但是不在接收到的校验和集合里，则将其标记为待删除
        if (!checksumMap.containsKey(key)) {
          toRemoveKeys.add(key);
        }
      }

      if (Loggers.DISTRO.isDebugEnabled()) {
        Loggers.DISTRO.info("to remove keys: {}, to update keys: {}, source: {}", toRemoveKeys, toUpdateKeys, server);
      }

      // 删除不再维护的服务信息
      for (String key : toRemoveKeys) {
        onRemove(key);
      }

      if (toUpdateKeys.isEmpty()) {
        return;
      }

      // 拉取待更新的服务信息并更新（或追加到）本地对应的版本
      try {
        byte[] result = NamingProxy.getData(toUpdateKeys, server);
        processData(result);
      } catch (Exception e) {
        Loggers.DISTRO.error("get data from " + server + " failed!", e);
      }
    } finally {
      // Remove this 'in process' flag:
      syncChecksumTasks.remove(server);
    }

  }

  /**
   * 从地址为 server 的 nacos 实例拉取数据到本地，然后进行处理（反序列化、放到内存存储、注册相关监听器等）
   * 
   * @param server
   * @return
   */
  public boolean syncAllDataFromRemote(Server server) {

    try {
      byte[] data = NamingProxy.getAllData(server.getKey());
      processData(data);
      return true;
    } catch (Exception e) {
      Loggers.DISTRO.error("sync full data from " + server + " failed!", e);
      return false;
    }
  }

  /**
   * 针对数据进行反序列化、放到内存存储、注册相关监听器等
   * 
   * @param data
   * @throws Exception
   */
  public void processData(byte[] data) throws Exception {
    if (data.length > 0) {
      Map<String, Datum<Instances>> datumMap = serializer.deserializeMap(data, Instances.class);

      for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {
        // 把服务及其对应的实力列表放到内存存储中
        dataStore.put(entry.getKey(), entry.getValue());

        // 如果服务未注册过监听器，则触发 SERVICE_META_KEY_PREFIX 对应监听器（即 ServiceManager）的 onChange
        // 方法告知服务变化
        if (!listeners.containsKey(entry.getKey())) {
          // 服务发现中的 Instance 对应信息都是临时的
          // pretty sure the service not exist:
          if (switchDomain.isDefaultInstanceEphemeral()) {
            // create empty service
            Loggers.DISTRO.info("creating service {}", entry.getKey());
            Service service = new Service();
            String serviceName = KeyBuilder.getServiceName(entry.getKey());
            String namespaceId = KeyBuilder.getNamespace(entry.getKey());
            service.setName(serviceName);
            service.setNamespaceId(namespaceId);
            service.setGroupName(Constants.DEFAULT_GROUP);
            // now validate the service. if failed, exception will be thrown
            service.setLastModifiedMillis(System.currentTimeMillis());
            service.recalculateChecksum();
            listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX).get(0)
                .onChange(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName), service);
          }
        }
      }

      for (Map.Entry<String, Datum<Instances>> entry : datumMap.entrySet()) {

        // 每个服务都会在 ServiceManager 中将自己作为监听器注册到一致性算法的监听器列表中
        if (!listeners.containsKey(entry.getKey())) {
          // Should not happen:
          Loggers.DISTRO.warn("listener of {} not found.", entry.getKey());
          continue;
        }

        try {
          // 调用服务对应的每个监听器的 Service.onChange 方法处理变更
          for (RecordListener listener : listeners.get(entry.getKey())) {
            listener.onChange(entry.getKey(), entry.getValue().value);
          }
        } catch (Exception e) {
          Loggers.DISTRO.error("[NACOS-DISTRO] error while execute listener of key: {}", entry.getKey(), e);
          continue;
        }

        // 如果监听器执行成功表示服务在本地对应的信息已经被更新，如果这样，那么就用拉取来的服务新信息存入内存存储。
        // Update data store if listener executed successfully:
        dataStore.put(entry.getKey(), entry.getValue());
      }
    }
  }

  /**
   * 注册 ServiceManager（仅当 key 为 SERVICE_META_KEY_PREFIX 时）或者 Service 到监听器列表中，
   * 当对应数据发生变化时，会根据变化类型调用其对应的方法。
   */
  @Override
  public void listen(String key, RecordListener listener) throws NacosException {
    if (!listeners.containsKey(key)) {
      listeners.put(key, new CopyOnWriteArrayList<>());
    }

    if (listeners.get(key).contains(listener)) {
      return;
    }

    listeners.get(key).add(listener);
  }

  /**
   * 从一致性算法维护的 key 对应的监听列表中一处 listener 对应的监听器。
   */
  @Override
  public void unlisten(String key, RecordListener listener) throws NacosException {
    if (!listeners.containsKey(key)) {
      return;
    }
    for (RecordListener recordListener : listeners.get(key)) {
      if (recordListener.equals(listener)) {
        listeners.get(key).remove(listener);
        break;
      }
    }
  }

  @Override
  public boolean isAvailable() {
    return isInitialized() || ServerStatus.UP.name().equals(switchDomain.getOverriddenServerStatus());
  }

  public boolean isInitialized() {
    return initialized || !globalConfig.isDataWarmup();
  }

  /**
   * Notifier 包含一个通知任务队列。
   * 当有服务发生变化时（一致性算法实现的 remove 方法或者 put 方法被调用），
   * 会间接调用其 addTask 方法向其队列追加一个任务，Notifier 事件循环负责取出并执行。
   */
  public class Notifier implements Runnable {

    /**
     * 任何加入任务队列的都会加入到该缓存中，直至对应的通知任务被执行完毕。 该结构可防止同一个服务的通知任务在被处理之前又被加进来一个新的同名的通知任务。
     */
    private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

    /**
     * 保存待处理的通知任务
     */
    private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<Pair>(1024 * 1024);

    public void addTask(String datumKey, ApplyAction action) {

      if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
        return;
      }
      if (action == ApplyAction.CHANGE) {
        services.put(datumKey, StringUtils.EMPTY);
      }
      tasks.add(Pair.with(datumKey, action));
    }

    public int getTaskSize() {
      return tasks.size();
    }

    @Override
    public void run() {
      Loggers.DISTRO.info("distro notifier started");

      while (true) {
        try {

          Pair pair = tasks.take();

          if (pair == null) {
            continue;
          }

          String datumKey = (String) pair.getValue0();
          ApplyAction action = (ApplyAction) pair.getValue1();

          services.remove(datumKey);

          int count = 0;

          if (!listeners.containsKey(datumKey)) {
            continue;
          }

          for (RecordListener listener : listeners.get(datumKey)) {

            count++;

            try {
              if (action == ApplyAction.CHANGE) {
                listener.onChange(datumKey, dataStore.get(datumKey).value);
                continue;
              }

              if (action == ApplyAction.DELETE) {
                listener.onDelete(datumKey);
                continue;
              }
            } catch (Throwable e) {
              Loggers.DISTRO.error("[NACOS-DISTRO] error while notifying listener of key: {}", datumKey, e);
            }
          }

          if (Loggers.DISTRO.isDebugEnabled()) {
            Loggers.DISTRO.debug("[NACOS-DISTRO] datum change notified, key: {}, listener count: {}, action: {}",
                datumKey, count, action.name());
          }
        } catch (Throwable e) {
          Loggers.DISTRO.error("[NACOS-DISTRO] Error while handling notifying task", e);
        }
      }
    }
  }
}
