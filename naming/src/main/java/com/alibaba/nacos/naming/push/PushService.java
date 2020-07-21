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
package com.alibaba.nacos.naming.push;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.Subscriber;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.util.VersionUtil;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;

/**
 * @author nacos
 */
@Component
public class PushService implements ApplicationContextAware, ApplicationListener<ServiceChangeEvent> {

  @Autowired
  private SwitchDomain switchDomain;

  private ApplicationContext applicationContext;

  private static final long ACK_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(10L);

  private static final int MAX_RETRY_TIMES = 1;

  /**
   * 存储已经推送但等待客户端确认的数据. 当前 nacos 节点收到 ack 后将对应客户端的待推送数据从内存移除(已经推送成功了).
   */
  private static volatile ConcurrentMap<String, Receiver.AckEntry> ackMap = new ConcurrentHashMap<String, Receiver.AckEntry>();

  private static ConcurrentMap<String, ConcurrentMap<String, PushClient>> clientMap = new ConcurrentHashMap<String, ConcurrentMap<String, PushClient>>();

  /**
   * 保存每次推送的时间戳, 收到 ack 时会用来计算推送耗时.
   */
  private static volatile ConcurrentHashMap<String, Long> udpSendTimeMap = new ConcurrentHashMap<String, Long>();

  public static volatile ConcurrentHashMap<String, Long> pushCostMap = new ConcurrentHashMap<String, Long>();

  private static int totalPush = 0;

  private static int failedPush = 0;

  private static ConcurrentHashMap<String, Long> lastPushMillisMap = new ConcurrentHashMap<>();

  private static DatagramSocket udpSocket;

  private static Map<String, Future> futureMap = new ConcurrentHashMap<>();
  /**
   * 专门负责重新推送(之前失败了)数据给客户端的线程池
   */
  private static ScheduledExecutorService executorService = Executors
      .newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(Runnable r) {
          Thread t = new Thread(r);
          t.setDaemon(true);
          t.setName("com.alibaba.nacos.naming.push.retransmitter");
          return t;
        }
      });

  /**
   * 专门负责推送数据给客户端的线程池
   */
  private static ScheduledExecutorService udpSender = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.setName("com.alibaba.nacos.naming.push.udpSender");
      return t;
    }
  });

  static {
    try {
      udpSocket = new DatagramSocket();

      Receiver receiver = new Receiver();

      // 专门用于跑 ACK 接收任务的线程
      Thread inThread = new Thread(receiver);
      inThread.setDaemon(true);
      inThread.setName("com.alibaba.nacos.naming.push.receiver");
      inThread.start();

      // 周期性的扫描本地维护的推送客户端是否还活着, 挂了的移除之
      executorService.scheduleWithFixedDelay(new Runnable() {
        @Override
        public void run() {
          try {
            removeClientIfZombie();
          } catch (Throwable e) {
            Loggers.PUSH.warn("[NACOS-PUSH] failed to remove client zombie");
          }
        }
      }, 0, 20, TimeUnit.SECONDS);

    } catch (SocketException e) {
      Loggers.SRV_LOG.error("[NACOS-PUSH] failed to init push service");
    }
  }

  @Override
  public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
    this.applicationContext = applicationContext;
  }

  /**
   * 其它 nacos 节点检测到服务变更时会发布服务变更事件, 当前 nacos 节点通过该接口进行接收, 然后推送给订阅自己的客户端.
   */
  @Override
  public void onApplicationEvent(ServiceChangeEvent event) {
    // 解析服务变更事件
    Service service = event.getService();
    String serviceName = service.getName();
    String namespaceId = service.getNamespaceId();

    // 然后推送给订阅自己的客户端
    Future future = udpSender.schedule(new Runnable() {
      @Override
      public void run() {
        try {
          Loggers.PUSH.info(serviceName + " is changed, add it to push queue.");
          // 取出服务对应的全部推送客户端
          ConcurrentMap<String, PushClient> clients = clientMap
              .get(UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));
          if (MapUtils.isEmpty(clients)) {
            return;
          }

          Map<String, Object> cache = new HashMap<>(16);
          long lastRefTime = System.nanoTime();
          for (PushClient client : clients.values()) {
            if (client.zombie()) {
              Loggers.PUSH.debug("client is zombie: " + client.toString());
              clients.remove(client.toString());
              Loggers.PUSH.debug("client is zombie: " + client.toString());
              continue;
            }

            Receiver.AckEntry ackEntry;
            Loggers.PUSH.debug("push serviceName: {} to client: {}", serviceName, client.toString());
            String key = getPushCacheKey(serviceName, client.getIp(), client.getAgent());
            byte[] compressData = null;
            Map<String, Object> data = null;
            if (switchDomain.getDefaultPushCacheMillis() >= 20000 && cache.containsKey(key)) {
              org.javatuples.Pair pair = (org.javatuples.Pair) cache.get(key);
              compressData = (byte[]) (pair.getValue0());
              data = (Map<String, Object>) pair.getValue1();

              Loggers.PUSH.debug("[PUSH-CACHE] cache hit: {}:{}", serviceName, client.getAddrStr());
            }

            if (compressData != null) {
              ackEntry = prepareAckEntry(client, compressData, data, lastRefTime);
            } else {
              ackEntry = prepareAckEntry(client, prepareHostsData(client), lastRefTime);
              // 先放入缓存再发送, 如果先发送后放到缓存, 可能放入前响应就回来了
              if (ackEntry != null) {
                cache.put(key, new org.javatuples.Pair<>(ackEntry.origin.getData(), ackEntry.data));
              }
            }

            Loggers.PUSH.info("serviceName: {} changed, schedule push for: {}, agent: {}, key: {}",
                client.getServiceName(), client.getAddrStr(), client.getAgent(),
                (ackEntry == null ? null : ackEntry.key));

            udpPush(ackEntry);
          }
        } catch (Exception e) {
          Loggers.PUSH.error("[NACOS-PUSH] failed to push serviceName: {} to client, error: {}", serviceName, e);

        } finally {
          futureMap.remove(UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));
        }

      }
    }, 1000, TimeUnit.MILLISECONDS);

    futureMap.put(UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName), future);

  }

  public int getTotalPush() {
    return totalPush;
  }

  public void setTotalPush(int totalPush) {
    PushService.totalPush = totalPush;
  }

  /**
   * 根据客户端信息生成对应的推送客户端, 并维护到其对应的服务所对应的客户端列表中(后续服务变更时, 会向其推送消息).
   * 
   * @param namespaceId
   * @param serviceName
   * @param clusters
   * @param agent
   * @param socketAddr  客户端地址
   * @param dataSource
   * @param tenant
   * @param app
   */
  public void addClient(String namespaceId, String serviceName, String clusters, String agent,
      InetSocketAddress socketAddr, DataSource dataSource, String tenant, String app) {

    PushClient client = new PushClient(namespaceId, serviceName, clusters, agent, socketAddr, dataSource, tenant, app);
    addClient(client);
  }

  /**
   * 一个服务对应一组推送客户端, 当服务发生变更时会将相关信息推送到这些客户端.
   * 
   * @param client
   */
  public static void addClient(PushClient client) {
    // client is stored by key 'serviceName' because notify event is driven by
    // serviceName change
    String serviceKey = UtilsAndCommons.assembleFullServiceName(client.getNamespaceId(), client.getServiceName());
    ConcurrentMap<String, PushClient> clients = clientMap.get(serviceKey);
    if (clients == null) {
      clientMap.putIfAbsent(serviceKey, new ConcurrentHashMap<String, PushClient>(1024));
      clients = clientMap.get(serviceKey);
    }

    // 如果该客户端之前生成过推送客户端, 则刷新之(避免被判为 zombie)
    PushClient oldClient = clients.get(client.toString());
    if (oldClient != null) {
      oldClient.refresh();
    } else {
      // 否则将其添加到列表中(下面代码可能并发执行, 以第一个 put 为准)
      PushClient res = clients.putIfAbsent(client.toString(), client);
      if (res != null) {
        Loggers.PUSH.warn("client: {} already associated with key {}", res.getAddrStr(), res.toString());
      }
      Loggers.PUSH.debug("client: {} added for serviceName: {}", client.getAddrStr(), client.getServiceName());
    }
  }

  public List<Subscriber> getClients(String serviceName, String namespaceId) {
    String serviceKey = UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName);
    ConcurrentMap<String, PushClient> clientConcurrentMap = clientMap.get(serviceKey);
    if (Objects.isNull(clientConcurrentMap)) {
      return null;
    }
    List<Subscriber> clients = new ArrayList<Subscriber>();
    clientConcurrentMap.forEach((key, client) -> {
      clients.add(new Subscriber(client.getAddrStr(), client.getAgent(), client.getApp(), client.getIp(), namespaceId,
          serviceName));
    });
    return clients;
  }

  public static void removeClientIfZombie() {

    int size = 0;
    for (Map.Entry<String, ConcurrentMap<String, PushClient>> entry : clientMap.entrySet()) {
      ConcurrentMap<String, PushClient> clientConcurrentMap = entry.getValue();
      for (Map.Entry<String, PushClient> entry1 : clientConcurrentMap.entrySet()) {
        PushClient client = entry1.getValue();
        if (client.zombie()) {
          clientConcurrentMap.remove(entry1.getKey());
        }
      }

      size += clientConcurrentMap.size();
    }

    if (Loggers.PUSH.isDebugEnabled()) {
      Loggers.PUSH.debug("[NACOS-PUSH] clientMap size: {}", size);
    }

  }

  private static Receiver.AckEntry prepareAckEntry(PushClient client, byte[] dataBytes, Map<String, Object> data,
      long lastRefTime) {
    String key = getACKKey(client.getSocketAddr().getAddress().getHostAddress(), client.getSocketAddr().getPort(),
        lastRefTime);
    DatagramPacket packet = null;
    try {
      packet = new DatagramPacket(dataBytes, dataBytes.length, client.socketAddr);
      Receiver.AckEntry ackEntry = new Receiver.AckEntry(key, packet);
      ackEntry.data = data;

      // we must store the key be fore send, otherwise there will be a chance the
      // ack returns before we put in
      ackEntry.data = data;

      return ackEntry;
    } catch (Exception e) {
      Loggers.PUSH.error("[NACOS-PUSH] failed to prepare data: {} to client: {}, error: {}", data,
          client.getSocketAddr(), e);
    }

    return null;
  }

  public static String getPushCacheKey(String serviceName, String clientIP, String agent) {
    return serviceName + UtilsAndCommons.CACHE_KEY_SPLITER + agent;
  }

  /**
   * 用于当前 nacos 节点向其它 nacos 节点推送服务变更事件.
   * 
   * @param service
   */
  public void serviceChanged(Service service) {
    // merge some change events to reduce the push frequency:
    if (futureMap.containsKey(UtilsAndCommons.assembleFullServiceName(service.getNamespaceId(), service.getName()))) {
      return;
    }

    this.applicationContext.publishEvent(new ServiceChangeEvent(this, service));
  }

  /**
   * 确认当前 nacos 节点是否开启推送模式, 以及根据 UA 判断客户端是否支持推送
   */
  public boolean canEnablePush(String agent) {

    // 确认当前 nacos 节点是否开启了推送模式
    if (!switchDomain.isPushEnabled()) {
      return false;
    }

    // 根据客户端 UA 构造客户端信息
    ClientInfo clientInfo = new ClientInfo(agent);

    // 如果客户端语言为 Java, 并且客户端版本大于 0.1.0 则可以支持推送
    if (ClientInfo.ClientType.JAVA == clientInfo.type
        && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushJavaVersion())) >= 0) {
      return true;
    } else if (ClientInfo.ClientType.DNS == clientInfo.type
        && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushPythonVersion())) >= 0) {
      return true;
    } else if (ClientInfo.ClientType.C == clientInfo.type
        && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushCVersion())) >= 0) {
      return true;
    } else if (ClientInfo.ClientType.GO == clientInfo.type
        && clientInfo.version.compareTo(VersionUtil.parseVersion(switchDomain.getPushGoVersion())) >= 0) {
      return true;
    }

    return false;
  }

  public static List<Receiver.AckEntry> getFailedPushes() {
    return new ArrayList<Receiver.AckEntry>(ackMap.values());
  }

  public int getFailedPushCount() {
    return ackMap.size() + failedPush;
  }

  public void setFailedPush(int failedPush) {
    PushService.failedPush = failedPush;
  }

  public static void resetPushState() {
    ackMap.clear();
  }

  /**
   * PushClient 代表一个服务发现客户端.
   */
  public class PushClient {
    private String namespaceId;
    private String serviceName;
    private String clusters;
    private String agent;
    private String tenant;
    private String app;
    private InetSocketAddress socketAddr;
    private DataSource dataSource;
    private Map<String, String[]> params;

    public Map<String, String[]> getParams() {
      return params;
    }

    public void setParams(Map<String, String[]> params) {
      this.params = params;
    }

    /**
     * 客户端上次刷新时间(ref 即 refresh 缩写)
     */
    public long lastRefTime = System.currentTimeMillis();

    /**
     * 用客户端传来的 ip 和 port 等信息构造对应的推送客户端.
     * 
     * @param namespaceId
     * @param serviceName
     * @param clusters
     * @param agent
     * @param socketAddr
     * @param dataSource
     * @param tenant
     * @param app
     */
    public PushClient(String namespaceId, String serviceName, String clusters, String agent,
        InetSocketAddress socketAddr, DataSource dataSource, String tenant, String app) {
      this.namespaceId = namespaceId;
      this.serviceName = serviceName;
      this.clusters = clusters;
      this.agent = agent;
      this.socketAddr = socketAddr;
      this.dataSource = dataSource;
      this.tenant = tenant;
      this.app = app;
    }

    public DataSource getDataSource() {
      return dataSource;
    }

    public PushClient(InetSocketAddress socketAddr) {
      this.socketAddr = socketAddr;
    }

    public boolean zombie() {
      return System.currentTimeMillis() - lastRefTime > switchDomain.getPushCacheMillis(serviceName);
    }

    @Override
    public String toString() {
      return "serviceName: " + serviceName + ", clusters: " + clusters + ", ip: "
          + socketAddr.getAddress().getHostAddress() + ", port: " + socketAddr.getPort() + ", agent: " + agent;
    }

    public String getAgent() {
      return agent;
    }

    public String getAddrStr() {
      return socketAddr.getAddress().getHostAddress() + ":" + socketAddr.getPort();
    }

    public String getIp() {
      return socketAddr.getAddress().getHostAddress();
    }

    @Override
    public int hashCode() {
      return Objects.hash(serviceName, clusters, socketAddr);
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof PushClient)) {
        return false;
      }

      PushClient other = (PushClient) obj;

      return serviceName.equals(other.serviceName) && clusters.equals(other.clusters)
          && socketAddr.equals(other.socketAddr);
    }

    public String getClusters() {
      return clusters;
    }

    public void setClusters(String clusters) {
      this.clusters = clusters;
    }

    public String getNamespaceId() {
      return namespaceId;
    }

    public void setNamespaceId(String namespaceId) {
      this.namespaceId = namespaceId;
    }

    public String getServiceName() {
      return serviceName;
    }

    public void setServiceName(String serviceName) {
      this.serviceName = serviceName;
    }

    public String getTenant() {
      return tenant;
    }

    public void setTenant(String tenant) {
      this.tenant = tenant;
    }

    public String getApp() {
      return app;
    }

    public void setApp(String app) {
      this.app = app;
    }

    public InetSocketAddress getSocketAddr() {
      return socketAddr;
    }

    public void refresh() {
      lastRefTime = System.currentTimeMillis();
    }
  }

  /**
   * 针对超过 1KB 的数据会进行压缩.
   * 
   * @param dataBytes
   * @return
   * @throws IOException
   */
  private static byte[] compressIfNecessary(byte[] dataBytes) throws IOException {
    // enable compression when data is larger than 1KB
    int maxDataSizeUncompress = 1024;
    if (dataBytes.length < maxDataSizeUncompress) {
      return dataBytes;
    }

    ByteArrayOutputStream out = new ByteArrayOutputStream();
    GZIPOutputStream gzip = new GZIPOutputStream(out);
    gzip.write(dataBytes);
    gzip.close();

    return out.toByteArray();
  }

  /**
   * 从数据源获取 client 订阅的信息(此处是其订阅的服务的实例列表), 组装推送内容.
   * 
   * @param client
   * @return
   * @throws Exception
   */
  private static Map<String, Object> prepareHostsData(PushClient client) throws Exception {
    Map<String, Object> cmd = new HashMap<String, Object>(2);
    cmd.put("type", "dom");
    // 获取 client 订阅的服务对应的实例列表
    cmd.put("data", client.getDataSource().getData(client));

    return cmd;
  }

  private static Receiver.AckEntry prepareAckEntry(PushClient client, Map<String, Object> data, long lastRefTime) {
    if (MapUtils.isEmpty(data)) {
      Loggers.PUSH.error("[NACOS-PUSH] pushing empty data for client is not allowed: {}", client);
      return null;
    }

    data.put("lastRefTime", lastRefTime);

    // we apply lastRefTime as sequence num for further ack
    // 把最近刷新时间作为 ack 的序列号
    String key = getACKKey(client.getSocketAddr().getAddress().getHostAddress(), client.getSocketAddr().getPort(),
        lastRefTime);

    String dataStr = JSON.toJSONString(data);

    try {
      byte[] dataBytes = dataStr.getBytes(StandardCharsets.UTF_8);
      dataBytes = compressIfNecessary(dataBytes);

      // 把从数据源获取的数据组装为 UDP 数据包
      DatagramPacket packet = new DatagramPacket(dataBytes, dataBytes.length, client.socketAddr);

      // we must store the key be fore send, otherwise there will be a chance the
      // ack returns before we put in
      // 发送前就要将 UDP 数据包存储下来, 否则先发送后存储可能来不及存储, 响应就回来了.
      Receiver.AckEntry ackEntry = new Receiver.AckEntry(key, packet);
      ackEntry.data = data;

      return ackEntry;
    } catch (Exception e) {
      Loggers.PUSH.error("[NACOS-PUSH] failed to prepare data: {} to client: {}, error: {}", data,
          client.getSocketAddr(), e);
      return null;
    }
  }

  /**
   * 通过 UDP 发送数据, 并做一些统计工作.
   * 
   * @param ackEntry
   * @return
   */
  private static Receiver.AckEntry udpPush(Receiver.AckEntry ackEntry) {
    if (ackEntry == null) {
      Loggers.PUSH.error("[NACOS-PUSH] ackEntry is null.");
      return null;
    }

    // 如果重试次数超过上限
    if (ackEntry.getRetryTimes() > MAX_RETRY_TIMES) {
      Loggers.PUSH.warn("max re-push times reached, retry times {}, key: {}", ackEntry.retryTimes, ackEntry.key);
      ackMap.remove(ackEntry.key);
      udpSendTimeMap.remove(ackEntry.key);
      // 推送失败计数
      failedPush += 1;
      return ackEntry;
    }

    try {
      // 如果不在 adcMap 中, 表示本次为第一次推送
      if (!ackMap.containsKey(ackEntry.key)) {
        totalPush++;
      }
      ackMap.put(ackEntry.key, ackEntry);
      udpSendTimeMap.put(ackEntry.key, System.currentTimeMillis());

      Loggers.PUSH.info("send udp packet: " + ackEntry.key);
      // 推送给客户端
      udpSocket.send(ackEntry.origin);

      // 重试计数
      ackEntry.increaseRetryTime();

      // 为防止推送失败, 新起一个任务, 一段时间后重新尝试推送
      executorService.schedule(new Retransmitter(ackEntry), TimeUnit.NANOSECONDS.toMillis(ACK_TIMEOUT_NANOS),
          TimeUnit.MILLISECONDS);

      return ackEntry;
    } catch (Exception e) {
      Loggers.PUSH.error("[NACOS-PUSH] failed to push data: {} to client: {}, error: {}", ackEntry.data,
          ackEntry.origin.getAddress().getHostAddress(), e);
      ackMap.remove(ackEntry.key);
      udpSendTimeMap.remove(ackEntry.key);
      failedPush += 1;

      return null;
    }
  }

  private static String getACKKey(String host, int port, long lastRefTime) {
    return StringUtils.strip(host) + "," + port + "," + lastRefTime;
  }

  /**
   * 一个 one-shot 类型的任务, 负责重新尝试推送某些数据给客户端.
   */
  public static class Retransmitter implements Runnable {
    Receiver.AckEntry ackEntry;

    /**
     * 重新尝试推送数据给客户端.
     * 
     * @param ackEntry 待推送数据
     */
    public Retransmitter(Receiver.AckEntry ackEntry) {
      this.ackEntry = ackEntry;
    }

    @Override
    public void run() {
      if (ackMap.containsKey(ackEntry.key)) {
        Loggers.PUSH.info("retry to push data, key: " + ackEntry.key);
        // 推送数据
        udpPush(ackEntry);
      }
    }
  }

  /**
   * 负责接收客户端的 ACK 
   */
  public static class Receiver implements Runnable {
    @Override
    public void run() {
      while (true) {
        byte[] buffer = new byte[1024 * 64];
        DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

        try {
          // 接收 ack
          udpSocket.receive(packet);

          String json = new String(packet.getData(), 0, packet.getLength(), Charset.forName("UTF-8")).trim();
          AckPacket ackPacket = JSON.parseObject(json, AckPacket.class);

          InetSocketAddress socketAddress = (InetSocketAddress) packet.getSocketAddress();
          String ip = socketAddress.getAddress().getHostAddress();
          int port = socketAddress.getPort();

          if (System.nanoTime() - ackPacket.lastRefTime > ACK_TIMEOUT_NANOS) {
            Loggers.PUSH.warn("ack takes too long from {} ack json: {}", packet.getSocketAddress(), json);
          }

          String ackKey = getACKKey(ip, port, ackPacket.lastRefTime);
          // 收到 ack 后将对应客户端的待推送数据从内存移除(已经推送成功了), 避免重复被推送.
          AckEntry ackEntry = ackMap.remove(ackKey);
          if (ackEntry == null) {
            throw new IllegalStateException("unable to find ackEntry for key: " + ackKey + ", ack json: " + json);
          }

          // 计算推送耗时
          long pushCost = System.currentTimeMillis() - udpSendTimeMap.get(ackKey);

          Loggers.PUSH.info("received ack: {} from: {}:, cost: {} ms, unacked: {}, total push: {}", json, ip, port,
              pushCost, ackMap.size(), totalPush);

          pushCostMap.put(ackKey, pushCost);

          udpSendTimeMap.remove(ackKey);

        } catch (Throwable e) {
          Loggers.PUSH.error("[NACOS-PUSH] error while receiving ack data", e);
        }
      }
    }

    public static class AckEntry {

      public AckEntry(String key, DatagramPacket packet) {
        this.key = key;
        this.origin = packet;
      }

      public void increaseRetryTime() {
        retryTimes.incrementAndGet();
      }

      public int getRetryTimes() {
        return retryTimes.get();
      }

      public String key;
      public DatagramPacket origin;
      private AtomicInteger retryTimes = new AtomicInteger(0);
      public Map<String, Object> data;
    }

    public static class AckPacket {
      public String type;
      public long lastRefTime;

      public String data;
    }
  }

}
