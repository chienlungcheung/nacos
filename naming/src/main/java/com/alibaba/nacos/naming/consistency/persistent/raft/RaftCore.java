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
package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.consistency.ApplyAction;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.monitor.MetricsMonitor;
import com.alibaba.nacos.naming.pojo.Record;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.javatuples.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.zip.GZIPOutputStream;

import static com.alibaba.nacos.core.utils.SystemUtils.STANDALONE_MODE;

/**
 * RaftCore 实现了 raft 描述的 leader 选举、log 复制等具体行为方法。
 * <p>
 * 其成员 RaftPeerSet 保存着当前 raft 集群最新的节点集合，每个节点维护者类似 raft 描述的各个状态。
 * 
 * @author nacos
 */
@Component
public class RaftCore {

  /**
   * 投票 RPC
   */
  public static final String API_VOTE = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/vote";

  /**
   * 心跳 RPC
   */
  public static final String API_BEAT = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/beat";

  /**
   * AppendEntries RPC(append)
   */
  public static final String API_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

  /**
   * AppendEntries RPC(delete)
   */
  public static final String API_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

  /**
   * read RPC
   */
  public static final String API_GET = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum";

  public static final String API_ON_PUB = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

  public static final String API_ON_DEL = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/datum/commit";

  public static final String API_GET_PEER = UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft/peer";

  /**
   * 专门用来运行 notifier
   */
  private ScheduledExecutorService executor = new ScheduledThreadPoolExecutor(1, new ThreadFactory() {
    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);

      t.setDaemon(true);
      t.setName("com.alibaba.nacos.naming.raft.notifier");

      return t;
    }
  });

  public static final Lock OPERATE_LOCK = new ReentrantLock();

  public static final int PUBLISH_TERM_INCREASE_COUNT = 100;

  /**
   * raft 集群中每个数据可以对应多个监听器
   */
  private volatile Map<String, List<RecordListener>> listeners = new ConcurrentHashMap<>();

  /**
   * 保存 raft 集群全部的数据
   */
  private volatile ConcurrentMap<String, Datum> datums = new ConcurrentHashMap<>();

  @Autowired
  private RaftPeerSet peers;

  @Autowired
  private SwitchDomain switchDomain;

  @Autowired
  private GlobalConfig globalConfig;

  @Autowired
  private RaftProxy raftProxy;

  @Autowired
  private RaftStore raftStore;

  public volatile Notifier notifier = new Notifier();

  private boolean initialized = false;

  /**
   * init 负责在当前 raft 节点启动时做初始化工作，包括：
   * <li>读取持久化存储中的日志数据到内存（nacos 的使用方式，即作为服务发现和配置管理中心，确保了数据量不会很大导致爆内存）
   * <li>读取持久化存储中的节点状态信息（若有）来初始化当前 raft 节点（目前仅 term），如果是第一次加入集群则会将 term 设置为 0
   * 
   * @throws Exception
   */
  @PostConstruct
  public void init() throws Exception {

    Loggers.RAFT.info("initializing Raft sub-system");

    // 激活通知器，当数据被删除或者更新时会去调用这些数据对应的监听器
    executor.submit(notifier);

    long start = System.currentTimeMillis();

    // 加载 data 目录文件内容到内存并设置通知器
    raftStore.loadDatums(notifier, datums);

    // 加载元文件内容，并设置当前 term，如果为空则默认为 0.
    setTerm(NumberUtils.toLong(raftStore.loadMeta().getProperty("term"), 0L));

    Loggers.RAFT.info("cache loaded, datum count: {}, current term: {}", datums.size(), peers.getTerm());

    // 等待 notifier 处理完全部通知任务
    while (true) {
      if (notifier.tasks.size() <= 0) {
        break;
      }
      Thread.sleep(1000L);
    }

    // 初始化完成
    initialized = true;

    Loggers.RAFT.info("finish to load data from disk, cost: {} ms.", (System.currentTimeMillis() - start));

    // 注册选举任务和心跳任务
    GlobalExecutor.registerMasterElection(new MasterElection());
    GlobalExecutor.registerHeartbeat(new HeartBeat());

    Loggers.RAFT.info("timer started: leader timeout ms: {}, heart-beat timeout ms: {}",
        GlobalExecutor.LEADER_TIMEOUT_MS, GlobalExecutor.HEARTBEAT_INTERVAL_MS);
  }

  public Map<String, List<RecordListener>> getListeners() {
    return listeners;
  }

  /**
   * 将 {@code <key, value>} 发布到集群
   *
   * @param key
   * @param value
   * @throws Exception
   */
  public void signalPublish(String key, Record value) throws Exception {

    // 如果当前节点非 leader，则组装好数据后借助 raftProxy 将写请求发送给 leader，然后由 leader 发布到整个集群。
    // 这里跟 raft 论文有出入，原文是这样的：
    // Clients of Raft send all of their requests to the leader.
    // When a client first starts up, it connects to a randomly-
    // chosen server. If the client’s first choice is not the leader,
    // that server will reject the client’s request and supply in-
    // formation about the most recent leader it has heard from
    // (AppendEntries requests include the network address of
    // the leader). If the leader crashes, client requests will time
    // out; clients then try again with randomly-chosenservers.
    if (!isLeader()) {
      JSONObject params = new JSONObject();
      params.put("key", key);
      params.put("value", value);
      Map<String, String> parameters = new HashMap<>(1);
      parameters.put("key", key);

      // 获取当前 leader 地址，然后组装 POST 请求发送给它
      raftProxy.proxyPostLarge(getLeader().ip, API_PUB, params.toJSONString(), parameters);
      return;
    }

    try {
      // 确保写操作串行进行
      OPERATE_LOCK.lock();
      long start = System.currentTimeMillis();
      final Datum datum = new Datum();
      datum.key = key;
      datum.value = value;
      if (getDatum(key) == null) {
        datum.timestamp.set(1L);
      } else {
        // 如果 key 已经存在于集群中了，则在原基础上递增时间戳，用以标识数据更新
        datum.timestamp.set(getDatum(key).timestamp.incrementAndGet());
      }

      JSONObject json = new JSONObject();
      json.put("datum", datum);
      json.put("source", peers.local());

      // 先写到本地节点（此时本地节点即 leader）
      onPublish(datum, peers.local());

      final String content = JSON.toJSONString(json);

      // 写到其它节点，总写入个数要占到集群大多数
      final CountDownLatch latch = new CountDownLatch(peers.majorityCount());
      // 遍历集群全部节点
      for (final String server : peers.allServersIncludeMyself()) {
        // 如果遇到 leader 节点，则不用再写入了；因为进入循环之前已经调用 onPublish 写入过 leader 了。
        if (isLeader(server)) {
          latch.countDown();
          continue;
        }
        // 注意这里用的 API 接口与上面向 leader 发送的不同
        final String url = buildURL(server, API_ON_PUB);
        // 发送异步的 POST 请求到相应节点
        HttpClient.asyncHttpPostLarge(url, Arrays.asList("key=" + key), content, new AsyncCompletionHandler<Integer>() {
          @Override
          public Integer onCompleted(Response response) throws Exception {
            // 如果写入节点失败，则不计数
            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
              Loggers.RAFT.warn("[RAFT] failed to publish data to peer, datumId={}, peer={}, http code={}", datum.key,
                  server, response.getStatusCode());
              return 1;
            }
            latch.countDown();
            return 0;
          }

          @Override
          public STATE onContentWriteCompleted() {
            return STATE.CONTINUE;
          }
        });

      }

      // 如果超时且没有实现大多数节点写入成功，则本次发布失败
      if (!latch.await(UtilsAndCommons.RAFT_PUBLISH_TIMEOUT, TimeUnit.MILLISECONDS)) {
        // only majority servers return success can we consider this update success
        Loggers.RAFT.error("data publish failed, caused failed to notify majority, key={}", key);
        throw new IllegalStateException("data publish failed, caused failed to notify majority, key=" + key);
      }

      long end = System.currentTimeMillis();
      Loggers.RAFT.info("signalPublish cost {} ms, key: {}", (end - start), key);
    } finally {
      // 发布完毕，释放锁
      OPERATE_LOCK.unlock();
    }
  }

  /**
   * 从集群删除 {@code key} 对应的数据。
   *
   * @param key
   * @throws Exception
   */
  public void signalDelete(final String key) throws Exception {

    // 更改之前先获取锁
    OPERATE_LOCK.lock();
    try {

      // 如果接收到删除操作的本地节点非 leader，则将删除操作借助 raftProxy 路由给 leader
      if (!isLeader()) {
        Map<String, String> params = new HashMap<>(1);
        params.put("key", URLEncoder.encode(key, "UTF-8"));
        raftProxy.proxy(getLeader().ip, API_DEL, params, HttpMethod.DELETE);
        return;
      }

      JSONObject json = new JSONObject();
      // construct datum:
      Datum datum = new Datum();
      datum.key = key;
      json.put("datum", datum);
      json.put("source", peers.local());

      // 从本地节点删除该数据
      onDelete(datum.key, peers.local());

      // 从其它 follower 节点删除该数据。todo 删除与发布不同，不需要检查大多数节点成功？
      for (final String server : peers.allServersWithoutMySelf()) {
        String url = buildURL(server, API_ON_DEL);
        HttpClient.asyncHttpDeleteLarge(url, null, JSON.toJSONString(json), new AsyncCompletionHandler<Integer>() {
          @Override
          public Integer onCompleted(Response response) throws Exception {
            if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
              Loggers.RAFT.warn("[RAFT] failed to delete data from peer, datumId={}, peer={}, http code={}", key,
                  server, response.getStatusCode());
              return 1;
            }

            // 如果某个 follower 节点删除成功，则重置本地节点（即 leader）的选举超时；
            // 否则说明有情况，可能网络或者 follower 出问题了，可能要重新进行选举。
            RaftPeer local = peers.local();

            local.resetLeaderDue();

            return 0;
          }
        });
      }
    } finally {
      // 删除完毕，释放锁
      OPERATE_LOCK.unlock();
    }
  }

  /**
   * 将 datum 更新到本地节点，更新完毕后会调用 Notifier 发变更通知。
   *
   * @param datum  待发布数据
   * @param source 集群当前 leader
   * @throws Exception
   */
  public void onPublish(Datum datum, RaftPeer source) throws Exception {
    RaftPeer local = peers.local();
    if (datum.value == null) {
      Loggers.RAFT.warn("received empty datum");
      throw new IllegalStateException("received empty datum");
    }

    // source 必须是 leader，只有 leader 才有资格将数据发布到集群
    if (!peers.isLeader(source.ip)) {
      Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}", JSON.toJSONString(source),
          JSON.toJSONString(getLeader()));
      throw new IllegalStateException("peer(" + source.ip + ") tried to publish " + "data but wasn't leader");
    }

    // 如果 leader 的任期小于本地节点的任期，则说明在这之前应该又发生了 leader 选举，本次数据发布失败
    if (source.term.get() < local.term.get()) {
      Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JSON.toJSONString(source),
          JSON.toJSONString(local));
      throw new IllegalStateException(
          "out of date publish, pub-term:" + source.term.get() + ", cur-term: " + local.term.get());
    }

    // 重置本地节点的选举超时
    local.resetLeaderDue();

    // 如果本次要写入的数据不是临时的而是要持久化的，则写入磁盘。
    // if data should be persistent, usually this is always true:
    if (KeyBuilder.matchPersistentKey(datum.key)) {
      raftStore.write(datum);
    }

    // 数据临时存放地（内存中）
    datums.put(datum.key, datum);

    // 如果本地节点为集群当前 leader，则增加其对应的任期值
    if (isLeader()) {
      local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
    } else {
      // 本地节点不是 leader
      //
      // 如果本地节点任期增长后大于 source 的当前任期值
      if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
        // set leader term:
        getLeader().term.set(source.term.get());
        local.term.set(getLeader().term.get());
      } else {
        // 否则直接递增本地节点任期值
        local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
      }
    }
    // 将本地节点新的任期值持久化到磁盘中的 meta.properties 文件中
    raftStore.updateTerm(local.term.get());

    notifier.addTask(datum.key, ApplyAction.CHANGE);

    Loggers.RAFT.info("data added/updated, key={}, term={}", datum.key, local.term);
  }

  /**
   * 将 {@code datumKey} 从本地节点删除，更新完毕后会通过 {@code deleteDatum()} 调用 Notifier 发删除通知。
   *
   * @param datumKey
   * @param source
   * @throws Exception
   */
  public void onDelete(String datumKey, RaftPeer source) throws Exception {

    RaftPeer local = peers.local();

    // 发起删除操作的 source 必须是 leader
    if (!peers.isLeader(source.ip)) {
      Loggers.RAFT.warn("peer {} tried to publish data but wasn't leader, leader: {}", JSON.toJSONString(source),
          JSON.toJSONString(getLeader()));
      throw new IllegalStateException("peer(" + source.ip + ") tried to publish data but wasn't leader");
    }

    // source 作为 leader，term 不能小于本地节点
    if (source.term.get() < local.term.get()) {
      Loggers.RAFT.warn("out of date publish, pub-term: {}, cur-term: {}", JSON.toJSONString(source),
          JSON.toJSONString(local));
      throw new IllegalStateException("out of date publish, pub-term:" + source.term + ", cur-term: " + local.term);
    }

    // 本地节点重置本地选举超时，避免删除期间发起选举
    local.resetLeaderDue();

    // 从本地节点删除 key
    // do apply
    String key = datumKey;
    deleteDatum(key);

    // 如果是 com.alibaba.nacos.naming.domains.meta. 类的 key，则删除完毕要更新本地 term，更新后的 term
    // 不许超过 leader
    if (KeyBuilder.matchServiceMetaKey(key)) {

      if (local.term.get() + PUBLISH_TERM_INCREASE_COUNT > source.term.get()) {
        // set leader term:
        getLeader().term.set(source.term.get());
        local.term.set(getLeader().term.get());
      } else {
        local.term.addAndGet(PUBLISH_TERM_INCREASE_COUNT);
      }

      raftStore.updateTerm(local.term.get());
    }

    Loggers.RAFT.info("data removed, key={}, term={}", datumKey, local.term);

  }

  public class MasterElection implements Runnable {
    @Override
    public void run() {
      try {

        // 集群节点还未就绪，不进行选举
        if (!peers.isReady()) {
          return;
        }

        // 仅当本地节点的 leaderDueMs 不大于 0 时才参与投票
        RaftPeer local = peers.local();
        local.leaderDueMs -= GlobalExecutor.TICK_PERIOD_MS;

        if (local.leaderDueMs > 0) {
          return;
        }

        // 重置 leaderDue 和 heartbeatDue
        // reset timeout
        local.resetLeaderDue();
        local.resetHeartbeatDue();

        // 发起选举
        sendVote();
      } catch (Exception e) {
        Loggers.RAFT.warn("[RAFT] error while master election {}", e);
      }

    }

    /**
     * 发起选举
     */
    public void sendVote() {

      RaftPeer local = peers.get(NetUtils.localServer());
      Loggers.RAFT.info("leader timeout, start voting,leader: {}, term: {}", JSON.toJSONString(getLeader()),
          local.term);

      peers.reset();

      // 本地节点发起选举：
      // - 递增任期
      // - 投自己一票
      // - 进入候选人状态
      // - 向其它全部节点发起 RequestVote RPC
      local.term.incrementAndGet();
      local.voteFor = local.ip;
      local.state = RaftPeer.State.CANDIDATE;

      // 构造选举参数，参数就是发起选举的节点；其它节点收到请求后会调用 receivedVote 方法进行解析。
      Map<String, String> params = new HashMap<>(1);
      params.put("vote", JSON.toJSONString(local));
      for (final String server : peers.allServersWithoutMySelf()) {
        final String url = buildURL(server, API_VOTE);
        try {
          HttpClient.asyncHttpPost(url, null, params, new AsyncCompletionHandler<Integer>() {
            @Override
            public Integer onCompleted(Response response) throws Exception {
              if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                Loggers.RAFT.error("NACOS-RAFT vote failed: {}, url: {}", response.getResponseBody(), url);
                return 1;
              }

              RaftPeer peer = JSON.parseObject(response.getResponseBody(), RaftPeer.class);

              Loggers.RAFT.info("received approve from peer: {}", JSON.toJSONString(peer));

              // 收到一票就去检查是否能决出 leader
              peers.decideLeader(peer);

              return 0;
            }
          });
        } catch (Exception e) {
          Loggers.RAFT.warn("error while sending vote to server: {}", server);
        }
      }
    }
  }

  /**
   * 接收到 {@code remote} 发起选举请求，本地节点判断把票投给谁，返回值即为投票对象。
   * <p>
   * 相比 raft 算法定义，这里的处理非常简单，只是校验了 term，并没有考虑 candidate（即这里的 remote 参数）数据是否不比自己更老。
   *
   * @param remote
   * @return
   */
  public RaftPeer receivedVote(RaftPeer remote) {
    if (!peers.contains(remote)) {
      throw new IllegalStateException("can not find peer: " + remote.ip);
    }

    RaftPeer local = peers.get(NetUtils.localServer());
    // 如果发起选举的节点对应的任期不大于本地节点的任期，则本地节点把票投给自己，而不是投给发起选举的节点。
    if (remote.term.get() <= local.term.get()) {
      String msg = "received illegitimate vote" + ", voter-term:" + remote.term + ", votee-term:" + local.term;

      Loggers.RAFT.info(msg);
      if (StringUtils.isEmpty(local.voteFor)) {
        local.voteFor = local.ip;
      }

      return local;
    }

    // 确定投票给发起人，则
    // - 重置本地节点的选举超时
    // - 将当前节点状态置为 FOLLOWER
    // - 记录将票投给了谁
    // - 将本地任期改为投票对象的任期
    local.resetLeaderDue();

    local.state = RaftPeer.State.FOLLOWER;
    local.voteFor = remote.ip;
    local.term.set(remote.term.get());

    Loggers.RAFT.info("vote {} as leader, term: {}", remote.ip, remote.term);

    return local;
  }

  public class HeartBeat implements Runnable {
    @Override
    public void run() {
      try {

        if (!peers.isReady()) {
          return;
        }

        // 仅当本地节点的心跳计时器超时后才发送心跳
        RaftPeer local = peers.local();
        local.heartbeatDueMs -= GlobalExecutor.TICK_PERIOD_MS;
        if (local.heartbeatDueMs > 0) {
          return;
        }

        // 重设心跳超时
        local.resetHeartbeatDue();

        // 发送心跳
        sendBeat();
      } catch (Exception e) {
        Loggers.RAFT.warn("[RAFT] error while sending beat {}", e);
      }

    }

    /**
     * 用于 leader 给集群其它全部节点发送心跳；针对返回响应的节点，leader 会用响应更新对应节点在本地维护的数据。
     *
     * @throws IOException
     * @throws InterruptedException
     */
    public void sendBeat() throws IOException, InterruptedException {
      RaftPeer local = peers.local();
      // 如果当前是集群模式，且本地节点不是 leader，则无权发送心跳给其它节点
      if (local.state != RaftPeer.State.LEADER && !STANDALONE_MODE) {
        return;
      }

      if (Loggers.RAFT.isDebugEnabled()) {
        Loggers.RAFT.debug("[RAFT] send beat with {} keys.", datums.size());
      }

      // leader 重置自己的选举超时，能发送心跳就没必要发起选举
      local.resetLeaderDue();

      // 发送心跳时要携带的数据
      // build data
      JSONObject packet = new JSONObject();
      // leader 会把自己的节点数据全部发送给其它节点
      packet.put("peer", local);

      // 保存随心跳一起发送的集群数据
      JSONArray array = new JSONArray();

      if (switchDomain.isSendBeatOnly()) {
        Loggers.RAFT.info("[SEND-BEAT-ONLY] {}", String.valueOf(switchDomain.isSendBeatOnly()));
      }

      // 发心跳时也可以发送数据，可配置
      if (!switchDomain.isSendBeatOnly()) {
        for (Datum datum : datums.values()) {

          JSONObject element = new JSONObject();

          // 只在心跳中携带两种键类型的集群数据，注意这里只携带键，对应的实际数据不在心跳中携带，否则太重了；
          // 实际数据由心跳接收者根据这里的键自己拉取。
          if (KeyBuilder.matchServiceMetaKey(datum.key)) {
            element.put("key", KeyBuilder.briefServiceMetaKey(datum.key));
          } else if (KeyBuilder.matchInstanceListKey(datum.key)) {
            element.put("key", KeyBuilder.briefInstanceListkey(datum.key));
          }
          element.put("timestamp", datum.timestamp);

          array.add(element);
        }
      }

      packet.put("datums", array);
      // broadcast
      Map<String, String> params = new HashMap<String, String>(1);
      params.put("beat", JSON.toJSONString(packet));

      String content = JSON.toJSONString(params);

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      GZIPOutputStream gzip = new GZIPOutputStream(out);
      gzip.write(content.getBytes(StandardCharsets.UTF_8));
      gzip.close();

      byte[] compressedBytes = out.toByteArray();
      String compressedContent = new String(compressedBytes, StandardCharsets.UTF_8);

      if (Loggers.RAFT.isDebugEnabled()) {
        Loggers.RAFT.debug("raw beat data size: {}, size of compressed data: {}", content.length(),
            compressedContent.length());
      }

      // 将心跳广播给全部非 leader 节点
      for (final String server : peers.allServersWithoutMySelf()) {
        try {
          final String url = buildURL(server, API_BEAT);
          if (Loggers.RAFT.isDebugEnabled()) {
            Loggers.RAFT.debug("send beat to server " + server);
          }
          HttpClient.asyncHttpPostLarge(url, null, compressedBytes, new AsyncCompletionHandler<Integer>() {
            @Override
            public Integer onCompleted(Response response) throws Exception {
              if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                Loggers.RAFT.error("NACOS-RAFT beat failed: {}, peer: {}", response.getResponseBody(), server);
                MetricsMonitor.getLeaderSendBeatFailedException().increment();
                return 1;
              }

              // 使用节点返回的心跳数据更新本地的对应的节点数据
              peers.update(JSON.parseObject(response.getResponseBody(), RaftPeer.class));
              if (Loggers.RAFT.isDebugEnabled()) {
                Loggers.RAFT.debug("receive beat response from: {}", url);
              }
              return 0;
            }

            @Override
            public void onThrowable(Throwable t) {
              Loggers.RAFT.error("NACOS-RAFT error while sending heart-beat to peer: {} {}", server, t);
              MetricsMonitor.getLeaderSendBeatFailedException().increment();
            }
          });
        } catch (Exception e) {
          Loggers.RAFT.error("error while sending heart-beat to peer: {} {}", server, e);
          MetricsMonitor.getLeaderSendBeatFailedException().increment();
        }
      }

    }
  }

  /**
   * 处理 raft leader 节点的心跳(以及可能随心跳到达的日志数据)
   * 
   * @param beat
   * @return
   * @throws Exception
   */
  public RaftPeer receivedBeat(JSONObject beat) throws Exception {
    final RaftPeer local = peers.local();
    // 解析心跳中包含的发送者节点的信息
    final RaftPeer remote = new RaftPeer();
    remote.ip = beat.getJSONObject("peer").getString("ip");
    remote.state = RaftPeer.State.valueOf(beat.getJSONObject("peer").getString("state"));
    remote.term.set(beat.getJSONObject("peer").getLongValue("term"));
    remote.heartbeatDueMs = beat.getJSONObject("peer").getLongValue("heartbeatDueMs");
    remote.leaderDueMs = beat.getJSONObject("peer").getLongValue("leaderDueMs");
    remote.voteFor = beat.getJSONObject("peer").getString("voteFor");

    // 如果心跳发送者不是 leader，则报错并终止
    if (remote.state != RaftPeer.State.LEADER) {
      Loggers.RAFT.info("[RAFT] invalid state from master, state: {}, remote peer: {}", remote.state,
          JSON.toJSONString(remote));
      throw new IllegalArgumentException("invalid state from master, state: " + remote.state);
    }

    // 如果本地节点 term 大于心跳发送者（即 leader）的 term，则报错并终止
    if (local.term.get() > remote.term.get()) {
      Loggers.RAFT.info(
          "[RAFT] out of date beat, beat-from-term: {}, beat-to-term: {}, remote peer: {}, and leaderDueMs: {}",
          remote.term.get(), local.term.get(), JSON.toJSONString(remote), local.leaderDueMs);
      throw new IllegalArgumentException(
          "out of date beat, beat-from-term: " + remote.term.get() + ", beat-to-term: " + local.term.get());
    }

    // 如果本地节点不是 follower 状态，则可能是本地节点选举超时定时器超时前未接收到 leader 的心跳，
    // 本地节点认为 leader 可能挂了，于是要发起选举，此时其状态为 candidate；
    // 但此处因为又收到心跳了，于是将自己的状态恢复为 follower，同时将 voteFor 改为 leader。
    if (local.state != RaftPeer.State.FOLLOWER) {

      Loggers.RAFT.info("[RAFT] make remote as leader, remote peer: {}", JSON.toJSONString(remote));
      // mk follower
      local.state = RaftPeer.State.FOLLOWER;
      local.voteFor = remote.ip;
    }

    // 处理随 leader 心跳一起到达的集群数据
    final JSONArray beatDatums = beat.getJSONArray("datums");
    // 本地节点收到了来自 leader 的心跳，于是重置自己的选举超时和心跳超时
    local.resetLeaderDue();
    local.resetHeartbeatDue();

    // 使用 remote 作为本地节点的 leader
    peers.makeLeader(remote);

    Map<String, Integer> receivedKeysMap = new HashMap<>(datums.size());

    // 将本地存储的数据键放入 map，value 为计数值，后面如果从心跳中发现了对应的数据键则会将计数值改为 1；
    // 方法结束时会查看每个数据键的计数值，如果为 0 则说明 leader 侧已经删除了相关数据，即为 deadKey，本地节点也应将其删除。
    for (Map.Entry<String, Datum> entry : datums.entrySet()) {
      receivedKeysMap.put(entry.getKey(), 0);
    }

    // now check datums
    List<String> batch = new ArrayList<>();
    if (!switchDomain.isSendBeatOnly()) {
      int processedCount = 0;
      if (Loggers.RAFT.isDebugEnabled()) {
        Loggers.RAFT.debug(
            "[RAFT] received beat with {} keys, RaftCore.datums' size is {}, remote server: {}, term: {}, local term: {}",
            beatDatums.size(), datums.size(), remote.ip, remote.term, local.term);
      }
      for (Object object : beatDatums) {
        processedCount = processedCount + 1;

        // 每个 entry 包含一个 datum 相关 key 和 timestamp 信息
        JSONObject entry = (JSONObject) object;
        String key = entry.getString("key");
        final String datumKey;

        if (KeyBuilder.matchServiceMetaKey(key)) {
          datumKey = KeyBuilder.detailServiceMetaKey(key);
        } else if (KeyBuilder.matchInstanceListKey(key)) {
          datumKey = KeyBuilder.detailInstanceListkey(key);
        } else {
          // ignore corrupted key:
          continue;
        }

        long timestamp = entry.getLong("timestamp");

        receivedKeysMap.put(datumKey, 1);

        try {
          // 如果本地保存有同样的 key 且时间戳更新，则忽略随心跳过来的 key
          if (datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp
              && processedCount < beatDatums.size()) {
            continue;
          }

          // 其它情况下，将该数据加入 batch 中
          if (!(datums.containsKey(datumKey) && datums.get(datumKey).timestamp.get() >= timestamp)) {
            batch.add(datumKey);
          }

          // 一批数据达到 50 个才进行下面的处理
          if (batch.size() < 50 && processedCount < beatDatums.size()) {
            continue;
          }

          // 用逗号将 50 个数据键级联起来，如果太多下面拉取数据时数据量将会非常大
          String keys = StringUtils.join(batch, ",");

          // todo 这个判断有点莫名，上面不是用 < 50 拦截过了吗
          if (batch.size() <= 0) {
            continue;
          }

          Loggers.RAFT.info(
              "get datums from leader: {}, batch size is {}, processedCount is {}, datums' size is {}, RaftCore.datums' size is {}",
              getLeader().ip, batch.size(), processedCount, beatDatums.size(), datums.size());

          // 携带着上述 50 个数据键去请求 leader，leader 会返回这些数据键对应的数据，
          // 然后本地节点用这些数据更新本地的数据。
          // update datum entry
          String url = buildURL(remote.ip, API_GET) + "?keys=" + URLEncoder.encode(keys, "UTF-8");
          HttpClient.asyncHttpGet(url, null, null, new AsyncCompletionHandler<Integer>() {
            @Override
            public Integer onCompleted(Response response) throws Exception {
              if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
                return 1;
              }

              List<JSONObject> datumList = JSON.parseObject(response.getResponseBody(),
                  new TypeReference<List<JSONObject>>() {
                  });

              for (JSONObject datumJson : datumList) {
                // 更新本地数据时获取锁
                OPERATE_LOCK.lock();
                Datum newDatum = null;
                try {

                  Datum oldDatum = getDatum(datumJson.getString("key"));

                  // 只有 leader 返回的数据比本地的数据更新时才进行更新
                  if (oldDatum != null && datumJson.getLongValue("timestamp") <= oldDatum.timestamp.get()) {
                    Loggers.RAFT.info(
                        "[NACOS-RAFT] timestamp is smaller than that of mine, key: {}, remote: {}, local: {}",
                        datumJson.getString("key"), datumJson.getLongValue("timestamp"), oldDatum.timestamp);
                    continue;
                  }

                  if (KeyBuilder.matchServiceMetaKey(datumJson.getString("key"))) {
                    Datum<Service> serviceDatum = new Datum<>();
                    serviceDatum.key = datumJson.getString("key");
                    serviceDatum.timestamp.set(datumJson.getLongValue("timestamp"));
                    serviceDatum.value = JSON.parseObject(JSON.toJSONString(datumJson.getJSONObject("value")),
                        Service.class);
                    newDatum = serviceDatum;
                  }

                  if (KeyBuilder.matchInstanceListKey(datumJson.getString("key"))) {
                    Datum<Instances> instancesDatum = new Datum<>();
                    instancesDatum.key = datumJson.getString("key");
                    instancesDatum.timestamp.set(datumJson.getLongValue("timestamp"));
                    instancesDatum.value = JSON.parseObject(JSON.toJSONString(datumJson.getJSONObject("value")),
                        Instances.class);
                    newDatum = instancesDatum;
                  }

                  if (newDatum == null || newDatum.value == null) {
                    Loggers.RAFT.error("receive null datum: {}", datumJson);
                    continue;
                  }

                  // 将新数据持久化
                  raftStore.write(newDatum);

                  // 用新数据替换本地节点内存中的旧数据
                  datums.put(newDatum.key, newDatum);
                  // 数据有变更，触发通知
                  notifier.addTask(newDatum.key, ApplyAction.CHANGE);

                  // 重置本地节点选举超时 todo 上面不是已经重置过来吗？怕上面拉取数据的 http 请求耗时太多？
                  local.resetLeaderDue();

                  // 更新本地节点的 term 但是不能超过 leader 的 term
                  if (local.term.get() + 100 > remote.term.get()) {
                    getLeader().term.set(remote.term.get());
                    local.term.set(getLeader().term.get());
                  } else {
                    // todo 加 100 意义何在呢？仅仅是为了表示本地有更新吗？那直接用 leader 的不是更好吗？
                    local.term.addAndGet(100);
                  }

                  // 将本地节点 term 持久化
                  raftStore.updateTerm(local.term.get());

                  Loggers.RAFT.info("data updated, key: {}, timestamp: {}, from {}, local term: {}", newDatum.key,
                      newDatum.timestamp, JSON.toJSONString(remote), local.term);

                } catch (Throwable e) {
                  Loggers.RAFT.error("[RAFT-BEAT] failed to sync datum from leader, datum: {}", newDatum, e);
                } finally {
                  // 更新完毕，释放锁
                  OPERATE_LOCK.unlock();
                }
              }
              // todo 这个等待意义何在？
              TimeUnit.MILLISECONDS.sleep(200);
              return 0;
            }
          });

          // 清空，处理下一批
          batch.clear();

        } catch (Exception e) {
          Loggers.RAFT.error("[NACOS-RAFT] failed to handle beat entry, key: {}", datumKey);
        }

      }

      // 如果随心跳到达的数据中，有数据失效了，则从本地节点删除之
      List<String> deadKeys = new ArrayList<>();
      for (Map.Entry<String, Integer> entry : receivedKeysMap.entrySet()) {
        if (entry.getValue() == 0) {
          deadKeys.add(entry.getKey());
        }
      }

      for (String deadKey : deadKeys) {
        try {
          deleteDatum(deadKey);
        } catch (Exception e) {
          Loggers.RAFT.error("[NACOS-RAFT] failed to remove entry, key={} {}", deadKey, e);
        }
      }

    }

    return local;
  }

  /**
   * 将监听器 {@code listener} 添加到 {@code key} 监听列表中，添加完毕针对该监听器感兴趣的数据要触发该监听器的相关方法。
   *
   * @param key
   * @param listener
   */
  public void listen(String key, RecordListener listener) {

    // 如果监听器已存在则什么也不做
    List<RecordListener> listenerList = listeners.get(key);
    if (listenerList != null && listenerList.contains(listener)) {
      return;
    }

    if (listenerList == null) {
      listenerList = new CopyOnWriteArrayList<>();
      listeners.put(key, listenerList);
    }

    Loggers.RAFT.info("add listener: {}", key);

    // 将新的监听器加入到数据对应的监听器列表中
    listenerList.add(listener);

    // 遍历全部数据，遇到该监听器感兴趣的数据就触发该监听器相关方法
    // if data present, notify immediately
    for (Datum datum : datums.values()) {
      if (!listener.interests(datum.key)) {
        continue;
      }

      try {
        listener.onChange(datum.key, datum.value);
      } catch (Exception e) {
        Loggers.RAFT.error("NACOS-RAFT failed to notify listener", e);
      }
    }
  }

  /**
   * 将监听器 {@code listener} 从 {@code key} 对应的监听列表中移除
   *
   * @param key
   * @param listener
   */
  public void unlisten(String key, RecordListener listener) {

    if (!listeners.containsKey(key)) {
      return;
    }

    for (RecordListener dl : listeners.get(key)) {
      // TODO maybe use equal:
      if (dl == listener) {
        listeners.get(key).remove(listener);
        break;
      }
    }
  }

  public void unlistenAll(String key) {
    listeners.remove(key);
  }

  public void setTerm(long term) {
    peers.setTerm(term);
  }

  public boolean isLeader(String ip) {
    return peers.isLeader(ip);
  }

  public boolean isLeader() {
    return peers.isLeader(NetUtils.localServer());
  }

  public static String buildURL(String ip, String api) {
    if (!ip.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
      ip = ip + UtilsAndCommons.IP_PORT_SPLITER + RunningConfig.getServerPort();
    }
    return "http://" + ip + RunningConfig.getContextPath() + api;
  }

  /**
   * 响应客户端的查询需求，根据 key 返回对应的数据
   * 
   * @param key
   * @return
   */
  public Datum<?> getDatum(String key) {
    // 集群全部数据都会在内存中保留一份
    return datums.get(key);
  }

  public RaftPeer getLeader() {
    return peers.getLeader();
  }

  public List<RaftPeer> getPeers() {
    return new ArrayList<>(peers.allPeers());
  }

  public RaftPeerSet getPeerSet() {
    return peers;
  }

  public void setPeerSet(RaftPeerSet peerSet) {
    peers = peerSet;
  }

  public int datumSize() {
    return datums.size();
  }

  public void addDatum(Datum datum) {
    datums.put(datum.key, datum);
    notifier.addTask(datum.key, ApplyAction.CHANGE);
  }

  /**
   * 加载 key 对应的 datum
   * 
   * @param key
   */
  public void loadDatum(String key) {
    try {
      Datum datum = raftStore.load(key);
      if (datum == null) {
        return;
      }
      datums.put(key, datum);
    } catch (Exception e) {
      Loggers.RAFT.error("load datum failed: " + key, e);
    }

  }

  /**
   * {@code key} 已经失效，从本地节点中删除之。
   *
   * @param key
   */
  private void deleteDatum(String key) {
    Datum deleted;
    try {
      // 先从内存删除；如果存在则再从持久化存储删除之。
      deleted = datums.remove(URLDecoder.decode(key, "UTF-8"));
      if (deleted != null) {
        raftStore.delete(deleted);
        Loggers.RAFT.info("datum deleted, key: {}", key);
      }
      // 将变更通知该数据的全部监听器
      notifier.addTask(URLDecoder.decode(key, "UTF-8"), ApplyAction.DELETE);
    } catch (UnsupportedEncodingException e) {
      Loggers.RAFT.warn("datum key decode failed: {}", key);
    }
  }

  public boolean isInitialized() {
    return initialized || !globalConfig.isDataWarmup();
  }

  public int getNotifyTaskCount() {
    return notifier.getTaskSize();
  }

  /**
   * Notifier 负责监听数据变化，然后调用注册到对应数据上的监听器
   */
  public class Notifier implements Runnable {

    private ConcurrentHashMap<String, String> services = new ConcurrentHashMap<>(10 * 1024);

    private BlockingQueue<Pair> tasks = new LinkedBlockingQueue<>(1024 * 1024);

    /**
     * 如果用户期望在 {@code datumKey} 发生 {@code action} 类型的变更时触发 {@code datumKey}
     * 对应的监听器，则将其 添加到 Notifier 中。
     * 
     * @param datumKey
     * @param action
     */
    public void addTask(String datumKey, ApplyAction action) {

      // 如果是服务变更，并且已经添加过（上次添加的还没处理，因为处理完成会被删除）不再重复添加
      if (services.containsKey(datumKey) && action == ApplyAction.CHANGE) {
        return;
      }

      // CHANGE 类型变更目前只对应服务发现（配置类的更改，猜测是先删除后新建所以用 DELETE）
      if (action == ApplyAction.CHANGE) {
        services.put(datumKey, StringUtils.EMPTY);
      }

      Loggers.RAFT.info("add task {}", datumKey);

      // 将本次变化抽象为任务添加到队列等待 notifier 处理
      tasks.add(Pair.with(datumKey, action));
    }

    public int getTaskSize() {
      return tasks.size();
    }

    @Override
    public void run() {
      Loggers.RAFT.info("raft notifier started");

      while (true) {
        try {

          // 获取一个通知类任务进行处理
          Pair pair = tasks.take();

          if (pair == null) {
            continue;
          }

          String datumKey = (String) pair.getValue0();
          ApplyAction action = (ApplyAction) pair.getValue1();

          // 如果是服务发现类变化，则从缓存移除标记
          services.remove(datumKey);

          Loggers.RAFT.info("remove task {}", datumKey);

          int count = 0;

          // 先触发元数据类型的监听器（如果 datumKey 包含元数据特定前缀的话）
          if (listeners.containsKey(KeyBuilder.SERVICE_META_KEY_PREFIX)) {

            if (KeyBuilder.matchServiceMetaKey(datumKey) && !KeyBuilder.matchSwitchKey(datumKey)) {

              for (RecordListener listener : listeners.get(KeyBuilder.SERVICE_META_KEY_PREFIX)) {
                try {
                  if (action == ApplyAction.CHANGE) {
                    listener.onChange(datumKey, getDatum(datumKey).value);
                  }

                  if (action == ApplyAction.DELETE) {
                    listener.onDelete(datumKey);
                  }
                } catch (Throwable e) {
                  Loggers.RAFT.error("[NACOS-RAFT] error while notifying listener of key: {}", datumKey, e);
                }
              }
            }
          }

          // 没人对这个数据感兴趣
          if (!listeners.containsKey(datumKey)) {
            continue;
          }

          // 依次调用 datumKey 对应的监听器
          for (RecordListener listener : listeners.get(datumKey)) {

            count++;

            try {
              if (action == ApplyAction.CHANGE) {
                listener.onChange(datumKey, getDatum(datumKey).value);
                continue;
              }

              if (action == ApplyAction.DELETE) {
                listener.onDelete(datumKey);
                continue;
              }
            } catch (Throwable e) {
              Loggers.RAFT.error("[NACOS-RAFT] error while notifying listener of key: {}", datumKey, e);
            }
          }

          if (Loggers.RAFT.isDebugEnabled()) {
            Loggers.RAFT.debug("[NACOS-RAFT] datum change notified, key: {}, listener count: {}", datumKey, count);
          }
        } catch (Throwable e) {
          Loggers.RAFT.error("[NACOS-RAFT] Error while handling notifying task", e);
        }
      }
    }
  }
}
