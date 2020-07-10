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
package com.alibaba.nacos.naming.cluster;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.cluster.servers.Server;
import com.alibaba.nacos.naming.cluster.servers.ServerChangeListener;
import com.alibaba.nacos.naming.misc.*;
import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.nacos.core.utils.SystemUtils.*;

/**
 * The manager to globally refresh and operate server list.
 * <p>
 * ServerListManager 是 nacos 的一个全局管理器，负责周期性刷新和操作服务器列表。
 *
 * @author nkorange
 * @since 1.0.0
 */
@Component("serverListManager")
public class ServerListManager {

    private static final int STABLE_PERIOD = 60 * 1000;

    @Autowired
    private SwitchDomain switchDomain;

    private List<ServerChangeListener> listeners = new ArrayList<>();

    /**
     * servers 负责保存当前最新的 raft 集群节点集合
     */
    private List<Server> servers = new ArrayList<>();

    /**
     * healthyServers 用于保存当前 raft 集群可达实例列表
     */
    private List<Server> healthyServers = new ArrayList<>();

    /**
     * 保存每个服务器实例发送给本机的状态报告（包含了时间戳、可用处理器等，类似于心跳，会周期性发送）
     */
    private Map<String, List<Server>> distroConfig = new ConcurrentHashMap<>();

    /**
     * 保存接收到每个服务器状态报告的时间戳
     */
    private Map<String, Long> distroBeats = new ConcurrentHashMap<>(16);

    /**
     * 存活的站点集合
     */
    private Set<String> liveSites = new HashSet<>();

    /**
     * nacos 集群的站点名称
     */
    private final static String LOCALHOST_SITE = UtilsAndCommons.UNKNOWN_SITE;

    private long lastHealthServerMillis = 0L;

    private boolean autoDisabledHealthCheck = false;

    private Synchronizer synchronizer = new ServerStatusSynchronizer();

    /**
     * 将监听器添加到列表中，有消息发生的时候会调用之。
     *
     * @param listener
     */
    public void listen(ServerChangeListener listener) {
        listeners.add(listener);
    }

    @PostConstruct
    public void init() {
        // 将服务器列表刷新任务和服务器状态报告任务加入到全局定时调度器中
        GlobalExecutor.registerServerListUpdater(new ServerListUpdater());
        GlobalExecutor.registerServerStatusReporter(new ServerStatusReporter(), 5000);
    }

    /**
     * refreshServerList 从集群配置文件或者环境变量中读取集群的当前服务器列表，返回的最新节点列表会被
     *
     * @return
     */
    private List<Server> refreshServerList() {

        List<Server> result = new ArrayList<>();

        if (STANDALONE_MODE) {
            Server server = new Server();
            server.setIp(NetUtils.getLocalAddress());
            server.setServePort(RunningConfig.getServerPort());
            result.add(server);
            return result;
        }

        List<String> serverList = new ArrayList<>();
        try {
            // 从 conf/cluster.conf 文件读取集群服务器 ip[:port] 列表
            serverList = readClusterConf();
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("failed to get config: " + CLUSTER_CONF_FILE_PATH, e);
        }

        if (Loggers.SRV_LOG.isDebugEnabled()) {
            Loggers.SRV_LOG.debug("SERVER-LIST from cluster.conf: {}", result);
        }

        // 如果集群配置文件为空，则取环境变量 naming_self_service_cluster_ips 的值
        //use system env
        if (CollectionUtils.isEmpty(serverList)) {
            serverList = SystemUtils.getIPsBySystemEnv(UtilsAndCommons.SELF_SERVICE_CLUSTER_ENV);
            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("SERVER-LIST from system variable: {}", result);
            }
        }

        if (CollectionUtils.isNotEmpty(serverList)) {

            for (int i = 0; i < serverList.size(); i++) {

                String ip;
                int port;
                String server = serverList.get(i);
                // 解析 ip:port
                if (server.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
                    ip = server.split(UtilsAndCommons.IP_PORT_SPLITER)[0];
                    port = Integer.parseInt(server.split(UtilsAndCommons.IP_PORT_SPLITER)[1]);
                } else {
                    // 如果只有 ip，则用运行时配置的端口号
                    ip = server;
                    port = RunningConfig.getServerPort();
                }

                Server member = new Server();
                member.setIp(ip);
                member.setServePort(port);
                result.add(member);
            }
        }

        return result;
    }

    public boolean contains(String s) {
        for (Server server : servers) {
            if (server.getKey().equals(s)) {
                return true;
            }
        }
        return false;
    }

    public List<Server> getServers() {
        return servers;
    }

    public List<Server> getHealthyServers() {
        return healthyServers;
    }

    /**
     * notifyListeners 向全局调度器提交一个 one-shot 类型任务，负责调用监听器列表中每个监听器对应方法，更新其服务器列表和服务器健康状态
     */
    private void notifyListeners() {

        GlobalExecutor.notifyServerListChange(new Runnable() {
            @Override
            public void run() {
                // 遍历监听器列表，分别调用每个监听器的相关方法（监听器根据需要实现这两个方法）
                for (ServerChangeListener listener : listeners) {
                    listener.onChangeServerList(servers);
                    listener.onChangeHealthyServerList(healthyServers);
                }
            }
        });
    }

    public Map<String, List<Server>> getDistroConfig() {
        return distroConfig;
    }

    /**
     * 每个服务器会周期性的将自己的状态报告（类似于心跳）发送给集群全部服务器。
     * 该方法负责解析并更新本地维护的心跳信息。
     *
     * @param configInfo
     */
    public synchronized void onReceiveServerStatus(String configInfo) {

        Loggers.SRV_LOG.info("receive config info: {}", configInfo);

        String[] configs = configInfo.split("\r\n");
        if (configs.length == 0) {
            return;
        }

        List<Server> newHealthyList = new ArrayList<>();
        List<Server> tmpServerList = new ArrayList<>();

        for (String config : configs) {
            tmpServerList.clear();
            // site:ip:lastReportTime:weight
            String[] params = config.split("#");
            if (params.length <= 3) {
                Loggers.SRV_LOG.warn("received malformed distro map data: {}", config);
                continue;
            }

            // 基于参数中传过来的信息构造 server
            Server server = new Server();

            server.setSite(params[0]);
            server.setIp(params[1].split(UtilsAndCommons.IP_PORT_SPLITER)[0]);
            server.setServePort(Integer.parseInt(params[1].split(UtilsAndCommons.IP_PORT_SPLITER)[1]));
            server.setLastRefTime(Long.parseLong(params[2]));

            // 如果状态所属的 server 不在当前服务器列表中，则报错（集群服务器列表发生了变更，但是这个接收报告的机器还不知道）
            if (!contains(server.getKey())) {
                throw new IllegalArgumentException("server: " + server.getKey() + " is not in serverlist");
            }

            // 检查 server 是否还活着，并更新其最后一次心跳发生时间；
            // 注意，状态报告就是上面提到的心跳。
            // 每个服务器会周期性的将自己的状态报告发送给集群全部服务器。
            Long lastBeat = distroBeats.get(server.getKey());
            long now = System.currentTimeMillis();
            if (null != lastBeat) {
                server.setAlive(now - lastBeat < switchDomain.getDistroServerExpiredMillis());
            }
            distroBeats.put(server.getKey(), now);

            // 取出状态报告中携带的时间戳进行格式化
            Date date = new Date(Long.parseLong(params[2]));
            server.setLastRefTimeStr(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date));

            // 如果参数中包含 weight 则解析并设置，否则设置为 1
            server.setWeight(params.length == 4 ? Integer.parseInt(params[3]) : 1);
            List<Server> list = distroConfig.get(server.getSite());
            // 如果 list 不为空则 server 不会提前加入到 list 中
            if (list == null || list.size() <= 0) {
                list = new ArrayList<>();
                list.add(server);
                distroConfig.put(server.getSite(), list);
            }

            for (Server s : list) {
                String serverId = s.getKey() + "_" + s.getSite();
                String newServerId = server.getKey() + "_" + server.getSite();

                // 如果 server 出现在过 list 中，则将其加入到 tmpServerList 中
                if (serverId.equals(newServerId)) {
                    if (s.isAlive() != server.isAlive() || s.getWeight() != server.getWeight()) {
                        Loggers.SRV_LOG.warn("server beat out of date, current: {}, last: {}",
                            JSON.toJSONString(server), JSON.toJSONString(s));
                    }
                    tmpServerList.add(server);
                    continue;
                }
                // 将非本次状态报告发送机器节点直接加入到 tmpServerList 中
                tmpServerList.add(s);
            }

            // 如果上面 server 没有机会（如 list 初始不为空，且没有与 server 同 key 的 s）加入到 tmpServerList 则加入
            if (!tmpServerList.contains(server)) {
                tmpServerList.add(server);
            }

            distroConfig.put(server.getSite(), tmpServerList);
        }
        liveSites.addAll(distroConfig.keySet());
    }

    public void clean() {
        cleanInvalidServers();

        for (Map.Entry<String, List<Server>> entry : distroConfig.entrySet()) {
            for (Server server : entry.getValue()) {
                //request other server to clean invalid servers
                if (!server.getKey().equals(NetUtils.localServer())) {
                    requestOtherServerCleanInvalidServers(server.getKey());
                }
            }

        }
    }

    public Set<String> getLiveSites() {
        return liveSites;
    }

    private void cleanInvalidServers() {
        for (Map.Entry<String, List<Server>> entry : distroConfig.entrySet()) {
            List<Server> currentServers = entry.getValue();
            if (null == currentServers) {
                distroConfig.remove(entry.getKey());
                continue;
            }

            currentServers.removeIf(server -> !server.isAlive());
        }
    }

    private void requestOtherServerCleanInvalidServers(String serverIP) {
        Map<String, String> params = new HashMap<String, String>(1);

        params.put("action", "without-diamond-clean");
        try {
            NamingProxy.reqAPI("distroStatus", params, serverIP, false);
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("[DISTRO-STATUS-CLEAN] Failed to request to clean server status to " + serverIP, e);
        }
    }

    /**
     * ServerListUpdater 周期性被调用，负责通过读取配置文件或环境变量获取 raft 集群的当前最新节点列表（这也是新增或者下线节点的办法）；
     * 如果有变化则通知 RaftPeerSet 注册的监听器。
     * <p>
     * 该任务会被加入到全局定时调度器中周期性执行。
     */
    public class ServerListUpdater implements Runnable {

        @Override
        public void run() {
            try {
                List<Server> refreshedServers = refreshServerList();
                List<Server> oldServers = servers;

                if (CollectionUtils.isEmpty(refreshedServers)) {
                    Loggers.RAFT.warn("refresh server list failed, ignore it.");
                    return;
                }

                boolean changed = false;

                // refreshed - old 的差集即为刷新后服务器的增量
                List<Server> newServers = (List<Server>) CollectionUtils.subtract(refreshedServers, oldServers);
                if (CollectionUtils.isNotEmpty(newServers)) {
                    // 将新增服务器加入到当前服务器列表中
                    servers.addAll(newServers);
                    changed = true;
                    Loggers.RAFT.info("server list is updated, new: {} servers: {}", newServers.size(), newServers);
                }

                // old - refreshed 的差集即为原服务器列表中挂掉或下掉的机器
                List<Server> deadServers = (List<Server>) CollectionUtils.subtract(oldServers, refreshedServers);
                if (CollectionUtils.isNotEmpty(deadServers)) {
                    // 从当前服务器列表中移除挂掉的机器
                    servers.removeAll(deadServers);
                    changed = true;
                    Loggers.RAFT.info("server list is updated, dead: {}, servers: {}", deadServers.size(), deadServers);
                }

                // 如果当前服务器列表有更新，则通知各个监听器
                if (changed) {
                    notifyListeners();
                }

            } catch (Exception e) {
                Loggers.RAFT.info("error while updating server list.", e);
            }
        }
    }


    private class ServerStatusReporter implements Runnable {

        @Override
        public void run() {
            try {

                if (RunningConfig.getServerPort() <= 0) {
                    return;
                }

                checkDistroHeartbeat();

                // 服务器可用的处理器数目的一半作为服务器的 weight（服务器处理器数也算服务器状态）
                int weight = Runtime.getRuntime().availableProcessors() / 2;
                if (weight <= 0) {
                    weight = 1;
                }

                // 当前时间戳
                long curTime = System.currentTimeMillis();
                // site:ip:lastReportTime:weight
                String status = LOCALHOST_SITE + "#" + NetUtils.localServer() + "#" + curTime + "#" + weight + "\r\n";

                // 服务器的状态信息发送给全部 server，当然包括自己
                //send status to itself
                onReceiveServerStatus(status);

                List<Server> allServers = getServers();

                if (!contains(NetUtils.localServer())) {
                    Loggers.SRV_LOG.error("local ip is not in serverlist, ip: {}, serverlist: {}", NetUtils.localServer(), allServers);
                    return;
                }

                if (allServers.size() > 0 && !NetUtils.localServer().contains(UtilsAndCommons.LOCAL_HOST_IP)) {
                    for (com.alibaba.nacos.naming.cluster.servers.Server server : allServers) {
                        // 上面给自己发过一遍了，这里不用再重复了，而且还走网络。
                        if (server.getKey().equals(NetUtils.localServer())) {
                            continue;
                        }

                        Message msg = new Message();
                        msg.setData(status);

                        // 通过 get 请求将本地服务器的状态信息，即上面组装的 status，发送给目标 server；
                        // 与该方法对应的是 ServerListManager.onReceiveServerStatus()
                        synchronizer.send(server.getKey(), msg);

                    }
                }
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[SERVER-STATUS] Exception while sending server status", e);
            } finally {
                // 本来是 one-shot 的，但是每次执行完都重新注册变相实现了周期性调用
                GlobalExecutor.registerServerStatusReporter(this, switchDomain.getServerStatusSynchronizationPeriodMillis());
            }

        }
    }

    private void checkDistroHeartbeat() {

        Loggers.SRV_LOG.debug("check distro heartbeat.");

        List<Server> servers = distroConfig.get(LOCALHOST_SITE);
        if (CollectionUtils.isEmpty(servers)) {
            return;
        }

        List<Server> newHealthyList = new ArrayList<>(servers.size());
        long now = System.currentTimeMillis();
        for (Server s: servers) {
            // 读取当前服务器的上个心跳时间戳
            Long lastBeat = distroBeats.get(s.getKey());
            if (null == lastBeat) {
                continue;
            }
            // 如果未超时，则认为服务器还活着，否则就认为挂了
            s.setAlive(now - lastBeat < switchDomain.getDistroServerExpiredMillis());
        }

        //local site servers
        List<String> allLocalSiteSrvs = new ArrayList<>();
        for (Server server : servers) {

            // todo 这个端口号是个什么鬼？
            if (server.getKey().endsWith(":0")) {
                continue;
            }

            server.setAdWeight(switchDomain.getAdWeight(server.getKey()) == null ? 0 : switchDomain.getAdWeight(server.getKey()));

            // todo 这个循环条件是啥意思？
            for (int i = 0; i < server.getWeight() + server.getAdWeight(); i++) {

                if (!allLocalSiteSrvs.contains(server.getKey())) {
                    allLocalSiteSrvs.add(server.getKey());
                }

                if (server.isAlive() && !newHealthyList.contains(server)) {
                    newHealthyList.add(server);
                }
            }
        }

        Collections.sort(newHealthyList);
        // 计算集群当前健康节点数相对于全部节点数的比例
        float curRatio = (float) newHealthyList.size() / allLocalSiteSrvs.size();

        // 如果健康检查关闭了 && 当前存活率超过阈值 && 又该进行健康检查了
        if (autoDisabledHealthCheck
            && curRatio > switchDomain.getDistroThreshold()
            && System.currentTimeMillis() - lastHealthServerMillis > STABLE_PERIOD) {
            Loggers.SRV_LOG.info("[NACOS-DISTRO] distro threshold restored and " +
                "stable now, enable health check. current ratio: {}", curRatio);

            // 开启健康检查
            switchDomain.setHealthCheckEnabled(true);

            // we must set this variable, otherwise it will conflict with user's action
            autoDisabledHealthCheck = false;
        }

        // 如果新的存活列表与老的不一致
        if (!CollectionUtils.isEqualCollection(healthyServers, newHealthyList)) {
            // todo 每次存活列表有变更时，关闭一会健康检查，为啥？
            // for every change disable healthy check for some while
            if (switchDomain.isHealthCheckEnabled()) {
                Loggers.SRV_LOG.info("[NACOS-DISTRO] healthy server list changed, " +
                        "disable health check for {} ms from now on, old: {}, new: {}", STABLE_PERIOD,
                    healthyServers, newHealthyList);

                switchDomain.setHealthCheckEnabled(false);
                autoDisabledHealthCheck = true;

                lastHealthServerMillis = System.currentTimeMillis();
            }

            // 用新的集群可达实例列表替换老的
            healthyServers = newHealthyList;
            // 通知监听器，集群可达实例列表更新了
            notifyListeners();
        }
    }

}
