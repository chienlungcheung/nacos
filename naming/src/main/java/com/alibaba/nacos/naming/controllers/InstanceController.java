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
package com.alibaba.nacos.naming.controllers;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.exception.NacosException;
import com.alibaba.nacos.naming.healthcheck.RsInfo;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.ClientInfo;
import com.alibaba.nacos.naming.push.DataSource;
import com.alibaba.nacos.naming.push.PushService;
import com.alibaba.nacos.naming.web.CanDistro;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.util.VersionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Instance operation controller
 * <p>
 * 负责处理客户端的服务注册、服务发现、集群或实例状态查询等请求。
 * 
 * @author nkorange
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance")
public class InstanceController {

  @Autowired
  private DistroMapper distroMapper;

  @Autowired
  private SwitchDomain switchDomain;

  @Autowired
  private PushService pushService;

  @Autowired
  private ServiceManager serviceManager;

  /**
   * 推送服务的数据源(即各个服务对应的实例列表)
   */
  private DataSource pushDataSource = new DataSource() {

    @Override
    public String getData(PushService.PushClient client) throws Exception {

      JSONObject result = new JSONObject();
      try {
        // isCheck 参数为 false, 此处会返回 client 对应的服务所对应的实例列表.
        result = doSrvIPXT(client.getNamespaceId(), client.getServiceName(), client.getAgent(), client.getClusters(),
            client.getSocketAddr().getAddress().getHostAddress(), 0, StringUtils.EMPTY, false, StringUtils.EMPTY,
            StringUtils.EMPTY, false);
      } catch (Exception e) {
        Loggers.SRV_LOG.warn("PUSH-SERVICE: service is not modified", e);
      }

      // overdrive the cache millis to push mode
      result.put("cacheMillis", switchDomain.getPushCacheMillis(client.getServiceName()));

      return result.toJSONString();
    }
  };

  /**
   * 服务注册接口，响应客户端的服务注册请求。
   * <p>
   * example: curl -X POST
   * 'http://nacoshost2:8848/nacos/v1/ns/instance?serviceName=nacos.naming.serviceName&ip=20.18.7.10&port=8080'
   */
  @CanDistro
  @RequestMapping(value = "", method = RequestMethod.POST)
  public String register(HttpServletRequest request) throws Exception {

    String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
    String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);

    // 提取信息构造 Instance 并注册到相关服务中。
    serviceManager.registerInstance(namespaceId, serviceName, parseInstance(request));
    return "ok";
  }

  /**
   * 从服务对应的实例列表移除参数中指定的待删除的实例
   * 
   * @param request
   * @return
   * @throws Exception
   */
  @CanDistro
  @RequestMapping(value = "", method = RequestMethod.DELETE)
  public String deregister(HttpServletRequest request) throws Exception {
    Instance instance = getIPAddress(request);
    String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
    String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);

    Service service = serviceManager.getService(namespaceId, serviceName);
    if (service == null) {
      Loggers.SRV_LOG.warn("remove instance from non-exist service: {}", serviceName);
      return "ok";
    }

    serviceManager.removeInstance(namespaceId, serviceName, instance.isEphemeral(), instance);

    return "ok";
  }

  @CanDistro
  @RequestMapping(value = "", method = RequestMethod.PUT)
  public String update(HttpServletRequest request) throws Exception {
    String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
    String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);

    String agent = request.getHeader("Client-Version");
    if (StringUtils.isBlank(agent)) {
      agent = request.getHeader("User-Agent");
    }

    ClientInfo clientInfo = new ClientInfo(agent);

    if (clientInfo.type == ClientInfo.ClientType.JAVA
        && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
      serviceManager.updateInstance(namespaceId, serviceName, parseInstance(request));
    } else {
      serviceManager.registerInstance(namespaceId, serviceName, parseInstance(request));
    }
    return "ok";
  }

  /**
   * 服务发现接口，响应客户端的服务发现请求。
   * <p>
   * example: curl -X GET
   * 'http://naocshost2:8848/nacos/v1/ns/instance/list?serviceName=group@@serviceName'
   * 
   * @param request
   * @return
   * @throws Exception
   */
  @RequestMapping(value = "/list", method = RequestMethod.GET)
  public JSONObject list(HttpServletRequest request) throws Exception {

    String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);

    // 从请求中提取出服务名称,注意这里的 serviceName 不纯为 web ui 看到的服务名称, 而是形如 group@@serviceName
    String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
    // 形如 Client-Version: Nacos-Java-Client:v1.0.0
    String agent = request.getHeader("Client-Version");
    if (StringUtils.isBlank(agent)) {
      // 形如 User-Agent: Nacos-Java-Client:v1.0.0
      agent = request.getHeader("User-Agent");
    }
    String clusters = WebUtils.optional(request, "clusters", StringUtils.EMPTY);
    String clientIP = WebUtils.optional(request, "clientIP", StringUtils.EMPTY);
    Integer udpPort = Integer.parseInt(WebUtils.optional(request, "udpPort", "0"));
    String env = WebUtils.optional(request, "env", StringUtils.EMPTY);
    // 如果设置了 isCheck 为 true 说明此次请求只为检查服务是否达到保护阈值了（健康实例比例小于等于某个值）
    boolean isCheck = Boolean.parseBoolean(WebUtils.optional(request, "isCheck", "false"));

    String app = WebUtils.optional(request, "app", StringUtils.EMPTY);

    String tenant = WebUtils.optional(request, "tid", StringUtils.EMPTY);

    boolean healthyOnly = Boolean.parseBoolean(WebUtils.optional(request, "healthyOnly", "false"));

    // 检查保护阈值或者返回实例列表
    return doSrvIPXT(namespaceId, serviceName, agent, clusters, clientIP, udpPort, env, isCheck, app, tenant,
        healthyOnly);
  }

  /**
   * 响应客户端（根据 IP、端口、服务名等）查询某个实例具体信息的请求。
   * 
   * @param request
   * @return
   * @throws Exception
   */
  @RequestMapping(value = "", method = RequestMethod.GET)
  public JSONObject detail(HttpServletRequest request) throws Exception {

    String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
    String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
    String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, UtilsAndCommons.DEFAULT_CLUSTER_NAME);
    String ip = WebUtils.required(request, "ip");
    int port = Integer.parseInt(WebUtils.required(request, "port"));

    Service service = serviceManager.getService(namespaceId, serviceName);
    if (service == null) {
      throw new NacosException(NacosException.NOT_FOUND, "no service " + serviceName + " found!");
    }

    List<String> clusters = new ArrayList<>();
    clusters.add(cluster);

    List<Instance> ips = service.allIPs(clusters);
    if (ips == null || ips.isEmpty()) {
      throw new NacosException(NacosException.NOT_FOUND,
          "no ips found for cluster " + cluster + " in service " + serviceName);
    }

    for (Instance instance : ips) {
      if (instance.getIp().equals(ip) && instance.getPort() == port) {
        JSONObject result = new JSONObject();
        result.put("service", serviceName);
        result.put("ip", ip);
        result.put("port", port);
        result.put("clusterName", cluster);
        result.put("weight", instance.getWeight());
        result.put("healthy", instance.isHealthy());
        result.put("metadata", instance.getMetadata());
        result.put("instanceId", instance.generateInstanceId());
        return result;
      }
    }

    throw new NacosException(NacosException.NOT_FOUND, "no matched ip found!");
  }

  /**
   * 响应客户端发来的心跳消息。
   * 
   * @param request
   * @return
   * @throws Exception
   */
  @CanDistro
  @RequestMapping(value = "/beat", method = RequestMethod.PUT)
  public JSONObject beat(HttpServletRequest request) throws Exception {

    JSONObject result = new JSONObject();

    result.put("clientBeatInterval", switchDomain.getClientBeatInterval());

    String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
    String beat = WebUtils.required(request, "beat");
    // 将客户端 param key 为 beat 的参数值反序列化为 RsInfo
    RsInfo clientBeat = JSON.parseObject(beat, RsInfo.class);

    if (!switchDomain.isDefaultInstanceEphemeral() && !clientBeat.isEphemeral()) {
      return result;
    }

    // 如果注册时未给 cluster 命名，则默认值为 DEFAULT
    if (StringUtils.isBlank(clientBeat.getCluster())) {
      clientBeat.setCluster(UtilsAndCommons.DEFAULT_CLUSTER_NAME);
    }
    String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);

    String clusterName = clientBeat.getCluster();

    if (Loggers.SRV_LOG.isDebugEnabled()) {
      Loggers.SRV_LOG.debug("[CLIENT-BEAT] full arguments: beat: {}, serviceName: {}", clientBeat, serviceName);
    }

    Instance instance = serviceManager.getInstance(namespaceId, serviceName, clientBeat.getCluster(),
        clientBeat.getIp(), clientBeat.getPort());

    // 如果未查找到发送心跳的实例，则为该实例进行注册.
    if (instance == null) {
      instance = new Instance();
      instance.setPort(clientBeat.getPort());
      instance.setIp(clientBeat.getIp());
      instance.setWeight(clientBeat.getWeight());
      instance.setMetadata(clientBeat.getMetadata());
      instance.setClusterName(clusterName);
      instance.setServiceName(serviceName);
      instance.setInstanceId(instance.generateInstanceId());
      instance.setEphemeral(clientBeat.isEphemeral());

      serviceManager.registerInstance(namespaceId, serviceName, instance);
    }

    Service service = serviceManager.getService(namespaceId, serviceName);

    if (service == null) {
      throw new NacosException(NacosException.SERVER_ERROR, "service not found: " + serviceName + "@" + namespaceId);
    }

    // 由对应服务处理心跳消息
    service.processClientBeat(clientBeat);
    result.put("clientBeatInterval", instance.getInstanceHeartBeatInterval());
    return result;
  }

  /**
   * 响应客户端查看某个服务的某个集群全部实例及其健康状态的请求。
   * 
   * @param request
   * @return
   * @throws NacosException
   */
  @RequestMapping("/statuses")
  public JSONObject listWithHealthStatus(HttpServletRequest request) throws NacosException {

    String key = WebUtils.required(request, "key");

    String serviceName;
    String namespaceId;

    if (key.contains(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)) {
      namespaceId = key.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[0];
      serviceName = key.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[1];
    } else {
      namespaceId = Constants.DEFAULT_NAMESPACE_ID;
      serviceName = key;
    }

    Service service = serviceManager.getService(namespaceId, serviceName);

    if (service == null) {
      throw new NacosException(NacosException.NOT_FOUND, "service: " + serviceName + " not found.");
    }

    List<Instance> ips = service.allIPs();

    JSONObject result = new JSONObject();
    JSONArray ipArray = new JSONArray();

    for (Instance ip : ips) {
      ipArray.add(ip.toIPAddr() + "_" + ip.isHealthy());
    }

    result.put("ips", ipArray);
    return result;
  }

  /**
   * 从请求解析相关信息构造 Instance。
   * 
   * @param request
   * @return
   * @throws Exception
   */
  private Instance parseInstance(HttpServletRequest request) throws Exception {

    String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
    String app = WebUtils.optional(request, "app", "DEFAULT");
    String metadata = WebUtils.optional(request, "metadata", StringUtils.EMPTY);

    Instance instance = getIPAddress(request);
    instance.setApp(app);
    instance.setServiceName(serviceName);
    instance.setInstanceId(instance.generateInstanceId());
    instance.setLastBeat(System.currentTimeMillis());
    if (StringUtils.isNotEmpty(metadata)) {
      instance.setMetadata(UtilsAndCommons.parseMetadata(metadata));
    }

    if (!instance.validate()) {
      throw new NacosException(NacosException.INVALID_PARAM, "instance format invalid:" + instance);
    }

    return instance;
  }

  /**
   * 从请求提取 ip/port 等信息构造 Instance。
   * 
   * @param request
   * @return
   */
  private Instance getIPAddress(HttpServletRequest request) {

    String ip = WebUtils.required(request, "ip");
    String port = WebUtils.required(request, "port");
    String weight = WebUtils.optional(request, "weight", "1");
    String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, StringUtils.EMPTY);
    if (StringUtils.isBlank(cluster)) {
      cluster = WebUtils.optional(request, "cluster", UtilsAndCommons.DEFAULT_CLUSTER_NAME);
    }
    boolean healthy = BooleanUtils.toBoolean(WebUtils.optional(request, "healthy", "true"));

    String enabledString = WebUtils.optional(request, "enabled", StringUtils.EMPTY);
    boolean enabled;
    if (StringUtils.isBlank(enabledString)) {
      enabled = BooleanUtils.toBoolean(WebUtils.optional(request, "enable", "true"));
    } else {
      enabled = BooleanUtils.toBoolean(enabledString);
    }

    boolean ephemeral = BooleanUtils
        .toBoolean(WebUtils.optional(request, "ephemeral", String.valueOf(switchDomain.isDefaultInstanceEphemeral())));

    Instance instance = new Instance();
    instance.setPort(Integer.parseInt(port));
    instance.setIp(ip);
    instance.setWeight(Double.parseDouble(weight));
    instance.setClusterName(cluster);
    instance.setHealthy(healthy);
    instance.setEnabled(enabled);
    instance.setEphemeral(ephemeral);

    return instance;
  }

  public void checkIfDisabled(Service service) throws Exception {
    if (!service.getEnabled()) {
      throw new Exception("service is disabled now.");
    }
  }

  /**
   * 若 isCheck 为 true，则检查服务是否达到保护阈值了（健康实例比例小于等于某个值）；
   * <p>
   * 否则组装服务对应的实例列表并返回。
   * 
   * @param namespaceId
   * @param serviceName
   * @param agent
   * @param clusters
   * @param clientIP
   * @param udpPort
   * @param env
   * @param isCheck
   * @param app
   * @param tid
   * @param healthyOnly
   * @return
   * @throws Exception
   */
  public JSONObject doSrvIPXT(String namespaceId, String serviceName, String agent, String clusters, String clientIP,
      int udpPort, String env, boolean isCheck, String app, String tid, boolean healthyOnly) throws Exception {

    // 解析客户端信息(语言与版本)
    ClientInfo clientInfo = new ClientInfo(agent);
    JSONObject result = new JSONObject();
    // 获取对应 Service 对象
    Service service = serviceManager.getService(namespaceId, serviceName);

    if (service == null) {
      if (Loggers.SRV_LOG.isDebugEnabled()) {
        Loggers.SRV_LOG.debug("no instance to serve for service: " + serviceName);
      }
      result.put("name", serviceName);
      result.put("clusters", clusters);
      result.put("hosts", new JSONArray());
      return result;
    }

    // 检查服务是否被禁用
    checkIfDisabled(service);

    long cacheMillis = switchDomain.getDefaultCacheMillis();

    // now try to enable the push
    // 如果客户端支持推模式, 则启用之
    try {
      // 如果客户端传了 udp 端口, 并且客户端和当前 nacos 节点之间可以支持推送模式, 则将其加到推送服务中
      if (udpPort > 0 && pushService.canEnablePush(agent)) {
        pushService.addClient(namespaceId, serviceName, clusters, agent, new InetSocketAddress(clientIP, udpPort),
            pushDataSource, tid, app);
        cacheMillis = switchDomain.getPushCacheMillis(serviceName);
      }
    } catch (Exception e) {
      Loggers.SRV_LOG.error("[NACOS-API] failed to added push client", e);
      cacheMillis = switchDomain.getDefaultCacheMillis();
    }

    List<Instance> srvedIPs;

    // 获取服务对应的全部实例
    srvedIPs = service.srvIPs(Arrays.asList(StringUtils.split(clusters, ",")));

    // 根据自定义负载均衡筛选实例集合子集
    // filter ips using selector:
    if (service.getSelector() != null && StringUtils.isNotBlank(clientIP)) {
      srvedIPs = service.getSelector().select(clientIP, srvedIPs);
    }

    if (CollectionUtils.isEmpty(srvedIPs)) {

      if (Loggers.SRV_LOG.isDebugEnabled()) {
        Loggers.SRV_LOG.debug("no instance to serve for service: " + serviceName);
      }

      if (clientInfo.type == ClientInfo.ClientType.JAVA
          && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
        // 如果客户端语言为 Java, 且版本号不小于 1.0.0, 则把 serviceNameWithGroupName 作为 serviceName
        result.put("dom", serviceName);
      } else {
        // 否则根据分隔符(group@@serviceName)把 serviceName 部分提取出来
        result.put("dom", NamingUtils.getServiceName(serviceName));
      }

      result.put("hosts", new JSONArray());
      result.put("name", serviceName);
      result.put("cacheMillis", cacheMillis);
      result.put("lastRefTime", System.currentTimeMillis());
      result.put("checksum", service.getChecksum());
      result.put("useSpecifiedURL", false);
      result.put("clusters", clusters);
      result.put("env", env);
      result.put("metadata", service.getMetadata());
      return result;
    }

    Map<Boolean, List<Instance>> ipMap = new HashMap<>(2);
    ipMap.put(Boolean.TRUE, new ArrayList<>());
    ipMap.put(Boolean.FALSE, new ArrayList<>());

    for (Instance ip : srvedIPs) {
      ipMap.get(ip.isHealthy()).add(ip);
    }

    if (isCheck) {
      result.put("reachProtectThreshold", false);
    }

    // 查询服务保护阈值
    double threshold = service.getProtectThreshold();

    // 如果健康实例数目/总实例数 <= protectThreshold，表示达到保护阈值了。
    if ((float) ipMap.get(Boolean.TRUE).size() / srvedIPs.size() <= threshold) {

      Loggers.SRV_LOG.warn("protect threshold reached, return all ips, service: {}", serviceName);
      if (isCheck) {
        result.put("reachProtectThreshold", true);
      }

      ipMap.get(Boolean.TRUE).addAll(ipMap.get(Boolean.FALSE));
      ipMap.get(Boolean.FALSE).clear();
    }

    // 如果仅为查询服务是否达到保护阈值，到此处可直接返回了。客户端会检查上面设置的 header、
    if (isCheck) {
      result.put("protectThreshold", service.getProtectThreshold());
      result.put("reachLocalSiteCallThreshold", false);

      return new JSONObject();
    }

    // 否则组装实例列表并返回
    JSONArray hosts = new JSONArray();

    for (Map.Entry<Boolean, List<Instance>> entry : ipMap.entrySet()) {
      List<Instance> ips = entry.getValue();

      if (healthyOnly && !entry.getKey()) {
        continue;
      }

      for (Instance instance : ips) {

        // remove disabled instance:
        if (!instance.isEnabled()) {
          continue;
        }

        JSONObject ipObj = new JSONObject();

        ipObj.put("ip", instance.getIp());
        ipObj.put("port", instance.getPort());
        // deprecated since nacos 1.0.0:
        ipObj.put("valid", entry.getKey());
        ipObj.put("healthy", entry.getKey());
        ipObj.put("marked", instance.isMarked());
        ipObj.put("instanceId", instance.getInstanceId());
        ipObj.put("metadata", instance.getMetadata());
        ipObj.put("enabled", instance.isEnabled());
        ipObj.put("weight", instance.getWeight());
        ipObj.put("clusterName", instance.getClusterName());
        if (clientInfo.type == ClientInfo.ClientType.JAVA
            && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
          ipObj.put("serviceName", instance.getServiceName());
        } else {
          ipObj.put("serviceName", NamingUtils.getServiceName(instance.getServiceName()));
        }

        ipObj.put("ephemeral", instance.isEphemeral());
        hosts.add(ipObj);

      }
    }

    result.put("hosts", hosts);
    if (clientInfo.type == ClientInfo.ClientType.JAVA
        && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
      result.put("dom", serviceName);
    } else {
      result.put("dom", NamingUtils.getServiceName(serviceName));
    }
    result.put("name", serviceName);
    result.put("cacheMillis", cacheMillis);
    result.put("lastRefTime", System.currentTimeMillis());
    result.put("checksum", service.getChecksum());
    result.put("useSpecifiedURL", false);
    result.put("clusters", clusters);
    result.put("env", env);
    result.put("metadata", service.getMetadata());
    return result;
  }
}
