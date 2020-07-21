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

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.core.utils.SystemUtils;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.cluster.ServerListManager;
import com.alibaba.nacos.naming.cluster.ServerStatusManager;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftCore;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeer;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeerSet;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.misc.*;
import com.alibaba.nacos.naming.pojo.ClusterStateView;
import com.alibaba.nacos.naming.push.PushService;
import com.alibaba.nacos.naming.web.NeedAuth;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Operation for operators
 * <p>
 * 运维接口, 管理员可以通过提供的接口对当前 nacos 节点进行一些查询和更改.
 *
 * @author nkorange
 */
@RestController
@RequestMapping({ UtilsAndCommons.NACOS_NAMING_CONTEXT + "/operator", UtilsAndCommons.NACOS_NAMING_CONTEXT + "/ops" })
public class OperatorController {

  @Autowired
  private PushService pushService;

  @Autowired
  private SwitchManager switchManager;

  @Autowired
  private ServiceManager serviceManager;

  @Autowired
  private ServerListManager serverListManager;

  @Autowired
  private ServerStatusManager serverStatusManager;

  @Autowired
  private SwitchDomain switchDomain;

  @Autowired
  private DistroMapper distroMapper;

  @Autowired
  private RaftCore raftCore;

  @Autowired
  private RaftPeerSet raftPeerSet;

  /**
   * 查询或者重置当前 nacos 节点得推送统计.
   * 
   * @param request
   * @return
   */
  @RequestMapping("/push/state")
  public JSONObject pushState(HttpServletRequest request) {

    JSONObject result = new JSONObject();

    boolean detail = Boolean.parseBoolean(WebUtils.optional(request, "detail", "false"));
    boolean reset = Boolean.parseBoolean(WebUtils.optional(request, "reset", "false"));

    List<PushService.Receiver.AckEntry> failedPushes = PushService.getFailedPushes();
    int failedPushCount = pushService.getFailedPushCount();
    result.put("succeed", pushService.getTotalPush() - failedPushCount);
    result.put("total", pushService.getTotalPush());

    if (pushService.getTotalPush() > 0) {
      result.put("ratio", ((float) pushService.getTotalPush() - failedPushCount) / pushService.getTotalPush());
    } else {
      result.put("ratio", 0);
    }

    JSONArray dataArray = new JSONArray();
    if (detail) {
      for (PushService.Receiver.AckEntry entry : failedPushes) {
        try {
          dataArray.add(new String(entry.origin.getData(), "UTF-8"));
        } catch (UnsupportedEncodingException e) {
          dataArray.add("[encoding failure]");
        }
      }
      result.put("data", dataArray);
    }

    if (reset) {
      PushService.resetPushState();
    }

    result.put("reset", reset);

    return result;
  }

  /**
   * 返回当前全部开关设置.
   * 
   * @param request
   * @return
   */
  @RequestMapping(value = "/switches", method = RequestMethod.GET)
  public SwitchDomain switches(HttpServletRequest request) {
    return switchDomain;
  }

  /**
   * 更新某个开关项.
   * 
   * @param request
   * @return
   * @throws Exception
   */
  @NeedAuth
  @RequestMapping(value = "/switches", method = RequestMethod.PUT)
  public String updateSwitch(HttpServletRequest request) throws Exception {
    Boolean debug = Boolean.parseBoolean(WebUtils.optional(request, "debug", "false"));
    String entry = WebUtils.required(request, "entry");
    String value = WebUtils.required(request, "value");

    switchManager.update(entry, value, debug);

    return "ok";
  }

  /**
   * 返回当前 nacos 节点的状态(维护的服务数,实例数,负载,内存等等).
   * 
   * @param request
   * @return
   */
  @RequestMapping(value = "/metrics", method = RequestMethod.GET)
  public JSONObject metrics(HttpServletRequest request) {

    JSONObject result = new JSONObject();

    int serviceCount = serviceManager.getServiceCount();
    int ipCount = serviceManager.getInstanceCount();

    int responsibleDomCount = serviceManager.getResponsibleServiceCount();
    int responsibleIPCount = serviceManager.getResponsibleInstanceCount();

    result.put("status", serverStatusManager.getServerStatus().name());
    result.put("serviceCount", serviceCount);
    result.put("instanceCount", ipCount);
    result.put("raftNotifyTaskCount", raftCore.getNotifyTaskCount());
    result.put("responsibleServiceCount", responsibleDomCount);
    result.put("responsibleInstanceCount", responsibleIPCount);
    result.put("cpu", SystemUtils.getCPU());
    result.put("load", SystemUtils.getLoad());
    result.put("mem", SystemUtils.getMem());

    return result;
  }

  /**
   * 响应外部查询某个服务由哪个 nacos 节点管理,返回其地址.
   * 
   * @param request
   * @return
   */
  @RequestMapping(value = "/distro/server", method = RequestMethod.GET)
  public JSONObject getResponsibleServer4Service(HttpServletRequest request) {
    String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
    String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
    Service service = serviceManager.getService(namespaceId, serviceName);

    if (service == null) {
      throw new IllegalArgumentException("service not found");
    }

    JSONObject result = new JSONObject();

    result.put("responsibleServer", distroMapper.mapSrv(serviceName));

    return result;
  }

  /**
   * 查看或清理(参数 action 取值决定)遵从 Partition 协议的 nacos 节点的状态.
   * 
   * @param request
   * @return
   */
  @RequestMapping(value = "/distro/status", method = RequestMethod.GET)
  public JSONObject distroStatus(HttpServletRequest request) {

    JSONObject result = new JSONObject();
    String action = WebUtils.optional(request, "action", "view");

    if (StringUtils.equals(SwitchEntry.ACTION_VIEW, action)) {
      result.put("status", serverListManager.getDistroConfig());
      return result;
    }

    if (StringUtils.equals(SwitchEntry.ACTION_CLEAN, action)) {
      serverListManager.clean();
      return result;
    }

    return result;
  }

  /**
   * 响应查询当前 nacos 集群状态的请求, 会返回全部(或健康,取决于)节点的状态信息.
   * 
   * @param request
   * @return
   */
  @RequestMapping(value = "/servers", method = RequestMethod.GET)
  public JSONObject getHealthyServerList(HttpServletRequest request) {

    boolean healthy = Boolean.parseBoolean(WebUtils.optional(request, "healthy", "false"));
    JSONObject result = new JSONObject();
    if (healthy) {
      result.put("servers", serverListManager.getHealthyServers());
    } else {
      result.put("servers", serverListManager.getServers());
    }

    return result;
  }

  /**
   * 接收其它 nacos 节点发来的服务器状态报告, 形如(已进行 URL 解码):
   * <p>
   * GET
   * /nacos/v1/ns/operator/server/status?encoding=UTF-8&serverStatus=unknown#172.21.51.101:8848#1594774221278#16&nofix=1
   * 
   * @param request
   * @return
   */
  @RequestMapping("/server/status")
  public String serverStatus(HttpServletRequest request) {
    String serverStatus = WebUtils.required(request, "serverStatus");
    serverListManager.onReceiveServerStatus(serverStatus);
    return "ok";
  }

  /**
   * 通过 HTTP 接口设置当前 nacos 节点指定 logger 的 log level.
   * 
   * @param request
   * @return
   */
  @RequestMapping(value = "/log", method = RequestMethod.PUT)
  public String setLogLevel(HttpServletRequest request) {
    String logName = WebUtils.required(request, "logName");
    String logLevel = WebUtils.required(request, "logLevel");
    Loggers.setLogLevel(logName, logLevel);
    return "ok";
  }

  /**
   * 分页返回 nacos 集群节点机器状态.
   * 
   * @param request
   * @return
   */
  @RequestMapping(value = "/cluster/states", method = RequestMethod.GET)
  public Object listStates(HttpServletRequest request) {

    String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
    JSONObject result = new JSONObject();
    int page = Integer.parseInt(WebUtils.required(request, "pageNo"));
    int pageSize = Integer.parseInt(WebUtils.required(request, "pageSize"));
    String keyword = WebUtils.optional(request, "keyword", StringUtils.EMPTY);
    String containedInstance = WebUtils.optional(request, "instance", StringUtils.EMPTY);

    List<RaftPeer> raftPeerLists = new ArrayList<>();

    int total = serviceManager.getPagedClusterState(namespaceId, page - 1, pageSize, keyword, containedInstance,
        raftPeerLists, raftPeerSet);

    if (CollectionUtils.isEmpty(raftPeerLists)) {
      result.put("clusterStateList", Collections.emptyList());
      result.put("count", 0);
      return result;
    }

    JSONArray clusterStateJsonArray = new JSONArray();
    for (RaftPeer raftPeer : raftPeerLists) {
      ClusterStateView clusterStateView = new ClusterStateView();
      clusterStateView.setClusterTerm(raftPeer.term.intValue());
      clusterStateView.setNodeIp(raftPeer.ip);
      clusterStateView.setNodeState(raftPeer.state.name());
      clusterStateView.setVoteFor(raftPeer.voteFor);
      clusterStateView.setHeartbeatDueMs(raftPeer.heartbeatDueMs);
      clusterStateView.setLeaderDueMs(raftPeer.leaderDueMs);
      clusterStateJsonArray.add(clusterStateView);
    }
    result.put("clusterStateList", clusterStateJsonArray);
    result.put("count", total);
    return result;
  }
}
