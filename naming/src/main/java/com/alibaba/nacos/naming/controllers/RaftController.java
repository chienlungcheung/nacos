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
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.common.util.IoUtils;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftConsistencyServiceImpl;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftCore;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeer;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.exception.NacosException;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.web.NeedAuth;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Methods for Raft consistency protocol. These methods should only be invoked
 * by Nacos server itself.
 * <p>
 * Raft 相关的 REST API 接口, 处理诸如选举, 心跳, 读数据, 写数据, 数据同步等请求.
 * <p>
 * 这些方法只能被 nacos 自身调用, 而不能被当作库给客户端用.
 *
 * @author nkorange
 * @since 1.0.0
 */
@RestController
@RequestMapping({ UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft",
    UtilsAndCommons.NACOS_SERVER_CONTEXT + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/raft" })
public class RaftController {

  @Autowired
  private RaftConsistencyServiceImpl raftConsistencyService;

  @Autowired
  private ServiceManager serviceManager;

  @Autowired
  private RaftCore raftCore;

  /**
   * 响应想当 leader 的 raft 节点发起的投票请求.
   * 
   * @param request
   * @param response
   * @return
   * @throws Exception
   */
  @NeedAuth
  @RequestMapping(value = "/vote", method = RequestMethod.POST)
  public JSONObject vote(HttpServletRequest request, HttpServletResponse response) throws Exception {

    RaftPeer peer = raftCore.receivedVote(JSON.parseObject(WebUtils.required(request, "vote"), RaftPeer.class));

    return JSON.parseObject(JSON.toJSONString(peer));
  }

  /**
   * 响应 raft leader 节点发起的心跳请求(可能带有日志数据).
   * 
   * @param request
   * @param response
   * @return
   * @throws Exception
   */
  @NeedAuth
  @RequestMapping(value = "/beat", method = RequestMethod.POST)
  public JSONObject beat(HttpServletRequest request, HttpServletResponse response) throws Exception {

    String entity = new String(IoUtils.tryDecompress(request.getInputStream()), StandardCharsets.UTF_8);
    String value = URLDecoder.decode(entity, "UTF-8");
    value = URLDecoder.decode(value, "UTF-8");

    JSONObject json = JSON.parseObject(value);
    JSONObject beat = JSON.parseObject(json.getString("beat"));

    RaftPeer peer = raftCore.receivedBeat(beat);

    return JSON.parseObject(JSON.toJSONString(peer));
  }

  /**
   * 响应外部查询当前节点信息的请求.
   * 
   * @param request
   * @param response
   * @return
   */
  @NeedAuth
  @RequestMapping(value = "/peer", method = RequestMethod.GET)
  public JSONObject getPeer(HttpServletRequest request, HttpServletResponse response) {
    List<RaftPeer> peers = raftCore.getPeers();
    RaftPeer peer = null;

    for (RaftPeer peer1 : peers) {
      if (StringUtils.equals(peer1.ip, NetUtils.localServer())) {
        peer = peer1;
      }
    }

    if (peer == null) {
      peer = new RaftPeer();
      peer.ip = NetUtils.localServer();
    }

    return JSON.parseObject(JSON.toJSONString(peer));
  }

  /**
   * 接收外部要求本节点重加载某个数据的请求.
   * 
   * @param request
   * @param response
   * @return
   * @throws Exception
   */
  @NeedAuth
  @RequestMapping(value = "/datum/reload", method = RequestMethod.PUT)
  public String reloadDatum(HttpServletRequest request, HttpServletResponse response) throws Exception {
    String key = WebUtils.required(request, "key");
    raftCore.loadDatum(key);
    return "ok";
  }

  /**
   * 响应外部数据写请求.
   * 
   * @param request
   * @param response
   * @return
   * @throws Exception
   */
  @NeedAuth
  @RequestMapping(value = "/datum", method = RequestMethod.POST)
  public String publish(HttpServletRequest request, HttpServletResponse response) throws Exception {

    response.setHeader("Content-Type", "application/json; charset=" + getAcceptEncoding(request));
    response.setHeader("Cache-Control", "no-cache");
    response.setHeader("Content-Encode", "gzip");

    String entity = IOUtils.toString(request.getInputStream(), "UTF-8");
    String value = URLDecoder.decode(entity, "UTF-8");
    JSONObject json = JSON.parseObject(value);

    String key = json.getString("key");
    if (KeyBuilder.matchInstanceListKey(key)) {
      raftConsistencyService.put(key, JSON.parseObject(json.getString("value"), Instances.class));
      return "ok";
    }

    if (KeyBuilder.matchSwitchKey(key)) {
      raftConsistencyService.put(key, JSON.parseObject(json.getString("value"), SwitchDomain.class));
      return "ok";
    }

    if (KeyBuilder.matchServiceMetaKey(key)) {
      raftConsistencyService.put(key, JSON.parseObject(json.getString("value"), Service.class));
      return "ok";
    }

    throw new NacosException(NacosException.INVALID_PARAM, "unknown type publish key: " + key);
  }

  /**
   * 响应外部的数据删除请求.
   */
  @NeedAuth
  @RequestMapping(value = "/datum", method = RequestMethod.DELETE)
  public String delete(HttpServletRequest request, HttpServletResponse response) throws Exception {

    response.setHeader("Content-Type", "application/json; charset=" + getAcceptEncoding(request));
    response.setHeader("Cache-Control", "no-cache");
    response.setHeader("Content-Encode", "gzip");
    raftConsistencyService.remove(WebUtils.required(request, "key"));
    return "ok";
  }

  /**
   * 响应外部的数据读请求.
   * 
   * @param request
   * @param response
   * @return
   * @throws Exception
   */
  @NeedAuth
  @RequestMapping(value = "/datum", method = RequestMethod.GET)
  public String get(HttpServletRequest request, HttpServletResponse response) throws Exception {

    response.setHeader("Content-Type", "application/json; charset=" + getAcceptEncoding(request));
    response.setHeader("Cache-Control", "no-cache");
    response.setHeader("Content-Encode", "gzip");
    String keysString = WebUtils.required(request, "keys");
    keysString = URLDecoder.decode(keysString, "UTF-8");
    String[] keys = keysString.split(",");
    List<Datum> datums = new ArrayList<Datum>();

    for (String key : keys) {
      Datum datum = raftCore.getDatum(key);
      datums.add(datum);
    }

    return JSON.toJSONString(datums);
  }

  /**
   * 响应外部集群状态查询请求(多少服务发现, 多少 raft 节点等)
   */
  @RequestMapping(value = "/state", method = RequestMethod.GET)
  public JSONObject state(HttpServletRequest request, HttpServletResponse response) throws Exception {

    response.setHeader("Content-Type", "application/json; charset=" + getAcceptEncoding(request));
    response.setHeader("Cache-Control", "no-cache");
    response.setHeader("Content-Encode", "gzip");

    JSONObject result = new JSONObject();
    result.put("services", serviceManager.getServiceCount());
    result.put("peers", raftCore.getPeers());

    return result;
  }

  /**
   * 响应其它 raft 节点(必须是 leader)的发布数据请求, 对应 AppendEntries RPC.
   * 
   * @param request
   * @param response
   * @return
   * @throws Exception
   */
  @NeedAuth
  @RequestMapping(value = "/datum/commit", method = RequestMethod.POST)
  public String onPublish(HttpServletRequest request, HttpServletResponse response) throws Exception {

    response.setHeader("Content-Type", "application/json; charset=" + getAcceptEncoding(request));
    response.setHeader("Cache-Control", "no-cache");
    response.setHeader("Content-Encode", "gzip");

    String entity = IOUtils.toString(request.getInputStream(), "UTF-8");
    String value = URLDecoder.decode(entity, "UTF-8");
    JSONObject jsonObject = JSON.parseObject(value);
    String key = "key";

    RaftPeer source = JSON.parseObject(jsonObject.getString("source"), RaftPeer.class);
    JSONObject datumJson = jsonObject.getJSONObject("datum");

    Datum datum = null;
    if (KeyBuilder.matchInstanceListKey(datumJson.getString(key))) {
      datum = JSON.parseObject(jsonObject.getString("datum"), new TypeReference<Datum<Instances>>() {
      });
    } else if (KeyBuilder.matchSwitchKey(datumJson.getString(key))) {
      datum = JSON.parseObject(jsonObject.getString("datum"), new TypeReference<Datum<SwitchDomain>>() {
      });
    } else if (KeyBuilder.matchServiceMetaKey(datumJson.getString(key))) {
      datum = JSON.parseObject(jsonObject.getString("datum"), new TypeReference<Datum<Service>>() {
      });
    }

    raftConsistencyService.onPut(datum, source);
    return "ok";
  }

  /**
   * 响应其它 raft 节点(此处必须为 leader)的数据删除请求.
   * 
   * @param request
   * @param response
   * @return
   * @throws Exception
   */
  @NeedAuth
  @RequestMapping(value = "/datum/commit", method = RequestMethod.DELETE)
  public String onDelete(HttpServletRequest request, HttpServletResponse response) throws Exception {

    response.setHeader("Content-Type", "application/json; charset=" + getAcceptEncoding(request));
    response.setHeader("Cache-Control", "no-cache");
    response.setHeader("Content-Encode", "gzip");

    String entity = IOUtils.toString(request.getInputStream(), "UTF-8");
    String value = URLDecoder.decode(entity, "UTF-8");
    value = URLDecoder.decode(value, "UTF-8");
    JSONObject jsonObject = JSON.parseObject(value);

    Datum datum = JSON.parseObject(jsonObject.getString("datum"), Datum.class);
    RaftPeer source = JSON.parseObject(jsonObject.getString("source"), RaftPeer.class);

    raftConsistencyService.onRemove(datum, source);
    return "ok";
  }

  /**
   * 响应外部查询当前 leader 是谁的请求.
   */
  @RequestMapping(value = "/leader", method = RequestMethod.GET)
  public JSONObject getLeader(HttpServletRequest request, HttpServletResponse response) {

    JSONObject result = new JSONObject();
    result.put("leader", JSONObject.toJSONString(raftCore.getLeader()));
    return result;
  }

  @RequestMapping(value = "/listeners", method = RequestMethod.GET)
  public JSONObject getAllListeners(HttpServletRequest request, HttpServletResponse response) {

    JSONObject result = new JSONObject();
    Map<String, List<RecordListener>> listeners = raftCore.getListeners();

    JSONArray listenerArray = new JSONArray();
    for (String key : listeners.keySet()) {
      listenerArray.add(key);
    }
    result.put("listeners", listenerArray);

    return result;
  }

  public static String getAcceptEncoding(HttpServletRequest req) {
    String encode = StringUtils.defaultIfEmpty(req.getHeader("Accept-Charset"), "UTF-8");
    encode = encode.contains(",") ? encode.substring(0, encode.indexOf(",")) : encode;
    return encode.contains(";") ? encode.substring(0, encode.indexOf(";")) : encode;
  }
}
