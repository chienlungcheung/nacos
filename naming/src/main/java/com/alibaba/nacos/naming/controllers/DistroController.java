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
import com.alibaba.fastjson.TypeReference;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.DataStore;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.DistroConsistencyServiceImpl;
import com.alibaba.nacos.naming.core.Instances;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.exception.NacosException;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Restful methods for Partition protocol.
 * <p>
 * 针对 Partition 一致性算法的 http 接口.
 * 
 * @author nkorange
 * @since 1.0.0
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + "/distro")
public class DistroController {

  @Autowired
  private Serializer serializer;

  @Autowired
  private DistroConsistencyServiceImpl consistencyService;

  @Autowired
  private DataStore dataStore;

  @Autowired
  private ServiceManager serviceManager;

  @Autowired
  private SwitchDomain switchDomain;

  /**
   * 接收处理其它 nacos 节点发来的服务发现相关的信息([service-key: Instances]), 这些信息将会用来更新本地内存数据.
   * 
   * @param request
   * @param response
   * @return
   * @throws Exception
   */
  @RequestMapping(value = "/datum", method = RequestMethod.PUT)
  public String onSyncDatum(HttpServletRequest request, HttpServletResponse response) throws Exception {

    String entity = IOUtils.toString(request.getInputStream(), "UTF-8");

    if (StringUtils.isBlank(entity)) {
      Loggers.DISTRO.error("[onSync] receive empty entity!");
      throw new NacosException(NacosException.INVALID_PARAM, "receive empty entity!");
    }

    Map<String, Datum<Instances>> dataMap = serializer.deserializeMap(entity.getBytes(), Instances.class);

    for (Map.Entry<String, Datum<Instances>> entry : dataMap.entrySet()) {
      // 只处理临时的实例数据(这是 Partiton 算法负责的)
      if (KeyBuilder.matchEphemeralInstanceListKey(entry.getKey())) {
        String namespaceId = KeyBuilder.getNamespace(entry.getKey());
        String serviceName = KeyBuilder.getServiceName(entry.getKey());
        if (!serviceManager.containService(namespaceId, serviceName) && switchDomain.isDefaultInstanceEphemeral()) {
          serviceManager.createEmptyService(namespaceId, serviceName, true);
        }
        // 将服务信息放到本地内存存储
        consistencyService.onPut(entry.getKey(), entry.getValue().value);
      }
    }
    return "ok";
  }

  /**
   * 接收其它 nacos 节点发来的服务校验和信息, 与本地比较后进行更新或者删除.
   * 
   * @param request
   * @param response
   * @return
   * @throws Exception
   */
  @RequestMapping(value = "/checksum", method = RequestMethod.PUT)
  public String syncChecksum(HttpServletRequest request, HttpServletResponse response) throws Exception {
    String source = WebUtils.required(request, "source");
    String entity = IOUtils.toString(request.getInputStream(), "UTF-8");
    Map<String, String> dataMap = serializer.deserialize(entity.getBytes(), new TypeReference<Map<String, String>>() {
    });
    consistencyService.onReceiveChecksums(dataMap, source);
    return "ok";
  }

  /**
   * 响应其它 nacos 节点的查询请求, 根据对方传来的服务 key, 从本地内存存储查询对应的实例列表信息.
   * 
   * @param request
   * @param response
   * @throws Exception
   */
  @RequestMapping(value = "/datum", method = RequestMethod.GET)
  public void get(HttpServletRequest request, HttpServletResponse response) throws Exception {

    String entity = IOUtils.toString(request.getInputStream(), "UTF-8");
    String keys = JSON.parseObject(entity).getString("keys");
    String keySplitter = ",";
    Map<String, Datum> datumMap = new HashMap<>(64);
    for (String key : keys.split(keySplitter)) {
      datumMap.put(key, consistencyService.get(key));
    }
    response.getWriter().write(new String(serializer.serialize(datumMap), StandardCharsets.UTF_8));
  }

  /**
   * 响应其它 nacos 节点全量数据查询请求, 会把本地内存存储全部数据(都是服务发现相关的)返回给对方.
   * 
   * @param request
   * @param response
   * @throws Exception
   */
  @RequestMapping(value = "/datums", method = RequestMethod.GET)
  public void getAllDatums(HttpServletRequest request, HttpServletResponse response) throws Exception {
    response.getWriter().write(new String(serializer.serialize(dataStore.getDataMap()), StandardCharsets.UTF_8));
  }
}
