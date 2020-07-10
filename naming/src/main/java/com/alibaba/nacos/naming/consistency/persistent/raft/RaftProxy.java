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

import com.alibaba.nacos.naming.boot.RunningConfig;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import org.springframework.http.HttpMethod;
import org.springframework.stereotype.Component;

import java.net.HttpURLConnection;
import java.util.Map;

/**
 * RaftProxy 提供了 raft 集群内节点之间通信用的 RPC 实现
 * 
 * @author nacos
 */
@Component
public class RaftProxy {

  /**
   * 转发 GET 请求给拼装好的 URL
   *
   * @param server
   * @param api
   * @param params
   * @throws Exception
   */
  public void proxyGET(String server, String api, Map<String, String> params) throws Exception {
    // do proxy
    if (!server.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
      server = server + UtilsAndCommons.IP_PORT_SPLITER + RunningConfig.getServerPort();
    }
    String url = "http://" + server + RunningConfig.getContextPath() + api;

    HttpClient.HttpResult result = HttpClient.httpGet(url, null, params);
    if (result.code != HttpURLConnection.HTTP_OK) {
      throw new IllegalStateException("leader failed, caused by: " + result.content);
    }
  }

  /**
   * 比较通用的方法，用户可以指定 HTTP METHOD
   *
   * @param server
   * @param api
   * @param params
   * @param method
   * @throws Exception
   */
  public void proxy(String server, String api, Map<String, String> params, HttpMethod method) throws Exception {
    // do proxy
    if (!server.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
      server = server + UtilsAndCommons.IP_PORT_SPLITER + RunningConfig.getServerPort();
    }
    String url = "http://" + server + RunningConfig.getContextPath() + api;
    HttpClient.HttpResult result;
    switch (method) {
      case GET:
        result = HttpClient.httpGet(url, null, params);
        break;
      case POST:
        result = HttpClient.httpPost(url, null, params);
        break;
      case DELETE:
        result = HttpClient.httpDelete(url, null, params);
        break;
      default:
        throw new RuntimeException("unsupported method:" + method);
    }

    if (result.code != HttpURLConnection.HTTP_OK) {
      throw new IllegalStateException("leader failed, caused by: " + result.content);
    }
  }

  /**
   * 转发大数据量的 POST 请求到拼装好的 URL
   *
   * @param server  目标机器的 IP[:port]
   * @param api     restful api，URL path 的一部分，与 server 一起组装成目标 URL
   * @param content POST body
   * @param headers POST headers
   * @throws Exception
   */
  public void proxyPostLarge(String server, String api, String content, Map<String, String> headers) throws Exception {
    // do proxy
    if (!server.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
      server = server + UtilsAndCommons.IP_PORT_SPLITER + RunningConfig.getServerPort();
    }
    String url = "http://" + server + RunningConfig.getContextPath() + api;

    HttpClient.HttpResult result = HttpClient.httpPostLarge(url, headers, content);
    if (result.code != HttpURLConnection.HTTP_OK) {
      throw new IllegalStateException("leader failed, caused by: " + result.content);
    }
  }
}
