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
package com.alibaba.nacos.naming.misc;

import com.alibaba.nacos.naming.boot.RunningConfig;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.springframework.util.StringUtils;

import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;

/**
 * Report local server status to other server
 *
 * 把本地服务器的状态信息报告给其它服务器的 operator/server/status 接口。
 *
 * @author nacos
 */
public class ServerStatusSynchronizer implements Synchronizer {
  @Override
  public void send(final String serverIP, Message msg) {
    if (StringUtils.isEmpty(serverIP)) {
      return;
    }

    final Map<String, String> params = new HashMap<String, String>(2);

    params.put("serverStatus", msg.getData());

    // 注意只有 IP 是传进来的，端口和上下文路径用的都是跟本机相同的。
    // 这跟 nacos 启动配置有关。
    // 如果 cluster.conf 仅包含 IP，这就意味着每个 node 只能部署一各 naocs 实例，且全部实例端口要相同。
    // 如果 cluster.conf 列出的某个实例包含了端口号，则其就用接下来的拼接方式。
    String url = "http://" + serverIP + ":" + RunningConfig.getServerPort() + RunningConfig.getContextPath()
        + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/operator/server/status";

    if (serverIP.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
      url = "http://" + serverIP + RunningConfig.getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT
          + "/operator/server/status";
    }

    try {
      HttpClient.asyncHttpGet(url, null, params, new AsyncCompletionHandler() {
        @Override
        public Integer onCompleted(Response response) throws Exception {
          if (response.getStatusCode() != HttpURLConnection.HTTP_OK) {
            Loggers.SRV_LOG.warn("[STATUS-SYNCHRONIZE] failed to request serverStatus, remote server: {}", serverIP);

            return 1;
          }
          return 0;
        }
      });
    } catch (Exception e) {
      Loggers.SRV_LOG.warn("[STATUS-SYNCHRONIZE] failed to request serverStatus, remote server: {}", serverIP, e);
    }
  }

  @Override
  public Message get(String server, String key) {
    return null;
  }
}
