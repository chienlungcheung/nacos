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

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.boot.RunningConfig;
import com.ning.http.client.AsyncCompletionHandler;
import com.ning.http.client.Response;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.*;

/**
 * @author nacos
 */
public class NamingProxy {

  private static final String DATA_ON_SYNC_URL = "/distro/datum";

  private static final String DATA_GET_URL = "/distro/datum";

  private static final String ALL_DATA_GET_URL = "/distro/datums";

  private static final String TIMESTAMP_SYNC_URL = "/distro/checksum";

  /**
   * 将时间戳同步给地址为 server 的 distro/checksum 接口（异步模式发送）
   * 
   * @param checksumMap
   * @param server
   */
  public static void syncCheckSums(Map<String, String> checksumMap, String server) {

    try {
      Map<String, String> headers = new HashMap<>(128);

      headers.put("Client-Version", UtilsAndCommons.SERVER_VERSION);
      headers.put("User-Agent", UtilsAndCommons.SERVER_VERSION);
      headers.put("Connection", "Keep-Alive");

      HttpClient.asyncHttpPutLarge(
          "http://" + server + RunningConfig.getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT
              + TIMESTAMP_SYNC_URL + "?source=" + NetUtils.localServer(),
          headers, JSON.toJSONBytes(checksumMap), new AsyncCompletionHandler() {
            @Override
            public Object onCompleted(Response response) throws Exception {
              if (HttpURLConnection.HTTP_OK != response.getStatusCode()) {
                Loggers.DISTRO.error(
                    "failed to req API: {}, code: {}, msg: {}", "http://" + server + RunningConfig.getContextPath()
                        + UtilsAndCommons.NACOS_NAMING_CONTEXT + TIMESTAMP_SYNC_URL,
                    response.getStatusCode(), response.getResponseBody());
              }
              return null;
            }

            @Override
            public void onThrowable(Throwable t) {
              Loggers.DISTRO.error("failed to req API:" + "http://" + server + RunningConfig.getContextPath()
                  + UtilsAndCommons.NACOS_NAMING_CONTEXT + TIMESTAMP_SYNC_URL, t);
            }
          });
    } catch (Exception e) {
      Loggers.DISTRO.warn("NamingProxy", e);
    }
  }

  /**
   * 从地址为 server 的 nacos 实例的 distro/datum 接口拉取与 keys 对应的服务信息
   * 
   * @param keys
   * @param server
   * @return
   * @throws Exception
   */
  public static byte[] getData(List<String> keys, String server) throws Exception {

    Map<String, String> params = new HashMap<>(8);
    params.put("keys", StringUtils.join(keys, ","));
    HttpClient.HttpResult result = HttpClient.httpGetLarge(
        "http://" + server + RunningConfig.getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT + DATA_GET_URL,
        new HashMap<>(8), JSON.toJSONString(params));

    if (HttpURLConnection.HTTP_OK == result.code) {
      return result.content.getBytes();
    }

    throw new IOException("failed to req API: " + "http://" + server + RunningConfig.getContextPath()
        + UtilsAndCommons.NACOS_NAMING_CONTEXT + DATA_GET_URL + ". code: " + result.code + " msg: " + result.content);
  }

  /**
   * 从地址为 server 的 nacos 实例的 distro/datum 接口拉取数据（以同步请求模式）
   * 
   * @param server
   * @return
   * @throws Exception
   */
  public static byte[] getAllData(String server) throws Exception {

    Map<String, String> params = new HashMap<>(8);
    HttpClient.HttpResult result = HttpClient.httpGet(
        "http://" + server + RunningConfig.getContextPath() + UtilsAndCommons.NACOS_NAMING_CONTEXT + ALL_DATA_GET_URL,
        new ArrayList<>(), params);

    if (HttpURLConnection.HTTP_OK == result.code) {
      return result.content.getBytes();
    }

    throw new IOException("failed to req API: " + "http://" + server + RunningConfig.getContextPath()
        + UtilsAndCommons.NACOS_NAMING_CONTEXT + DATA_GET_URL + ". code: " + result.code + " msg: " + result.content);
  }

  /**
   * 将序列化后的数据 data 同步到地址为 curServer 的 distro/datum 接口（同步模式发送）
   * 
   * @param data
   * @param curServer
   * @return
   * @throws Exception
   */
  public static boolean syncData(byte[] data, String curServer) throws Exception {
    try {
      Map<String, String> headers = new HashMap<>(128);

      headers.put("Client-Version", UtilsAndCommons.SERVER_VERSION);
      headers.put("User-Agent", UtilsAndCommons.SERVER_VERSION);
      headers.put("Accept-Encoding", "gzip,deflate,sdch");
      headers.put("Connection", "Keep-Alive");
      headers.put("Content-Encoding", "gzip");

      HttpClient.HttpResult result = HttpClient.httpPutLarge("http://" + curServer + RunningConfig.getContextPath()
          + UtilsAndCommons.NACOS_NAMING_CONTEXT + DATA_ON_SYNC_URL, headers, data);

      if (HttpURLConnection.HTTP_OK == result.code) {
        return true;
      }

      if (HttpURLConnection.HTTP_NOT_MODIFIED == result.code) {
        return true;
      }

      throw new IOException("failed to req API:" + "http://" + curServer + RunningConfig.getContextPath()
          + UtilsAndCommons.NACOS_NAMING_CONTEXT + DATA_ON_SYNC_URL + ". code:" + result.code + " msg: "
          + result.content);
    } catch (Exception e) {
      Loggers.SRV_LOG.warn("NamingProxy", e);
    }
    return false;
  }

  /**
   * 向地址为 curServer 的服务实例发起 API 请求（同步地），获取相关信息。
   * 
   * @param api
   * @param params
   * @param curServer
   * @return
   * @throws Exception
   */
  public static String reqAPI(String api, Map<String, String> params, String curServer) throws Exception {
    try {
      List<String> headers = Arrays.asList("Client-Version", UtilsAndCommons.SERVER_VERSION, "User-Agent",
          UtilsAndCommons.SERVER_VERSION, "Accept-Encoding", "gzip,deflate,sdch", "Connection", "Keep-Alive",
          "Content-Encoding", "gzip");

      HttpClient.HttpResult result;

      if (!curServer.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
        curServer = curServer + UtilsAndCommons.IP_PORT_SPLITER + RunningConfig.getServerPort();
      }

      result = HttpClient.httpGet("http://" + curServer + api, headers, params);

      if (HttpURLConnection.HTTP_OK == result.code) {
        return result.content;
      }

      if (HttpURLConnection.HTTP_NOT_MODIFIED == result.code) {
        return StringUtils.EMPTY;
      }

      throw new IOException(
          "failed to req API:" + "http://" + curServer + api + ". code:" + result.code + " msg: " + result.content);
    } catch (Exception e) {
      Loggers.SRV_LOG.warn("NamingProxy", e);
    }
    return StringUtils.EMPTY;
  }

  /**
   * 发送 GET/POST 请求给目的地址的目的端口.
   * 
   * @param api       要发送到的目的接口
   * @param params    要发送的参数
   * @param curServer 目的地址
   * @param isPost    true, 发送 POST 请求; 否则, 发送 GET 请求.
   * @return
   * @throws Exception
   */
  public static String reqAPI(String api, Map<String, String> params, String curServer, boolean isPost)
      throws Exception {
    try {
      List<String> headers = Arrays.asList("Client-Version", UtilsAndCommons.SERVER_VERSION, "User-Agent",
          UtilsAndCommons.SERVER_VERSION, "Accept-Encoding", "gzip,deflate,sdch", "Connection", "Keep-Alive",
          "Content-Encoding", "gzip");

      HttpClient.HttpResult result;

      if (!curServer.contains(UtilsAndCommons.IP_PORT_SPLITER)) {
        curServer = curServer + UtilsAndCommons.IP_PORT_SPLITER + RunningConfig.getServerPort();
      }

      if (isPost) {
        result = HttpClient.httpPost("http://" + curServer + RunningConfig.getContextPath()
            + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/api/" + api, headers, params);
      } else {
        result = HttpClient.httpGet("http://" + curServer + RunningConfig.getContextPath()
            + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/api/" + api, headers, params);
      }

      if (HttpURLConnection.HTTP_OK == result.code) {
        return result.content;
      }

      if (HttpURLConnection.HTTP_NOT_MODIFIED == result.code) {
        return StringUtils.EMPTY;
      }

      throw new IOException("failed to req API:" + "http://" + curServer + RunningConfig.getContextPath()
          + UtilsAndCommons.NACOS_NAMING_CONTEXT + "/api/" + api + ". code:" + result.code + " msg: " + result.content);
    } catch (Exception e) {
      Loggers.SRV_LOG.warn("NamingProxy", e);
    }
    return StringUtils.EMPTY;
  }

  public static class Request {

    private Map<String, String> params = new HashMap<>(8);

    public static Request newRequest() {
      return new Request();
    }

    public Request appendParam(String key, String value) {
      params.put(key, value);
      return this;
    }

    public String toUrl() {
      StringBuilder sb = new StringBuilder();
      for (String key : params.keySet()) {
        sb.append(key).append("=").append(params.get(key)).append("&");
      }
      return sb.toString();
    }
  }
}
