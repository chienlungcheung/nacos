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
package com.alibaba.nacos.naming.cluster.servers;

import com.alibaba.fastjson.JSON;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;

/**
 * Member node of Nacos cluster
 * <p>
 * Server 表示一个 Nacos 节点.
 *
 * @author nkorange
 * @since 1.0.0
 */
public class Server implements Comparable<Server> {

    /**
     * IP of member
     */
    private String ip;

    /**
     * serving port of member.
     */
    private int servePort;

    /**
     * Nacos 集群的 site 名称为 unknown
     */
    private String site = UtilsAndCommons.UNKNOWN_SITE;

    private int weight = 1;

    /**
     * additional weight, used to adjust manually
     * <p>
     * adWeigth 在人为干预服务器实例承载流量权重时候使用（见 Web UI 的 ServiceManagement->ServiceList->Details->cluster->某个 IP 行的 weight 编辑）。
     */
    private int adWeight;

    private boolean alive = false;

    /**
     * 接收到其它 nacos 节点发来状态报告时会生成一个临时的 Server 对象, 该字段用于记录报告中的上次报告时间戳 lastReportTime.
     */
    private long lastRefTime = 0L;

    private String lastRefTimeStr;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public int getServePort() {
        return servePort;
    }

    public void setServePort(int servePort) {
        this.servePort = servePort;
    }

    public String getSite() {
        return site;
    }

    public void setSite(String site) {
        this.site = site;
    }

    public int getWeight() {
        return weight;
    }

    public void setWeight(int weight) {
        this.weight = weight;
    }

    public int getAdWeight() {
        return adWeight;
    }

    public void setAdWeight(int adWeight) {
        this.adWeight = adWeight;
    }

    public boolean isAlive() {
        return alive;
    }

    public void setAlive(boolean alive) {
        this.alive = alive;
    }

    public long getLastRefTime() {
        return lastRefTime;
    }

    public void setLastRefTime(long lastRefTime) {
        this.lastRefTime = lastRefTime;
    }

    public String getLastRefTimeStr() {
        return lastRefTimeStr;
    }

    public void setLastRefTimeStr(String lastRefTimeStr) {
        this.lastRefTimeStr = lastRefTimeStr;
    }

    /**
     * 以 IP:Port 作为一个实例的 key
     * @return
     */
    public String getKey() {
        return ip + UtilsAndCommons.IP_PORT_SPLITER + servePort;
    }

    /**
     * IP:Port 相同即为同一个 server
     */
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Server server = (Server) o;
        return servePort == server.servePort && ip.equals(server.ip);
    }

    @Override
    public int hashCode() {
        int result = ip.hashCode();
        result = 31 * result + servePort;
        return result;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }

    @Override
    public int compareTo(Server server) {
        if (server == null) {
            return 1;
        }
        return this.getKey().compareTo(server.getKey());
    }
}
