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
package com.alibaba.nacos.naming.consistency.ephemeral.distro;

import java.util.List;

/**
 * 同步任务，将一批数据同步给从当前实例可达的 nacos 集群其它服务实例（该类包含了要同步到的实例地址），实现 AP。
 * 
 * @author nkorange
 * @since 1.0.0
 */
public class SyncTask {

    /**
     * 待同步的数据（只发送 keys，对方收到后过滤出感兴趣的部分 keys 然后主动拉取对应的 values）
     */
    private List<String> keys;

    /**
     * 同步任务已重试次数
     */
    private int retryCount;

    /**
     * 上次执行时间
     */
    private long lastExecuteTime;

    /**
     * 同步目的地服务实例
     */
    private String targetServer;

    public List<String> getKeys() {
        return keys;
    }

    public void setKeys(List<String> keys) {
        this.keys = keys;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public long getLastExecuteTime() {
        return lastExecuteTime;
    }

    public void setLastExecuteTime(long lastExecuteTime) {
        this.lastExecuteTime = lastExecuteTime;
    }

    public String getTargetServer() {
        return targetServer;
    }

    public void setTargetServer(String targetServer) {
        this.targetServer = targetServer;
    }
}
