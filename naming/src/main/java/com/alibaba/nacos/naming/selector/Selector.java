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
package com.alibaba.nacos.naming.selector;

import com.alibaba.nacos.naming.core.Instance;

import java.util.List;

/**
 * Selector defines a rule for load-balance for service discovery.
 * <p>
 * Every service in Nacos can apply an existing selector and uses it to give the
 * consumer a subset of selected providers.
 * <p>
 * This selector itself does not implement any specific behavior of
 * load-balance, every real life selector should extend this class and implement
 * the select method.
 * <p>
 * Every extended selector should also register its type to class SelectorType
 * so Nacos recognizes it and can correctly create this type of selector.
 * <p>
 * Selector 定义了为保证服务负载均衡而设计的一套规则。
 * <p>
 * naocs 中定义的每个服务都可以应用一个现有的 selector 实现负载均衡，即每次客户端发送服务发现请求的时候，可以返回服务实例的一个子集。
 * <p>
 * 该接口的具体实现都需要注册其类型到 SelectorType，这样 nacos 才能识别并创建对应的 selector。
 * 
 * @author nkorange
 * @see com.alibaba.nacos.api.selector.SelectorType
 * @see SelectorJsonAdapter
 * @since 0.7.0
 */
public interface Selector {

  /**
   * Get the type of this selector
   * <p>
   * 获取 Selector 类型。
   *
   * @return type of selector
   */
  String getType();

  /**
   * Select qualified instances from providers
   * <p>
   * 为 consumer（即客户端） 从 providers 中根据自定义规则挑选一批合格的实例。
   *
   * @param consumer  consumer address
   * @param providers candidate provider addresses
   * @return selected provider addresses
   */
  List<Instance> select(String consumer, List<Instance> providers);
}
