/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.rocketmq.namesrv.routeinfo;

import com.alibaba.rocketmq.common.DataVersion;
import com.alibaba.rocketmq.common.MixAll;
import com.alibaba.rocketmq.common.TopicConfig;
import com.alibaba.rocketmq.common.constant.LoggerName;
import com.alibaba.rocketmq.common.constant.PermName;
import com.alibaba.rocketmq.common.namesrv.RegisterBrokerResult;
import com.alibaba.rocketmq.common.protocol.body.ClusterInfo;
import com.alibaba.rocketmq.common.protocol.body.TopicConfigSerializeWrapper;
import com.alibaba.rocketmq.common.protocol.body.TopicList;
import com.alibaba.rocketmq.common.protocol.route.BrokerData;
import com.alibaba.rocketmq.common.protocol.route.QueueData;
import com.alibaba.rocketmq.common.protocol.route.TopicRouteData;
import com.alibaba.rocketmq.common.sysflag.TopicSysFlag;
import com.alibaba.rocketmq.remoting.common.RemotingUtil;
import io.netty.channel.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;


/**
 * @author shijia.wxr
 *         <p>
 *         commit by medusar
 *         <p>
 *         集群信息对应关系
 *         <p>
 *         cluster1--->brokerName1 ---> brokerID:brokerAddress
 *         cluster:brokerName:brokerID = 1:m:n
 *         cluster可以有多个
 *         <p>
 *         Topic 与 brokerName对应关系
 *         Topic:BrokerName = 1:n
 *         <p>
 *         保存关系：
 *         1、Topic-->QueueData
 *         2、BrokerName-->BrokerData
 *         3、ClusterName-->BrokerName
 *         4、存活的Broker信息（BrokerLiveInfo)
 *         5、BrokerAddr-->FilterServerAddrs
 *
 *         QueueData与Broker关系：每个QueueData对应一个BrokerName
 */
public class RouteInfoManager {
    private static final Logger log = LoggerFactory.getLogger(LoggerName.NamesrvLoggerName);
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /**
     * Topic名称与QueueData对应关系
     */
    private final HashMap<String/* topic */, List<QueueData>> topicQueueTable;

    /**
     * Broker名字-->Broker信息(brokerID<-->broker地址)
     * Broker信息：同一个brokerName可能有多个机器，比如主、从
     * BrokerID:同一个BrokerName中的brokerID不能相同
     */
    private final HashMap<String/* brokerName */, BrokerData> brokerAddrTable;


    /**
     * 集群名字-->集群中Broker的名字
     * PS:变量名字命名的不咋地！
     */
    private final HashMap<String/* clusterName */, Set<String/* brokerName */>> clusterAddrTable;

    /**
     * 活动状态的Broker信息，broker连接断开将从该Map中移除
     */
    private final HashMap<String/* brokerAddr */, BrokerLiveInfo> brokerLiveTable;

    /**
     * Broker地址与FilterServer的地址
     */
    private final HashMap<String/* brokerAddr */, List<String>/* Filter Server */> filterServerTable;


    public RouteInfoManager() {
        this.topicQueueTable = new HashMap<String, List<QueueData>>(1024);
        this.brokerAddrTable = new HashMap<String, BrokerData>(128);
        this.clusterAddrTable = new HashMap<String, Set<String>>(32);
        this.brokerLiveTable = new HashMap<String, BrokerLiveInfo>(256);
        this.filterServerTable = new HashMap<String, List<String>>(256);
    }


    public byte[] getAllClusterInfo() {
        ClusterInfo clusterInfoSerializeWrapper = new ClusterInfo();
        clusterInfoSerializeWrapper.setBrokerAddrTable(this.brokerAddrTable);
        clusterInfoSerializeWrapper.setClusterAddrTable(this.clusterAddrTable);
        return clusterInfoSerializeWrapper.encode();
    }


    public void deleteTopic(final String topic) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                this.topicQueueTable.remove(topic);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("deleteTopic Exception", e);
        }
    }


    public byte[] getAllTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                topicList.getTopicList().addAll(this.topicQueueTable.keySet());
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }


    /**
     * 注册Broker，当有新的Broker注册到NameServ中的时候，更新相关信息
     *
     * @param clusterName        所属集群名字
     * @param brokerAddr         broker地址
     * @param brokerName         broker名字
     * @param brokerId           brokerID
     * @param haServerAddr       HAserver地址
     * @param topicConfigWrapper
     * @param filterServerList
     * @param channel            Broker与NameServ连接的Channel
     * @return
     */
    public RegisterBrokerResult registerBroker(//
                                               final String clusterName,// 1
                                               final String brokerAddr,// 2
                                               final String brokerName,// 3
                                               final long brokerId,// 4
                                               final String haServerAddr,// 5
                                               final TopicConfigSerializeWrapper topicConfigWrapper,// 6
                                               final List<String> filterServerList, // 7
                                               final Channel channel// 8
    ) {
        RegisterBrokerResult result = new RegisterBrokerResult();

        try {
            try {
                this.lock.writeLock().lockInterruptibly();

                /**
                 * Broker添加到对应的集群配置中
                 */
                Set<String> brokerNames = this.clusterAddrTable.get(clusterName);
                if (null == brokerNames) {
                    brokerNames = new HashSet<String>();
                    this.clusterAddrTable.put(clusterName, brokerNames);
                }
                brokerNames.add(brokerName);


                /**
                 * 更新BrokerData
                 */
                boolean registerFirst = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null == brokerData) {
                    registerFirst = true;

                    brokerData = new BrokerData();
                    brokerData.setBrokerName(brokerName);
                    HashMap<Long, String> brokerAddrs = new HashMap<Long, String>();
                    brokerData.setBrokerAddrs(brokerAddrs);

                    this.brokerAddrTable.put(brokerName, brokerData);
                }

                /**
                 * 从这里可以看出，同一个brokerName中，如果存在相同的BrokerID，那么新注册的将会替代老的
                 * 也就是说，一个BrokerName中，只能有一台主节点
                 */
                String oldAddr = brokerData.getBrokerAddrs().put(brokerId, brokerAddr);
                //判断当前Broker是否首次注册
                registerFirst = registerFirst || (null == oldAddr);

                /**
                 * 注册的节点是主节点，并且含有topic配置信息
                 */
                if (null != topicConfigWrapper && MixAll.MASTER_ID == brokerId) {
                    /**
                     * 首次注册 或者 topicConfig信息发生了变化
                     */
                    if (this.isBrokerTopicConfigChanged(brokerAddr, topicConfigWrapper.getDataVersion())//
                            || registerFirst) {
                        /**
                         * 获取配置信息并更新队列数据
                         * 一个broker中可能配置了多个topic，所以需要都处理
                         */
                        ConcurrentHashMap<String, TopicConfig> tcTable = topicConfigWrapper.getTopicConfigTable();
                        if (tcTable != null) {
                            for (String topic : tcTable.keySet()) {
                                TopicConfig topicConfig = tcTable.get(topic);
                                this.createAndUpdateQueueData(brokerName, topicConfig);
                            }
                        }
                    }
                }

                /**
                 * 创建存活Broker信息并添加到map中
                 */
                BrokerLiveInfo prevBrokerLiveInfo = this.brokerLiveTable.put(brokerAddr, //
                        new BrokerLiveInfo(//
                                System.currentTimeMillis(), //
                                topicConfigWrapper.getDataVersion(),//
                                channel, //
                                haServerAddr));

                if (null == prevBrokerLiveInfo) {
                    log.info("new broker registerd, {} HAServer: {}", brokerAddr, haServerAddr);
                }

                /**
                 * 设置当前broker对应的filterserver地址
                 */
                if (filterServerList != null) {
                    if (filterServerList.isEmpty()) {
                        this.filterServerTable.remove(brokerAddr);
                    } else {
                        this.filterServerTable.put(brokerAddr, filterServerList);
                    }
                }

                /**
                 * 如果注册的broker是从节点
                 * 则把该broker对应的主节点的HAserver地址和master地址返回给客户端
                 */
                if (MixAll.MASTER_ID != brokerId) {

                    String masterAddr = brokerData.getBrokerAddrs().get(MixAll.MASTER_ID);
                    if (masterAddr != null) {
                        BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.get(masterAddr);
                        if (brokerLiveInfo != null) {
                            result.setHaServerAddr(brokerLiveInfo.getHaServerAddr());
                            result.setMasterAddr(masterAddr);
                        }
                    }
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("registerBroker Exception", e);
        }

        return result;
    }

    private boolean isBrokerTopicConfigChanged(final String brokerAddr, final DataVersion dataVersion) {
        BrokerLiveInfo prev = this.brokerLiveTable.get(brokerAddr);
        if (null == prev || !prev.getDataVersion().equals(dataVersion)) {
            return true;
        }

        return false;
    }


    /**
     * 通过加锁的方式 删除brokerName对应的对QueueData的写权限
     *
     * @param brokerName
     * @return
     */
    public int wipeWritePermOfBrokerByLock(final String brokerName) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();
                return wipeWritePermOfBroker(brokerName);
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("wipeWritePermOfBrokerByLock Exception", e);
        }

        return 0;
    }


    private int wipeWritePermOfBroker(final String brokerName) {
        int wipeTopicCnt = 0;
        Iterator<Entry<String, List<QueueData>>> itTopic = this.topicQueueTable.entrySet().iterator();
        while (itTopic.hasNext()) {
            Entry<String, List<QueueData>> entry = itTopic.next();
            List<QueueData> qdList = entry.getValue();
            Iterator<QueueData> it = qdList.iterator();
            /**
             * 需要把所有Topic中，只有有该BrokerName的，都把QueueData的写权限清除
             */
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    int perm = qd.getPerm();
                    //将写权限位置为0，即取消写权限，这是个很好的技巧
                    perm &= ~PermName.PERM_WRITE;
                    qd.setPerm(perm);
                    wipeTopicCnt++;
                }
            }
        }

        return wipeTopicCnt;
    }


    /**
     * 创建并且更新QueueData
     *
     * @param brokerName
     * @param topicConfig
     */
    private void createAndUpdateQueueData(final String brokerName, final TopicConfig topicConfig) {
        QueueData queueData = new QueueData();
        queueData.setBrokerName(brokerName);

        queueData.setWriteQueueNums(topicConfig.getWriteQueueNums());
        //允许读的队列数
        //消费者在读取的时候，会根据这个数量创建多个MessageQueue
        queueData.setReadQueueNums(topicConfig.getReadQueueNums());

        queueData.setPerm(topicConfig.getPerm());
        queueData.setTopicSynFlag(topicConfig.getTopicSysFlag());


        List<QueueData> queueDataList = this.topicQueueTable.get(topicConfig.getTopicName());
        if (null == queueDataList) { //如果原来Topic不存在则新增
            queueDataList = new LinkedList<QueueData>();
            queueDataList.add(queueData);
            this.topicQueueTable.put(topicConfig.getTopicName(), queueDataList);
            log.info("new topic registerd, {} {}", topicConfig.getTopicName(), queueData);
        } else {
            /**
             * Topic存在，需要判断当前brokerName是否已经包含该Topic
             */

            boolean addNewOne = true;

            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    if (qd.equals(queueData)) {//Topic已经在当前brokerName对应的broker中了，则不再新增，不做操作
                        addNewOne = false;
                    } else { //Topic在当前broker中，但是QueueData发生了变化，则用新的替换旧的
                        log.info("topic changed, {} OLD: {} NEW: {}", topicConfig.getTopicName(), qd, queueData);
                        it.remove();
                    }
                }
            }

            if (addNewOne) {
                queueDataList.add(queueData);
            }
        }
    }


    /**
     * Broker取消注册
     *
     * @param clusterName
     * @param brokerAddr
     * @param brokerName
     * @param brokerId
     */
    public void unregisterBroker(//
                                 final String clusterName,// 1
                                 final String brokerAddr,// 2
                                 final String brokerName,// 3
                                 final long brokerId// 4
    ) {
        try {
            try {
                this.lock.writeLock().lockInterruptibly();

                /**
                 * 判断取消注册的broker是否在存活Broker中
                 */
                BrokerLiveInfo brokerLiveInfo = this.brokerLiveTable.remove(brokerAddr);
                if (brokerLiveInfo != null) {
                    log.info("unregisterBroker, remove from brokerLiveTable {}, {}", //
                            (brokerLiveInfo != null ? "OK" : "Failed"),//
                            brokerAddr//
                    );
                }

                /**
                 * 移除对应的filterserver
                 */
                this.filterServerTable.remove(brokerAddr);

                /**
                 * 判断是否要移除整个brokerName代表的broker组
                 */
                boolean removeBrokerName = false;
                BrokerData brokerData = this.brokerAddrTable.get(brokerName);
                if (null != brokerData) {
                    // 移除
                    String addr = brokerData.getBrokerAddrs().remove(brokerId);
                    log.info("unregisterBroker, remove addr from brokerAddrTable {}, {}", //
                            (addr != null ? "OK" : "Failed"),//
                            brokerAddr//
                    );

                    // 如果移除后broker地址为空，则表示该brokerName下没有任何broker了，那么该brokerName也需要删除
                    if (brokerData.getBrokerAddrs().isEmpty()) {
                        this.brokerAddrTable.remove(brokerName);
                        log.info("unregisterBroker, remove name from brokerAddrTable OK, {}", //
                                brokerName//
                        );

                        removeBrokerName = true;
                    }
                }

                /**
                 * 删除brokerName对应的信息
                 */
                if (removeBrokerName) {
                    Set<String> nameSet = this.clusterAddrTable.get(clusterName);
                    if (nameSet != null) {
                        boolean removed = nameSet.remove(brokerName);
                        log.info("unregisterBroker, remove name from clusterAddrTable {}, {}", //
                                (removed ? "OK" : "Failed"),//
                                brokerName//
                        );

                        /**
                         * 如果删除了BrokerName之后发现cluster中也没有任何broker了，那么需要删除cluster
                         */
                        if (nameSet.isEmpty()) {
                            this.clusterAddrTable.remove(clusterName);
                            log.info("unregisterBroker, remove cluster from clusterAddrTable {}", //
                                    clusterName//
                            );
                        }
                    }

                    /**
                     * 移除brokerName对应的topic
                     */
                    this.removeTopicByBrokerName(brokerName);
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        } catch (Exception e) {
            log.error("unregisterBroker Exception", e);
        }
    }


    private void removeTopicByBrokerName(final String brokerName) {
        Iterator<Entry<String, List<QueueData>>> itMap = this.topicQueueTable.entrySet().iterator();
        while (itMap.hasNext()) {
            Entry<String, List<QueueData>> entry = itMap.next();

            String topic = entry.getKey();
            List<QueueData> queueDataList = entry.getValue();
            Iterator<QueueData> it = queueDataList.iterator();
            while (it.hasNext()) {
                QueueData qd = it.next();
                if (qd.getBrokerName().equals(brokerName)) {
                    log.info("removeTopicByBrokerName, remove one broker's topic {} {}", topic, qd);
                    it.remove();
                }
            }

            if (queueDataList.isEmpty()) {
                log.info("removeTopicByBrokerName, remove the topic all queue {}", topic);
                itMap.remove();
            }
        }
    }


    /**
     * 获取Topic的路由信息
     *
     * @param topic
     * @return
     */
    public TopicRouteData pickupTopicRouteData(final String topic) {
        TopicRouteData topicRouteData = new TopicRouteData();

        boolean foundQueueData = false;
        boolean foundBrokerData = false;

        /**
         * Broker信息
         */
        Set<String> brokerNameSet = new HashSet<String>();
        List<BrokerData> brokerDataList = new LinkedList<BrokerData>();
        topicRouteData.setBrokerDatas(brokerDataList);

        /**
         * FilterServer信息
         */
        HashMap<String, List<String>> filterServerMap = new HashMap<String, List<String>>();
        topicRouteData.setFilterServerTable(filterServerMap);

        try {
            try {
                this.lock.readLock().lockInterruptibly();

                /**
                 * 通过Topic找到QueueData列表
                 * 然后根据QueueData 查找Broker信息
                 * 然后根据Broker信息 查找FilterServer信息
                 */
                List<QueueData> queueDataList = this.topicQueueTable.get(topic);
                if (queueDataList != null) {
                    topicRouteData.setQueueDatas(queueDataList);

                    foundQueueData = true;

                    Iterator<QueueData> it = queueDataList.iterator();
                    while (it.hasNext()) {
                        QueueData qd = it.next();
                        brokerNameSet.add(qd.getBrokerName());
                    }

                    for (String brokerName : brokerNameSet) {
                        BrokerData brokerData = this.brokerAddrTable.get(brokerName);

                        if (null != brokerData) {
                            BrokerData brokerDataClone = new BrokerData();
                            brokerDataClone.setBrokerName(brokerData.getBrokerName());
                            brokerDataClone.setBrokerAddrs((HashMap<Long, String>) brokerData.getBrokerAddrs().clone());
                            brokerDataList.add(brokerDataClone);
                            foundBrokerData = true;

                            for (final String brokerAddr : brokerDataClone.getBrokerAddrs().values()) {
                                List<String> filterServerList = this.filterServerTable.get(brokerAddr);
                                filterServerMap.put(brokerAddr, filterServerList);
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("pickupTopicRouteData Exception", e);
        }

        if (log.isDebugEnabled()) {
            log.debug("pickupTopicRouteData {} {}", topic, topicRouteData);
        }

        if (foundBrokerData && foundQueueData) {
            return topicRouteData;
        }

        return null;
    }

    private final static long BrokerChannelExpiredTime = 1000 * 60 * 2;

    /**
     * 扫描不活动的Broker，并把它移除，然后关闭对应的Channel
     * 不活动：超过了过期时间的
     * 默认过期时间：120000ms = 2min
     */
    public void scanNotActiveBroker() {
        Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<String, BrokerLiveInfo> next = it.next();
            long last = next.getValue().getLastUpdateTimestamp();
            if ((last + BrokerChannelExpiredTime) < System.currentTimeMillis()) {

                /**
                 * 关闭对应Channel
                 */
                RemotingUtil.closeChannel(next.getValue().getChannel());
                /**
                 * 从存活Broker列表中移除
                 */
                it.remove();

                log.warn("The broker channel expired, {} {}ms", next.getKey(), BrokerChannelExpiredTime);

                /**
                 * 关闭Channel的后续收尾操作
                 */
                this.onChannelDestroy(next.getKey(), next.getValue().getChannel());
            }
        }
    }

    /**
     * 某个Channel关闭的后续收尾操作
     *
     * @param remoteAddr channel的客户端地址
     * @param channel    channel本身
     */
    public void onChannelDestroy(String remoteAddr, Channel channel) {
        String brokerAddrFound = null;

        /**
         * 根据Channel查找当前存活broker列表中是否还存在该Channel，找到则返回保存的broker地址，否则返回null
         */
        if (channel != null) {
            try {
                try {
                    this.lock.readLock().lockInterruptibly();

                    Iterator<Entry<String, BrokerLiveInfo>> itBrokerLiveTable = this.brokerLiveTable.entrySet().iterator();

                    while (itBrokerLiveTable.hasNext()) {
                        Entry<String, BrokerLiveInfo> entry = itBrokerLiveTable.next();

                        if (entry.getValue().getChannel() == channel) {
                            brokerAddrFound = entry.getKey();
                            break;
                        }
                    }

                } finally {
                    this.lock.readLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }

        /**
         * 如果没有找到，则使用参数中的远端地址
         */
        if (null == brokerAddrFound) {
            brokerAddrFound = remoteAddr;
        } else {
            /**
             * 找到了说明什么？？？？
             */
            log.info("the broker's channel destroyed, {}, clean it's data structure at once", brokerAddrFound);
        }

        if (brokerAddrFound != null && brokerAddrFound.length() > 0) {

            try {
                try {
                    this.lock.writeLock().lockInterruptibly();

                    /**
                     * 再次从存活列表中移除
                     */
                    this.brokerLiveTable.remove(brokerAddrFound);

                    /**
                     * 移除对应的filterserver
                     */
                    this.filterServerTable.remove(brokerAddrFound);


                    String brokerNameFound = null;
                    boolean removeBrokerName = false;

                    /**
                     * brokerAddrTable中找到对应broker地址并删除
                     * 如果某个brokerName在删除了broker地址之后为空了，那么也要把这个brokerName删掉
                     */
                    Iterator<Entry<String, BrokerData>> itBrokerAddrTable = this.brokerAddrTable.entrySet().iterator();
                    while (itBrokerAddrTable.hasNext() && (null == brokerNameFound)) {
                        BrokerData brokerData = itBrokerAddrTable.next().getValue();
                        Iterator<Entry<Long, String>> it = brokerData.getBrokerAddrs().entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<Long, String> entry = it.next();
                            Long brokerId = entry.getKey();
                            String brokerAddr = entry.getValue();
                            if (brokerAddr.equals(brokerAddrFound)) {
                                brokerNameFound = brokerData.getBrokerName();
                                it.remove();
                                log.info(
                                        "remove brokerAddr[{}, {}] from brokerAddrTable, because channel destroyed",
                                        brokerId, brokerAddr);
                                break;
                            }
                        }

                        if (brokerData.getBrokerAddrs().isEmpty()) {
                            removeBrokerName = true;
                            itBrokerAddrTable.remove();
                            log.info("remove brokerName[{}] from brokerAddrTable, because channel destroyed",
                                    brokerData.getBrokerName());
                        }
                    }

                    /**
                     * 如果brokerName删除了
                     * 则需要从cluster中把brokerName删掉，删掉后如果clusterName对应的集群中没有broker，那么该cluster也要删除
                     */
                    if (brokerNameFound != null && removeBrokerName) {
                        Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                        while (it.hasNext()) {
                            Entry<String, Set<String>> entry = it.next();
                            String clusterName = entry.getKey();
                            Set<String> brokerNames = entry.getValue();

                            boolean removed = brokerNames.remove(brokerNameFound);
                            // 如果当前集群中包含该brokerName
                            if (removed) {
                                log.info(
                                        "remove brokerName[{}], clusterName[{}] from clusterAddrTable, because channel destroyed",
                                        brokerNameFound, clusterName);

                                /**
                                 * 如果clusterName对应的集群中没有broker，那么该cluster也要删除
                                 */
                                if (brokerNames.isEmpty()) {
                                    log.info(
                                            "remove the clusterName[{}] from clusterAddrTable, because channel destroyed and no broker in this cluster",
                                            clusterName);
                                    it.remove();
                                }

                                break;
                            }
                        }
                    }

                    /**
                     * 删除Topic配置中对应的QueueData信息
                     */
                    if (removeBrokerName) {
                        Iterator<Entry<String, List<QueueData>>> itTopicQueueTable = this.topicQueueTable.entrySet().iterator();
                        while (itTopicQueueTable.hasNext()) {
                            Entry<String, List<QueueData>> entry = itTopicQueueTable.next();
                            String topic = entry.getKey();
                            List<QueueData> queueDataList = entry.getValue();

                            Iterator<QueueData> itQueueData = queueDataList.iterator();
                            while (itQueueData.hasNext()) {
                                QueueData queueData = itQueueData.next();
                                //找到brokerName 并把该QueueData删除
                                if (queueData.getBrokerName().equals(brokerNameFound)) {
                                    itQueueData.remove();
                                    log.info("remove topic[{} {}], from topicQueueTable, because channel destroyed", topic, queueData);
                                }
                            }

                            if (queueDataList.isEmpty()) {
                                itTopicQueueTable.remove();
                                log.info("remove topic[{}] all queue, from topicQueueTable, because channel destroyed", topic);
                            }
                        }
                    }
                } finally {
                    this.lock.writeLock().unlock();
                }
            } catch (Exception e) {
                log.error("onChannelDestroy Exception", e);
            }
        }
    }

    public void printAllPeriodically() {
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                log.info("--------------------------------------------------------");
                {
                    log.info("topicQueueTable SIZE: {}", this.topicQueueTable.size());
                    Iterator<Entry<String, List<QueueData>>> it = this.topicQueueTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, List<QueueData>> next = it.next();
                        log.info("topicQueueTable Topic: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerAddrTable SIZE: {}", this.brokerAddrTable.size());
                    Iterator<Entry<String, BrokerData>> it = this.brokerAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerData> next = it.next();
                        log.info("brokerAddrTable brokerName: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("brokerLiveTable SIZE: {}", this.brokerLiveTable.size());
                    Iterator<Entry<String, BrokerLiveInfo>> it = this.brokerLiveTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, BrokerLiveInfo> next = it.next();
                        log.info("brokerLiveTable brokerAddr: {} {}", next.getKey(), next.getValue());
                    }
                }

                {
                    log.info("clusterAddrTable SIZE: {}", this.clusterAddrTable.size());
                    Iterator<Entry<String, Set<String>>> it = this.clusterAddrTable.entrySet().iterator();
                    while (it.hasNext()) {
                        Entry<String, Set<String>> next = it.next();
                        log.info("clusterAddrTable clusterName: {} {}", next.getKey(), next.getValue());
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("printAllPeriodically Exception", e);
        }
    }

    public byte[] getSystemTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                for (String cluster : clusterAddrTable.keySet()) {
                    topicList.getTopicList().add(cluster);
                    topicList.getTopicList().addAll(this.clusterAddrTable.get(cluster));
                }

                if (brokerAddrTable != null && !brokerAddrTable.isEmpty()) {
                    Iterator<String> it = brokerAddrTable.keySet().iterator();
                    while (it.hasNext()) {
                        BrokerData bd = brokerAddrTable.get(it.next());
                        HashMap<Long, String> brokerAddrs = bd.getBrokerAddrs();
                        if (bd.getBrokerAddrs() != null && !bd.getBrokerAddrs().isEmpty()) {
                            Iterator<Long> it2 = brokerAddrs.keySet().iterator();
                            topicList.setBrokerAddr(brokerAddrs.get(it2.next()));
                            break;
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    /**
     * 获取集群下的Topic
     * Topic:BrokerName = 1:n
     * 所以需要先根据cluster查找到brokerName
     *
     * @param cluster
     * @return
     */
    public byte[] getTopicsByCluster(String cluster) {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();

                Set<String> brokerNameSet = this.clusterAddrTable.get(cluster);

                for (String brokerName : brokerNameSet) {
                    Iterator<Entry<String, List<QueueData>>> topicTableIt =
                            this.topicQueueTable.entrySet().iterator();
                    while (topicTableIt.hasNext()) {
                        Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                        String topic = topicEntry.getKey();
                        List<QueueData> queueDatas = topicEntry.getValue();
                        for (QueueData queueData : queueDatas) {
                            if (brokerName.equals(queueData.getBrokerName())) {
                                topicList.getTopicList().add(topic);
                                break;
                            }
                        }
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getUnitTopics() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                            && TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getHasUnitSubTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                            && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }

    public byte[] getHasUnitSubUnUnitTopicList() {
        TopicList topicList = new TopicList();
        try {
            try {
                this.lock.readLock().lockInterruptibly();
                Iterator<Entry<String, List<QueueData>>> topicTableIt =
                        this.topicQueueTable.entrySet().iterator();
                while (topicTableIt.hasNext()) {
                    Entry<String, List<QueueData>> topicEntry = topicTableIt.next();
                    String topic = topicEntry.getKey();
                    List<QueueData> queueDatas = topicEntry.getValue();
                    if (queueDatas != null && queueDatas.size() > 0
                            && !TopicSysFlag.hasUnitFlag(queueDatas.get(0).getTopicSynFlag())
                            && TopicSysFlag.hasUnitSubFlag(queueDatas.get(0).getTopicSynFlag())) {
                        topicList.getTopicList().add(topic);
                    }
                }
            } finally {
                this.lock.readLock().unlock();
            }
        } catch (Exception e) {
            log.error("getAllTopicList Exception", e);
        }

        return topicList.encode();
    }
}

/**
 * 处于存活状态的broker信息
 */
class BrokerLiveInfo {
    private long lastUpdateTimestamp;

    //Broker数据版本
    private DataVersion dataVersion;


    //Broker与nameserv的长连接
    private Channel channel;

    private String haServerAddr;


    public BrokerLiveInfo(long lastUpdateTimestamp, DataVersion dataVersion, Channel channel, String haServerAddr) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
        this.dataVersion = dataVersion;
        this.channel = channel;
        this.haServerAddr = haServerAddr;
    }


    public long getLastUpdateTimestamp() {
        return lastUpdateTimestamp;
    }


    public void setLastUpdateTimestamp(long lastUpdateTimestamp) {
        this.lastUpdateTimestamp = lastUpdateTimestamp;
    }


    public DataVersion getDataVersion() {
        return dataVersion;
    }


    public void setDataVersion(DataVersion dataVersion) {
        this.dataVersion = dataVersion;
    }


    public Channel getChannel() {
        return channel;
    }


    public void setChannel(Channel channel) {
        this.channel = channel;
    }


    public String getHaServerAddr() {
        return haServerAddr;
    }


    public void setHaServerAddr(String haServerAddr) {
        this.haServerAddr = haServerAddr;
    }


    @Override
    public String toString() {
        return "BrokerLiveInfo [lastUpdateTimestamp=" + lastUpdateTimestamp + ", dataVersion=" + dataVersion
                + ", channel=" + channel + ", haServerAddr=" + haServerAddr + "]";
    }
}
