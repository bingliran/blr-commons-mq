package com.blr.common.rocket.util;

import com.blr.common.rocket.config.RocketInfo;
import com.blr.common.rocket.transport.GroupConsumeInfo;
import com.blr.common.rocket.transport.MessageView;
import com.blr.common.auto.util.EnvironmentUtil;
import com.blr.common.rocket.config.RocketInfo;
import com.blr.common.rocket.transport.GroupConsumeInfo;
import com.blr.common.rocket.transport.MessageView;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullResult;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.SubscriptionGroupWrapper;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.route.BrokerData;
import org.apache.rocketmq.common.protocol.route.TopicRouteData;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PreDestroy;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.rocketmq.client.consumer.PullStatus.FOUND;

/**
 * rocket管理工具
 * 注意:如果项目中存在多个producer且连接的host不同那么只会按default中的配置进行操作
 */
@Slf4j
public class RocketAdminUtil {
    @Autowired
    private MQAdminExt mqAdminExt;
    @Autowired
    private RocketInfo rocketInfo;

    /**
     * 获取所有消费者分组信息
     * 一个消费者分组可能包含多个消费者
     */
    public List<GroupConsumeInfo> queryConsumeList() {
        Set<String> consumerGroupSet = new HashSet<>();
        try {
            /*
             * 获取broker集群信息
             */
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            /*
             * 通过broker名称列表获取每个broker的比较详细信息
             */
            for (BrokerData brokerData : clusterInfo.getBrokerAddrTable().values()) {
                /*
                 * 根据broker地址获取消费者信息
                 */
                SubscriptionGroupWrapper subscriptionGroupWrapper = mqAdminExt.getAllSubscriptionGroup(brokerData.selectBrokerAddr(), 3000L);
                consumerGroupSet.addAll(subscriptionGroupWrapper.getSubscriptionGroupTable().keySet());
            }
        } catch (Exception e) {
            log.warn("获取消费者分组信息失败", e);
            return new ArrayList<>();
        }
        List<GroupConsumeInfo> groupConsumeInfoList = new ArrayList<>();
        /*
         * 拿到每一组消费者的信息
         */
        for (String consumerGroup : consumerGroupSet) groupConsumeInfoList.add(queryGroup(consumerGroup));
        return groupConsumeInfoList;
    }

    /**
     * 获取所有topic信息
     */
    public TopicList queryTopicList() {
        try {
            return mqAdminExt.fetchAllTopicList();
        } catch (Exception e) {
            log.warn("获取所有topic信息失败", e);
            return null;
        }
    }

    /**
     * 删除一个topic
     * 因为程序正在运行中,可能会引发一系列问题,不建议使用
     */
    public void deleteTopic(String topic) {
        try {
            /*
             * 获取每个broker的信息
             */
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            for (String clusterName : clusterInfo.getClusterAddrTable().keySet()) {
                Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdminExt, clusterName);
                /*
                 * 在每个broker和nameServer删掉此条记录
                 */
                mqAdminExt.deleteTopicInBroker(masterSet, topic);
                Set<String> nameServerSet = null;
                if (StringUtils.isNotBlank(rocketInfo.getHost())) {
                    String[] ns = rocketInfo.getHost().split(";");
                    nameServerSet = new HashSet<>(Arrays.asList(ns));
                }
                mqAdminExt.deleteTopicInNameServer(nameServerSet, topic);
            }
        } catch (Exception e) {
            log.error("删除一个topic出现异常,也或许topic已经删除或者继续存在于broke中", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建一个topic如果存在则改为修改
     *
     * @param topicName 名称
     * @param order     是否为有序的
     * @param perm      6同时支持读写,4禁写,2禁读
     * @param read      决定了consume消费的MessageQueue共有几个
     * @param write     决定了producer发送消息的MessageQueue共有几个
     */
    public void createTopic(String topicName, Boolean order, Integer perm, Integer read, Integer write) {
        TopicConfig topicConfig = new TopicConfig();
        if (order != null) topicConfig.setOrder(order);
        if (perm != null) if (order != null) topicConfig.setPerm(perm);
        if (read != null) topicConfig.setReadQueueNums(read);
        if (write != null) topicConfig.setWriteQueueNums(write);
        topicConfig.setTopicName(topicName);
        try {
            ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
            List<String> clusterNameList = new ArrayList<>(clusterInfo.getClusterAddrTable().keySet());
            List<String> brokerNameList = new ArrayList<>(clusterInfo.getBrokerAddrTable().keySet());
            /*
             * 拿到所有的brokeName,在每个broke创建一次
             */
            Set<String> brokerNameSet = changeToBrokerNameSet(clusterInfo.getClusterAddrTable(), clusterNameList, brokerNameList);
            for (String brokerName : brokerNameSet) {
                mqAdminExt.createAndUpdateTopicConfig(clusterInfo.getBrokerAddrTable().get(brokerName).selectBrokerAddr(), topicConfig);
            }
        } catch (Exception e) {
            log.error("创建topic失败", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 简单创建方式都使用默认值
     */
    public void createTopic(String topicName) {
        createTopic(topicName, null, null, null, null);
    }

    /**
     * 查询一个topic的状态
     */
    public TopicStatsTable queryTopicStats(String topicName) {
        try {
            return mqAdminExt.examineTopicStats(topicName);
        } catch (Exception e) {
            log.warn("查询一个topic的状态", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 查询一个topic的路由状态
     */
    public TopicRouteData queryTopicRoute(String topic) {
        try {
            return mqAdminExt.examineTopicRouteInfo(topic);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }


    /**
     * topic按照时间范围查找message
     * 严格模式:有错即终止并且向上抛出异常 else 出现错误尽可能的返回正确结果且不会抛出异常
     *
     * @param topic      名称
     * @param begin      开始时间戳
     * @param end        结束时间戳
     * @param num        查找的数量默认2000
     * @param strictMode 是否为严格模式
     */
    public List<MessageView> queryMessage(String topic, final long begin, final long end, Integer num, boolean strictMode) {
        DefaultMQPullConsumer consumer = new DefaultMQPullConsumer(MixAll.TOOLS_CONSUMER_GROUP, null);
        List<MessageView> messageViewList = new ArrayList<>();
        try {
            consumer.start();
            num = num == null ? 2000 : num;
            for (MessageQueue mq : consumer.fetchSubscribeMessageQueues(topic)) {
                /*
                 * 最小和最大偏移量
                 */
                long minOffset = consumer.searchOffset(mq, begin);
                long maxOffset = consumer.searchOffset(mq, end);
                while (minOffset <= maxOffset) {
                    try {
                        if (messageViewList.size() >= num) break;
                        /*
                         * 拿到32条并将minOffset设置为minOffset+32+1
                         */
                        PullResult pullResult = consumer.pull(mq, "*", minOffset, 32);
                        minOffset = pullResult.getNextBeginOffset();
                        if (!FOUND.equals(pullResult.getPullStatus())) break;
                        /*
                         * 如果是FOUND(时间成立的) 则将pullResult的MessageExt转为MessageView
                         * 然后对比消费者消费时间都成立的添加到messageViewList
                         */
                        /*
                         * 因此方法可能会出现泛型异常所以采用stream
                         *
                         *List<MessageView> list = Lists.transform(pullResult.getMsgFoundList(), (e) -> MessageView.fromMessageExt(e));
                         *list = list.stream().filter((m) -> m.getStoreTimestamp() >= begin && m.getStoreTimestamp() <= end).collect(Collectors.toList());
                         */
                        List<MessageView> list = pullResult.getMsgFoundList().stream().map(MessageView::fromMessageExt)
                                .filter((m) -> m.getStoreTimestamp() >= begin && m.getStoreTimestamp() <= end).collect(Collectors.toList());
                        messageViewList.addAll(list);
                    } catch (Exception e) {
                        if (strictMode) throw new RuntimeException(e);
                        log.warn("topic按照时间范围查找出现异常可能返回结果并不是完整的", e);
                        break;
                    }
                }
            }
        } catch (Exception e) {
            if (strictMode) throw new RuntimeException(e);
            log.warn("topic按照时间范围查找出现异常可能返回结果并不是完整的", e);
        } finally {
            consumer.shutdown();
        }
        /*
         * 按时间排序
         */
        return messageViewList.stream().sorted((o1, o2) -> {
            long i, k;
            if ((i = o1.getStoreTimestamp()) == (k = o2.getStoreTimestamp())) return 0;
            return i > k ? -1 : 1;
        }).collect(Collectors.toList());
    }


    //------下面没人看(战术后仰)


    private Set<String> changeToBrokerNameSet(HashMap<String, Set<String>> clusterAddrTable, List<String> clusterNameList, List<String> brokerNameList) {
        Set<String> finalBrokerNameList = new HashSet<>();
        if (CollectionUtils.isNotEmpty(clusterNameList)) {
            for (String clusterName : clusterNameList)
                finalBrokerNameList.addAll(clusterAddrTable.get(clusterName));
        }
        if (CollectionUtils.isNotEmpty(brokerNameList)) finalBrokerNameList.addAll(brokerNameList);
        return finalBrokerNameList;
    }

    private GroupConsumeInfo queryGroup(String consumerGroup) {
        GroupConsumeInfo groupConsumeInfo = new GroupConsumeInfo();
        try {
            ConsumeStats consumeStats = mqAdminExt.examineConsumeStats(consumerGroup);
            ConsumerConnection consumerConnection = mqAdminExt.examineConsumerConnectionInfo(consumerGroup);
            groupConsumeInfo.setGroup(consumerGroup);
            if (consumeStats != null) {
                groupConsumeInfo.setConsumeTps((int) consumeStats.getConsumeTps());
                groupConsumeInfo.setDiffTotal(consumeStats.computeTotalDiff());
            }
            if (consumerConnection != null) {
                groupConsumeInfo.setCount(consumerConnection.getConnectionSet().size());
                groupConsumeInfo.setMessageModel(consumerConnection.getMessageModel());
                groupConsumeInfo.setConsumeType(consumerConnection.getConsumeType());
                groupConsumeInfo.setVersion(MQVersion.getVersionDesc(consumerConnection.computeMinVersion()));
            }
        } catch (Exception e) {
            log.warn("获取某组消费者信息失败", e);
        }
        return groupConsumeInfo;
    }

    @PreDestroy
    public void close() {
        mqAdminExt.shutdown();
    }

    /**
     * 因为mqAdminExt初始化及其耗费系统资源所以采用手动控制是否开启RocketAdminUtil
     * 由于是手动加载的bean所以可能初始化时如果你的bean优先级很高那可能会出现找不到rocketAdminUtil的情况
     * 这时请尝试更改@Autowired注解注入方式为 rocketUtil.getAdminUtil();
     */
    public static RocketAdminUtil getInstance(boolean enable, EnvironmentUtil environmentUtil) throws MQClientException {
        if (!enable) return null;
        DefaultMQAdminExt mqAdminExt = environmentUtil.registerBean("mqAdminExt", DefaultMQAdminExt.class);
        mqAdminExt.setInstanceName(Long.toString(System.currentTimeMillis()));
        mqAdminExt.start();
        RocketAdminUtil rocketAdminUtil = environmentUtil.registerBean("rocketAdminUtil", RocketAdminUtil.class);
        if (log.isDebugEnabled()) log.debug("rocketAdminUtil加载完成");
        return rocketAdminUtil;
    }

}
