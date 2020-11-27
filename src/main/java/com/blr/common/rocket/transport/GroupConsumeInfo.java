package com.blr.common.rocket.transport;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * 分组消费
 */
@Getter
@Setter
public class GroupConsumeInfo implements Comparable<GroupConsumeInfo> {
    private String group;
    private String version;
    private int count;
    private ConsumeType consumeType;
    private MessageModel messageModel;
    private int consumeTps;
    private long diffTotal = -1;

    @Override
    public int compareTo(GroupConsumeInfo o) {
        if (this.count != o.count) {
            return o.count - this.count;
        }
        return (int) (o.diffTotal - diffTotal);
    }
}
