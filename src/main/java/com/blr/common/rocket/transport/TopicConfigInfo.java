package com.blr.common.rocket.transport;

import com.google.common.base.Objects;
import lombok.Getter;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
public class TopicConfigInfo {

    private List<String> clusterNameList;
    private List<String> brokerNameList;
    private String topicName;
    private int writeQueueNums;
    private int readQueueNums;
    private int perm;
    private boolean order;

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TopicConfigInfo that = (TopicConfigInfo) o;
        return writeQueueNums == that.writeQueueNums && readQueueNums == that.readQueueNums &&
                perm == that.perm && order == that.order && Objects.equal(topicName, that.topicName);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(topicName, writeQueueNums, readQueueNums, perm, order);
    }

}
