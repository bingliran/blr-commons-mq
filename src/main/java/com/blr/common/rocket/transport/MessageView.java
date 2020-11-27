package com.blr.common.rocket.transport;

import com.google.common.base.Charsets;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.rocketmq.common.message.MessageExt;
import org.springframework.beans.BeanUtils;

import java.net.SocketAddress;
import java.util.Map;

@Getter
@Setter
@ToString
public class MessageView {

    private int queueId;
    private int storeSize;
    private long queueOffset;
    private int sysFlag;
    private long bornTimestamp;
    private SocketAddress bornHost;
    private long storeTimestamp;
    private SocketAddress storeHost;
    private String msgId;
    private long commitLogOffset;
    private int bodyCRC;
    private int reconsumeTimes;
    private long preparedTransactionOffset;
    private String topic;
    private int flag;
    private Map<String, String> properties;
    private String messageBody;

    public static MessageView fromMessageExt(MessageExt messageExt) {
        MessageView messageView = new MessageView();
        BeanUtils.copyProperties(messageExt, messageView);
        if (messageExt.getBody() != null) {
            messageView.setMessageBody(new String(messageExt.getBody(), Charsets.UTF_8));
        }
        return messageView;
    }
}
