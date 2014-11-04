package pl.azurro.kafka.groupmonitor.model;

import static org.apache.commons.lang.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang.builder.HashCodeBuilder.reflectionHashCode;
import static org.apache.commons.lang.builder.ToStringBuilder.reflectionToString;

import java.util.HashMap;
import java.util.Map;

public class GroupInfo {

    private String groupId;
    private Map<String, TopicInfo> topicsInfos;

    public GroupInfo(String groupId) {
        this.groupId = groupId;
        topicsInfos = new HashMap<String, TopicInfo>();
    }

    public String getGroupId() {
        return groupId;
    }

    public Map<String, TopicInfo> getTopicsInfos() {
        return topicsInfos;
    }

    public void addInfoFor(String topic, int partitionId, long logSize, long offset) {
        TopicInfo topicInfo = topicsInfos.get(topic);
        if (topicInfo == null) {
            topicInfo = new TopicInfo(topic);
            topicsInfos.put(topic, topicInfo);
        }
        topicInfo.addInfoFor(partitionId, logSize, offset);
    }

    public String toString() {
        return reflectionToString(this);
    }

    @Override
    public int hashCode() {
        return reflectionHashCode(this);
    }

    @Override
    public boolean equals(Object obj) {
        return reflectionEquals(this, obj);
    }
}
