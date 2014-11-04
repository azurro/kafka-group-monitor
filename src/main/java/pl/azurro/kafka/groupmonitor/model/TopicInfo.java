package pl.azurro.kafka.groupmonitor.model;

import static org.apache.commons.lang.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang.builder.HashCodeBuilder.reflectionHashCode;
import static org.apache.commons.lang.builder.ToStringBuilder.reflectionToString;

import java.util.HashMap;
import java.util.Map;

public class TopicInfo {

    private String topicName;
    private Map<Integer, PartitionInfo> partitionsInfos;

    public TopicInfo(String topicName) {
        this.topicName = topicName;
        partitionsInfos = new HashMap<Integer, PartitionInfo>();
    }

    public String getTopicName() {
        return topicName;
    }

    public Map<Integer, PartitionInfo> getPartitionsInfos() {
        return partitionsInfos;
    }

    public void addInfoFor(int partitionId, long logSize, long offset) {
        PartitionInfo info = partitionsInfos.get(partitionId);
        if (info == null) {
            info = new PartitionInfo(partitionId);
            partitionsInfos.put(partitionId, info);
        }
        info.setLogSize(logSize);
        info.setOffset(offset);
    }

    @Override
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
