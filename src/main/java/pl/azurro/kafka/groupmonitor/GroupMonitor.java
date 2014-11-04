package pl.azurro.kafka.groupmonitor;

import static kafka.api.OffsetRequest.CurrentVersion;
import static kafka.api.OffsetRequest.EarliestTime;
import static kafka.api.OffsetRequest.LatestTime;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import kafka.api.PartitionOffsetRequestInfo;
import kafka.cluster.Broker;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.I0Itec.zkclient.ZkClient;

import pl.azurro.kafka.groupmonitor.model.GroupInfo;
import pl.azurro.kafka.zookeeper.ZookeeperUtils;

public class GroupMonitor {

    private static final String CONSUMER_OFFSET_CHECKER = "ConsumerOffsetChecker";

    private String zkServers;

    public GroupMonitor(String zkServers) {
        this.zkServers = zkServers;
    }

    public GroupInfo getGroupInfo(String groupId) throws IOException {
        ZookeeperUtils zkUtils = createZookeeperUtils();
        ZkClient zkClient = null;
        GroupInfo info = new GroupInfo(groupId);
        try {
            zkClient = zkUtils.getZkClient(zkServers);

            List<Broker> brokers = zkUtils.getBrokers(zkClient);
            List<String> topicsForGroup = zkUtils.getTopicsForGroup(zkClient, groupId);

            for (Broker broker : brokers) {
                SimpleConsumer consumer = null;
                try {
                    consumer = createSimpleConsumerFor(broker);
                    TopicMetadataRequest req = new TopicMetadataRequest(topicsForGroup);
                    TopicMetadataResponse resp = consumer.send(req);
                    List<TopicMetadata> metaData = resp.topicsMetadata();
                    for (TopicMetadata item : metaData) {
                        for (PartitionMetadata part : item.partitionsMetadata()) {
                            if (part.leader().getConnectionString().equalsIgnoreCase(broker.getConnectionString())) {
                                long logSize = getLastOffset(consumer, item.topic(), part.partitionId(), LatestTime(), CONSUMER_OFFSET_CHECKER);
                                if (logSize == -1) {
                                    throw new IllegalStateException("Log size cannot be -1");
                                }
                                long offset = zkUtils.getLastConsumedOffset(zkClient, groupId, item.topic(), part.partitionId());
                                if (offset > logSize) {
                                    offset = getLastOffset(consumer, item.topic(), part.partitionId(), EarliestTime(), CONSUMER_OFFSET_CHECKER);
                                }
                                if (offset == -1) {
                                    throw new IllegalStateException("Last offset cannot be -1");
                                }
                                info.addInfoFor(item.topic(), part.partitionId(), logSize, offset);
                            }
                        }
                    }
                } finally {
                    if (consumer != null)
                        consumer.close();
                }
            }
        } finally {
            zkUtils.close(zkClient);
        }

        return info;
    }

    public long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String consumerId) throws IOException {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfo, CurrentVersion(), consumerId);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if (response.hasError()) {
            throw new IOException("Error fetching data from Broker. Reason: " + response.errorCode(topic, partition));
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets != null && offsets.length > 0 ? offsets[0] : -1;
    }

    SimpleConsumer createSimpleConsumerFor(Broker broker) {
        return new SimpleConsumer(broker.host(), broker.port(), 10000, 64 * 1024, CONSUMER_OFFSET_CHECKER);
    }

    ZookeeperUtils createZookeeperUtils() {
        return new ZookeeperUtils();
    }

}
