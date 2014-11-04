package pl.azurro.kafka.groupmonitor;

import static java.util.Collections.singletonList;
import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import kafka.cluster.Broker;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import pl.azurro.kafka.groupmonitor.model.GroupInfo;
import pl.azurro.kafka.zookeeper.ZookeeperUtils;

@RunWith(MockitoJUnitRunner.class)
public class GroupMonitorTest {
    private static final String TOPIC_NAME = "topicName";

    private static final String GROUP_ID = "testGroup";

    private static final String zkServers = "host1:2181,host2:2181,host3:2181/mychroot";

    @Mock
    private SimpleConsumer consumer;
    @Mock
    private ZookeeperUtils zookeeperUtils;
    @Mock
    private ZkClient zkClient;
    @Mock
    private Broker broker;

    private GroupMonitor monitor = new GroupMonitor(zkServers) {
        @Override
        SimpleConsumer createSimpleConsumerFor(Broker broker) {
            return consumer;
        }

        @Override
        ZookeeperUtils createZookeeperUtils() {
            return zookeeperUtils;
        }
    };

    @Before
    public void setUp() {
        when(zookeeperUtils.getZkClient(zkServers)).thenReturn(zkClient);
        when(zookeeperUtils.getBrokers(zkClient)).thenReturn(singletonList(broker));
        when(broker.getConnectionString()).thenReturn("host1:9092");
    }

    @Test
    public void shouldReturnGroupInfo() throws Exception {
        OffsetResponse offsetResponse = mock(OffsetResponse.class);
        TopicMetadataResponse response = mock(TopicMetadataResponse.class);
        TopicMetadata topicMetadata = mock(TopicMetadata.class);
        PartitionMetadata partitionMetadata = mock(PartitionMetadata.class);
        when(topicMetadata.partitionsMetadata()).thenReturn(singletonList(partitionMetadata));
        when(response.topicsMetadata()).thenReturn(singletonList(topicMetadata));
        when(consumer.send(any(TopicMetadataRequest.class))).thenReturn(response);
        when(topicMetadata.topic()).thenReturn(TOPIC_NAME);
        when(partitionMetadata.leader()).thenReturn(broker);
        when(partitionMetadata.partitionId()).thenReturn(0);
        when(zookeeperUtils.getLastConsumedOffset(zkClient, GROUP_ID, TOPIC_NAME, 0)).thenReturn(90L);
        when(consumer.getOffsetsBefore(any(OffsetRequest.class))).thenReturn(offsetResponse);
        when(offsetResponse.hasError()).thenReturn(false);
        when(offsetResponse.offsets(TOPIC_NAME, 0)).thenReturn(new long[] { 1112 });

        GroupInfo info = monitor.getGroupInfo(GROUP_ID);

        GroupInfo expectedInfo = new GroupInfo(GROUP_ID);
        expectedInfo.addInfoFor(TOPIC_NAME, 0, 1112, 90);
        assertThat(info).isEqualTo(expectedInfo);
    }
}
