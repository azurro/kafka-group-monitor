package pl.azurro.kafka.zookeeper;

import static org.fest.assertions.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.I0Itec.zkclient.ZkClient;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class ZookeeperUtilsTest {

    private ZookeeperUtils utils = new ZookeeperUtils();
    @Mock
    private ZkClient zkClient;

    @Test
    public void shouldReturnMinusOneForNotExistingOffset() {
        long lastConsumedOffset = utils.getLastConsumedOffset(zkClient, "testGroup", "testTopic", 0);

        assertThat(lastConsumedOffset).isEqualTo(-1L);
    }

    @Test
    public void shouldReturnOneForExistingOffset() {
        when(zkClient.readData("/consumers/testGroup/offsets/testTopic/0")).thenReturn(1L);
        long lastConsumedOffset = utils.getLastConsumedOffset(zkClient, "testGroup", "testTopic", 0);

        assertThat(lastConsumedOffset).isEqualTo(-1L);
    }

}
