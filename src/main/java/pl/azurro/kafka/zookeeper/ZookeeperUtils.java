package pl.azurro.kafka.zookeeper;

import static kafka.utils.ZkUtils.ConsumersPath;
import static kafka.utils.ZkUtils.getAllBrokersInCluster;

import java.util.ArrayList;
import java.util.List;

import kafka.cluster.Broker;
import kafka.utils.ZKStringSerializer$;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkNoNodeException;

import scala.collection.Iterator;
import scala.collection.Seq;

public class ZookeeperUtils {

    private static final int sessionTimeoutMs = 30000;
    private static final int connectionTimeoutMs = 30000;

    public ZkClient getZkClient(String zkServers, int sessionTimeoutMs, int connectionTimeoutMs) {
        return new ZkClient(zkServers, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    }

    public ZkClient getZkClient(String zkServers) {
        return new ZkClient(zkServers, sessionTimeoutMs, connectionTimeoutMs, ZKStringSerializer$.MODULE$);
    }

    public void close(ZkClient zkClient) {
        if (zkClient != null) {
            zkClient.close();
        }
    }

    public long getLastConsumedOffset(ZkClient zkClient, String groupId, String topic, int partition) {
        String znode = getOffsetsPathFor(groupId, topic, partition);
        String offset = zkClient.readData(znode, true);
        if (offset == null) {
            return -1L;
        }
        return Long.valueOf(offset);
    }

    public List<String> getTopicsForGroup(ZkClient zkClient, String groupId) {
        return getChildrenParentMayNotExist(zkClient, String.format("%s/%s/offsets", ConsumersPath(), groupId));
    }

    public List<Broker> getBrokers(ZkClient zkClient) {
        List<Broker> brokers = new ArrayList<Broker>();
        Seq<Broker> allBrokersInCluster = getAllBrokersInCluster(zkClient);
        Iterator<Broker> it = allBrokersInCluster.iterator();
        while (it.hasNext()) {
            Broker broker = it.next();
            brokers.add(broker);
        }
        return brokers;
    }

    private String getOffsetsPathFor(String groupId, String topic, int partition) {
        return String.format("%s/%s/offsets/%s/%d", ConsumersPath(), groupId, topic, partition);
    }

    private List<String> getChildrenParentMayNotExist(ZkClient zkClient, String path) {
        try {
            return zkClient.getChildren(path);
        } catch (ZkNoNodeException e) {
            return new ArrayList<String>();
        }
    }
}
