package pl.azurro.kafka;

import java.io.IOException;
import java.util.Map.Entry;

import pl.azurro.kafka.groupmonitor.GroupMonitor;
import pl.azurro.kafka.groupmonitor.model.GroupInfo;
import pl.azurro.kafka.groupmonitor.model.PartitionInfo;
import pl.azurro.kafka.groupmonitor.model.TopicInfo;

public class KafkaGroupMonitor {
    public static void main(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: KafkaGroupMonitor <zk servers> <group Id>");
            System.out.println("<zk servers> - comma separated Zookeper connection string, e.g.: host1:2181,host2:2181,host3:2181/mychroot");
            System.out.println("<group Id> - the name of consumers group, e.g.: mygroup1");
            System.exit(1);
        }

        String zkServers = args[0];
        String groupId = args[1];

        GroupMonitor groupMonitor = new GroupMonitor(zkServers);

        GroupInfo groupInfo;
        try {
            groupInfo = groupMonitor.getGroupInfo(groupId);
            long groupLag = 0;
            for (Entry<String, TopicInfo> topicInfo : groupInfo.getTopicsInfos().entrySet()) {
                System.out.println("Topic: " + topicInfo.getKey());
                long topicLag = 0;
                for (Entry<Integer, PartitionInfo> partitionInfo : topicInfo.getValue().getPartitionsInfos().entrySet()) {
                    PartitionInfo p = partitionInfo.getValue();
                    System.out.println(String.format("Parition Id=%d, logSize=%d, offset=%d, lag=%d", p.getId(), p.getLogSize(), p.getOffset(), p.getLag()));
                    topicLag += p.getLag();
                }
                System.out.println("Topic lag=" + groupLag);
                groupLag += topicLag;
            }
            System.out.println("Group lag=" + groupLag);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
