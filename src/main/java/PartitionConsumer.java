import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetAndMetadata;
import kafka.common.OffsetMetadataAndError;
import kafka.common.TopicAndPartition;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created by Fasten on 2016/3/30.
 */
public class PartitionConsumer {
    private static final Logger logger = LoggerFactory.getLogger(PartitionConsumer.class);
    private static final int correlationId = 0;
    private List<String> m_replicaBrokers;

    private List<String> seedBrokers;
    private int port;

    private String topic;
    private int partition;
    private String groupId;

    private SimpleConsumer consumer;
    private String clientName;
    private String leadBroker;

    public PartitionConsumer(List<String> seedBrokers, int port,
                             String topic, int partition, String groupId) {
        this.seedBrokers = seedBrokers;
        this.port = port;
        this.topic = topic;
        this.partition = partition;
        this.groupId = groupId;
        clientName = "Client_" + topic + "_" + partition;

        m_replicaBrokers = new ArrayList<>();
    }

    private SimpleConsumer initSimpleConsumer(){
        // find the meta data about the topic and partition we are interested in
        //
        PartitionMetadata metadata = findLeader();
        if (metadata == null) {
            System.out.println("Can't find metadata for Topic and Partition. Exiting");
            return null;
        }
        if (metadata.leader() == null) {
            System.out.println("Can't find Leader for Topic and Partition. Exiting");
            return null;
        }
        leadBroker = metadata.leader().host();
        clientName = "Client_" + topic + "_" + partition;

        return new SimpleConsumer(leadBroker, port, 100000, 64 * 1024, clientName);
    }

    public void run(long a_readOffset) throws Exception {

        consumer = initSimpleConsumer();

        long readOffset = Math.max(Math.max(a_readOffset,
                        fetchOffset(consumer, groupId, topic, partition)),
                getLastOffset(consumer, topic, partition,
                        kafka.api.OffsetRequest.EarliestTime(), clientName));
        logger.info("Partition {} fetching offset from {}", partition, readOffset);
        int numErrors = 0;

        while (true) {
            if (consumer == null) {
                consumer = new SimpleConsumer(
                        leadBroker, port, 100000, 64 * 1024, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, readOffset, 100000) // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
                    .build();
            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                numErrors++;
                // Something went wrong!
                short code = fetchResponse.errorCode(topic, partition);
                System.out.println("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    // We asked for an invalid offset. For simple case ask for the last element to reset
                    readOffset = getLastOffset(consumer, topic, partition,
                            kafka.api.OffsetRequest.LatestTime(), clientName);
                    continue;
                }
                consumer.close();
                consumer = null;
                leadBroker = findNewLeader(leadBroker);
                continue;
            }
            numErrors = 0;

            long numRead = 0;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < readOffset) {
                    System.out.println("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                    continue;
                }
                readOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);

                // TODO: your logic code
                // if success
                commitOffset(readOffset);

                numRead++;
            }

            if (numRead == 0) {
                try {
                    // TODO
                    Thread.sleep(5000);
                } catch (InterruptedException ie) {
                }
            }
        }
//        if (consumer != null) consumer.close();
    }

    public boolean commitOffset(long offset) {
        return commitOffset(consumer, groupId, topic, partition, offset);
    }

    public static boolean commitOffset(SimpleConsumer consumer, String groupId,
                                       String topic, int partition, long offset) {
        String clientName = "Client_" + topic + "_" + partition;
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

        Map<TopicAndPartition, OffsetAndMetadata> requestInfo = new HashMap<>();
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, OffsetAndMetadata.NoMetadata(), -1L);
        requestInfo.put(topicAndPartition, offsetAndMetadata);

        OffsetCommitRequest commitRequest = new OffsetCommitRequest(groupId, requestInfo, correlationId,clientName,kafka.api.OffsetRequest.CurrentVersion());

        OffsetCommitResponse response = consumer.commitOffsets(commitRequest);
        return response.hasError();
    }

    public static long fetchOffset(SimpleConsumer consumer, String groupId,
                                   String topic, int partition) {

        String clientName = "Client_" + topic + "_" + partition;
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

        List<TopicAndPartition> requestInfo = new ArrayList<>();
        requestInfo.add(topicAndPartition);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(groupId,requestInfo,
                kafka.api.OffsetRequest.CurrentVersion(), correlationId, clientName);

        OffsetFetchResponse response = consumer.fetchOffsets(fetchRequest);

        OffsetMetadataAndError offset = response.offsets().get(topicAndPartition);
        if(offset.error() == 0)
            return offset.offset();
        else
            return 0;
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition,
                                     long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(
                requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out.println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    private String findNewLeader(String a_oldLeader) throws Exception {
        for (int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader();
            if (metadata == null) {
                goToSleep = true;
            } else if (metadata.leader() == null) {
                goToSleep = true;
            } else if (a_oldLeader.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                // first time through if the leader hasn't changed give ZooKeeper a second to recover
                // second time, assume the broker did recover before failover, or it was a non-Broker issue
                //
                goToSleep = true;
            } else {
                return metadata.leader().host();
            }
            if (goToSleep) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                }
            }
        }
        System.out.println("Unable to find new leader after Broker failure. Exiting");
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    private PartitionMetadata findLeader() {
        PartitionMetadata returnMetaData = null;
        loop:
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = null;
            try {
                consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "leaderLookup");
                List<String> topics = Collections.singletonList(topic);
                TopicMetadataRequest req = new TopicMetadataRequest(topics);
                kafka.javaapi.TopicMetadataResponse resp = consumer.send(req);

                List<TopicMetadata> metaData = resp.topicsMetadata();
                for (TopicMetadata item : metaData) {
                    for (PartitionMetadata part : item.partitionsMetadata()) {
                        if (part.partitionId() == partition) {
                            returnMetaData = part;
                            break loop;
                        }
                    }
                }
            } catch (Exception e) {
                System.out.println("Error communicating with Broker [" + seed + "] to find Leader for [" + topic
                        + ", " + partition + "] Reason: " + e);
            } finally {
                if (consumer != null) consumer.close();
            }
        }
        if (returnMetaData != null) {
            m_replicaBrokers.clear();
            for (kafka.cluster.Broker replica : returnMetaData.replicas()) {
                m_replicaBrokers.add(replica.host());
            }
        }
        return returnMetaData;
    }
}
