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
public abstract class PartitionConsumer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(PartitionConsumer.class);

    private static final int correlationId = 0;
    private List<String> m_replicaBrokers = new ArrayList();
    private List<String> seedBrokers;
    private int port;
    protected String topic;
    protected int partition;
    protected long readOffset;
    private String groupId;

    private SimpleConsumer consumer;

    public PartitionConsumer(List<String> seedBrokers,
                             int port,
                             String topic,
                             int partition,
                             long readOffset,
                             String groupId) {
        this.seedBrokers = seedBrokers;
        this.port = port;
        this.topic = topic;
        this.partition = partition;
        this.readOffset = readOffset;
        this.groupId = groupId;
    }

    public boolean init() {
        return true;
    }

    /**
     * @param offset offset
     * @param bytes  message
     * @return true for continue, false for break loop
     */
    public boolean handleMessage(long offset, byte[] bytes) {
        return true;
    }

    public void release(SimpleConsumer consumer) {
        if (consumer == null)
            consumer.close();
    }

    public void run() {
        if (!init()) {
            release(null);
            return;
        }

        PartitionMetadata metadata = findLeader(seedBrokers);
        if (metadata == null) {
            logger.error("Can't find metadata for Topic and Partition. Exiting");
            return;
        }
        if (metadata.leader() == null) {
            logger.error("Can't find Leader for Topic and Partition. Exiting");
            return;
        }
        String leadBroker = metadata.leader().host();
        String clientName = "Client_" + topic + "_" + partition;

        consumer = new SimpleConsumer(leadBroker, port, 100000, 65536, clientName);

        long lastReadOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime(), clientName);
        logger.info("Earliest offset for partition {} : {}", partition, lastReadOffset);
        if (readOffset > lastReadOffset) {
            lastReadOffset = readOffset;
        }

        int numErrors = 0;

        while (true) {
            if (consumer == null) {
                consumer = new SimpleConsumer(leadBroker, port, 100000, 65536, clientName);
            }
            FetchRequest req = new FetchRequestBuilder()
                    .clientId(clientName)
                    .addFetch(topic, partition, lastReadOffset, 100000).build();

            FetchResponse fetchResponse = consumer.fetch(req);

            if (fetchResponse.hasError()) {
                ++numErrors;

                short code = fetchResponse.errorCode(topic, partition);
                logger.warn("Error fetching data from the Broker:" + leadBroker + " Reason: " + code);
                if (numErrors > 5) break;
                if (code == ErrorMapping.OffsetOutOfRangeCode()) {
                    lastReadOffset = getLastOffset(consumer, topic, partition, kafka.api.OffsetRequest.LatestTime(), clientName);
                }

                consumer.close();
                consumer = null;
                try {
                    leadBroker = findNewLeader(leadBroker);
                } catch (Exception e) {
                    logger.error("exit with exception", e);
                    return;
                }
            }

            numErrors = 0;

            long numRead = 0L;
            for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
                long currentOffset = messageAndOffset.offset();
                if (currentOffset < lastReadOffset) {
                    logger.info("Found an old offset: {} Expecting: {}", currentOffset, lastReadOffset);
                }

                lastReadOffset = messageAndOffset.nextOffset();
                ByteBuffer payload = messageAndOffset.message().payload();

                byte[] bytes = new byte[payload.limit()];
                payload.get(bytes);

                // TODO
                if (!handleMessage(messageAndOffset.offset(), bytes)) {
                    logger.debug("end for consume data");
                    release(consumer);
                    return;
                }
                numRead += 1L;
            }

            if (numRead == 0L)
                try {
                    Thread.sleep(5000L);
                } catch (InterruptedException ie) {
                }
        }
    }

    private String findNewLeader(String oldLeader) throws Exception {
        for (int i = 0; i < 3; ++i) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader(this.m_replicaBrokers);
            if (metadata == null)
                goToSleep = true;
            else if (metadata.leader() == null)
                goToSleep = true;
            else if ((oldLeader.equalsIgnoreCase(metadata.leader().host())) && (i == 0)) {
                goToSleep = true;
            } else return metadata.leader().host();

            if (!(goToSleep)) continue;
            try {
                Thread.sleep(1000L);
            } catch (InterruptedException ie) {
            }
        }
        throw new Exception("Unable to find new leader after Broker failure. Exiting");
    }

    private PartitionMetadata findLeader(List<String> seedBrokers) {
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
                logger.warn("Error communicating with Broker " +
                        "[{}] to find Leader for [{}, {}] " +
                        "Reason: {}", seed, topic, partition, e);
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

    public boolean commitOffset(SimpleConsumer consumer, long offset) {
        logger.info("Commit Offset:{} for Topic:{} Partition:{}",
                offset, topic, partition);
        return commitOffset(consumer, groupId, topic, partition, offset);
    }

    public static boolean commitOffset(SimpleConsumer consumer, String groupId,
                                       String topic, int partition, long offset) {
        String clientName = "Client_" + topic + "_" + partition;
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

        Map<TopicAndPartition, OffsetAndMetadata> requestInfo = new HashMap<>();
        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset, OffsetAndMetadata.NoMetadata(), -1L);
        requestInfo.put(topicAndPartition, offsetAndMetadata);

        OffsetCommitRequest commitRequest = new OffsetCommitRequest(groupId, requestInfo, correlationId, clientName, kafka.api.OffsetRequest.CurrentVersion());

        OffsetCommitResponse response = consumer.commitOffsets(commitRequest);
        return response.hasError();
    }

    public long fetchOffset() {
        for (String seed : seedBrokers) {
            SimpleConsumer consumer = new SimpleConsumer(seed, port, 100000, 64 * 1024, "fetchOffset");
            return fetchOffset(consumer, groupId, topic, partition);
        }
        return kafka.api.OffsetRequest.EarliestTime();
    }

    public static long fetchOffset(SimpleConsumer consumer, String groupId,
                                   String topic, int partition) {

        String clientName = "Client_" + topic + "_" + partition;
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);

        List<TopicAndPartition> requestInfo = new ArrayList<>();
        requestInfo.add(topicAndPartition);
        OffsetFetchRequest fetchRequest = new OffsetFetchRequest(groupId, requestInfo,
                kafka.api.OffsetRequest.CurrentVersion(), correlationId, clientName);

        OffsetFetchResponse response = consumer.fetchOffsets(fetchRequest);

        OffsetMetadataAndError offset = response.offsets().get(topicAndPartition);
        if (offset.error() == 0)
            return offset.offset();
        else
            return 0;
    }

    public static long getLastOffset(
            SimpleConsumer consumer,
            String topic,
            int partition,
            long whichTime,
            String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map requestInfo = new HashMap();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName);

        OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            logger.warn("Error fetching data Offset Data the Broker. Reason: {}", response.errorCode(topic, partition));
            return 0L;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
}
