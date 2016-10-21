import kafka.javaapi.consumer.SimpleConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by Fasten on 2016/10/21.
 */
public class SampleConsumer extends PartitionConsumer{
    private static final Logger logger = LoggerFactory.getLogger(SampleConsumer.class);

    public SampleConsumer(List<String> seedBrokers, int port, String topic, int partition, long readOffset, String groupId) {
        super(seedBrokers, port, topic, partition, readOffset, groupId);
    }

    @Override
    public boolean init() {

        long lastTimeOffset = fetchOffset();
        if (lastTimeOffset != -1) {
            readOffset = lastTimeOffset;
        }
        logger.info("Get offset {} for partition {}", readOffset, partition);

        boolean meetError = false;
        if (meetError) {
            return false;
        }
        return true;
    }

    @Override
    public boolean handleMessage(long offset, byte[] bytes, SimpleConsumer consumer) {
        System.out.println(String.format("Offset %d, message: %s", offset, new String(bytes)));
        commitOffset(consumer, offset);

        boolean stopToConsume = false;

        if (stopToConsume) {
            return false;
        }
        return true;
    }

    @Override
    public void release(SimpleConsumer consumer) {
        // Release resources
        super.release(consumer);
    }
}
