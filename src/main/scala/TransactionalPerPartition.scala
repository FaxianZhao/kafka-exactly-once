import _root_.kafka.common.TopicAndPartition
import _root_.kafka.message.MessageAndMetadata
import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.{Partition, SparkConf}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
    Reference https://github.com/koeninger/kafka-exactly-once

    exactly-once semantics from kafka, by storing offsets in the same transaction as the results
    Offsets and results will be stored per-partition, on the executors
  */
object TransactionalPerPartition {

  def main(args: Array[String]) {
    val conf = ConfigFactory.load
    val kafkaParams = Map(
      "metadata.broker.list" -> conf.getString("kafka.brokers")
    )
    // Currently, only support one topic
    val topic = conf.getString("kafka.topic.name")
    val groupId = conf.getString("kafka.topic.groupId")
    val topicPartitionCnt = conf.getInt("kafka.topic.partitionCnt")

    val ssc = new StreamingContext(new SparkConf, Seconds(60))

    // query previous offset by topicAndPartitions in zookeeper
    val topicAndPartitions = (
      for (i <- 0 to topicPartitionCnt - 1)
        yield TopicAndPartition(topic, i)
      ).toSet

    // if we can get the offsets from zookeeper, just use it.
    // otherwise, consume from the latest
    val stream = new ExactlyOnceUtil(kafkaParams).getConsumerOffsets(groupId, topicAndPartitions) match {
      // There is no previous offset for this groupId, consume from the latest offset
      case Left(err) => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, Set(topic))

      // Continue to consume, from the previous offset
      case Right(fromOffsets) => KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](
    ssc, kafkaParams, fromOffsets.asInstanceOf[Map[TopicAndPartition, Long]], (mmd: MessageAndMetadata[String, String]) => (mmd.key, mmd.message))
    }


    var offsetRanges = Array[OffsetRange]()

    stream.transform { rdd =>
      // Cast the rdd to an interface that lets us get an array of OffsetRange
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      val partitions = offsetRanges.zipWithIndex.map { case (o, i) =>
        (i -> new KafkaPartition(i, o.topic, o.partition, o.fromOffset, o.untilOffset))
      }.toMap

      // Ignore kafka messages' key
      rdd.map(_._2).
        mapPartitionsWithIndex { (index, iter) =>
          val parts = partitions.get(index).get
          for (i <- iter) yield ((parts.partition, parts.untilOffset), i)
        }
    }.foreachRDD { rdd =>
      val util = new ExactlyOnceUtil(kafkaParams)
      val offset = mutable.Map[TopicAndPartition, Long]()

      // for each partition & untilOffset
      for (kafkaPart <- rdd.map(line => (line._1, 1)).groupByKey().map(_._1).collect) {
        rdd.filter(_._1.equals(kafkaPart)).map(_._2).
        /*
        your logic code,
        foreach(println) just an example
        */foreach(println)

        offset += (TopicAndPartition(topic, kafkaPart._1) -> kafkaPart._2)
      }

      // update the offset to zookeeper
      util.setConsumerOffsets(groupId, offset.toMap)
    }

    ssc.start()
    ssc.awaitTermination()
  }
}

//Ref org.apache.spark.streaming.kafka.KafkaRDDPartition
case class KafkaPartition(
                           val index: Int,
                           val topic: String,
                           val partition: Int,
                           val fromOffset: Long,
                           val untilOffset: Long
                           ) extends Partition with Serializable {
  def count(): Long = untilOffset - fromOffset
}

// Reflect org.apache.spark.streaming.kafka.KafkaCluster
class ExactlyOnceUtil(kafkaParams: Map[String, String]) extends Serializable {
  type Err = ArrayBuffer[Throwable]
  val clazz = Class.forName("org.apache.spark.streaming.kafka.KafkaCluster")
  val constructor = clazz.getConstructor(classOf[Map[String, String]])
  val kc = constructor.newInstance(kafkaParams)

  def getConsumerOffsets(groupId: String,
                         topicAndPartitions: Set[TopicAndPartition]
                          ): Either[Err, Map[TopicAndPartition, Long]] = {
    clazz.getMethod(
      "getConsumerOffsets",
      classOf[String],
      classOf[Set[TopicAndPartition]]
    ).invoke(
      kc,
      groupId,
      topicAndPartitions
    ).asInstanceOf[Either[Err, Map[TopicAndPartition, Long]]]
  }

  def setConsumerOffsets(groupId: String,
                         offsets: Map[TopicAndPartition, Long]
                          ): Either[Err, Map[TopicAndPartition, Short]] = {
    clazz.getMethod(
      "setConsumerOffsets",
      classOf[String],
      classOf[Map[TopicAndPartition, Long]]
    ).invoke(
      kc,
      groupId,
      offsets
    ).asInstanceOf[Either[Err, Map[TopicAndPartition, Short]]]

  }
}