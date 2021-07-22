package weather_alerts.scala.services

import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import weather_alerts.configuration.Configuration

import java.util
import java.util.Properties
import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.JavaConverters.asJavaCollectionConverter
import java.util.{Collections, Properties}
object KafkaManage {




  def getOffsets(topic:String):  List[Long]= {
    val props: Properties = new Properties()
    props.put("group.id", "0")

    props.put("bootstrap.servers",Configuration.bootstrupServer)
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("enable.auto.commit", "true")
    props.put("auto.commit.interval.ms", "10000")
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

    val consumer = new KafkaConsumer(props)
    val partitions = consumer.partitionsFor(topic).map(it => new TopicPartition(topic, it.partition)).toList.asJavaCollection
    consumer.assign(partitions)
    consumer.seekToEnd(partitions)
    partitions.map(it => consumer.position(it)).asJavaCollection.toList
  }

}
