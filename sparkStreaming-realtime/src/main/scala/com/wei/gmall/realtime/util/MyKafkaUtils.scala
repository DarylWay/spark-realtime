package com.wei.gmall.realtime.util

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import java.util.Properties
import scala.collection.mutable

/**
 * kafka工具类, 用于生产数据和消费数据
 */

object MyKafkaUtils {
  /**
   * 消费者配置
   * ConsumerConfig
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object]( // mutable将不可变map改为可变map
    // kafka集群位置
    // ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092, hadoop103:9092, hadoop104:9092",
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS),
    // kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    // groupId, 由getKafkaDStream方法传参指定
    // offset提交, 默认指定为自动提交
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    // 自动提交的时间间隔, 默认为5000ms, 可通过下列语句修改
    //ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "",
    // offset重置, 常用包括"ealiest" "latest"
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"

  )

  /**
   * 基于SparkStreaming消费, 获取到KafkaDStream, 使用默认的offset
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }

  /**
   * 生产者对象
   */
  val producer: KafkaProducer[String, String] = createProducer()

  /**
   * 创建生产者对象
   */
  def createProducer(): KafkaProducer[String, String] = {
    val producerConfigs = new Properties()
    // 生产者配置类producerConfigs
    // kafka集群位置
    // producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092, hadoop103:9092, hadoop104:9092")
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, MyPropsUtils(MyConfig.KAFKA_BOOTSTRAP_SERVERS))
    // kv序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    // acks
    producerConfigs.put(ProducerConfig.ACKS_CONFIG, "all")
    // batch.size use default 16kb
    // linger.ms use default 0
    // retries use default 0
    // 幂等配置
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)
    producer
  }

  /**
   * 生产(按照默认的黏性分区策略)
   */
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * 生产(重载方法, 按照key进行分区)
   */
  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /**
   * 关闭生产者对象
   */
  def close(): Unit = {
    if(producer != null) producer.close()
  }
}
