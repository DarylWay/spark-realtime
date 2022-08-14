package com.wei.gmall.realtime.util

import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

import scala.collection.mutable

/**
 * kafka工具类, 用于生产数据和消费数据
 */

object MyKafkaUtils {
  /**
   * 消费者配置
   * ConsumerConfig
   */
  private val consumerConfigs: mutable.Map[String, Object] = mutable.Map[String, Object](// mutable将不可变map改为可变map
    // kafka集群位置
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop102:9092, hadoop103:9092, hadoop104:9092",
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
  def getKafkaDStream(ssc : StreamingContext, topic: String, groupId: String): InputDStream[ConsumerRecord[String, String]] ={
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)

    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs))
    kafkaDStream
  }

  /**
   * 生产
   */
}
