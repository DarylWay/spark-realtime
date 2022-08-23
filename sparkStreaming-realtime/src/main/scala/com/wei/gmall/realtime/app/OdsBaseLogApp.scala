package com.wei.gmall.realtime.app

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.alibaba.fastjson.serializer.SerializeConfig
import com.wei.gmall.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.wei.gmall.realtime.util.MyKafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Second
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * 日志数据的消费分流
 * 1. 准备实时处理环境 StreamingContext
 * 2. 从kafka中消费数据
 * 3. 处理数据
 *  3.1 转换数据结构
 *      专用结构: 需要单独封装Bean对象, 只适合当前项目
 *      通用结构: Map, JsonObject等, 对各种项目都通用
 *  3.2 分流
 * 4. 写出到DWD层
 */
object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    //1. 准备实时环境
    //TODO 注意并行度与Kafka中topic的分区个数的对应关系
    val sparkConf: SparkConf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val ssc: StreamingContext = new StreamingContext(sparkConf , Seconds(5))

    //2. 从kafka中消费数据
    val topicName : String = "ODS_BASE_LOG_1018"  //对应生成器配置中的主题名
    val groupId : String = "ODS_BASE_LOG_GROUP_1018"

    val KafkaDStream = MyKafkaUtils.getKafkaDStream(ssc, topicName, groupId)

    // KafkaDStream.print(100)
    // 3. 处理数据
    // 3.1 转换数据结构
    val jsonObjDStream = KafkaDStream.map(
      consumerRecord => {
        // 获取ConsumerRecord中的value, value即我们需要的日志数据
        val log = consumerRecord.value()
        // 转换成json对象
        val jsonObj = JSON.parseObject(log)
        // 返回
        jsonObj
      }
    )

    // jsonObjDStream.print(1000)

    // 3.2 分流
    // 日志数据:
    // ①页面访问数据
    //   公共字段
    //   页面数据
    //   曝光数据
    //   事件数据
    //   错误数据
    // ②启动数据
    //   公共字段
    //   启动数据
    //   错误数据

    val DWD_PAGE_LOG_TOPIC:String = "DWD_PAGE_LOG_TOPIC_1018" // 页面访问
    val DWD_PAGE_DISPLAY_TOPIC:String = "DWD_PAGE_DISPLAY_TOPIC_1018" // 页面曝光
    val DWD_PAGE_ACTION_TOPIC:String = "DWD_PAGE_ACTION_TOPIC_1018" // 页面事件
    val DWD_START_LOG_TOPIC:String = "DWD_START_LOG_TOPIC_1018" // 启动数据
    val DWD_ERROR_LOG_TOPIC:String = "DWD_ERROR_LOG_TOPIC_1018" // 错误数据

    // 分流规则
    // 错误数据: 不做任何的拆分, 只要包含错误字段, 直接整条数据发送到对应的topic
    // 页面数据: 拆分成页面访问, 曝光, 事件, 分别发送到对应的topic
    // 启动数据: 发送到对应的topic

    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreach(
          jsonObj => {
            // 分流过程
            // 1. 分流错误数据
            val errObj = jsonObj.getJSONObject("err")
            if(errObj != null){
              // 将错误数据发送到Kafka的DWD_ERROR_LOG_TOPIC主题下
              MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
            }else{
              // 提取公共字段
              val commonObj = jsonObj.getJSONObject("common")
              val ar = commonObj.getString("ar")
              val uid = commonObj.getString("uid")
              val os = commonObj.getString("os")
              val ch = commonObj.getString("ch")
              val isNew = commonObj.getString("is_new")
              val md = commonObj.getString("md")
              val mid = commonObj.getString("mid")
              val vc = commonObj.getString("vc")
              val ba = commonObj.getString("ba")

              // 提取时间戳
              val ts = jsonObj.getLong("ts")

              // 页面数据
              val pageObj = jsonObj.getJSONObject("page")
              if(pageObj != null){
                // 提取page字段
                val pageId = pageObj.getString("page_id")
                val pageItem = pageObj.getString("item")
                val pageItemType = pageObj.getString("item_type")
                val duringTime = pageObj.getLong("during_time")
                val lastPageId = pageObj.getString("last_page_id")
                val sourceType = pageObj.getString("source_type")

                // 封装成PageLog
                val pageLog = PageLog(mid, uid, ar, ch, isNew, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, sourceType, duringTime , ts)

                // 发送到DWD_PAGE_LOG_TOPIC主题中
                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                // 提取曝光数据
                val displaysJsonArr: JSONArray = jsonObj.getJSONArray("displays")
                if(displaysJsonArr != null && displaysJsonArr.size() > 0 ){// 保证有数组兵并且数组size>=0, 即存在数据
                  for(i <- 0 until displaysJsonArr.size()){// 由于是数组对象需要进行迭代
                    //循环拿到每个曝光
                    val displayObj: JSONObject = displaysJsonArr.getJSONObject(i)
                    //提取曝光字段
                    val displayType: String = displayObj.getString("display_type")
                    val displayItem: String = displayObj.getString("item")
                    val displayItemType: String = displayObj.getString("item_type")
                    val posId: String = displayObj.getString("pos_id")
                    val order: String = displayObj.getString("order")

                    //封装成PageDisplayLog
                    val pageDisplayLog =
                      PageDisplayLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,displayType,displayItem,displayItemType,order,posId,ts)
                    // 写到 DWD_PAGE_DISPLAY_TOPIC
                    MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC , JSON.toJSONString(pageDisplayLog , new SerializeConfig(true)))
                  }
                }

                // 提取事件数据
                val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                if(actionJsonArr != null && actionJsonArr.size() > 0 ){
                  for(i <- 0 until actionJsonArr.size()){
                    val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                    //提取字段
                    val actionId: String = actionObj.getString("action_id")
                    val actionItem: String = actionObj.getString("item")
                    val actionItemType: String = actionObj.getString("item_type")
                    val actionTs: Long = actionObj.getLong("ts")

                    //封装PageActionLog
                    var pageActionLog =
                      PageActionLog(mid,uid,ar,ch,isNew,md,os,vc,ba,pageId,lastPageId,pageItem,pageItemType,duringTime,sourceType,actionId,actionItem,actionItemType,actionTs,ts)
                    //写出到DWD_PAGE_ACTION_TOPIC
                    MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC , JSON.toJSONString(pageActionLog , new SerializeConfig(true)))
                  }
                }
              }
              // 启动数据
              val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
              if(startJsonObj != null ) {
                //提取字段
                val entry: String = startJsonObj.getString("entry")
                val loadingTime: Long = startJsonObj.getLong("loading_time")
                val openAdId: String = startJsonObj.getString("open_ad_id")
                val openAdMs: Long = startJsonObj.getLong("open_ad_ms")
                val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms")

                //封装StartLog
                var startLog =
                  StartLog(mid, uid, ar, ch, isNew, md, os, vc, ba, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)
                //写出DWD_START_LOG_TOPIC
                MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
              }
            }
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }
}
