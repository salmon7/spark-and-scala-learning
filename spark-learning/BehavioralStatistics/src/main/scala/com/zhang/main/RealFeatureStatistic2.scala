package com.zhang.main

/**
  * @author zhang 2019-11-30 00:58
  */

import com.zhang.util.Conf

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

// 主函数入口, reduceByKeyAndWindow
object RealFeatureStatistic2 {
  def main(args: Array[String]): Unit = {
    val realFeature = new RealFeatureStatistic2()
    realFeature.train()
  }
}

class RealFeatureStatistic2 {
  def constructKV(ssc: StreamingContext): DStream[(String, Set[(String, Int)])] = {
    // Kafka数据流，官方指引 http://spark.apache.org/docs/2.3.3/streaming-kafka-0-10-integration.html
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Conf.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Conf.group,
      // 当groupid不存在时（即第一次以该groupid消费），从latest/earlist读取partition的数据
      "auto.offset.reset" -> "latest",
      // 是否自动提交，如果为true，则在auto.commit.interval.ms时间内，自动提交offset
      // 1.有checkpoint时，不会自动提交offset，每次读取数据按默认的offset策略 或者 checkpoint中存的offset 读取kafka
      // 2.没有checkpoint时，需要手动提交offset
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](Conf.topics, kafkaParams))

    val KV = stream.map(record => {
      val arr = record.value.split(Conf.SEPERATOR)
      (arr(Conf.INDEX_LOG_USER), Set((arr(Conf.INDEX_LOG_TIME), arr(Conf.INDEX_LOG_ITEM).toInt)))
    })

    KV
  }

  def createContext(): StreamingContext = {
    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context: reduceByKeyAndWindow")

    val sc = SparkSession.builder().master(Conf.master).appName("Behavior Statistics: reduceByKeyAndWindow").getOrCreate().sparkContext
    val ssc = new StreamingContext(sc, Seconds(Conf.streamIntervel))
    printf("%s, %s\n", sc.master, sc.deployMode)

    // checkpoint 元数据，driver恢复时需要使用
    ssc.checkpoint(Conf.checkpointDir2)

    val view = constructKV(ssc)
    // ReducedWindowedDStream 默认会进行checkpoint，统计1小时内的用户点击行为，窗口每30s更新一次
    view.reduceByKeyAndWindow(_ ++ _, _ -- _, Seconds(Conf.windowSize), Seconds(Conf.windowUpdate)).filter(x => {
      // 清除过期后set为空的记录
      x._2.nonEmpty
    }).print()

    ssc
  }

  def train(): Unit = {
    val ssc = StreamingContext.getOrCreate(Conf.checkpointDir2, createContext)
    ssc.start()
    ssc.awaitTermination()
  }
}
