package com.zhang.main

/**
  * @author zhang 2019-11-30 00:58
  */

import com.zhang.util.Conf
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.{CanCommitOffsets, HasOffsetRanges, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

// 主函数入口, spark streaming consume kafka，测试spark streming 读取kafka
object SSConsumeKafka {
  def main(args: Array[String]): Unit = {
    val realFeature = new SSConsumeKafka()
    realFeature.train()
  }
}

class SSConsumeKafka {

  def createContext(): StreamingContext = {
    // If you do not see this printed, that means the StreamingContext has been loaded
    // from the new checkpoint
    println("Creating new context: SSConsumeKafka")

    val sc = SparkSession.builder().master(Conf.master).appName("SSConsumeKafka").getOrCreate().sparkContext
    val ssc = new StreamingContext(sc, Seconds(Conf.streamIntervel))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> Conf.brokers,
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> Conf.ss_consume_group,
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

    stream.foreachRDD(rdd => {

      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges

      rdd.foreachPartition(iter => {
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        println()
      })

      rdd.foreach(println(_))

      // some time later, after outputs have completed
      stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
    })

    ssc
  }

  def train(): Unit = {
    val ssc = createContext()
    ssc.start()
    ssc.awaitTermination()
  }
}
