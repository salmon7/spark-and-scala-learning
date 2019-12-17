package com.zhang.producer

/**
  * @author zhang 2019-11-28 22:26
  */

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.util.Random

// 模拟产生1000个用户按照时间戳点击100个商品
// 数据格式：
// key: ${user}
// value: ${timestamp}\t${user}\t${item}
object Producer extends App {
  val events = args(0).toInt
  val topic = args(1)
  val brokers = args(2)
  val rnd = new Random()
  val props = new Properties()
  props.put("bootstrap.servers", brokers)
  props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)
  val t = System.currentTimeMillis()

  for (nEvents <- Range(0, events)) {
    // 生成模拟数据(timestamp, user, item)
    val timestamp = System.currentTimeMillis
    val user = rnd.nextInt(1000)
    val item = rnd.nextInt(100)
    val data = new ProducerRecord[String, String](topic, user.toString, s"${timestamp}\t${user}\t${item}")

    //async
    //producer.send(data, (m,e) => {})
    //sync
    producer.send(data)
    if (rnd.nextInt(100) < 50) Thread.sleep(10)
  }

  System.out.println("sent per second: " + events * 1000 / (System.currentTimeMillis() - t))
  producer.close()
}
