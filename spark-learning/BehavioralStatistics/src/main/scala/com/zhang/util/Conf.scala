package com.zhang.util

/**
  * @author zhang 2019-11-30 01:00
  */

object Conf {

  // kafka config
  val brokers = "localhost:9092"
  val group = "behavior_group"
  val topics = List("test_behavior")
  val ss_consume_group = "ss_consume_group"

  // Kafka记录
  val INDEX_LOG_TIME = 0
  val INDEX_LOG_USER = 1
  val INDEX_LOG_ITEM = 2
  val SEPERATOR = "\t"

  // 窗口配置
  var windowSize = 3600
  val windowUpdate = 30

  // spark context config
  // 1. local mode, idea 直接运行
  // val master = "local"
  // val master = "local[*]"
  // val checkpointDir1 = "/Users/gzzhangqilong2017/Workspace/github/spark-and-scala-learning/spark-learning/BehavioralStatistics/tmp1"
  // val checkpointDir2 = "/Users/gzzhangqilong2017/Workspace/github/spark-and-scala-learning/spark-learning/BehavioralStatistics/tmp2"
  // val streamIntervel = 10

  // 2. standalone mode
//   val master = "spark://localhost:7077"
//   val master = "spark://zhangqilongdeMacBook-Air.local:7077"
//   val checkpointDir1 = "./BehavioralStatistics/tmp1"
//   val checkpointDir2 = "./BehavioralStatistics/tmp2"
//   val streamIntervel = 10

  // 3.yarn mode
   val master = "yarn"
   val checkpointDir1 = "./BehavioralStatistics/tmp1"
   val checkpointDir2 = "./BehavioralStatistics/tmp2"
   val streamIntervel = 10

}
