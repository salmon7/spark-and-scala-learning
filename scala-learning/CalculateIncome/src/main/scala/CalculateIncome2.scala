import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhang 2019-11-04 23:29
  */

object CalculateIncome2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Calculate Income").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(Array(("company-1", 88.2), ("company-1", 96.3), ("company-1", 85.9), ("company-2", 94.2), ("company-2", 74.9), ("company-3", 86.4), ("company-3", 88.5), ("company-1", 92.2)), 3)

    val res1 = data.mapValues(x => (x, 1)).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).mapValues(x => (x._1, x._1 / x._2))

    res1.repartition(1).saveAsTextFile("file:///Users/gzzhangqilong2017/Workspace/github/spark-and-scala-learning/scala-learning/CalculateIncome/result1")
  }
}
