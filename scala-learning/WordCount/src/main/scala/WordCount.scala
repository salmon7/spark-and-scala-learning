/**
  * @author zhang 2019-10-31 01:00
  */

import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]) {
    val inputFile =  "/Users/gzzhangqilong2017/Workspace/github/spark-and-scala-learning/scala-learning/WordCount/src/main/resources/word.txt"
    val conf = new SparkConf().setAppName("WordCount").setMaster("local")
    val sc = new SparkContext(conf)
    val textFile = sc.textFile(inputFile)
    val wordCount = textFile.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey((a, b) => a + b)
    wordCount.foreach(println)
  }
}