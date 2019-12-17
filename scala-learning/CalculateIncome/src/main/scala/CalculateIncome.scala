import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author zhang 2019-11-04 23:29
  */

object CalculateIncome {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Calculate Income").setMaster("local")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(Array(("company-1", 88.2), ("company-1", 96.3), ("company-1", 85.9), ("company-2", 94.2), ("company-2", 74.9), ("company-3", 86.4), ("company-3", 88.5), ("company-1", 92.2)),3)
    val res = data.combineByKey(
      income => {
        // create new value type, means (totalIncome, monthNum)
        (income, 1)
      },
      (income_month: (Double, Int), income) => {
        // with same key, merge value to exist one
        (income_month._1 + income, income_month._2 + 1)
      },
      (income_month_1: (Double, Int), income_month_2: (Double, Int)) => {
        // with same key in different partitions, merge all value
        (income_month_1._1 + income_month_2._1, income_month_1._2 + income_month_2._2)
      }
    ).map({ case (company: String, (total_income: Double, month: Int)) => (company, total_income, total_income / month) })

    res.repartition(1).saveAsTextFile("file:///Users/gzzhangqilong2017/Workspace/github/spark-and-scala-learning/scala-learning/CalculateIncome/result")
  }
}
